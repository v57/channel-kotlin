package channel.quic

import tech.kwik.core.QuicConnection
import tech.kwik.core.QuicStream
import java.io.BufferedInputStream
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The one framed stream of a connection, between kwik's blocking streams and the channel's single polling thread.
 *
 * A reader thread decodes frames into an inbox that holds about one message ahead (QUIC flow control pushes back on
 * the peer beyond that); [drain] hands them to the polling thread. [send] never blocks: a writer thread writes the
 * queued frames, and on a client sends a keepalive after [keepaliveMillis] without writing.
 */
internal class FramedStream(
  private val connection: QuicConnection,
  private val stream: QuicStream,
  private val maxPayload: Int,
  private val keepaliveMillis: Long,
) {
  private val inbox = ConcurrentLinkedQueue<Any>()
  private val budget = Object()
  private var inboxBytes = 0L
  private val outbox = LinkedBlockingQueue<ByteArray>()
  private val remoteClosed = AtomicBoolean(false)

  @Volatile
  private var localClosed = false

  /** When the write in progress started (System.nanoTime), or 0 while the writer isn't blocked in a write. */
  @Volatile
  var writingSince: Long = 0L
    private set

  /** Why the stream ended, when the reader stopped on an error. */
  @Volatile
  var failure: Throwable? = null
    private set

  fun start(): FramedStream {
    thread("channel-quic-reader", ::readLoop)
    thread("channel-quic-writer", ::writeLoop)
    return this
  }

  fun isConnected(): Boolean = !localClosed && !remoteClosed.get()

  fun isLocallyClosed(): Boolean = localClosed

  /** Queues one frame. */
  fun send(frame: ByteArray) {
    if (!localClosed) {
      outbox.add(frame)
    }
  }

  /**
   * Hands every received message to [onMessage], in order, then [onClosed] once the stream ended. Runs on the
   * polling thread; returns whether anything happened.
   */
  fun drain(onMessage: (String) -> Unit, onClosed: () -> Unit): Boolean {
    var progressed = false
    while (!localClosed) {
      val item = inbox.poll() ?: break
      progressed = true
      if (item === ENDED) {
        onClosed()
        return true
      }
      val text = item as String
      synchronized(budget) {
        inboxBytes -= text.length
        budget.notifyAll()
      }
      onMessage(text)
    }
    return progressed
  }

  /** The connection went away (kwik's listener): nothing more is sent. */
  fun markRemoteClosed() {
    remoteClosed.set(true)
  }

  /** Sends what is queued, then closes the connection. */
  fun close() {
    if (localClosed) {
      return
    }
    localClosed = true
    outbox.add(CLOSE)
    if (writingSince != 0L) {
      // Blocked on a peer that doesn't read: don't wait for it
      runCatching { connection.close() }
    }
    synchronized(budget) {
      budget.notifyAll()
    }
  }

  private fun readLoop() {
    try {
      val reader = FrameReader(BufferedInputStream(stream.inputStream, 64 * 1024), maxPayload)
      while (!localClosed) {
        val payload = reader.next() ?: break
        val text = String(payload, Charsets.UTF_8)
        synchronized(budget) {
          while (!localClosed && inboxBytes > 0 && inboxBytes + text.length > maxPayload) {
            budget.wait(100)
          }
          inboxBytes += text.length
        }
        inbox.add(text)
      }
    } catch (error: Throwable) {
      if (!localClosed) {
        failure = error
      }
    }
    remoteClosed.set(true)
    inbox.add(ENDED)
    runCatching { connection.close() }
  }

  private fun writeLoop() {
    val output = stream.outputStream
    try {
      while (true) {
        var frame = if (keepaliveMillis > 0) outbox.poll(keepaliveMillis, TimeUnit.MILLISECONDS) else outbox.take()
        if (frame == null) {
          frame = KEEPALIVE_FRAME
        }
        if (frame === CLOSE) {
          break
        }
        writingSince = System.nanoTime()
        output.write(frame)
        var next = outbox.poll()
        while (next != null && next !== CLOSE) {
          output.write(next)
          next = outbox.poll()
        }
        output.flush()
        writingSince = 0L
        if (next === CLOSE) {
          break
        }
      }
      runCatching { output.close() }
    } catch (_: Throwable) {
      remoteClosed.set(true)
    }
    writingSince = 0L
    runCatching { connection.close() }
  }

  private companion object {
    val CLOSE = ByteArray(0)
    val ENDED = Any()

    fun thread(name: String, body: () -> Unit) {
      Thread(body, name).apply {
        isDaemon = true
        start()
      }
    }
  }
}
