package channel

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.util.Locale
import java.util.concurrent.locks.LockSupport

internal fun <State> createServer(channel: Channel<State>, address: String, options: ListenOptions<State>): Server<State> {
  val transport = ServerTransport(channel, address, options)
  return Server(transport, transport::stop, transport.port)
}

private class ServerTransport<State>(
  private val channel: Channel<State>,
  address: String,
  private val options: ListenOptions<State>,
) : Pollable {
  private val listener: ServerSocketChannel = ServerSocketChannel.open()
  private val connections = mutableListOf<ServerConnection<State>>()
  private var stopped = false

  val port: Int

  init {
    val (host, port) = parseAddress(address)
    listener.configureBlocking(false)
    listener.bind(InetSocketAddress(host, port))
    this.port = (listener.localAddress as InetSocketAddress).port
    PumpRegistry.register(this)
  }

  fun stop() {
    if (stopped) {
      return
    }
    stopped = true
    PumpRegistry.unregister(this)
    connections.toList().forEach { connection ->
      connection.destroy()
    }
    runCatching { listener.close() }
  }

  override fun poll(): Boolean {
    if (stopped) {
      return false
    }

    var progressed = false
    while (true) {
      val accepted = listener.accept() ?: break
      accepted.configureBlocking(false)
      accepted.socket().tcpNoDelay = true
      connections += ServerConnection(this, accepted, channel, options)
      progressed = true
    }

    val snapshot = connections.toList()
    snapshot.forEach { connection ->
      progressed = try {
        connection.poll() || progressed
      } catch (error: Throwable) {
        if (error is VirtualMachineError && error !is StackOverflowError) {
          throw error
        }
        // Bad handshake, malformed message, deep JSON, ...: drop this connection only and
        // keep serving every other client.
        connection.destroy()
        true
      }
    }
    return progressed
  }

  fun remove(connection: ServerConnection<State>) {
    connections.remove(connection)
  }
}

private class ServerConnection<State>(
  private val owner: ServerTransport<State>,
  private val socket: SocketChannel,
  private val channel: Channel<State>,
  private val options: ListenOptions<State>,
) : Transport {
  private val queue = ByteQueue()
  private val maxPayload = options.maxPayload
  private val frameAssembler = FrameAssembler(maxPayload)
  private var closed = false
  private var handshaken = false
  private var peer: Peer<State>? = null
  private var stalledSince = 0L

  override fun sendFrame(payload: String) {
    if (closed) {
      return
    }
    writeFully(encodeFrame(payload.toByteArray(Charsets.UTF_8), mask = false))
  }

  override fun close() {
    if (closed) {
      return
    }
    closed = true
    runCatching { socket.close() }
  }

  override fun isConnected(): Boolean = !closed && socket.isOpen && socket.isConnected

  fun poll(): Boolean {
    if (closed) {
      return false
    }

    var progressed = false
    // Read at most about one message ahead; frames are decoded (or rejected) below before reading more.
    while (queue.size.toLong() < maxPayload.toLong() + MAX_HANDSHAKE_BYTES) {
      val buffer = ByteBuffer.allocate(16_384)
      val read = try {
        socket.read(buffer)
      } catch (error: java.io.IOException) {
        // Abortive close (e.g. peer reset while we are reading): treat as a
        // disconnect instead of crashing the whole server.
        destroy()
        return true
      }
      if (read > 0) {
        progressed = true
        queue.append(buffer.array().copyOf(read))
        continue
      }
      if (read < 0) {
        destroy()
        return true
      }
      break
    }

    if (!handshaken) {
      if (progressHandshake()) {
        return true
      }
      if (queue.size > MAX_HANDSHAKE_BYTES) {
        destroy()
        return true
      }
      return progressed
    }

    while (!closed) {
      val frame = try {
        tryReadFrame(queue, expectMasked = true, assembler = frameAssembler, maxPayload = maxPayload)
      } catch (error: Throwable) {
        // Protocol error on this connection only (bad masking, unsupported
        // opcode, oversized frame, ...). Destroy the connection but do NOT
        // crash the whole server — other clients keep working.
        destroy()
        return true
      } ?: break
      progressed = true
      when (frame.opcode) {
        0x1,
        0x2 -> peer?.handleIncomingFrame(frame.payload.toString(Charsets.UTF_8))
        0x8 -> {
          destroy()
          return true
        }

        0x9 -> writeFully(encodeFrame(frame.payload, opcode = 0xA, mask = false))
        0xA -> Unit
        else -> {
          // Unsupported opcode: drop the connection, keep the server alive.
          destroy()
          return true
        }
      }
    }

    return progressed
  }

  private companion object {
    const val WRITE_STALL_TIMEOUT_NANOS = 10_000_000_000L
  }

  fun destroy() {
    if (closed) {
      owner.remove(this)
      return
    }
    owner.remove(this)
    val targetPeer = peer
    peer = null
    if (targetPeer != null) {
      targetPeer.stop()
      options.onDisconnect?.invoke(targetPeer.state, targetPeer.sender)
    }
    close()
  }

  private fun progressHandshake(): Boolean {
    val delimiter = "\r\n\r\n".toByteArray()
    val position = queue.indexOf(delimiter)
    if (position < 0) {
      return false
    }

    val requestText = queue.take(position + delimiter.size).toString(Charsets.UTF_8)
    val request = parseHttpRequest(requestText)
    val upgrade = request.headers["upgrade"]?.lowercase(Locale.ROOT)
    val connection = request.headers["connection"]?.lowercase(Locale.ROOT)
    val key = request.headers["sec-websocket-key"]
    if (upgrade != "websocket" || connection != "upgrade" || key.isNullOrBlank()) {
      throw ChannelError("invalid websocket request")
    }

    val (_, queryParams) = parseQueryParams(request.pathWithQuery)
    val headers = linkedMapOf<String, String>()
    headers.putAll(queryParams)
    headers.putAll(request.headers)

    val response = buildString {
      append("HTTP/1.1 101 Switching Protocols\r\n")
      append("Upgrade: websocket\r\n")
      append("Connection: Upgrade\r\n")
      append("Sec-WebSocket-Accept: ${websocketAccept(key)}\r\n")
      append("\r\n")
    }
    writeFully(response.toByteArray(Charsets.UTF_8))

    val state = options.state?.invoke(headers) ?: channel.makeDefaultState()
    val connectionTransport = this
    val createdPeer = Peer(
      channel = channel,
      state = state,
      transport = connectionTransport,
      codec = defaultJsonCodec(),
      mode = TransportMode.SERVER_CONNECTION,
      onStop = null,
    )
    peer = createdPeer
    handshaken = true
    options.onConnect?.invoke(state, createdPeer.sender)
    return true
  }

  private fun writeFully(bytes: ByteArray) {
    if (closed) {
      return
    }
    val buffer = ByteBuffer.wrap(bytes)
    stalledSince = 0L
    while (buffer.hasRemaining()) {
      val written = try {
        socket.write(buffer)
      } catch (error: java.io.IOException) {
        // Peer disconnected mid-write: close instead of crashing the server.
        destroy()
        return
      }
      if (written == 0) {
        // A client that stops reading must not block the server thread forever.
        if (stalledSince == 0L) {
          stalledSince = System.nanoTime()
        } else if (System.nanoTime() - stalledSince > WRITE_STALL_TIMEOUT_NANOS) {
          destroy()
          return
        }
        LockSupport.parkNanos(100_000L)
      } else {
        stalledSince = 0L
      }
    }
  }
}
