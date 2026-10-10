package channel

import org.junit.jupiter.api.Timeout
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.Base64
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail

/** Regression tests for AUDIT.md 2.3/3.1/3.2 and AUDIT2.md 1.4/3 (hostile peers, nested pumping). */
class HostileInputTest {
  // ---------- server side ----------

  @Test
  @Timeout(60)
  fun serverSurvivesHostileMessagesFramesAndHandshakes() {
    PolledServer().use { running ->
      val port = running.server.port()
      val deep = "[".repeat(100_000) + "]".repeat(100_000)
      val hostile = listOf(
        frame(0x1, true, "not json"),
        frame(0x1, true, "null"),
        frame(0x1, true, "123"),
        frame(0x1, true, "\"str\""),
        frame(0x1, true, "[null, 1]"),
        frame(0x1, true, "{\"id\":\"abc\",\"path\":\"echo\"}"),
        frame(0x1, true, "{\"cancel\":\"abc\"}"),
        frame(0x1, true, "{\"id\":1,\"path\":\"echo\",\"body\":$deep}"),
        frame(0x1, true, "{\"id\":1,\"path\":\"echo\",\"body\":\"x\"}"),
        frame(0x3, true, "{}"),
        frame(0x0, true, "orphan continuation"),
        lengthHeader(Long.MAX_VALUE),
        lengthHeader(-1L),
        lengthHeader(1L shl 30),
        frame(0x1, true, "{}", masked = false),
      )
      hostile.forEach { message ->
        rawConnect(port).use { socket ->
          socket.getOutputStream().write(message)
          socket.getOutputStream().flush()
          readUntilClosedOrTimeout(socket)
        }
        assertEquals("{\"id\":1,\"body\":5}", echo(port), "server stopped answering")
      }

      listOf("GET / HTTP/1.1\r\nHost: x\r\n\r\n", "garbage\r\n\r\n").forEach { handshake ->
        Socket().use { socket ->
          socket.connect(InetSocketAddress("127.0.0.1", port), 2000)
          socket.soTimeout = 2000
          socket.getOutputStream().write(handshake.toByteArray(Charsets.US_ASCII))
          readUntilClosedOrTimeout(socket)
        }
        assertEquals("{\"id\":1,\"body\":5}", echo(port), "server stopped answering after a bad handshake")
      }
      assertEquals(null, running.failure.get(), "server poll() threw")
    }
  }

  @Test
  @Timeout(60)
  fun serverDoesNotLeakDecoderMessagesToClients() {
    PolledServer().use { running ->
      rawConnect(running.server.port()).use { socket ->
        socket.getOutputStream().write(frame(0x1, true, "{\"id\":7,\"path\":\"echo\",\"body\":\"x\"}"))
        assertEquals("{\"id\":7,\"error\":\"invalid value for Integer\"}", readFrame(socket.getInputStream(), 0x1))
      }
    }
  }

  @Test
  @Timeout(60)
  fun serverClosesEndlessHandshakeAndOversizedFrame() {
    PolledServer().use { running ->
      val port = running.server.port()
      Socket().use { socket ->
        socket.connect(InetSocketAddress("127.0.0.1", port), 2000)
        socket.soTimeout = 5000
        val chunk = ("X-Junk: " + "a".repeat(1000) + "\r\n").toByteArray(Charsets.US_ASCII)
        var closed = false
        try {
          socket.getOutputStream().write("GET / HTTP/1.1\r\n".toByteArray(Charsets.US_ASCII))
          repeat(2_000) { socket.getOutputStream().write(chunk) }
        } catch (_: IOException) {
          closed = true
        }
        if (!closed) {
          closed = readUntilClosedOrTimeout(socket)
        }
        assertTrue(closed, "2 MB handshake without terminator must close the connection")
      }
      rawConnect(port).use { socket ->
        socket.getOutputStream().write(lengthHeader(1L shl 30))
        assertTrue(readUntilClosedOrTimeout(socket), "a declared 1 GiB frame must close the connection")
      }
      assertEquals("{\"id\":1,\"body\":5}", echo(port))
    }
  }

  @Test
  @Timeout(60)
  fun serverAnswersFragmentedBinaryAndLargeBatches() {
    PolledServer().use { running ->
      rawConnect(running.server.port()).use { socket ->
        val out = socket.getOutputStream()
        val input = socket.getInputStream()
        out.write(frame(0x1, false, "{\"id\":1,"))
        out.write(frame(0x9, true, "ping"))
        out.write(frame(0x0, false, "\"path\":\"echo\","))
        out.write(frame(0x0, true, "\"body\":5}"))
        out.flush()
        assertEquals("ping", readFrame(input, 0xA))
        assertEquals("{\"id\":1,\"body\":5}", readFrame(input, 0x1))

        out.write(frame(0x2, true, "{\"id\":2,\"path\":\"echo\",\"body\":6}"))
        assertEquals("{\"id\":2,\"body\":6}", readFrame(input, 0x1))

        // The benchmark sends 2048 requests per frame; replying must not recurse per message.
        val batch = (1..2048).joinToString(",", "[", "]") { "{\"id\":$it,\"path\":\"echo\",\"body\":$it}" }
        out.write(frame(0x1, true, batch))
        out.flush()
        repeat(2048) { index ->
          assertEquals("{\"id\":${index + 1},\"body\":${index + 1}}", readFrame(input, 0x1))
        }
      }
      assertEquals(null, running.failure.get())
    }
  }

  // ---------- client side ----------

  @Test
  @Timeout(60)
  fun clientSettlesCallsWhenTheServerMisbehaves() {
    val deep = "[".repeat(100_000) + "]".repeat(100_000)
    val cases = listOf(
      "badjson" to serverFrame(0x1, true, "{oops"),
      "len 2^63-1" to serverLength(Long.MAX_VALUE),
      "len 2^64-1" to serverLength(-1L),
      "unknown opcode" to serverFrame(0x3, true, "{}"),
      "masked server frame" to frame(0x1, true, "{\"id\":1,\"body\":5}"),
    )
    cases.forEach { (name, reply) ->
      HostileServer(reply).use { hostile ->
        val client = Channel<MutableMap<String, Any?>>().connect(hostile.port)
        val result = client.send<Int>("echo", 5)
        assertEquals(CallResult.RemoteErr("disconnected"), result, name)
        client.stop()
      }
    }

    val fragmented = serverFrame(0x1, false, "{\"id\":1,") + serverFrame(0x0, true, "\"body\":5}")
    listOf(fragmented, serverFrame(0x2, true, "{\"id\":1,\"body\":5}")).forEach { reply ->
      HostileServer(reply).use { hostile ->
        val client = Channel<MutableMap<String, Any?>>().connect(hostile.port)
        assertEquals(5, client.send<Int>("echo", 5).requireOk())
        client.stop()
      }
    }

    // The envelope parser skips the body without recursion; the typed decode then fails cleanly.
    listOf("\"not a number\"", deep).forEach { body ->
      HostileServer(serverFrame(0x1, true, "{\"id\":1,\"body\":$body}")).use { hostile ->
        val client = Channel<MutableMap<String, Any?>>().connect(hostile.port)
        assertEquals(CallResult.RemoteErr("invalid response body"), client.send<Int>("echo", 5))
        client.stop()
      }
    }
  }

  @Test
  @Timeout(60)
  fun requestTimeoutSettlesASilentServer() {
    HostileServer(ByteArray(0)).use { hostile ->
      val client = Channel<MutableMap<String, Any?>>().requestTimeout(300).connect(hostile.port)
      val started = System.nanoTime()
      assertEquals(CallResult.RemoteErr("timeout"), client.send<Int>("echo", 5))
      assertTrue(System.nanoTime() - started < 5_000_000_000L)
      client.stop()
    }
  }

  @Test
  fun frameReaderRejectsNegativeAndOversizedLengths() {
    listOf(Long.MAX_VALUE, -1L, DEFAULT_MAX_PAYLOAD + 1L).forEach { length ->
      val queue = ByteQueue()
      queue.append(serverLength(length))
      try {
        tryReadFrame(queue, expectMasked = false)
        fail("length $length accepted")
      } catch (error: ChannelError) {
        assertEquals("websocket frame too large", error.message)
      }
    }
  }

  @Test
  fun jsonDepthIsBounded() {
    val codec = defaultJsonCodec()
    assertEquals(listOf(listOf<Any?>()), codec.decode("[[]]"))
    codec.decode("[".repeat(MAX_JSON_DEPTH) + "]".repeat(MAX_JSON_DEPTH))
    val deep = "[".repeat(100_000) + "]".repeat(100_000)
    try {
      codec.decode(deep)
      fail("deep json accepted")
    } catch (error: ChannelError) {
      assertEquals("json nesting too deep", error.message)
    }
    // Brackets inside strings don't count.
    assertEquals("[[[[", codec.decode("\"[[[[\"", String::class.java))
  }

  // ---------- helpers ----------

  private class PolledServer : AutoCloseable {
    val server: Server<MutableMap<String, Any?>> = Channel<MutableMap<String, Any?>>()
      .post<Int, Int, Unit>("echo") { request -> request.body }
      .listen(0)
    val failure = AtomicReference<Throwable?>(null)
    private val running = AtomicBoolean(true)
    private val worker = thread(name = "polled-server") {
      while (running.get()) {
        try {
          if (!server.poll()) {
            Thread.sleep(1)
          }
        } catch (error: Throwable) {
          failure.set(error)
          return@thread
        }
      }
    }

    override fun close() {
      running.set(false)
      worker.join(5000)
      server.stop()
    }
  }

  /** One-shot raw server: completes the upgrade, reads one client frame, then writes [reply]. */
  private class HostileServer(reply: ByteArray) : AutoCloseable {
    private val listener = ServerSocket(0, 1, InetAddress.getLoopbackAddress())
    val port: Int = listener.localPort
    private val worker = thread(isDaemon = true) {
      try {
        listener.accept().use { socket ->
          val input = socket.getInputStream()
          val head = readHead(input)
          val key = head.lines().first { it.lowercase().startsWith("sec-websocket-key:") }.substring(18).trim()
          val out = socket.getOutputStream()
          out.write(
            ("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
              "Sec-WebSocket-Accept: ${accept(key)}\r\n\r\n").toByteArray(Charsets.US_ASCII),
          )
          readClientFrame(input)
          out.write(reply)
          out.flush()
          Thread.sleep(3000)
        }
      } catch (_: Exception) {
      }
    }

    override fun close() {
      listener.close()
      worker.interrupt()
      worker.join(3000)
    }
  }

  private companion object {
    const val HANDSHAKE = "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
      "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"

    fun accept(key: String): String {
      val digest = MessageDigest.getInstance("SHA-1")
        .digest((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").toByteArray(Charsets.US_ASCII))
      return Base64.getEncoder().encodeToString(digest)
    }

    fun readHead(input: InputStream): String {
      val head = StringBuilder()
      while (!head.endsWith("\r\n\r\n")) {
        val b = input.read()
        if (b < 0) throw IOException("eof in head")
        head.append(b.toChar())
      }
      return head.toString()
    }

    fun readClientFrame(input: InputStream) {
      input.readNBytes(1)
      var len = input.read() and 0x7F
      if (len == 126) {
        len = ((input.read() and 0xFF) shl 8) or (input.read() and 0xFF)
      } else if (len == 127) {
        len = ByteBuffer.wrap(input.readNBytes(8)).long.toInt()
      }
      input.readNBytes(4 + len)
    }

    fun rawConnect(port: Int): Socket {
      val socket = Socket()
      socket.connect(InetSocketAddress("127.0.0.1", port), 2000)
      socket.soTimeout = 5000
      socket.getOutputStream().write(HANDSHAKE.toByteArray(Charsets.US_ASCII))
      val head = readHead(socket.getInputStream())
      assertTrue(head.startsWith("HTTP/1.1 101"), head)
      return socket
    }

    fun echo(port: Int): String = rawConnect(port).use { socket ->
      socket.getOutputStream().write(frame(0x1, true, "{\"id\":1,\"path\":\"echo\",\"body\":5}"))
      readFrame(socket.getInputStream(), 0x1)
    }

    fun readFrame(input: InputStream, expectedOpcode: Int): String {
      val first = input.read()
      assertEquals(expectedOpcode, first and 0x0F)
      var len = input.read() and 0x7F
      if (len == 126) {
        len = ((input.read() and 0xFF) shl 8) or (input.read() and 0xFF)
      }
      return String(input.readNBytes(len), Charsets.UTF_8)
    }

    /** True when the peer closed the socket (EOF/reset) before the read timeout. */
    fun readUntilClosedOrTimeout(socket: Socket): Boolean = try {
      val buffer = ByteArray(4096)
      while (socket.getInputStream().read(buffer) >= 0) {
        // drain
      }
      true
    } catch (_: SocketTimeoutException) {
      false
    } catch (_: IOException) {
      true
    }

    fun frame(opcode: Int, fin: Boolean, text: String, masked: Boolean = true): ByteArray =
      encode(opcode, fin, text.toByteArray(Charsets.UTF_8), masked)

    fun serverFrame(opcode: Int, fin: Boolean, text: String): ByteArray =
      encode(opcode, fin, text.toByteArray(Charsets.UTF_8), false)

    fun encode(opcode: Int, fin: Boolean, payload: ByteArray, masked: Boolean): ByteArray {
      val out = ByteArrayOutputStream()
      out.write((if (fin) 0x80 else 0) or opcode)
      val maskBit = if (masked) 0x80 else 0
      when {
        payload.size < 126 -> out.write(maskBit or payload.size)
        payload.size <= 0xFFFF -> {
          out.write(maskBit or 126)
          out.write(payload.size ushr 8)
          out.write(payload.size and 0xFF)
        }

        else -> {
          out.write(maskBit or 127)
          out.writeBytes(ByteBuffer.allocate(8).putLong(payload.size.toLong()).array())
        }
      }
      if (masked) {
        val mask = byteArrayOf(1, 2, 3, 4)
        out.writeBytes(mask)
        payload.forEachIndexed { index, byte -> out.write(byte.toInt() xor mask[index % 4].toInt()) }
      } else {
        out.writeBytes(payload)
      }
      return out.toByteArray()
    }

    fun lengthHeader(length: Long): ByteArray =
      ByteBuffer.allocate(14).put(0x81.toByte()).put((0x80 or 127).toByte()).putLong(length).put(byteArrayOf(1, 2, 3, 4)).array()

    fun serverLength(length: Long): ByteArray =
      ByteBuffer.allocate(10).put(0x81.toByte()).put(127.toByte()).putLong(length).array()
  }
}
