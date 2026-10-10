package channel

import java.net.InetSocketAddress
import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.util.Locale
import java.util.concurrent.locks.LockSupport

internal fun <State> createClient(channel: Channel<State>, address: String, options: ConnectOptions<State>): Client<State> {
  val target = normalizeClientAddress(address)
  val transport = ClientTransport<State>(channel, target, options)
  return Client(transport.peer, transport)
}

private fun normalizeClientAddress(address: String): URI {
  val normalized = if (address.startsWith("ws://") || address.startsWith("wss://")) {
    address
  } else {
    "ws://$address"
  }
  return URI(normalized)
}

private class ClientTransport<State>(
  channel: Channel<State>,
  private val uri: URI,
  options: ConnectOptions<State>,
) : Transport, Pollable {
  private val socket: SocketChannel = SocketChannel.open()
  private val queue = ByteQueue()
  private val maxPayload = options.maxPayload
  private val frameAssembler = FrameAssembler(maxPayload)
  private var closed = false

  val peer: Peer<State>

  init {
    if ((uri.scheme ?: "ws").lowercase(Locale.ROOT) == "wss") {
      throw ChannelError("tls is unsupported")
    }

    socket.configureBlocking(false)
    socket.connect(
      InetSocketAddress(
        uri.host ?: "127.0.0.1",
        if (uri.port > 0) uri.port else 80,
      ),
    )

    try {
      while (!socket.finishConnect()) {
        if (!PumpRegistry.pumpAll()) {
          LockSupport.parkNanos(100_000L)
        }
      }
      performHandshake(options.headers)
    } catch (error: Throwable) {
      runCatching { socket.close() }
      throw error
    }
    peer = Peer(
      channel = channel,
      state = options.state ?: channel.makeDefaultState(),
      transport = this,
      codec = defaultJsonCodec(),
      mode = TransportMode.CLIENT,
    )
    PumpRegistry.register(this)
  }

  override fun sendFrame(payload: String) {
    if (closed) {
      return
    }
    try {
      writeFully(encodeFrame(payload.toByteArray(Charsets.UTF_8), mask = true))
    } catch (error: java.io.IOException) {
      // Peer went away mid-write: settle pending calls instead of throwing out of send().
      disconnect()
    }
  }

  override fun close() {
    if (closed) {
      return
    }
    closed = true
    PumpRegistry.unregister(this)
    runCatching { socket.close() }
  }

  override fun isConnected(): Boolean = !closed && socket.isOpen && socket.isConnected

  override fun poll(): Boolean {
    if (!isConnected()) {
      return false
    }
    return try {
      pollOnce()
    } catch (error: Throwable) {
      if (error is VirtualMachineError && error !is StackOverflowError) {
        throw error
      }
      // A broken or hostile server (bad frame, oversized length, invalid JSON, ...) only
      // disconnects this client: pending calls settle with "disconnected" instead of the
      // exception escaping into whoever is pumping.
      disconnect()
      true
    }
  }

  private fun disconnect() {
    close()
    peer.stop()
  }

  private fun pollOnce(): Boolean {
    var progressed = false
    // Read at most about one message ahead; frames are decoded (or rejected) below before reading more.
    while (queue.size.toLong() < maxPayload.toLong() + MAX_HANDSHAKE_BYTES) {
      val temp = ByteBuffer.allocate(16_384)
      val read = socket.read(temp)
      if (read > 0) {
        progressed = true
        queue.append(temp.array().copyOf(read))
        continue
      }
      if (read < 0) {
        disconnect()
        return true
      }
      break
    }

    while (isConnected()) {
      val frame = tryReadFrame(queue, expectMasked = false, assembler = frameAssembler, maxPayload = maxPayload) ?: break
      progressed = true
      when (frame.opcode) {
        0x1,
        0x2 -> peer.handleIncomingFrame(frame.payload.toString(Charsets.UTF_8))
        0x8 -> {
          disconnect()
          return true
        }

        0x9 -> writeFully(encodeFrame(frame.payload, opcode = 0xA, mask = true))
        0xA -> Unit
        else -> {
          disconnect()
          return true
        }
      }
    }

    return progressed
  }

  private fun performHandshake(extraHeaders: String?) {
    val key = generateClientKey()
    val path = buildString {
      append(if (uri.rawPath.isNullOrEmpty()) "/" else uri.rawPath)
      uri.rawQuery?.let {
        append('?')
        append(it)
      }
    }
    val request = buildString {
      append("GET $path HTTP/1.1\r\n")
      append("Host: ${uri.host ?: "127.0.0.1"}:${if (uri.port > 0) uri.port else 80}\r\n")
      append("Upgrade: websocket\r\n")
      append("Connection: Upgrade\r\n")
      append("Sec-WebSocket-Version: 13\r\n")
      append("Sec-WebSocket-Key: $key\r\n")
      if (!extraHeaders.isNullOrBlank()) {
        append(extraHeaders.trimEnd())
        append("\r\n")
      }
      append("\r\n")
    }
    writeFully(request.toByteArray(Charsets.UTF_8))

    val delimiter = "\r\n\r\n".toByteArray()
    while (true) {
      val existing = queue.indexOf(delimiter)
      if (existing >= 0) {
        val response = queue.take(existing + delimiter.size).toString(Charsets.UTF_8)
        validateHandshake(response, key)
        return
      }

      if (queue.size > MAX_HANDSHAKE_BYTES) {
        throw ChannelError("websocket handshake response too large")
      }
      val temp = ByteBuffer.allocate(2048)
      val read = socket.read(temp)
      if (read < 0) {
        throw ChannelError("connection closed during handshake")
      }
      if (read == 0) {
        if (!PumpRegistry.pumpAll()) {
          LockSupport.parkNanos(100_000L)
        }
        continue
      }
      queue.append(temp.array().copyOf(read))
    }
  }

  private fun validateHandshake(raw: String, key: String) {
    val response = parseHttpResponse(raw)
    if (response.statusCode != 101) {
      throw ChannelError("invalid websocket status ${response.statusCode}")
    }

    val upgrade = response.headers["upgrade"]?.lowercase(Locale.ROOT)
    val connection = response.headers["connection"]?.lowercase(Locale.ROOT)
    val accept = response.headers["sec-websocket-accept"]
    if (upgrade != "websocket" || connection != "upgrade" || accept != websocketAccept(key)) {
      throw ChannelError("invalid websocket handshake")
    }
  }

  private fun writeFully(bytes: ByteArray) {
    val buffer = ByteBuffer.wrap(bytes)
    while (buffer.hasRemaining()) {
      val written = socket.write(buffer)
      if (written == 0) {
        if (!PumpRegistry.pumpIfIdle()) {
          LockSupport.parkNanos(100_000L)
        }
      }
    }
  }
}
