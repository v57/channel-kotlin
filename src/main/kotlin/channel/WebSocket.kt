package channel

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.security.MessageDigest
import java.util.Base64
import java.util.concurrent.ThreadLocalRandom

internal data class WebSocketFrame(
  val opcode: Int,
  val payload: ByteArray,
)

/**
 * Accumulates fragmented message frames (RFC 6455 §5.4). A message may be
 * split into a first frame (FIN=0, opcode = message type) followed by any
 * number of continuation frames (opcode 0) and a final continuation frame
 * (FIN=1). Control frames (ping/pong/close) may be interleaved.
 */
internal class FrameAssembler {
  private var active = false
  private var opcode = 0
  private var payload = ByteArray(0)

  fun isActive(): Boolean = active

  fun start(messageOpcode: Int, data: ByteArray) {
    active = true
    opcode = messageOpcode
    payload = data
  }

  fun append(data: ByteArray) {
    val merged = ByteArray(payload.size + data.size)
    payload.copyInto(merged)
    data.copyInto(merged, payload.size)
    payload = merged
  }

  fun finish(): WebSocketFrame {
    val frame = WebSocketFrame(opcode, payload)
    reset()
    return frame
  }

  fun reset() {
    active = false
    opcode = 0
    payload = ByteArray(0)
  }
}

internal class ByteQueue(initialCapacity: Int = 4096) {
  private var data = ByteArray(initialCapacity)
  private var start = 0
  private var end = 0

  val size: Int
    get() = end - start

  fun append(channel: SocketChannel): Int {
    ensureWritable(2048)
    val buffer = ByteBuffer.wrap(data, end, data.size - end)
    val read = channel.read(buffer)
    if (read > 0) {
      end += read
    }
    return read
  }

  fun append(bytes: ByteArray) {
    ensureWritable(bytes.size)
    bytes.copyInto(data, end)
    end += bytes.size
  }

  fun peek(offset: Int): Byte = data[start + offset]

  fun slice(offset: Int, length: Int): ByteArray {
    return data.copyOfRange(start + offset, start + offset + length)
  }

  fun take(length: Int): ByteArray {
    val out = slice(0, length)
    discard(length)
    return out
  }

  fun discard(length: Int) {
    start += length
    if (start == end) {
      start = 0
      end = 0
    } else if (start > data.size / 2) {
      compact()
    }
  }

  fun indexOf(sequence: ByteArray): Int {
    if (sequence.isEmpty()) {
      return 0
    }
    outer@ for (index in start..(end - sequence.size)) {
      for (offset in sequence.indices) {
        if (data[index + offset] != sequence[offset]) {
          continue@outer
        }
      }
      return index - start
    }
    return -1
  }

  private fun compact() {
    if (start == 0) {
      return
    }
    data.copyInto(data, 0, start, end)
    end -= start
    start = 0
  }

  private fun ensureWritable(extra: Int) {
    compact()
    if (end + extra <= data.size) {
      return
    }
    var capacity = data.size
    while (capacity < end + extra) {
      capacity *= 2
    }
    data = data.copyOf(capacity)
  }
}

internal fun tryReadFrame(
  queue: ByteQueue,
  expectMasked: Boolean,
  assembler: FrameAssembler? = null,
): WebSocketFrame? {
  if (queue.size < 2) {
    return null
  }

  val first = queue.peek(0).toInt() and 0xFF
  val second = queue.peek(1).toInt() and 0xFF
  val fin = first and 0x80 != 0
  val opcode = first and 0x0F
  val masked = second and 0x80 != 0
  if (masked != expectMasked) {
    throw ChannelError("unexpected websocket masking")
  }

  var payloadLength = second and 0x7F
  var offset = 2
  when (payloadLength) {
    126 -> {
      if (queue.size < 4) {
        return null
      }
      payloadLength = ((queue.peek(2).toInt() and 0xFF) shl 8) or (queue.peek(3).toInt() and 0xFF)
      offset = 4
    }

    127 -> {
      if (queue.size < 10) {
        return null
      }
      var longValue = 0L
      for (index in 2 until 10) {
        longValue = (longValue shl 8) or (queue.peek(index).toLong() and 0xFF)
      }
      if (longValue > Int.MAX_VALUE) {
        throw ChannelError("websocket frame too large")
      }
      payloadLength = longValue.toInt()
      offset = 10
    }
  }

  val maskKey = if (masked) {
    if (queue.size < offset + 4) {
      return null
    }
    val key = queue.slice(offset, 4)
    offset += 4
    key
  } else {
    null
  }

  if (queue.size < offset + payloadLength) {
    return null
  }

  val payload = queue.slice(offset, payloadLength)
  queue.discard(offset + payloadLength)

  if (maskKey != null) {
    for (index in payload.indices) {
      payload[index] = (payload[index].toInt() xor (maskKey[index % 4].toInt() and 0xFF)).toByte()
    }
  }

  if (opcode == 0x0) {
    // Continuation frame — must follow a fragmented message.
    val localAssembler = assembler
      ?: throw ChannelError("unexpected continuation frame")
    if (!localAssembler.isActive()) {
      throw ChannelError("unexpected continuation frame")
    }
    localAssembler.append(payload)
    if (fin) {
      return localAssembler.finish()
    }
    return null
  }

  if (!fin) {
    // First frame of a fragmented message.
    val localAssembler = assembler
      ?: throw ChannelError("fragmented frames are unsupported")
    if (localAssembler.isActive()) {
      throw ChannelError("new fragmented message before previous finished")
    }
    localAssembler.start(opcode, payload)
    return null
  }

  return WebSocketFrame(opcode, payload)
}

internal fun encodeFrame(payload: ByteArray, opcode: Int = 0x1, mask: Boolean): ByteArray {
  val header = mutableListOf<Byte>()
  header += (0x80 or (opcode and 0x0F)).toByte()

  when {
    payload.size < 126 -> header += ((if (mask) 0x80 else 0x00) or payload.size).toByte()
    payload.size <= 0xFFFF -> {
      header += ((if (mask) 0x80 else 0x00) or 126).toByte()
      header += ((payload.size ushr 8) and 0xFF).toByte()
      header += (payload.size and 0xFF).toByte()
    }

    else -> {
      header += ((if (mask) 0x80 else 0x00) or 127).toByte()
      for (shift in 7 downTo 0) {
        header += ((payload.size.toLong() ushr (shift * 8)) and 0xFF).toByte()
      }
    }
  }

  return if (mask) {
    val maskKey = ByteArray(4)
    ThreadLocalRandom.current().nextBytes(maskKey)
    val masked = payload.copyOf()
    for (index in masked.indices) {
      masked[index] = (masked[index].toInt() xor (maskKey[index % 4].toInt() and 0xFF)).toByte()
    }
    (header.toByteArray() + maskKey + masked)
  } else {
    header.toByteArray() + payload
  }
}

internal fun websocketAccept(key: String): String {
  val digest = MessageDigest.getInstance("SHA-1")
  val bytes = digest.digest((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").toByteArray(Charsets.UTF_8))
  return Base64.getEncoder().encodeToString(bytes)
}

internal fun generateClientKey(): String {
  val bytes = ByteArray(16)
  ThreadLocalRandom.current().nextBytes(bytes)
  return Base64.getEncoder().encodeToString(bytes)
}

internal data class HttpRequest(
  val pathWithQuery: String,
  val headers: Map<String, String>,
)

internal fun parseHttpRequest(raw: String): HttpRequest {
  val lines = raw.split("\r\n")
  val requestLine = lines.firstOrNull() ?: throw ChannelError("missing request line")
  val parts = requestLine.split(' ')
  if (parts.size < 3 || parts[0] != "GET") {
    throw ChannelError("invalid websocket request")
  }

  val headers = linkedMapOf<String, String>()
  lines.drop(1).forEach { line ->
    if (line.isEmpty()) {
      return@forEach
    }
    val index = line.indexOf(':')
    if (index <= 0) {
      return@forEach
    }
    val key = line.substring(0, index).trim().lowercase()
    val value = line.substring(index + 1).trim()
    headers[key] = value
  }

  return HttpRequest(parts[1], headers)
}

internal data class HttpResponse(
  val statusCode: Int,
  val headers: Map<String, String>,
)

internal fun parseHttpResponse(raw: String): HttpResponse {
  val lines = raw.split("\r\n")
  val statusLine = lines.firstOrNull() ?: throw ChannelError("missing response line")
  val parts = statusLine.split(' ')
  val statusCode = parts.getOrNull(1)?.toIntOrNull() ?: throw ChannelError("invalid websocket response")
  val headers = linkedMapOf<String, String>()
  lines.drop(1).forEach { line ->
    if (line.isEmpty()) {
      return@forEach
    }
    val index = line.indexOf(':')
    if (index <= 0) {
      return@forEach
    }
    headers[line.substring(0, index).trim().lowercase()] = line.substring(index + 1).trim()
  }
  return HttpResponse(statusCode, headers)
}

internal fun parseQueryParams(pathWithQuery: String): Pair<String, Map<String, String>> {
  val queryIndex = pathWithQuery.indexOf('?')
  if (queryIndex < 0) {
    return pathWithQuery to emptyMap()
  }
  val path = pathWithQuery.substring(0, queryIndex)
  val query = pathWithQuery.substring(queryIndex + 1)
  if (query.isEmpty()) {
    return path to emptyMap()
  }

  val values = linkedMapOf<String, String>()
  query.split('&').forEach { part ->
    if (part.isEmpty()) {
      return@forEach
    }
    val eq = part.indexOf('=')
    if (eq < 0) {
      values[decodeUrl(part)] = ""
    } else {
      values[decodeUrl(part.substring(0, eq))] = decodeUrl(part.substring(eq + 1))
    }
  }
  return path to values
}

private fun decodeUrl(value: String): String {
  val out = StringBuilder(value.length)
  var index = 0
  while (index < value.length) {
    when (val ch = value[index]) {
      '+' -> {
        out.append(' ')
        index += 1
      }

      '%' -> {
        val hex = value.substring(index + 1, index + 3)
        out.append(hex.toInt(16).toChar())
        index += 3
      }

      else -> {
        out.append(ch)
        index += 1
      }
    }
  }
  return out.toString()
}
