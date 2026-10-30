package channel.quic

import channel.DEFAULT_MAX_PAYLOAD
import java.io.EOFException
import java.io.IOException
import java.io.InputStream

/** ALPN of the Channel QUIC transport. */
const val ALPN: String = "channel/1"

/** QUIC idle timeout on both sides; the client sends a keepalive every [KEEPALIVE_MILLIS]. */
internal const val IDLE_TIMEOUT_SECONDS: Int = 30
internal const val KEEPALIVE_MILLIS: Long = 15_000L

/** A frame whose length is above the limit; the connection is closed. */
class FrameTooLargeException(length: Long, limit: Int) : IOException("quic frame of $length bytes is over the $limit byte limit")

/** Appends [value] as an RFC 9000 variable-length integer (1, 2, 4 or 8 bytes). */
internal fun encodeVarint(value: Long): ByteArray {
  require(value >= 0 && value < (1L shl 62)) { "varint out of range: $value" }
  return when {
    value < 64 -> byteArrayOf(value.toByte())
    value < 16_384 -> byteArrayOf((0x40 or (value ushr 8).toInt()).toByte(), value.toByte())
    value < (1L shl 30) -> byteArrayOf(
      (0x80 or (value ushr 24).toInt()).toByte(),
      (value ushr 16).toByte(),
      (value ushr 8).toByte(),
      value.toByte(),
    )
    else -> ByteArray(8) { index -> (value ushr (8 * (7 - index))).toByte() }.also { it[0] = (it[0].toInt() or 0xC0).toByte() }
  }
}

/** One frame: the payload length as a varint, then the payload. */
internal fun encodeFrame(payload: ByteArray): ByteArray {
  val header = encodeVarint(payload.size.toLong())
  val frame = ByteArray(header.size + payload.size)
  header.copyInto(frame)
  payload.copyInto(frame, header.size)
  return frame
}

internal fun encodeFrame(text: String): ByteArray = encodeFrame(text.toByteArray(Charsets.UTF_8))

/** The empty frame, sent as a keepalive and skipped by readers. */
internal val KEEPALIVE_FRAME: ByteArray = byteArrayOf(0)

/** Reads frames from a stream, skipping keepalives. */
internal class FrameReader(private val input: InputStream, private val maxPayload: Int = DEFAULT_MAX_PAYLOAD) {
  /** The next non-empty payload, or null at the end of the stream. */
  fun next(): ByteArray? {
    while (true) {
      val first = input.read()
      if (first < 0) {
        return null
      }
      val size = 1 shl (first ushr 6)
      var length = (first and 0x3F).toLong()
      for (index in 1 until size) {
        val next = input.read()
        if (next < 0) {
          throw EOFException("stream ended inside a frame length")
        }
        length = (length shl 8) or next.toLong()
      }
      if (length > maxPayload) {
        throw FrameTooLargeException(length, maxPayload)
      }
      if (length == 0L) {
        continue
      }
      val payload = input.readNBytes(length.toInt())
      if (payload.size.toLong() != length) {
        throw EOFException("stream ended inside a frame")
      }
      return payload
    }
  }
}
