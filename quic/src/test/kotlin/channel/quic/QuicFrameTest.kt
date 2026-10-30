package channel.quic

import channel.ChannelError
import java.io.ByteArrayInputStream
import java.io.EOFException
import java.security.MessageDigest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class QuicFrameTest {
  @Test
  fun framesUseTheShortestVarintAndSkipKeepalives() {
    for (size in listOf(0, 1, 63, 64, 16_383, 16_384, 70_000)) {
      val payload = ByteArray(size) { 'x'.code.toByte() }
      val frame = encodeFrame(payload)
      val header = frame.size - size
      val expected = when {
        size < 64 -> 1
        size < 16_384 -> 2
        else -> 4
      }
      assertEquals(expected, header, "size $size")
      // A keepalive in front is skipped, and an empty frame is one too
      val reader = FrameReader(ByteArrayInputStream(KEEPALIVE_FRAME + frame + encodeFrame("next")))
      if (size > 0) {
        assertContentEquals(payload, reader.next(), "size $size")
      }
      assertEquals("next", reader.next()?.toString(Charsets.UTF_8), "size $size")
      assertNull(reader.next())
    }
  }

  @Test
  fun varintsMatchRfc9000() {
    // RFC 9000 appendix A.1 examples
    assertContentEquals(byteArrayOf(0x25), encodeVarint(37))
    assertContentEquals(byteArrayOf(0x7b, 0xbd.toByte()), encodeVarint(15_293))
    assertContentEquals(byteArrayOf(0x9d.toByte(), 0x7f, 0x3e, 0x7d), encodeVarint(494_878_333))
    assertContentEquals(
      byteArrayOf(0xc2.toByte(), 0x19, 0x7c, 0x5e, 0xff.toByte(), 0x14, 0xe8.toByte(), 0x8c.toByte()),
      encodeVarint(151_288_809_941_952_652),
    )
    assertFailsWith<IllegalArgumentException> { encodeVarint(1L shl 62) }
  }

  @Test
  fun eightByteLengthsDecodeAndOversizeIsRefused() {
    val big = byteArrayOf(0xc0.toByte(), 0, 0, 0, 0, 0, 0, 5) + "hello".toByteArray()
    assertEquals("hello", FrameReader(ByteArrayInputStream(big)).next()?.toString(Charsets.UTF_8))

    assertFailsWith<FrameTooLargeException> { FrameReader(ByteArrayInputStream(encodeFrame(ByteArray(11))), 10).next() }
    // 16 MiB + 1 with the default limit: refused from the length alone
    assertFailsWith<FrameTooLargeException> {
      FrameReader(ByteArrayInputStream(byteArrayOf(0x81.toByte(), 0, 0, 1))).next()
    }
    assertFailsWith<EOFException> { FrameReader(ByteArrayInputStream(byteArrayOf(0x40))).next() }
    assertFailsWith<EOFException> { FrameReader(ByteArrayInputStream(byteArrayOf(5, 1, 2))).next() }
  }

  @Test
  fun addressesAndFingerprints() {
    assertEquals(QuicAddress("127.0.0.1", 2049), parseQuicAddress("quic://127.0.0.1:2049"))
    assertEquals(QuicAddress("::1", 4433), parseQuicAddress("quic://[::1]:4433"))
    assertEquals(QuicAddress("example.com", 4433), parseQuicAddress("example.com:4433"))
    assertFailsWith<ChannelError> { parseQuicAddress("ws://127.0.0.1:2049") }
    assertFailsWith<ChannelError> { parseQuicAddress("quic://127.0.0.1") }

    assertEquals(4433, parseListenAddress("0.0.0.0:4433").port)
    assertEquals("127.0.0.1", parseListenAddress(":4433").hostString)
    assertEquals(java.net.InetAddress.getByName("::1"), parseListenAddress("[::1]:0").address)

    val expected = MessageDigest.getInstance("SHA-256").digest(parseCertificates(testCert).first().encoded)
    assertContentEquals(expected, parseFingerprint(TEST_FINGERPRINT))
    assertContentEquals(expected, parseFingerprint(TEST_FINGERPRINT.replace(":", "").lowercase()))
    assertFailsWith<ChannelError> { parseFingerprint("nope") }
  }

  @Test
  fun certificateNames() {
    val certificate = parseCertificates(testCert).first()
    assertTrue(certificateMatches(certificate, "localhost"))
    assertTrue(certificateMatches(certificate, "LOCALHOST"))
    assertTrue(certificateMatches(certificate, "127.0.0.1"))
    assertTrue(certificateMatches(certificate, "::1"))
    assertFalse(certificateMatches(certificate, "example.com"))
    assertFalse(certificateMatches(certificate, "10.0.0.1"))
    // The key reads, and makes a key store kwik takes
    assertEquals("EC", parsePrivateKey(testKey).algorithm)
    assertTrue(serverKeyStore(testKey, testCert).isKeyEntry(KEY_ALIAS))
  }
}
