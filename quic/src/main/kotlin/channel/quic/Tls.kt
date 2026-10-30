package channel.quic

import channel.ChannelError
import java.io.ByteArrayOutputStream
import java.net.InetAddress
import java.security.KeyFactory
import java.security.KeyStore
import java.security.MessageDigest
import java.security.PrivateKey
import java.security.cert.CertificateException
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64
import java.util.Locale
import javax.naming.ldap.LdapName
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509TrustManager

private val pemBlock = Regex("-----BEGIN ([A-Z0-9 ]+)-----([A-Za-z0-9+/=\\s]+)-----END \\1-----")

/** The (type, DER bytes) of every PEM block in [pem]. */
internal fun pemBlocks(pem: String): List<Pair<String, ByteArray>> =
  pemBlock.findAll(pem).map { match ->
    match.groupValues[1] to Base64.getMimeDecoder().decode(match.groupValues[2])
  }.toList()

internal fun parseCertificates(pem: String): List<X509Certificate> {
  val factory = CertificateFactory.getInstance("X.509")
  val certificates = pemBlocks(pem)
    .filter { (type, _) -> type == "CERTIFICATE" }
    .map { (_, der) -> factory.generateCertificate(der.inputStream()) as X509Certificate }
  if (certificates.isEmpty()) {
    throw ChannelError("no PEM certificate found")
  }
  return certificates
}

/** A PKCS#8 (`PRIVATE KEY`), SEC1 (`EC PRIVATE KEY`) or PKCS#1 (`RSA PRIVATE KEY`) PEM key. */
internal fun parsePrivateKey(pem: String): PrivateKey {
  val (type, der) = pemBlocks(pem).firstOrNull { (type, _) -> type.endsWith("PRIVATE KEY") }
    ?: throw ChannelError("no PEM private key found")
  val (algorithms, pkcs8) = when (type) {
    "PRIVATE KEY" -> listOf("EC", "RSA", "Ed25519", "RSASSA-PSS") to der
    "EC PRIVATE KEY" -> listOf("EC") to ecToPkcs8(der)
    "RSA PRIVATE KEY" -> listOf("RSA") to wrapPkcs8(der(0x30, OID_RSA + byteArrayOf(0x05, 0x00)), der)
    else -> throw ChannelError("unsupported private key type $type")
  }
  for (algorithm in algorithms) {
    try {
      return KeyFactory.getInstance(algorithm).generatePrivate(PKCS8EncodedKeySpec(pkcs8))
    } catch (_: Exception) {
      continue
    }
  }
  throw ChannelError("unsupported private key")
}

/** An in-memory key store holding [key] and its certificate chain under [KEY_ALIAS]. */
internal fun serverKeyStore(keyPem: String, certPem: String): KeyStore {
  val store = KeyStore.getInstance("PKCS12")
  store.load(null, null)
  store.setKeyEntry(KEY_ALIAS, parsePrivateKey(keyPem), KEY_PASSWORD, parseCertificates(certPem).toTypedArray())
  return store
}

internal const val KEY_ALIAS = "channel"
internal val KEY_PASSWORD = "channel".toCharArray()

/** A SHA-256 fingerprint in hex, colons optional. */
internal fun parseFingerprint(fingerprint: String): ByteArray {
  val hex = fingerprint.replace(":", "").lowercase(Locale.ROOT)
  if (hex.length != 64 || hex.any { it !in '0'..'9' && it !in 'a'..'f' }) {
    throw ChannelError("invalid SHA-256 fingerprint: $fingerprint")
  }
  return ByteArray(32) { index -> hex.substring(index * 2, index * 2 + 2).toInt(16).toByte() }
}

/**
 * Checks the server certificate: against the pinned fingerprint when there is one (nothing else is checked then),
 * otherwise against [ca] (or the system roots) and [name], as a DNS name or an IP address.
 */
internal class ChannelTrustManager(
  private val ca: List<X509Certificate>?,
  private val pin: ByteArray?,
  private val name: String,
) : X509TrustManager {
  private val roots: X509TrustManager by lazy {
    val store = ca?.let { certificates ->
      KeyStore.getInstance(KeyStore.getDefaultType()).apply {
        load(null, null)
        certificates.forEachIndexed { index, certificate -> setCertificateEntry("ca$index", certificate) }
      }
    }
    val factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
    factory.init(store)
    factory.trustManagers.filterIsInstance<X509TrustManager>().first()
  }

  override fun checkServerTrusted(chain: Array<out X509Certificate>?, authType: String?) {
    val leaf = chain?.firstOrNull() ?: throw CertificateException("no server certificate")
    if (pin != null) {
      val actual = MessageDigest.getInstance("SHA-256").digest(leaf.encoded)
      if (!MessageDigest.isEqual(actual, pin)) {
        throw CertificateException("certificate fingerprint mismatch")
      }
      return
    }
    // TLS 1.3: the authType of TLS 1.2 key exchanges doesn't apply
    roots.checkServerTrusted(chain, "UNKNOWN")
    if (!certificateMatches(leaf, name)) {
      throw CertificateException("certificate is not valid for $name")
    }
  }

  override fun checkClientTrusted(chain: Array<out X509Certificate>?, authType: String?) {
    throw CertificateException("client certificates are not used")
  }

  override fun getAcceptedIssuers(): Array<X509Certificate> = emptyArray()
}

internal fun isIpLiteral(host: String): Boolean = host.contains(':') || host.matches(Regex("""\d{1,3}(\.\d{1,3}){3}"""))

/** Whether [certificate] is valid for [name]: an IP address SAN for an IP, a DNS SAN (or the CN without SANs) otherwise. */
internal fun certificateMatches(certificate: X509Certificate, name: String): Boolean {
  val alternatives = certificate.subjectAlternativeNames
  if (isIpLiteral(name)) {
    val address = runCatching { InetAddress.getByName(name) }.getOrNull() ?: return false
    return alternatives.orEmpty().any { entry ->
      entry[0] == 7 && runCatching { InetAddress.getByName(entry[1] as String) == address }.getOrDefault(false)
    }
  }
  if (alternatives != null && alternatives.isNotEmpty()) {
    return alternatives.any { entry -> entry[0] == 2 && dnsNameMatches(name, entry[1] as String) }
  }
  val commonName = runCatching {
    LdapName(certificate.subjectX500Principal.name).rdns.lastOrNull { it.type.equals("CN", ignoreCase = true) }?.value?.toString()
  }.getOrNull() ?: return false
  return dnsNameMatches(name, commonName)
}

private fun dnsNameMatches(name: String, pattern: String): Boolean {
  if (pattern.startsWith("*.")) {
    val dot = name.indexOf('.')
    return dot > 0 && name.substring(dot + 1).equals(pattern.substring(2), ignoreCase = true)
  }
  return name.equals(pattern, ignoreCase = true)
}

// Minimal DER, to turn SEC1 and PKCS#1 keys into the PKCS#8 the JDK reads

private val OID_EC = byteArrayOf(0x06, 0x07, 0x2A, 0x86.toByte(), 0x48, 0xCE.toByte(), 0x3D, 0x02, 0x01)
private val OID_RSA = byteArrayOf(0x06, 0x09, 0x2A, 0x86.toByte(), 0x48, 0x86.toByte(), 0xF7.toByte(), 0x0D, 0x01, 0x01, 0x01)

private fun der(tag: Int, content: ByteArray): ByteArray {
  val out = ByteArrayOutputStream()
  out.write(tag)
  val length = content.size
  when {
    length < 0x80 -> out.write(length)
    length < 0x100 -> { out.write(0x81); out.write(length) }
    length < 0x10000 -> { out.write(0x82); out.write(length ushr 8); out.write(length and 0xFF) }
    else -> { out.write(0x83); out.write(length ushr 16); out.write((length ushr 8) and 0xFF); out.write(length and 0xFF) }
  }
  out.write(content)
  return out.toByteArray()
}

private fun wrapPkcs8(algorithm: ByteArray, key: ByteArray): ByteArray =
  der(0x30, byteArrayOf(0x02, 0x01, 0x00) + algorithm + der(0x04, key))

/** The elements (tag, full TLV) inside the DER SEQUENCE [der]. */
private fun derChildren(der: ByteArray): List<Pair<Int, ByteArray>> {
  fun lengthAt(offset: Int): Pair<Int, Int> {
    val first = der[offset].toInt() and 0xFF
    if (first < 0x80) return first to 1
    val count = first and 0x7F
    var length = 0
    for (index in 1..count) length = (length shl 8) or (der[offset + index].toInt() and 0xFF)
    return length to 1 + count
  }
  val (_, headerLength) = lengthAt(1)
  var offset = 1 + headerLength
  val children = mutableListOf<Pair<Int, ByteArray>>()
  while (offset < der.size) {
    val tag = der[offset].toInt() and 0xFF
    val (length, lengthSize) = lengthAt(offset + 1)
    val end = offset + 1 + lengthSize + length
    children += tag to der.copyOfRange(offset, end)
    offset = end
  }
  return children
}

private fun ecToPkcs8(sec1: ByteArray): ByteArray {
  // ECPrivateKey ::= SEQUENCE { version, privateKey OCTET STRING, [0] parameters (the curve OID), [1] publicKey }
  val parameters = derChildren(sec1).firstOrNull { (tag, _) -> tag == 0xA0 }?.second
    ?: throw ChannelError("EC private key without its curve")
  val curve = parameters.copyOfRange(2, parameters.size)
  return wrapPkcs8(der(0x30, OID_EC + curve), sec1)
}
