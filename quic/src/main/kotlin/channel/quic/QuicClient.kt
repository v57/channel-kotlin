package channel.quic

import channel.Channel
import channel.ChannelError
import channel.ChannelTransports
import channel.Client
import channel.DEFAULT_MAX_PAYLOAD
import channel.MessageTransport
import channel.TransportPeer
import channel.TransportPollable
import channel.TransportRegistration
import channel.defaultJsonCodec
import tech.kwik.core.QuicClientConnection
import tech.kwik.core.QuicConnection.QuicVersion
import tech.kwik.core.log.NullLogger
import java.net.URI
import java.time.Duration

data class QuicConnectOptions<State>(
  val state: State? = null,
  /** Sent once in the hello frame, as the WebSocket client sends its upgrade headers. */
  val headers: Map<String, String> = emptyMap(),
  /** Largest frame accepted from the server; a bigger one disconnects (default 16 MiB). */
  val maxPayload: Int = DEFAULT_MAX_PAYLOAD,
  /** PEM certificate (or CA) to trust, for self-signed servers. Without it the system roots are used. */
  val ca: String? = null,
  /** SHA-256 fingerprint of the server certificate (hex, colons optional) to pin instead of verifying it. */
  val fingerprint: String? = null,
  /** The name sent (SNI) and checked against the certificate; defaults to the address host. */
  val servername: String? = null,
  val connectTimeoutMillis: Long = 10_000L,
)

/** Connects to `quic://host:port`. Blocks until the connection is open; throws [ChannelError] when it can't be. */
fun <State> Channel<State>.connectQuic(address: String, options: QuicConnectOptions<State> = QuicConnectOptions()): Client<State> {
  val transport = QuicClientTransport(this, parseQuicAddress(address), options)
  return ChannelTransports.client(transport.peer, transport)
}

fun <State> Channel<State>.connectQuic(port: Int, options: QuicConnectOptions<State> = QuicConnectOptions()): Client<State> =
  connectQuic("quic://127.0.0.1:$port", options)

internal data class QuicAddress(val host: String, val port: Int)

internal fun parseQuicAddress(address: String): QuicAddress {
  val text = when {
    address.startsWith("quic://") -> address
    address.contains("://") -> throw ChannelError("not a quic:// address: $address")
    else -> "quic://$address"
  }
  val uri = runCatching { URI(text) }.getOrNull() ?: throw ChannelError("invalid quic address: $address")
  val host = uri.host?.removePrefix("[")?.removeSuffix("]") ?: throw ChannelError("quic address without a host: $address")
  if (uri.port <= 0) {
    throw ChannelError("quic address without a port: $address")
  }
  return QuicAddress(host, uri.port)
}

private class QuicClientTransport<State>(
  channel: Channel<State>,
  address: QuicAddress,
  options: QuicConnectOptions<State>,
) : MessageTransport, TransportPollable {
  private val connection: QuicClientConnection
  private val framed: FramedStream
  private val registration: TransportRegistration
  private var closed = false

  val peer: TransportPeer<State>

  init {
    val pin = options.fingerprint?.let(::parseFingerprint)
    val ca = options.ca?.let(::parseCertificates)
    val name = options.servername ?: address.host
    // The trust manager below does all of the checking (kwik's own name check knows no IP addresses), so kwik's
    // warning about its disabled check doesn't apply
    if (System.getProperty("tech.kwik.core.no-security-warnings") == null) {
      System.setProperty("tech.kwik.core.no-security-warnings", "true")
    }
    val builder = QuicClientConnection.newBuilder()
      .version(QuicVersion.V1)
      .applicationProtocol(ALPN)
      .maxIdleTimeout(Duration.ofSeconds(IDLE_TIMEOUT_SECONDS.toLong()))
      .connectTimeout(Duration.ofMillis(options.connectTimeoutMillis))
      .maxOpenPeerInitiatedBidirectionalStreams(0)
      .maxOpenPeerInitiatedUnidirectionalStreams(0)
      .logger(NullLogger())
      .noServerCertificateCheck()
      .customTrustManager(ChannelTrustManager(ca, pin, name))
      // kwik opens a new UDP socket for every connection: Network.framework servers take one connection per
      // client address and port
      .host(name)
      .port(address.port)
    if (name != address.host) {
      // kwik sends its host as the server name; the "proxy" is where the packets go
      builder.proxy(address.host)
    }
    connection = try {
      builder.build()
    } catch (error: Exception) {
      throw ChannelError("quic: ${error.message ?: error}")
    }
    try {
      connection.connect()
      val stream = connection.createStream(true)
      framed = FramedStream(connection, stream, options.maxPayload, KEEPALIVE_MILLIS)
      connection.setConnectionListener { framed.markRemoteClosed() }
      framed.start()
      framed.send(encodeFrame(defaultJsonCodec().encode(mapOf("hello" to options.headers))))
    } catch (error: Throwable) {
      runCatching { connection.close() }
      if (error is VirtualMachineError) throw error
      throw ChannelError("quic connect to ${address.host}:${address.port} failed: ${error.message ?: error}")
    }
    peer = ChannelTransports.attach(channel, this, options.state)
    registration = ChannelTransports.register(this)
  }

  override fun sendMessage(text: String) {
    if (!closed) {
      framed.send(encodeFrame(text))
    }
  }

  override fun close() {
    if (closed) {
      return
    }
    closed = true
    registration.unregister()
    framed.close()
  }

  override fun isConnected(): Boolean = !closed && framed.isConnected()

  override fun poll(): Boolean {
    if (closed) {
      return false
    }
    return try {
      framed.drain(peer::receive) { disconnect() }
    } catch (error: Throwable) {
      if (error is VirtualMachineError && error !is StackOverflowError) {
        throw error
      }
      // Invalid JSON or a broken server: disconnect, pending calls fail with "disconnected"
      disconnect()
      true
    }
  }

  private fun disconnect() {
    close()
    peer.stop()
  }
}
