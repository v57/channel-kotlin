package channel.quic

import channel.Channel
import channel.ChannelError
import channel.ChannelTransports
import channel.DEFAULT_MAX_PAYLOAD
import channel.MessageTransport
import channel.Sender
import channel.Server
import channel.TransportPeer
import channel.TransportPollable
import channel.TransportRegistration
import channel.defaultJsonCodec
import tech.kwik.core.ConnectionListener
import tech.kwik.core.ConnectionTerminatedEvent
import tech.kwik.core.QuicConnection
import tech.kwik.core.QuicConnection.QuicVersion
import tech.kwik.core.QuicStream
import tech.kwik.core.log.NullLogger
import tech.kwik.core.server.ApplicationProtocolConnection
import tech.kwik.core.server.ApplicationProtocolConnectionFactory
import tech.kwik.core.server.ServerConnectionConfig
import tech.kwik.core.server.ServerConnector
import java.net.DatagramSocket
import java.net.InetSocketAddress
import java.util.Locale
import java.util.concurrent.ConcurrentLinkedQueue

data class QuicListenOptions<State>(
  /** PEM private key (PKCS#8, or SEC1 / PKCS#1). */
  val key: String,
  /** PEM certificate chain. */
  val cert: String,
  /** Builds a connection's state from its hello headers (names lowercased). Throwing closes the connection. */
  val state: ((Map<String, String>) -> State)? = null,
  val onConnect: ((State, Sender<State>) -> Unit)? = null,
  val onDisconnect: ((State, Sender<State>) -> Unit)? = null,
  /** Largest frame accepted from a client; a bigger one closes that connection (default 16 MiB). */
  val maxPayload: Int = DEFAULT_MAX_PAYLOAD,
)

/** Serves QUIC on UDP 127.0.0.1:[port] (0 picks a free port; see [Server.port]). */
fun <State> Channel<State>.listenQuic(port: Int, options: QuicListenOptions<State>): Server<State> =
  listenQuic("127.0.0.1:$port", options)

/** Serves QUIC on UDP `host:port` (`0.0.0.0:4433` for every interface, `[::1]:4433`, `:4433`). */
fun <State> Channel<State>.listenQuic(address: String, options: QuicListenOptions<State>): Server<State> {
  val transport = QuicServerTransport(this, address, options)
  return ChannelTransports.server(transport, transport::stop, transport.port)
}

internal fun parseListenAddress(address: String): InetSocketAddress {
  val separator = address.lastIndexOf(':')
  val host = if (separator < 0) "127.0.0.1" else address.substring(0, separator).removePrefix("[").removeSuffix("]").ifEmpty { "127.0.0.1" }
  val port = address.substring(separator + 1).toIntOrNull() ?: throw ChannelError("invalid listen address: $address")
  return InetSocketAddress(host, port)
}

private class QuicServerTransport<State>(
  private val channel: Channel<State>,
  address: String,
  private val options: QuicListenOptions<State>,
) : TransportPollable {
  private val socket = DatagramSocket(parseListenAddress(address))
  private val connector: ServerConnector
  private val incoming = ConcurrentLinkedQueue<ServerConnection>()
  private val connections = mutableListOf<ServerConnection>()
  private val registration: TransportRegistration

  @Volatile
  private var stopped = false

  val port: Int = socket.localPort

  init {
    try {
      val config = ServerConnectionConfig.builder()
        .maxIdleTimeoutInSeconds(IDLE_TIMEOUT_SECONDS)
        .maxOpenPeerInitiatedBidirectionalStreams(1)
        .maxOpenPeerInitiatedUnidirectionalStreams(0)
        .retryRequired(ServerConnectionConfig.RetryRequired.Never)
        .build()
      connector = ServerConnector.builder()
        .withSocket(socket)
        // QUIC v1. Clients such as Bun's (ngtcp2) offer v2 through compatible version negotiation (RFC 9368), and
        // kwik switches to it whatever the supported list says, so v2 must be accepted too
        .withSupportedVersions(listOf(QuicVersion.V1, QuicVersion.V2))
        .withKeyStore(serverKeyStore(options.key, options.cert), KEY_ALIAS, KEY_PASSWORD)
        .withConfiguration(config)
        .withLogger(NullLogger())
        .build()
      connector.registerApplicationProtocol(ALPN, Protocol())
      connector.start()
    } catch (error: Throwable) {
      socket.close()
      if (error is ChannelError || error is VirtualMachineError) throw error
      throw ChannelError("quic listen on $address failed: ${error.message ?: error}")
    }
    registration = ChannelTransports.register(this)
  }

  /** One bidirectional stream per connection, opened by the client; no unidirectional streams. */
  private inner class Protocol : ApplicationProtocolConnectionFactory {
    override fun maxConcurrentPeerInitiatedBidirectionalStreams(): Int = 1

    override fun maxTotalPeerInitiatedBidirectionalStreams(): Long = 1L

    override fun maxConcurrentPeerInitiatedUnidirectionalStreams(): Int = 0

    override fun maxTotalPeerInitiatedUnidirectionalStreams(): Long = 0L

    override fun createConnection(protocol: String, quicConnection: QuicConnection): ApplicationProtocolConnection? {
      // kwik accepts 0-RTT and starts the connection before the handshake then; refuse it, a replayed request
      // could run twice
      if (stopped || acceptedEarlyData(quicConnection)) {
        quicConnection.close()
        return null
      }
      val connection = ServerConnection(quicConnection)
      quicConnection.setConnectionListener(connection)
      incoming.add(connection)
      return connection
    }
  }

  fun stop() {
    if (stopped) {
      return
    }
    stopped = true
    registration.unregister()
    while (true) {
      connections += incoming.poll() ?: break
    }
    connections.toList().forEach { it.destroy() }
    runCatching { connector.close() }
    socket.close()
  }

  override fun poll(): Boolean {
    if (stopped) {
      return false
    }
    var progressed = false
    while (true) {
      connections += incoming.poll() ?: break
      progressed = true
    }
    connections.toList().forEach { connection ->
      progressed = try {
        connection.poll() || progressed
      } catch (error: Throwable) {
        if (error is VirtualMachineError && error !is StackOverflowError) {
          throw error
        }
        // Not a hello, invalid JSON, a rejected state, ...: drop this connection only
        connection.destroy()
        true
      }
    }
    return progressed
  }

  private inner class ServerConnection(
    private val quicConnection: QuicConnection,
  ) : ApplicationProtocolConnection, ConnectionListener, MessageTransport {
    @Volatile
    private var framed: FramedStream? = null

    @Volatile
    private var gone = false
    private var destroyed = false
    private var peer: TransportPeer<State>? = null

    override fun acceptPeerInitiatedStream(stream: QuicStream) {
      // The stream limits refuse any other stream already; this only guards against a misbehaving stack
      if (!stream.isBidirectional || framed != null) {
        runCatching { stream.abortReading(0) }
        runCatching { stream.resetStream(0) }
        return
      }
      framed = FramedStream(quicConnection, stream, options.maxPayload, keepaliveMillis = 0L).start()
    }

    override fun disconnected(event: ConnectionTerminatedEvent) {
      gone = true
      framed?.markRemoteClosed()
    }

    fun poll(): Boolean {
      if (destroyed) {
        return false
      }
      val stream = framed
      if (stream == null) {
        if (gone) {
          destroy()
          return true
        }
        return false
      }
      if (stream.isLocallyClosed()) {
        // Stopped from this side (sender.stop()): finish like a dropped connection
        destroy()
        return true
      }
      val progressed = stream.drain(::receive) { destroy() }
      val since = stream.writingSince
      if (!destroyed && since != 0L && System.nanoTime() - since > WRITE_STALL_TIMEOUT_NANOS) {
        // A client that stops reading must not make the server queue for it forever
        destroy()
        return true
      }
      return progressed
    }

    private fun receive(text: String) {
      val current = peer
      if (current != null) {
        current.receive(text)
        return
      }
      // First frame: {"hello": {header: value}}, used like the WebSocket upgrade request's headers
      val hello = (defaultJsonCodec().decode(text) as? Map<*, *>)?.get("hello") as? Map<*, *>
        ?: throw ChannelError("the first quic frame isn't a hello")
      val headers = linkedMapOf<String, String>()
      hello.forEach { (name, value) ->
        if (name is String && value is String) {
          headers[name.lowercase(Locale.ROOT)] = value
        }
      }
      val state = options.state?.invoke(headers) ?: ChannelTransports.defaultState(channel)
      val created = ChannelTransports.attach(channel, this, state)
      peer = created
      options.onConnect?.invoke(state, created.sender)
    }

    fun destroy() {
      if (destroyed) {
        return
      }
      destroyed = true
      connections.remove(this)
      val target = peer
      peer = null
      if (target != null) {
        target.stop()
        options.onDisconnect?.invoke(target.state, target.sender)
      }
      close()
    }

    override fun sendMessage(text: String) {
      framed?.send(encodeFrame(text))
    }

    override fun close() {
      val stream = framed
      if (stream != null) {
        stream.close()
      } else {
        runCatching { quicConnection.close() }
      }
    }

    override fun isConnected(): Boolean = !destroyed && !gone && framed?.isConnected() == true
  }

  private companion object {
    const val WRITE_STALL_TIMEOUT_NANOS = 10_000_000_000L
  }
}

/** Whether kwik accepted 0-RTT data on [connection] (its server connection keeps that in a private field). */
private fun acceptedEarlyData(connection: QuicConnection): Boolean {
  var type: Class<*>? = connection.javaClass
  while (type != null) {
    val field = runCatching { type.getDeclaredField("acceptedEarlyData") }.getOrNull()
    if (field != null) {
      return runCatching {
        field.isAccessible = true
        field.getBoolean(connection)
      }.getOrDefault(false)
    }
    type = type.superclass
  }
  return false
}
