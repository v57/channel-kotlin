package channel

/**
 * Marks the small API that lets a separate transport module (channel-kotlin-quic) plug a connection into a
 * [Channel]. It is not meant for applications and may change between versions.
 */
@RequiresOptIn(
  message = "Channel's transport API is for transport modules such as channel-kotlin-quic and may change.",
  level = RequiresOptIn.Level.ERROR,
)
@Retention(AnnotationRetention.BINARY)
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION, AnnotationTarget.PROPERTY)
annotation class ChannelTransportApi

/** One connection that carries whole text messages (the JSON a WebSocket text frame would carry) both ways. */
@ChannelTransportApi
interface MessageTransport {
  /** Queues or writes one message. Called on the thread that polls the channel. */
  fun sendMessage(text: String)

  fun close()

  fun isConnected(): Boolean
}

/** Something the single-threaded pump drives: read what arrived, dispatch it, return whether anything happened. */
@ChannelTransportApi
fun interface TransportPollable {
  fun poll(): Boolean
}

/** A pollable registered with the pump; [unregister] removes it. */
@ChannelTransportApi
class TransportRegistration internal constructor(private val entry: Pollable) {
  fun unregister() {
    PumpRegistry.unregister(entry)
  }
}

/** The channel side of one connection: hand it every received message, stop it when the connection ends. */
@ChannelTransportApi
class TransportPeer<State> internal constructor(internal val peer: Peer<State>) {
  val state: State
    get() = peer.state

  val sender: Sender<State>
    get() = peer.sender

  /** Dispatches one received message (a single message object or a batch). Throws on invalid JSON. */
  fun receive(text: String) {
    peer.handleIncomingFrame(text)
  }

  /** Ends the connection: closes the transport, fails what still waits and runs the channel's onDisconnect. */
  fun stop() {
    peer.stop()
  }

  fun isConnected(): Boolean = peer.isConnected()
}

@ChannelTransportApi
object ChannelTransports {
  /** Wraps [transport] in a peer of [channel], with [state] or the channel's default state. */
  fun <State> attach(channel: Channel<State>, transport: MessageTransport, state: State? = null): TransportPeer<State> {
    val adapter = object : Transport {
      override fun sendFrame(payload: String) = transport.sendMessage(payload)

      override fun close() = transport.close()

      override fun isConnected(): Boolean = transport.isConnected()
    }
    val peer = Peer(
      channel = channel,
      state = state ?: channel.makeDefaultState(),
      transport = adapter,
      codec = defaultJsonCodec(),
      mode = TransportMode.CLIENT,
    )
    return TransportPeer(peer)
  }

  /** Adds [pollable] to the pump that blocking calls (`send`, `subscribe`, `values().next()`) drive. */
  fun register(pollable: TransportPollable): TransportRegistration {
    val entry = pollableOf(pollable)
    PumpRegistry.register(entry)
    return TransportRegistration(entry)
  }

  /** Polls every registered transport once; true when any made progress. */
  fun pumpAll(): Boolean = PumpRegistry.pumpAll()

  /** A [Client] over [peer]; its `poll()` runs [pollable]. */
  fun <State> client(peer: TransportPeer<State>, pollable: TransportPollable): Client<State> =
    Client(peer.peer, pollableOf(pollable))

  /** A [Server] whose `poll()` runs [pollable] and whose `stop()` runs [stop]. */
  fun <State> server(pollable: TransportPollable, stop: () -> Unit, port: Int): Server<State> =
    Server(pollableOf(pollable), stop, port)

  fun <State> defaultState(channel: Channel<State>): State = channel.makeDefaultState()

  private fun pollableOf(pollable: TransportPollable): Pollable = object : Pollable {
    override fun poll(): Boolean = pollable.poll()
  }
}
