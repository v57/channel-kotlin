package channel

import java.util.ArrayDeque
import java.util.LinkedHashMap
import java.util.concurrent.locks.LockSupport

class ChannelError(message: String) : RuntimeException(message)

sealed interface CallResult<out T> {
  data class Ok<T>(val value: T?) : CallResult<T>

  data class RemoteErr(val error: String) : CallResult<Nothing>
}

sealed interface StreamNext<out T> {
  data class Item<T>(val value: T) : StreamNext<T>

  data object Done : StreamNext<Nothing>

  data class RemoteErr(val error: String) : StreamNext<Nothing>
}

sealed interface SubscribeResult {
  data class Ok(val handle: SubscriptionHandle) : SubscribeResult

  data class RemoteErr(val error: String) : SubscribeResult
}

fun interface Cancellable {
  fun cancel()
}

class SubscriptionHandle internal constructor(
  private val cancelAction: () -> Unit,
) : Cancellable {
  private var cancelled = false

  override fun cancel() {
    if (cancelled) {
      return
    }
    cancelled = true
    cancelAction()
  }
}

data class ConnectOptions<State>(
  val state: State? = null,
  /** Raw extra header lines sent once with the upgrade request (there is no reconnect yet). */
  val headers: String? = null,
  /** Largest websocket message accepted from the server; bigger ones disconnect (default 16 MiB). */
  val maxPayload: Int = DEFAULT_MAX_PAYLOAD,
)

data class ListenOptions<State>(
  val state: ((Map<String, String>) -> State)? = null,
  val onConnect: ((State, Sender<State>) -> Unit)? = null,
  val onDisconnect: ((State, Sender<State>) -> Unit)? = null,
  /** Largest websocket message accepted from a client; bigger ones drop that connection (default 16 MiB). */
  val maxPayload: Int = DEFAULT_MAX_PAYLOAD,
)

data class PostRequest<State, Req, Ctx>(
  val body: Req,
  val context: Ctx,
  val sender: Sender<State>,
  val state: State,
  val path: String,
)

class StreamEmitter<Item> internal constructor(
  private val emitAction: (Item) -> Unit,
  private val cancelledAction: () -> Boolean,
) {
  fun emit(item: Item) {
    emitAction(item)
  }

  fun isCancelled(): Boolean = cancelledAction()
}

data class StreamRequest<State, Req, Ctx, Item>(
  val body: Req,
  val context: Ctx,
  val sender: Sender<State>,
  val state: State,
  val path: String,
  val emitter: StreamEmitter<Item>,
)

abstract class Sender<State> {
  abstract fun <T> send(responseType: Class<T>, path: String, body: Any? = Unit, context: Any? = Unit): CallResult<T>
  abstract fun <Item> values(itemType: Class<Item>, path: String, body: Any? = Unit, context: Any? = Unit): Values<Item>
  abstract fun <Event> subscribe(eventType: Class<Event>, path: String, body: Any? = Unit, context: Any? = Unit, callback: (Event) -> Unit): SubscribeResult
  abstract fun stop()

  inline fun <reified T> send(
    path: String,
    body: Any? = Unit,
    context: Any? = Unit,
  ): CallResult<T> = send(typeClass(), path, body, context)

  inline fun <reified Item> values(
    path: String,
    body: Any? = Unit,
    context: Any? = Unit,
  ): Values<Item> = values(typeClass(), path, body, context)

  inline fun <reified Event> subscribe(
    path: String,
    body: Any? = Unit,
    context: Any? = Unit,
    noinline callback: (Event) -> Unit,
  ): SubscribeResult = subscribe(typeClass(), path, body, context, callback)
}

class Values<Item> internal constructor(
  private val pending: PendingStream,
  private val itemType: Class<Item>,
) : AutoCloseable {
  fun next(): StreamNext<Item> = pending.next(itemType)

  override fun close() {
    pending.close()
  }
}

sealed interface ApiNode<State> {
  fun register(channel: Channel<State>, prefix: String)
}

class ApiPost<State>(
  private val registerAction: (Channel<State>, String) -> Unit,
) : ApiNode<State> {
  override fun register(channel: Channel<State>, prefix: String) {
    registerAction(channel, prefix)
  }
}

class ApiStream<State>(
  private val registerAction: (Channel<State>, String) -> Unit,
) : ApiNode<State> {
  override fun register(channel: Channel<State>, prefix: String) {
    registerAction(channel, prefix)
  }
}

class ApiGroup<State>(
  val children: Map<String, ApiNode<State>>,
) : ApiNode<State> {
  override fun register(channel: Channel<State>, prefix: String) {
    children.forEach { (key, value) ->
      val path = when {
        key == "_" -> prefix
        prefix.isEmpty() -> key
        else -> "$prefix/$key"
      }
      value.register(channel, path)
    }
  }
}

fun <State> group(children: Map<String, ApiNode<State>>): ApiGroup<State> = ApiGroup(children)

inline fun <State, reified Req, reified Resp, reified Ctx> postSpec(
  noinline handler: (PostRequest<State, Req, Ctx>) -> Resp?,
): ApiPost<State> = ApiPost { channel, path -> channel.post(path, handler) }

inline fun <State, reified Req, reified Item, reified Ctx> streamSpec(
  noinline handler: (StreamRequest<State, Req, Ctx, Item>) -> Unit,
): ApiStream<State> = ApiStream { channel, path -> channel.stream(path, handler) }

data class SubscriptionEvent(
  val topic: String,
  val bodyJson: String?,
)

class Subscription<Req, Event> {
  private var topicFormatter: (Req) -> String = { request ->
    when (request) {
      Unit,
      null -> ""
      else -> request.toString()
    }
  }
  private val publishers = linkedMapOf<Int, (SubscriptionEvent) -> Unit>()
  private var nextPublisherId = 1
  internal var prefix: String = ""
    private set

  fun topic(make: (Req) -> String): Subscription<Req, Event> {
    topicFormatter = make
    return this
  }

  fun send(request: Req, event: Event, codec: JsonCodec = defaultJsonCodec()) {
    val eventType = dynamicClass(event)
    val bodyJson = when {
      normalizeClass(eventType) == Unit::class.java || event == Unit -> null
      else -> codec.encode(event)
    }
    if (normalizeClass(eventType) == Unit::class.java && bodyJson == null) {
      return
    }
    publish(topicOf(request), bodyJson)
  }

  internal fun setPrefix(prefix: String) {
    this.prefix = prefix
  }

  internal fun topicOf(request: Req): String {
    val topic = topicFormatter(request)
    return when {
      prefix.isEmpty() -> topic
      topic.isEmpty() -> prefix
      else -> "$prefix/$topic"
    }
  }

  internal fun addPublisher(publisher: (SubscriptionEvent) -> Unit): Int {
    val id = nextPublisherId++
    publishers[id] = publisher
    return id
  }

  internal fun removePublisher(id: Int) {
    publishers.remove(id)
  }

  private fun publish(topic: String, bodyJson: String?) {
    val snapshot = publishers.values.toList()
    snapshot.forEach { callback ->
      runCatching {
        callback(SubscriptionEvent(topic, bodyJson))
      }
    }
  }

  companion object {
    fun parse(objectMap: Map<String, *>, prefix: String, output: MutableMap<String, Subscription<Any?, Any?>>) {
      objectMap.forEach { (key, value) ->
        when (value) {
          is Subscription<*, *> -> {
            @Suppress("UNCHECKED_CAST")
            val subscription = value as Subscription<Any?, Any?>
            val nextPrefix = if (prefix.isEmpty()) key else "$prefix/$key"
            subscription.setPrefix(nextPrefix)
            output[nextPrefix] = subscription
          }

          is Map<*, *> -> {
            @Suppress("UNCHECKED_CAST")
            parse(value as Map<String, *>, if (prefix.isEmpty()) key else "$prefix/$key", output)
          }
        }
      }
    }
  }
}

class Channel<State>(
  private val defaultStateFactory: (() -> State)? = null,
  private val codec: JsonCodec = defaultJsonCodec(),
) {
  private val posts = linkedMapOf<String, RegisteredPost<State>>()
  private val streams = linkedMapOf<String, RegisteredStream<State>>()
  private val subscriptions = linkedMapOf<String, Subscription<Any?, Any?>>()
  private val otherPosts = mutableListOf<OtherPost<State>>()
  private val otherStreams = mutableListOf<OtherStream<State>>()
  private var onDisconnectAction: ((State, Sender<State>) -> Unit)? = null
  internal var requestTimeoutMillis: Long = 0L
    private set

  /**
   * Fails blocking `send`/`subscribe`/`values().next()` with "timeout" when no reply arrives within
   * [millis]. 0 (the default, same as TS) waits forever.
   */
  fun requestTimeout(millis: Long): Channel<State> {
    require(millis >= 0) { "timeout must be >= 0" }
    requestTimeoutMillis = millis
    return this
  }

  inline fun <reified Req, reified Resp, reified Ctx> post(
    path: String,
    noinline handler: (PostRequest<State, Req, Ctx>) -> Resp?,
  ): Channel<State> {
    @Suppress("UNCHECKED_CAST")
    registerPost(path, typeClass<Req>(), typeClass<Resp>(), typeClass<Ctx>(), handler as (PostRequest<State, *, *>) -> Any?)
    return this
  }

  inline fun <reified Req, reified Resp, reified Ctx> postOther(
    noinline matcher: (String) -> Boolean,
    noinline handler: (PostRequest<State, Req, Ctx>) -> Resp?,
  ): Channel<State> {
    @Suppress("UNCHECKED_CAST")
    registerOtherPost(matcher, typeClass<Req>(), typeClass<Resp>(), typeClass<Ctx>(), handler as (PostRequest<State, *, *>) -> Any?)
    return this
  }

  inline fun <reified Req, reified Item, reified Ctx> stream(
    path: String,
    noinline handler: (StreamRequest<State, Req, Ctx, Item>) -> Unit,
  ): Channel<State> {
    @Suppress("UNCHECKED_CAST")
    registerStream(path, typeClass<Req>(), typeClass<Item>(), typeClass<Ctx>(), handler as (StreamRequest<State, *, *, *>) -> Unit)
    return this
  }

  inline fun <reified Req, reified Item, reified Ctx> streamOther(
    noinline matcher: (String) -> Boolean,
    noinline handler: (StreamRequest<State, Req, Ctx, Item>) -> Unit,
  ): Channel<State> {
    @Suppress("UNCHECKED_CAST")
    registerOtherStream(matcher, typeClass<Req>(), typeClass<Item>(), typeClass<Ctx>(), handler as (StreamRequest<State, *, *, *>) -> Unit)
    return this
  }

  fun onDisconnect(action: (State, Sender<State>) -> Unit): Channel<State> {
    onDisconnectAction = action
    return this
  }

  fun api(api: Map<String, ApiNode<State>>): Channel<State> {
    api.forEach { (key, value) ->
      val path = if (key == "_") "" else key
      value.register(this, path)
    }
    return this
  }

  fun events(events: Map<String, *>, prefix: String = ""): Channel<State> {
    val parsed = linkedMapOf<String, Subscription<Any?, Any?>>()
    if (events.values.all { it is Subscription<*, *> }) {
      events.forEach { (key, value) ->
        @Suppress("UNCHECKED_CAST")
        val subscription = value as Subscription<Any?, Any?>
        val path = if (prefix.isEmpty()) key else "$prefix/$key"
        subscription.setPrefix(path)
        parsed[path] = subscription
      }
    } else {
      Subscription.parse(events, prefix, parsed)
    }
    subscriptions.putAll(parsed)
    return this
  }

  fun merge(other: Channel<State>, prefix: String = ""): Channel<State> {
    val pathPrefix = prefix.trim('/')
    other.posts.forEach { (key, value) ->
      posts[joinPath(pathPrefix, key)] = value
    }
    other.streams.forEach { (key, value) ->
      streams[joinPath(pathPrefix, key)] = value
    }
    other.subscriptions.forEach { (key, value) ->
      val mergedPath = joinPath(pathPrefix, key)
      value.setPrefix(mergedPath)
      subscriptions[mergedPath] = value
    }
    return this
  }

  fun process(state: State = makeDefaultState()): Process<State> {
    val transport = ProcessTransport()
    val peer = Peer(
      channel = this,
      state = state,
      transport = transport,
      codec = codec,
      mode = TransportMode.PROCESS,
    )
    transport.attach(peer)
    PumpRegistry.register(transport)
    return Process(peer)
  }

  fun connect(address: String, options: ConnectOptions<State> = ConnectOptions()): Client<State> {
    return createClient(this, address, options)
  }

  fun connect(port: Int, options: ConnectOptions<State> = ConnectOptions()): Client<State> {
    return connect("ws://127.0.0.1:$port", options)
  }

  fun listen(address: String, options: ListenOptions<State> = ListenOptions()): Server<State> {
    return createServer(this, address, options)
  }

  fun listen(port: Int, options: ListenOptions<State> = ListenOptions()): Server<State> {
    return listen("127.0.0.1:$port", options)
  }

  internal fun makeDefaultState(): State {
    @Suppress("UNCHECKED_CAST")
    return defaultStateFactory?.invoke() ?: linkedMapOf<String, Any?>() as State
  }

  internal fun invokeDisconnect(state: State, sender: Sender<State>) {
    runCatching {
      onDisconnectAction?.invoke(state, sender)
    }
  }

  internal fun dispatchIncoming(peer: Peer<State>, message: ParsedMessage) {
    when {
      message.path != null -> dispatchPost(peer, message)
      message.stream != null -> dispatchStream(peer, message)
      message.cancel != null -> peer.cancelIncoming(message.cancel)
      message.sub != null -> dispatchSubscription(peer, message)
      message.unsub != null -> peer.removeRemoteTopic(message.unsub)
      message.topic != null -> {
        message.id?.let { peer.completeResponse(it, message) }
        peer.dispatchLocalEvent(message.topic, message.bodyJson)
      }

      message.id != null -> peer.completeResponse(message.id, message)
    }
  }

  private fun dispatchPost(peer: Peer<State>, message: ParsedMessage) {
    val path = message.path ?: return
    val handler = posts[path] ?: otherPosts.firstOrNull { it.matcher(path) }?.handler
    if (handler == null) {
      message.id?.let { peer.sendMessage(OutgoingMessage(id = it, errorText = "api not found")) }
      return
    }
    handler.invokeWire(peer, path, message.bodyJson, message.contextJson, message.id)
  }

  private fun dispatchStream(peer: Peer<State>, message: ParsedMessage) {
    val path = message.stream ?: return
    val id = message.id ?: return
    val handler = streams[path] ?: otherStreams.firstOrNull { it.matcher(path) }?.handler
    if (handler == null) {
      peer.sendMessage(OutgoingMessage(id = id, errorText = "api not found"))
      return
    }
    handler.invokeWire(peer, path, message.bodyJson, message.contextJson, id)
  }

  private fun dispatchSubscription(peer: Peer<State>, message: ParsedMessage) {
    val id = message.id ?: return
    val name = message.sub ?: return
    val subscription = subscriptions[name]
    if (subscription == null) {
      peer.sendMessage(OutgoingMessage(id = id, errorText = "subscription not found"))
      return
    }

    try {
      val request = when {
        message.bodyJson == null -> Unit
        else -> try {
          codec.decode(message.bodyJson)
        } catch (error: Exception) {
          throw ChannelError("invalid subscription body")
        }
      }
      val topic = subscription.topicOf(request)
      peer.addRemoteTopic(topic)
      peer.sendMessage(
        OutgoingMessage(
          id = id,
          topic = topic,
        ),
      )
    } catch (error: Throwable) {
      peer.sendMessage(OutgoingMessage(id = id, errorText = error.message ?: error.toString()))
    }
  }

  @PublishedApi
  internal fun registerPost(
    path: String,
    requestType: Class<*>,
    responseType: Class<*>,
    contextType: Class<*>,
    handler: (PostRequest<State, *, *>) -> Any?,
  ) {
    posts[path] = buildPostRegistration(requestType, responseType, contextType, handler)
  }

  @PublishedApi
  internal fun registerOtherPost(
    matcher: (String) -> Boolean,
    requestType: Class<*>,
    responseType: Class<*>,
    contextType: Class<*>,
    handler: (PostRequest<State, *, *>) -> Any?,
  ) {
    otherPosts += OtherPost(matcher, buildPostRegistration(requestType, responseType, contextType, handler))
  }

  @PublishedApi
  internal fun buildPostRegistration(
    requestType: Class<*>,
    responseType: Class<*>,
    contextType: Class<*>,
    handler: (PostRequest<State, *, *>) -> Any?,
  ): RegisteredPost<State> {
    return RegisteredPost(
      requestType = normalizeClass(requestType),
      responseType = normalizeClass(responseType),
      contextType = normalizeClass(contextType),
      invokeWire = { peer, path, bodyJson, contextJson, id ->
        try {
          val body = decodeRequiredJson(codec, bodyJson, normalizeClass(requestType))
          val context = decodeRequiredJson(codec, contextJson, normalizeClass(contextType))
          val result = handler(PostRequest(body, context, peer.sender, peer.state, path))
          if (id != null) {
            peer.sendMessage(
              OutgoingMessage(
                id = id,
                bodyJson = encodeOptionalJson(codec, result),
              ),
            )
          }
        } catch (error: Throwable) {
          if (id != null) {
            peer.sendMessage(OutgoingMessage(id = id, errorText = error.message ?: error.toString()))
          }
        }
      },
      invokeDirect = { peer, path, body, context ->
        try {
          val result = handler(PostRequest(body, context, peer.sender, peer.state, path))
          DirectPostResult(value = result)
        } catch (error: Throwable) {
          DirectPostResult(error = error.message ?: error.toString())
        }
      },
    )
  }

  @PublishedApi
  internal fun registerStream(
    path: String,
    requestType: Class<*>,
    itemType: Class<*>,
    contextType: Class<*>,
    handler: (StreamRequest<State, *, *, *>) -> Unit,
  ) {
    streams[path] = buildStreamRegistration(requestType, itemType, contextType, handler)
  }

  @PublishedApi
  internal fun registerOtherStream(
    matcher: (String) -> Boolean,
    requestType: Class<*>,
    itemType: Class<*>,
    contextType: Class<*>,
    handler: (StreamRequest<State, *, *, *>) -> Unit,
  ) {
    otherStreams += OtherStream(matcher, buildStreamRegistration(requestType, itemType, contextType, handler))
  }

  @PublishedApi
  internal fun buildStreamRegistration(
    requestType: Class<*>,
    itemType: Class<*>,
    contextType: Class<*>,
    handler: (StreamRequest<State, *, *, *>) -> Unit,
  ): RegisteredStream<State> {
    return RegisteredStream(
      requestType = normalizeClass(requestType),
      itemType = normalizeClass(itemType),
      contextType = normalizeClass(contextType),
      invokeWire = { peer, path, bodyJson, contextJson, id ->
        try {
          val body = decodeRequiredJson(codec, bodyJson, normalizeClass(requestType))
          val context = decodeRequiredJson(codec, contextJson, normalizeClass(contextType))
          val task = peer.createIncomingStreamTask(id)
          try {
            val emitter = StreamEmitter<Any?>(
              emitAction = { item ->
                if (task.cancelled) {
                  throw ChannelError("Cancelled")
                }
                peer.sendMessage(
                  OutgoingMessage(
                    id = id,
                    bodyJson = if (normalizeClass(itemType) == Unit::class.java || item == Unit) null else codec.encode(item),
                  ),
                )
              },
              cancelledAction = { task.cancelled },
            )
            handler(StreamRequest(body, context, peer.sender, peer.state, path, emitter))
            if (!task.cancelled) {
              peer.sendMessage(OutgoingMessage(id = id, done = true))
            }
          } finally {
            peer.releaseIncomingStreamTask(id)
          }
        } catch (error: Throwable) {
          peer.sendMessage(OutgoingMessage(id = id, errorText = error.message ?: error.toString()))
        }
      },
    )
  }

  internal fun allSubscriptions(): Map<String, Subscription<Any?, Any?>> = subscriptions
}

class Process<State> internal constructor(
  private val peer: Peer<State>,
) : Sender<State>() {
  override fun <T> send(responseType: Class<T>, path: String, body: Any?, context: Any?): CallResult<T> =
    peer.sender.send(responseType, path, body, context)

  override fun <Item> values(itemType: Class<Item>, path: String, body: Any?, context: Any?): Values<Item> =
    peer.sender.values(itemType, path, body, context)

  override fun <Event> subscribe(eventType: Class<Event>, path: String, body: Any?, context: Any?, callback: (Event) -> Unit): SubscribeResult =
    peer.sender.subscribe(eventType, path, body, context, callback)

  override fun stop() = peer.sender.stop()

  fun isConnected(): Boolean = peer.isConnected()

  fun isRemoteSubscribed(topic: String): Boolean = peer.isRemoteSubscribed(topic)
}

class Client<State> internal constructor(
  private val peer: Peer<State>,
  private val poller: Pollable,
) : Sender<State>() {
  override fun <T> send(responseType: Class<T>, path: String, body: Any?, context: Any?): CallResult<T> =
    peer.sender.send(responseType, path, body, context)

  override fun <Item> values(itemType: Class<Item>, path: String, body: Any?, context: Any?): Values<Item> =
    peer.sender.values(itemType, path, body, context)

  override fun <Event> subscribe(eventType: Class<Event>, path: String, body: Any?, context: Any?, callback: (Event) -> Unit): SubscribeResult =
    peer.sender.subscribe(eventType, path, body, context, callback)

  override fun stop() = peer.sender.stop()

  fun poll(): Boolean = PumpRegistry.poll(poller)

  fun isConnected(): Boolean = peer.isConnected()

  fun isRemoteSubscribed(topic: String): Boolean = peer.isRemoteSubscribed(topic)
}

class Server<State> internal constructor(
  private val poller: Pollable,
  private val stopper: () -> Unit,
  private val portValue: Int,
) {
  fun poll(): Boolean = PumpRegistry.poll(poller)

  fun stop() {
    stopper()
  }

  fun port(): Int = portValue
}

internal interface Pollable {
  fun poll(): Boolean
}

internal interface Transport {
  fun sendFrame(payload: String)
  fun close()
  fun isConnected(): Boolean
}

internal object PumpRegistry {
  private val entries = mutableListOf<Pollable>()
  private var pumpingDepth = 0

  fun register(entry: Pollable) {
    entries += entry
  }

  fun unregister(entry: Pollable) {
    entries.remove(entry)
  }

  /** Polls one entry from a top-level `Server.poll()` / `Client.poll()`, counted as pumping. */
  fun poll(entry: Pollable): Boolean {
    pumpingDepth += 1
    try {
      return entry.poll()
    } finally {
      pumpingDepth -= 1
    }
  }

  /**
   * Polls every transport. Used by blocking waits (`send`, `subscribe`, `values().next()`), which
   * must make progress even when they run inside a handler that is itself being dispatched by an
   * outer poll: the reply they wait for can only arrive through a nested poll of the same
   * transports (the "mirror" pattern: client blocks in send, server handler calls the client back).
   * Nesting depth is bounded by the number of nested blocking calls.
   */
  fun pumpAll(): Boolean {
    pumpingDepth += 1
    try {
      var progressed = false
      val snapshot = entries.toList()
      snapshot.forEach { entry ->
        progressed = entry.poll() || progressed
      }
      return progressed
    } finally {
      pumpingDepth -= 1
    }
  }

  /**
   * Opportunistic pump after writing a frame. Skipped while any poll is already running: a frame
   * that batches many requests (the benchmark sends 2048 per frame) would otherwise re-enter the
   * transport for every reply and overflow the stack. The running poll picks the work up anyway.
   */
  fun pumpIfIdle(): Boolean {
    if (pumpingDepth > 0) {
      return false
    }
    return pumpAll()
  }
}

internal enum class TransportMode {
  PROCESS,
  CLIENT,
  SERVER_CONNECTION,
}

internal class Peer<State>(
  private val channel: Channel<State>,
  internal val state: State,
  private val transport: Transport,
  private val codec: JsonCodec,
  private val mode: TransportMode,
  private val onStop: (() -> Unit)? = null,
) {
  private val pending = linkedMapOf<Int, PendingEntry>()
  private val localTopics = linkedMapOf<String, MutableList<TopicCallbackEntry>>()
  private val callbackEntries = mutableListOf<TopicCallbackEntry>()
  private val remoteTopics = linkedSetOf<String>()
  private val incomingStreams = linkedMapOf<Int, IncomingStreamTask>()
  private val publishers = mutableListOf<PublisherRegistration>()
  private var nextId = 1
  private var stopped = false

  val sender: SenderImpl<State> = SenderImpl(this)

  init {
    attachPublishers()
  }

  fun stop() {
    if (stopped) {
      return
    }
    stopped = true
    transport.close()
    channel.invokeDisconnect(state, sender)
    onStop?.invoke()
    clearIncomingStreams()
    detachPublishers()
    failAllPending()
    clearCallbacks()
    remoteTopics.clear()
  }

  fun isConnected(): Boolean = !stopped && transport.isConnected()

  fun isRemoteSubscribed(topic: String): Boolean = remoteTopics.contains(topic)

  fun <T> sendTyped(responseType: Class<T>, path: String, body: Any?, context: Any?): CallResult<T> {
    val call = PendingCall(this)
    val id = nextRequestId()
    pending[id] = PendingEntry.Call(call)

    sendMessage(
      OutgoingMessage(
        id = id,
        path = path,
        bodyJson = encodeOptionalJson(codec, body),
        contextJson = encodeOptionalJson(codec, context),
      ),
    )

    val settled = waitFor { call.done || stopped }
    pending.remove(id)
    if (!settled) {
      return CallResult.RemoteErr("timeout")
    }

    val error = call.errorText ?: if (!call.done) "disconnected" else null
    if (error != null) {
      return CallResult.RemoteErr(error)
    }
    val responseBody = call.bodyJson
    if (normalizeClass(responseType) == Unit::class.java || responseBody == null) {
      return CallResult.Ok(null)
    }
    return try {
      CallResult.Ok(codec.decode(responseBody, responseType))
    } catch (error: Throwable) {
      if (error is VirtualMachineError && error !is StackOverflowError) throw error
      // An undecodable reply settles the call with an error instead of throwing out of send().
      CallResult.RemoteErr("invalid response body")
    }
  }

  fun <Item> valuesTyped(itemType: Class<Item>, path: String, body: Any?, context: Any?): Values<Item> {
    val id = nextRequestId()
    val pendingStream = PendingStream(this, id, codec)
    pending[id] = PendingEntry.Stream(pendingStream)

    sendMessage(
      OutgoingMessage(
        id = id,
        stream = path,
        bodyJson = encodeOptionalJson(codec, body),
        contextJson = encodeOptionalJson(codec, context),
      ),
    )

    return Values(pendingStream, itemType)
  }

  fun <Event> subscribeTyped(eventType: Class<Event>, path: String, body: Any?, context: Any?, callback: (Event) -> Unit): SubscribeResult {
    val pendingSubscribe = PendingSubscribe()
    val id = nextRequestId()
    pending[id] = PendingEntry.Subscribe(pendingSubscribe)

    sendMessage(
      OutgoingMessage(
        id = id,
        sub = path,
        bodyJson = encodeOptionalJson(codec, body),
        contextJson = encodeOptionalJson(codec, context),
      ),
    )

    val settled = waitFor { pendingSubscribe.done || stopped }
    pending.remove(id)
    if (!settled) {
      return SubscribeResult.RemoteErr("timeout")
    }

    val error = pendingSubscribe.errorText ?: if (!pendingSubscribe.done) "disconnected" else null
    if (error != null) {
      return SubscribeResult.RemoteErr(error)
    }

    val topic = pendingSubscribe.topic ?: return SubscribeResult.RemoteErr("subscription missing topic")
    val entry = TopicCallbackEntry(
      peer = this,
      topic = topic,
      onEvent = { json ->
        if (normalizeClass(eventType) == Unit::class.java) {
          @Suppress("UNCHECKED_CAST")
          callback(Unit as Event)
        } else {
          val raw = json ?: "null"
          callback(codec.decode(raw, eventType))
        }
      },
    )
    registerLocalCallback(entry)

    return SubscribeResult.Ok(
      SubscriptionHandle {
        removeLocalCallback(entry)
      },
    )
  }

  fun sendMessage(message: OutgoingMessage) {
    if (stopped) {
      return
    }
    transport.sendFrame(WireCodec.encodeMessage(message))
    PumpRegistry.pumpIfIdle()
  }

  fun completeResponse(id: Int, message: ParsedMessage) {
    when (val pendingEntry = pending[id] ?: return) {
      is PendingEntry.Call -> {
        pendingEntry.call.bodyJson = message.bodyJson
        pendingEntry.call.errorText = message.errorText
        pendingEntry.call.done = true
      }

      is PendingEntry.Stream -> {
        pendingEntry.stream.queue += RawStreamEvent(
          bodyJson = message.bodyJson,
          errorText = message.errorText,
          done = message.done == true,
        )
        if (message.errorText != null || message.done == true) {
          pendingEntry.stream.finished = true
          pending.remove(id)
        }
      }

      is PendingEntry.Subscribe -> {
        pendingEntry.subscribe.topic = message.topic
        pendingEntry.subscribe.errorText = message.errorText
        pendingEntry.subscribe.done = true
      }
    }
  }

  fun handleIncomingFrame(payload: String) {
    val messages = WireCodec.parseMessages(payload)
    messages.forEach { message ->
      channel.dispatchIncoming(this, message)
    }
  }

  fun removePending(id: Int) {
    pending.remove(id)
  }

  fun createIncomingStreamTask(id: Int): IncomingStreamTask {
    val task = IncomingStreamTask(id)
    incomingStreams[id] = task
    return task
  }

  fun releaseIncomingStreamTask(id: Int) {
    incomingStreams.remove(id)
  }

  fun cancelIncoming(id: Int) {
    incomingStreams[id]?.cancelled = true
  }

  fun addRemoteTopic(topic: String) {
    remoteTopics += topic
  }

  fun removeRemoteTopic(topic: String) {
    remoteTopics -= topic
  }

  fun dispatchLocalEvent(topic: String, bodyJson: String?) {
    val listeners = localTopics[topic]?.toList() ?: return
    listeners.forEach { entry ->
      if (!entry.cancelled) {
        entry.onEvent(bodyJson)
      }
    }
  }

  private fun registerLocalCallback(entry: TopicCallbackEntry) {
    callbackEntries += entry
    localTopics.getOrPut(entry.topic) { mutableListOf() }.add(entry)
  }

  private fun removeLocalCallback(entry: TopicCallbackEntry) {
    if (entry.cancelled) {
      return
    }
    entry.cancelled = true
    val values = localTopics[entry.topic]
    if (values != null) {
      values.remove(entry)
      if (values.isEmpty()) {
        localTopics.remove(entry.topic)
        sendMessage(OutgoingMessage(unsub = entry.topic))
      }
    }
    callbackEntries.remove(entry)
  }

  private fun attachPublishers() {
    channel.allSubscriptions().forEach { (_, subscription) ->
      val publisherId = subscription.addPublisher { event ->
        if (!isRemoteSubscribed(event.topic)) {
          return@addPublisher
        }
        sendMessage(
          OutgoingMessage(
            topic = event.topic,
            bodyJson = event.bodyJson,
          ),
        )
      }
      publishers += PublisherRegistration(subscription, publisherId)
    }
  }

  private fun detachPublishers() {
    publishers.forEach { registration ->
      registration.subscription.removePublisher(registration.publisherId)
    }
    publishers.clear()
  }

  private fun failAllPending() {
    pending.values.forEach { entry ->
      when (entry) {
        is PendingEntry.Call -> {
          entry.call.errorText = entry.call.errorText ?: "disconnected"
          entry.call.done = true
        }

        is PendingEntry.Stream -> {
          entry.stream.queue += RawStreamEvent(errorText = "disconnected")
          entry.stream.finished = true
        }

        is PendingEntry.Subscribe -> {
          entry.subscribe.errorText = entry.subscribe.errorText ?: "disconnected"
          entry.subscribe.done = true
        }
      }
    }
    pending.clear()
  }

  private fun clearIncomingStreams() {
    incomingStreams.values.forEach { task ->
      task.cancelled = true
    }
    incomingStreams.clear()
  }

  private fun clearCallbacks() {
    localTopics.clear()
    callbackEntries.clear()
  }

  private fun nextRequestId(): Int = nextId++

  internal val requestTimeoutMillis: Long
    get() = channel.requestTimeoutMillis

  /** Pumps until [condition] holds; returns false if the channel's request timeout elapsed first. */
  internal fun waitFor(condition: () -> Boolean): Boolean {
    val timeout = requestTimeoutMillis
    val deadline = if (timeout > 0) System.nanoTime() + timeout * 1_000_000L else 0L
    while (!condition()) {
      if (deadline != 0L && System.nanoTime() - deadline > 0) {
        return false
      }
      if (!PumpRegistry.pumpAll()) {
        LockSupport.parkNanos(100_000L)
      }
    }
    return true
  }
}

internal class SenderImpl<State>(
  private val peer: Peer<State>,
) : Sender<State>() {
  override fun <T> send(responseType: Class<T>, path: String, body: Any?, context: Any?): CallResult<T> {
    return peer.sendTyped(responseType, path, body, context)
  }

  override fun <Item> values(itemType: Class<Item>, path: String, body: Any?, context: Any?): Values<Item> {
    return peer.valuesTyped(itemType, path, body, context)
  }

  override fun <Event> subscribe(eventType: Class<Event>, path: String, body: Any?, context: Any?, callback: (Event) -> Unit): SubscribeResult {
    return peer.subscribeTyped(eventType, path, body, context, callback)
  }

  override fun stop() {
    peer.stop()
  }
}

internal class PendingStream(
  private val peer: Peer<*>,
  private val requestId: Int,
  private val codec: JsonCodec,
) {
  val queue = ArrayDeque<RawStreamEvent>()
  var finished = false
  private var closed = false

  fun <Item> next(itemType: Class<Item>): StreamNext<Item> {
    val settled = peer.waitFor { queue.isNotEmpty() || finished || closed }
    if (!settled) {
      return StreamNext.RemoteErr("timeout")
    }
    if (queue.isEmpty()) {
      return StreamNext.Done
    }

    val event = queue.removeFirst()
    if (event.errorText != null) {
      finished = true
      return StreamNext.RemoteErr(event.errorText)
    }
    if (event.done) {
      finished = true
      return StreamNext.Done
    }

    val raw = event.bodyJson ?: "null"
    return try {
      StreamNext.Item(codec.decode(raw, itemType))
    } catch (error: Throwable) {
      if (error is VirtualMachineError && error !is StackOverflowError) throw error
      StreamNext.RemoteErr("invalid stream item")
    }
  }

  fun close() {
    if (closed) {
      return
    }
    closed = true
    if (!finished) {
      peer.sendMessage(OutgoingMessage(cancel = requestId))
      peer.removePending(requestId)
    }
  }
}

internal data class RawStreamEvent(
  val bodyJson: String? = null,
  val errorText: String? = null,
  val done: Boolean = false,
)

internal class PendingCall(
  private val peer: Peer<*>,
) {
  var bodyJson: String? = null
  var errorText: String? = null
  var done = false
}

internal class PendingSubscribe {
  var topic: String? = null
  var errorText: String? = null
  var done = false
}

internal data class TopicCallbackEntry(
  val peer: Peer<*>,
  val topic: String,
  val onEvent: (String?) -> Unit,
  var cancelled: Boolean = false,
)

internal data class IncomingStreamTask(
  val id: Int,
  var cancelled: Boolean = false,
)

internal data class PublisherRegistration(
  val subscription: Subscription<Any?, Any?>,
  val publisherId: Int,
)

internal data class DirectPostResult(
  val value: Any? = null,
  val error: String? = null,
)

internal sealed interface PendingEntry {
  data class Call(val call: PendingCall) : PendingEntry

  data class Stream(val stream: PendingStream) : PendingEntry

  data class Subscribe(val subscribe: PendingSubscribe) : PendingEntry
}

internal data class RegisteredPost<State>(
  val requestType: Class<*>,
  val responseType: Class<*>,
  val contextType: Class<*>,
  val invokeWire: (Peer<State>, String, String?, String?, Int?) -> Unit,
  val invokeDirect: (Peer<State>, String, Any?, Any?) -> DirectPostResult,
)

internal data class RegisteredStream<State>(
  val requestType: Class<*>,
  val itemType: Class<*>,
  val contextType: Class<*>,
  val invokeWire: (Peer<State>, String, String?, String?, Int) -> Unit,
)

internal data class OtherPost<State>(
  val matcher: (String) -> Boolean,
  val handler: RegisteredPost<State>,
)

internal data class OtherStream<State>(
  val matcher: (String) -> Boolean,
  val handler: RegisteredStream<State>,
)

internal class ProcessTransport : Transport, Pollable {
  private val queue = ArrayDeque<String>()
  private var peer: Peer<*>? = null
  private var open = true

  fun attach(peer: Peer<*>) {
    this.peer = peer
  }

  override fun sendFrame(payload: String) {
    if (!open) {
      return
    }
    synchronized(queue) {
      queue.addLast(payload)
    }
  }

  override fun close() {
    open = false
    PumpRegistry.unregister(this)
    synchronized(queue) {
      queue.clear()
    }
  }

  override fun isConnected(): Boolean = open

  override fun poll(): Boolean {
    val localPeer = peer ?: return false
    if (!open) {
      return false
    }
    var progressed = false
    while (true) {
      val payload = synchronized(queue) {
        queue.pollFirst()
      } ?: break
      progressed = true
      localPeer.handleIncomingFrame(payload)
    }
    return progressed
  }
}

internal fun joinPath(prefix: String, path: String): String {
  if (prefix.isEmpty()) {
    return path
  }
  if (path.isEmpty()) {
    return prefix
  }
  return "$prefix/$path"
}

internal fun parseAddress(address: String, defaultHost: String = "127.0.0.1"): Pair<String, Int> {
  if (!address.contains(':')) {
    return defaultHost to address.toInt()
  }
  val parts = address.split(':', limit = 2)
  val host = parts[0].ifEmpty { defaultHost }
  val port = parts[1].toInt()
  return host to port
}
