package channel

import org.junit.jupiter.api.Timeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CoreParityTest {
  @Test
  fun processParityCoversPostsStreamsSubscriptionsMergeAndDisconnect() {
    var disconnects = 0
    val updates = Subscription<String, String>()
    val merged = Channel<MutableMap<String, Any?>>()
      .post<Unit, String, Unit>("hello") { "merged" }

    val channel = Channel<MutableMap<String, Any?>>()
      .post<Unit, String, Unit>("hello") { "world" }
      .post<Int, Int, Unit>("echo") { request -> request.body }
      .postOther<Unit, String, Unit>({ path -> path.startsWith("other/post/") }) { request -> request.path }
      .stream<Unit, Int, Unit>("numbers") { request ->
        request.emitter.emit(1)
        request.emitter.emit(2)
      }
      .streamOther<Unit, Int, Unit>({ path -> path.startsWith("other/stream/") }) { request ->
        request.emitter.emit(7)
        request.emitter.emit(8)
      }
      .events(mapOf("updates" to updates))
      .merge(merged, "merged")
      .onDisconnect { _, _ -> disconnects += 1 }

    val process = channel.process(mutableMapOf())

    assertEquals("world", process.send<String>("hello").requireOk())
    assertEquals(42, process.send<Int>("echo", 42).requireOk())
    assertEquals("other/post/demo", process.send<String>("other/post/demo").requireOk())
    assertEquals("merged", process.send<String>("merged/hello").requireOk())

    process.values<Int>("numbers").use { values ->
      values.next().requireItem(1)
      values.next().requireItem(2)
      values.next().requireDone()
    }

    process.values<Int>("other/stream/demo").use { values ->
      values.next().requireItem(7)
      values.next().requireItem(8)
      values.next().requireDone()
    }

    val received = mutableListOf<String>()
    val subscription = process.subscribe<String>("updates", "room-1") { event ->
      received += event
    }.requireOk()
    updates.send("room-1", "hello")
    waitUntil { received == listOf("hello") }
    subscription.cancel()

    process.stop()
    assertEquals(1, disconnects)
  }

  @Test
  fun mergeKeepsPostOtherAndStreamOtherRoutesUnderThePrefix() {
    val inner = Channel<MutableMap<String, Any?>>()
      .postOther<Unit, String, Unit>({ path -> path.startsWith("dyn/") }) { request -> "post:${request.path}" }
      .streamOther<Unit, Int, Unit>({ path -> path == "feed" }) { request -> request.emitter.emit(1) }

    val outer = Channel<MutableMap<String, Any?>>().merge(inner, "m").process(mutableMapOf())
    assertEquals("post:m/dyn/x", outer.send<String>("m/dyn/x").requireOk())
    assertTrue(outer.send<String>("dyn/x") is CallResult.RemoteErr)
    outer.values<Int>("m/feed").use { values ->
      values.next().requireItem(1)
      values.next().requireDone()
    }
    outer.stop()

    // Without a prefix the matchers are reused as-is
    val flat = Channel<MutableMap<String, Any?>>().merge(inner).process(mutableMapOf())
    assertEquals("post:dyn/x", flat.send<String>("dyn/x").requireOk())
    flat.stop()
  }

  @Test
  @Timeout(60) // used to deadlock forever (AUDIT 3.2): nested calls from a handler must not hang the suite
  fun websocketClientAndServerSupportNestedCallsEventsAndHeaders() {
    var mirroredEvents = 0
    var disconnects = 0
    var subscribedHandle: SubscriptionHandle? = null

    val serverUpdates = Subscription<String, String>()
    val merged = Channel<MutableMap<String, Any?>>()
      .post<Unit, String, Unit>("hello") { "merged" }

    val serverChannel = Channel<MutableMap<String, Any?>>()
      .post<Unit, Boolean, Unit>("state/check") { request ->
        request.state["headerWins"] == true
      }
      .post<Unit, String, Unit>("mirror") { request ->
        request.sender.send<String>("hello").requireOk()
      }
      .post<Unit, String, Unit>("subscribe-client") { request ->
        subscribedHandle = request.sender.subscribe<String>("names", "1") {
          mirroredEvents += 1
        }.requireOk()
        "subscribed"
      }
      .post<Unit, String, Unit>("unsubscribe-client") {
        subscribedHandle?.cancel()
        subscribedHandle = null
        "unsubscribed"
      }
      .postOther<Unit, String, Unit>({ path -> path.startsWith("other/post/") }) { request -> request.path }
      .streamOther<Unit, Int, Unit>({ path -> path.startsWith("other/stream/") }) { request ->
        request.emitter.emit(7)
        request.emitter.emit(8)
      }
      .api(
        mapOf(
          "api" to group(
            mapOf(
              "hello" to postSpec<MutableMap<String, Any?>, Unit, String, Unit> { "api" },
              "numbers" to streamSpec<MutableMap<String, Any?>, Unit, Int, Unit> { request ->
                request.emitter.emit(9)
              },
            ),
          ),
        ),
      )
      .events(mapOf("updates" to serverUpdates))
      .merge(merged, "merged")
      .onDisconnect { _, _ -> disconnects += 1 }

    val server = serverChannel.listen(
      0,
      ListenOptions(
        state = { headers ->
          mutableMapOf("headerWins" to (headers["name"] == "header"))
        },
      ),
    )

    val clientNames = Subscription<String, String>()
    val clientChannel = Channel<MutableMap<String, Any?>>()
      .post<Unit, String, Unit>("hello") { "client-world" }
      .events(mapOf("names" to clientNames))

    val client = clientChannel.connect(
      "ws://127.0.0.1:${server.port()}/?name=query",
      ConnectOptions(headers = "name: header\r\n"),
    )

    try {
      assertEquals(true, client.send<Boolean>("state/check").requireOk())
      assertEquals("other/post/demo", client.send<String>("other/post/demo").requireOk())
      assertEquals("api", client.send<String>("api/hello").requireOk())
      assertEquals("merged", client.send<String>("merged/hello").requireOk())
      assertEquals("client-world", client.send<String>("mirror").requireOk())

      client.values<Int>("other/stream/demo").use { values ->
        values.next().requireItem(7)
        values.next().requireItem(8)
        values.next().requireDone()
      }

      client.values<Int>("api/numbers").use { values ->
        values.next().requireItem(9)
        values.next().requireDone()
      }

      val serverEvents = mutableListOf<String>()
      val updatesHandle = client.subscribe<String>("updates", "1") { event ->
        serverEvents += event
      }.requireOk()
      serverUpdates.send("1", "hello")
      waitUntil {
        server.poll() || client.poll()
        serverEvents == listOf("hello")
      }
      updatesHandle.cancel()

      assertEquals("subscribed", client.send<String>("subscribe-client").requireOk())
      waitUntil {
        server.poll() || client.poll()
        client.isRemoteSubscribed("names/1")
      }
      assertTrue(client.isRemoteSubscribed("names/1"))

      clientNames.send("1", "event-1")
      waitUntil {
        server.poll() || client.poll()
        mirroredEvents == 1
      }

      assertEquals("unsubscribed", client.send<String>("unsubscribe-client").requireOk())
      waitUntil {
        server.poll() || client.poll()
        !client.isRemoteSubscribed("names/1")
      }
      assertFalse(client.isRemoteSubscribed("names/1"))
    } finally {
      client.stop()
      waitUntil {
        server.poll()
        disconnects == 1
      }
      server.stop()
    }
  }

  @Test
  fun websocketSerialSendsWorkWithoutWriterThread() {
    val serverChannel = Channel<MutableMap<String, Any?>>()
      .post<Int, Int, Unit>("echo") { request -> request.body }
    val server = serverChannel.listen(0)
    val client = Channel<MutableMap<String, Any?>>().connect("ws://127.0.0.1:${server.port()}/")

    try {
      repeat(256) { index ->
        assertEquals(index, client.send<Int>("echo", index).requireOk())
      }
    } finally {
      client.stop()
      server.stop()
    }
  }
}
