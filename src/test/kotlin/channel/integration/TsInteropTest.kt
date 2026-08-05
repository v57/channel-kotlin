package channel.integration

import channel.CallResult
import channel.Channel
import channel.ConnectOptions
import channel.StreamNext
import channel.Subscription
import channel.requireDone
import channel.requireItem
import channel.requireOk
import channel.requireRemoteErr
import channel.waitUntil
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TsInteropTest {
  private val clients = mutableListOf<channel.Client<MutableMap<String, Any?>>>()

  @BeforeTest
  fun startFixture() {
    TsFixture.ensureStarted()
  }

  @AfterTest
  fun stopClients() {
    clients.forEach { it.stop() }
    clients.clear()
  }

  @Test
  fun helloEchoEmptyAndErrors() {
    val client = createClient()

    assertEquals("world", client.send<String>("hello").requireOk())
    assertEquals(42.5, client.send<Double>("echo", 42.5).requireOk())
    assertNull(client.send<Unit>("empty").requireOk())
    client.send<String>("notfound").requireRemoteErr("api not found")
  }

  @Test
  fun streamValuesAndStreamCancel() {
    val client = createClient()

    client.values<Int>("stream/values").use { values ->
      values.next().requireItem(0)
      values.next().requireItem(1)
      values.next().requireItem(2)
      values.next().requireDone()
    }

    client.values<Int>("stream/cancel").use { values ->
      values.next().requireItem(0)
      values.next().requireItem(1)
      values.next().requireItem(2)
    }
  }

  @Test
  fun mirrorPostAndStreamsCallBackIntoKotlinClient() {
    val client = createClient()

    assertEquals("client world", client.send<String>("mirror").requireOk())

    client.values<Int>("mirror/stream").use { values ->
      values.next().requireItem(0)
      values.next().requireItem(1)
      values.next().requireItem(2)
      values.next().requireDone()
    }

    client.values<Int>("mirror/stream/cancel").use { values ->
      values.next().requireItem(0)
      values.next().requireItem(1)
      values.next().requireItem(2)
    }
  }

  @Test
  fun mirrorEventsUpdatesRemoteSubscriptionState() {
    val client = createClient()

    val result = client.send<Unit>("mirror/events")
    when (result) {
      is CallResult.Ok -> Unit
      is CallResult.RemoteErr -> error(result.error)
    }

    waitUntil {
      client.poll()
      client.isRemoteSubscribed("names/1")
    }
    assertTrue(client.isRemoteSubscribed("names/1"))

    clientNames.send("1", "hello")

    waitUntil {
      client.poll()
      !client.isRemoteSubscribed("names/1")
    }
  }

  @Test
  fun authStateAndUnauthorizedResponses() {
    val client = createClient()
    assertEquals("tester", client.send<String>("auth", mapOf("name" to "tester")).requireOk())
    assertEquals("tester", client.send<String>("auth/name").requireOk())

    val unauthorized = Channel<MutableMap<String, Any?>>().connect(2049)
    clients += unauthorized
    unauthorized.send<String>("auth/name").requireRemoteErr("unauthorized")
  }

  private val clientNames = Subscription<String, String>()

  private fun createClient(): channel.Client<MutableMap<String, Any?>> {
    val client = Channel<MutableMap<String, Any?>>()
      .post<Unit, String, Unit>("hello") { "client world" }
      .stream<Unit, Int, Unit>("stream/values") { request ->
        repeat(3) { index ->
          request.emitter.emit(index)
        }
      }
      .stream<Unit, Int, Unit>("stream/cancel") { request ->
        repeat(10) { index ->
          if (request.emitter.isCancelled()) {
            return@stream
          }
          request.emitter.emit(index)
          Thread.sleep(1)
        }
      }
      .events(mapOf("names" to clientNames))
      .connect(2049, ConnectOptions())

    clients += client
    return client
  }
}
