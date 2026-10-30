package channel.quic

import channel.Channel
import channel.Client
import channel.StreamNext
import channel.Subscription
import channel.defaultJsonCodec
import org.junit.jupiter.api.Timeout
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull

/**
 * The Kotlin QUIC client against the TypeScript reference's fixture (channel/tests/server.ts), which serves its
 * routes over QUIC on UDP [QuicTsFixture.port] next to WebSocket on TCP.
 */
@Timeout(120)
class QuicInteropTest {
  private val clients = mutableListOf<Client<State>>()
  private val names = Subscription<String, String>()

  @BeforeTest
  fun startFixture() {
    QuicTsFixture.ensureStarted()
  }

  @AfterTest
  fun stopClients() {
    clients.forEach { it.stop() }
  }

  private fun connect(headers: Map<String, String> = mapOf("X-Port" to "kotlin"), options: QuicConnectOptions<State>? = null): Client<State> {
    val client = Channel<State>()
      .post<Unit, String, Unit>("hello") { "client world" }
      .stream<Unit, Int, Unit>("stream/values") { request -> repeat(3) { request.emitter.emit(it) } }
      .events(mapOf("names" to names))
      .connectQuic("quic://127.0.0.1:${QuicTsFixture.port}", options ?: QuicConnectOptions(ca = testCert, headers = headers))
    clients += client
    return client
  }

  @Test
  fun postsStateAndHeaders() {
    val client = connect()
    assertEquals("world", client.send<String>("hello").requireOk())
    val body = mapOf("a" to listOf(1, "b"))
    assertEquals(defaultJsonCodec().decode(defaultJsonCodec().encode(body)), client.send<Map<String, Any?>>("echo", body).requireOk())
    assertNull(client.send<Unit>("empty").requireOk())
    assertEquals("api not found", client.send<String>("notfound").requireRemoteErr())
    assertEquals("client world", client.send<String>("mirror").requireOk())
    assertEquals("quic", client.send<String>("auth", mapOf("name" to "quic")).requireOk())
    assertEquals("quic", client.send<String>("auth/name").requireOk())
    assertEquals("unauthorized", connect().send<String>("auth/name").requireRemoteErr())
    val headers = client.send<Map<String, Any?>>("v2/headers").requireOk()
    assertEquals("kotlin", headers?.get("x-port"))
    assertEquals("slow down", client.send<String>("v2/error/code").requireRemoteErr())
  }

  @Test
  fun streamsAndEvents() {
    val client = connect()
    client.values<Int>("stream/values").use { values ->
      assertEquals(listOf(0, 1, 2), List(3) { values.next().item() })
      assertEquals(StreamNext.Done, values.next())
    }
    // The fixture streams this client's stream/values back
    client.values<Int>("mirror/stream").use { values ->
      assertEquals(listOf(0, 1, 2), List(3) { values.next().item() })
      assertEquals(StreamNext.Done, values.next())
    }
    // The fixture subscribes to this client's names/1 and cancels after the first event
    client.send<Unit>("mirror/events").requireOk()
    pumpUntil { client.isRemoteSubscribed("names/1") }
    names.send("1", "h")
    pumpUntil { !client.isRemoteSubscribed("names/1") }
  }

  @Test
  fun trustAndDrops() {
    val pinned = connect(options = QuicConnectOptions(fingerprint = TEST_FINGERPRINT))
    assertEquals("world", pinned.send<String>("hello").requireOk())
    val byName = connect(options = QuicConnectOptions(ca = testCert, servername = "localhost"))
    assertEquals("world", byName.send<String>("hello").requireOk())

    val client = connect()
    client.send<Unit>("v2/drops/reset").requireOk()
    // The fixture closes the connection without answering
    assertEquals("disconnected", client.send<String>("v2/drop").requireRemoteErr())
    assertFalse(client.isConnected())
    assertEquals(1L, connect().send<Long>("v2/drops").requireOk())
  }
}
