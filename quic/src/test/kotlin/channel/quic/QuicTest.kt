package channel.quic

import channel.Channel
import channel.ChannelError
import channel.Client
import channel.Server
import channel.StreamNext
import channel.Subscription
import channel.defaultJsonCodec
import org.junit.jupiter.api.Timeout
import tech.kwik.core.QuicClientConnection
import tech.kwik.core.QuicConnection.QuicVersion
import tech.kwik.core.QuicStream
import tech.kwik.core.log.NullLogger
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.concurrent.thread
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

typealias State = MutableMap<String, Any?>

@Timeout(120)
class QuicTest {
  private val servers = mutableListOf<Server<State>>()
  private val clients = mutableListOf<Client<State>>()
  private val raw = mutableListOf<QuicClientConnection>()
  private val ticks = Subscription<String, String>()
  private var connected = 0
  private var disconnected = 0

  @AfterTest
  fun cleanup() {
    clients.forEach { it.stop() }
    raw.forEach { runCatching { it.close() } }
    servers.forEach { it.stop() }
  }

  private fun serverChannel(): Channel<State> = Channel<State>()
    .post<Unit, String, Unit>("hello") { "world" }
    .post<Any?, Any?, Unit>("echo") { request -> request.body }
    .post<Unit, String, Unit>("name") { request -> request.state["x-name"] as String? }
    .post<Unit, String, Unit>("mirror") { request -> request.sender.send<String>("hello").requireOk() }
    .post<Unit, String, Unit>("disconnect") { request ->
      request.sender.stop()
      "dropped"
    }
    .stream<Unit, Int, Unit>("count") { request -> repeat(3) { request.emitter.emit(it) } }
    .post<Unit, Unit, Unit>("tick") { ticks.send("a", "tick") }
    .events(mapOf("ticks" to ticks))

  private fun listen(address: String = "127.0.0.1:0"): Server<State> {
    val server = serverChannel().listenQuic(
      address,
      QuicListenOptions(
        key = testKey,
        cert = testCert,
        state = { headers -> headers.toMutableMap<String, Any?>() },
        onConnect = { _, _ -> connected += 1 },
        onDisconnect = { _, _ -> disconnected += 1 },
      ),
    )
    servers += server
    return server
  }

  private fun clientChannel(): Channel<State> = Channel<State>().post<Unit, String, Unit>("hello") { "client world" }

  private fun connect(port: Int, options: QuicConnectOptions<State> = QuicConnectOptions(ca = testCert)): Client<State> {
    val client = clientChannel().connectQuic("quic://127.0.0.1:$port", options)
    clients += client
    return client
  }

  @Test
  fun clientAndServer() {
    val server = listen()
    val client = connect(server.port(), QuicConnectOptions(ca = testCert, headers = mapOf("X-Name" to "kotlin")))
    pumpUntil { connected == 1 }

    assertEquals("world", client.send<String>("hello").requireOk())
    val codec = defaultJsonCodec()
    val body = mapOf("list" to listOf(1, "two", null), "text" to "é".repeat(20_000))
    assertEquals(codec.decode(codec.encode(body)), client.send<Map<String, Any?>>("echo", body).requireOk())

    // hello headers become the state, names lowercased
    assertEquals("kotlin", client.send<String>("name").requireOk())
    // the server calls the client back
    assertEquals("client world", client.send<String>("mirror").requireOk())

    client.values<Int>("count").use { values ->
      assertEquals(listOf(0, 1, 2), List(3) { values.next().item() })
      assertEquals(StreamNext.Done, values.next())
    }

    var events = 0
    val subscription = client.subscribe<String>("ticks", "a") { events += 1 }.requireOk()
    ticks.send("a", "tick")
    ticks.send("b", "not for us")
    client.send<Unit>("tick").requireOk()
    pumpUntil { events == 2 }
    subscription.cancel()
    pumpFor(50)
    ticks.send("a", "after cancel")
    pumpFor(100)
    assertEquals(2, events)

    repeat(2_000) { index ->
      assertEquals(index.toLong(), client.send<Long>("echo", index).requireOk())
    }

    client.stop()
    pumpUntil { disconnected == 1 }
    assertFalse(client.isConnected())
    assertEquals("disconnected", client.send<String>("hello").requireRemoteErr())
  }

  @Test
  fun certificateTrust() {
    val port = listen().port()

    val pinned = connect(port, QuicConnectOptions(fingerprint = TEST_FINGERPRINT.replace(":", "").lowercase()))
    assertEquals("world", pinned.send<String>("hello").requireOk())
    val byName = connect(port, QuicConnectOptions(ca = testCert, servername = "localhost"))
    assertEquals("world", byName.send<String>("hello").requireOk())
    val ipv6 = listen("[::1]:0")
    val overIpv6 = clientChannel().connectQuic("quic://[::1]:${ipv6.port()}", QuicConnectOptions(ca = testCert))
    clients += overIpv6
    assertEquals("world", overIpv6.send<String>("hello").requireOk())

    val refused = mapOf(
      "system roots" to QuicConnectOptions<State>(),
      "wrong pin" to QuicConnectOptions(fingerprint = "00".repeat(32)),
      "name not in the certificate" to QuicConnectOptions(ca = testCert, servername = "example.com"),
    )
    refused.forEach { (name, options) ->
      assertFailsWith<ChannelError>(name) { connect(port, options.copy(connectTimeoutMillis = 3_000)) }
    }
    assertFailsWith<ChannelError> { connect(port, QuicConnectOptions(fingerprint = "nope")) }
    assertFailsWith<ChannelError> { clientChannel().connectQuic("ws://127.0.0.1:$port") }
    assertFailsWith<ChannelError> { clientChannel().connect("quic://127.0.0.1:$port") }
  }

  @Test
  fun eachConnectionHasItsOwnUdpSocket() {
    val port = listen().port()
    val first = connect(port)
    val second = connect(port)
    assertEquals("world", first.send<String>("hello").requireOk())
    assertEquals("world", second.send<String>("hello").requireOk())
    pumpUntil { connected == 2 }
  }

  @Test
  fun rawFramesAndKeepalives() {
    val port = listen().port()
    val (_, stream) = rawSession(port)
    val hello = defaultJsonCodec().encode(mapOf("hello" to mapOf("X-Name" to "raw", "ignored" to 1)))
    stream.outputStream.write(
      encodeFrame(hello) + KEEPALIVE_FRAME + encodeFrame("""[{"id":1,"path":"name"},{"id":2,"path":"hello"}]"""),
    )
    stream.outputStream.flush()
    val frames = readFrames(stream)
    val bodies = mutableMapOf<Any?, Any?>()
    pumpUntil {
      frames.poll()?.let { text ->
        val message = defaultJsonCodec().decode(text) as Map<*, *>
        bodies[message["id"].toString()] = message["body"]
      }
      bodies.size == 2
    }
    assertEquals(mapOf<Any?, Any?>("1" to "raw", "2" to "world"), bodies)
  }

  @Test
  fun oneStreamPerConnection() {
    val port = listen().port()
    val (connection, stream) = rawSession(port)
    stream.outputStream.write(encodeFrame("""{"hello":{}}"""))
    stream.outputStream.flush()
    pumpUntil { connected == 1 }
    val executor = Executors.newCachedThreadPool { runnable -> Thread(runnable).apply { isDaemon = true } }
    for (bidirectional in listOf(true, false)) {
      val attempt = executor.submit<QuicStream> { connection.createStream(bidirectional) }
      assertFailsWith<TimeoutException>("bidirectional=$bidirectional: another stream opened") {
        attempt.get(1, TimeUnit.SECONDS)
      }
      attempt.cancel(true)
    }
    executor.shutdownNow()
  }

  @Test
  fun badFirstFramesAndOversizeClose() {
    val port = listen().port()
    val cases = mapOf(
      "a first frame that isn't a hello" to encodeFrame("""{"id":1,"path":"hello"}"""),
      "invalid JSON" to encodeFrame("""{"hello":{}}""") + encodeFrame("{nope"),
      "invalid JSON in the hello" to encodeFrame("{nope"),
      "a frame over the limit" to encodeFrame("""{"hello":{}}""") + byteArrayOf(0x81.toByte(), 0, 0, 1),
    )
    cases.forEach { (name, bytes) ->
      val (connection, stream) = rawSession(port)
      val closed = CountDownLatch(1)
      connection.setConnectionListener { closed.countDown() }
      stream.outputStream.write(bytes)
      stream.outputStream.flush()
      pumpUntil(message = "$name: still open") { closed.count == 0L }
    }
    // The server still serves
    assertEquals("world", connect(port).send<String>("hello").requireOk())
  }

  @Test
  fun serverDropsAndStopsFailPendingCalls() {
    var server = listen()
    val port = server.port()
    val client = connect(port)
    assertEquals("world", client.send<String>("hello").requireOk())

    // There is no reconnect in this port (as with its WebSocket client): a dropped connection fails the call
    assertEquals("disconnected", client.send<String>("disconnect").requireRemoteErr())
    assertFalse(client.isConnected())
    pumpUntil { disconnected == 1 }

    // A server that goes away disconnects its clients
    val second = connect(port)
    assertEquals("world", second.send<String>("hello").requireOk())
    server.stop()
    pumpUntil(10_000) { !second.isConnected() }
    assertEquals("disconnected", second.send<String>("hello").requireRemoteErr())

    // The port can be served again
    server = listen("127.0.0.1:$port")
    assertEquals("world", connect(port).send<String>("hello").requireOk())
    server.stop()

    // Nothing listens: connect fails within its timeout
    assertFailsWith<ChannelError> { connect(port, QuicConnectOptions(ca = testCert, connectTimeoutMillis = 1_000)) }
  }

  @Test
  fun stateErrorsCloseTheConnection() {
    val server = serverChannel().listenQuic(
      0,
      QuicListenOptions<State>(key = testKey, cert = testCert, state = { throw ChannelError("refused") }),
    )
    servers += server
    val client = connect(server.port())
    assertEquals("disconnected", client.send<String>("hello").requireRemoteErr())
  }

  @Test
  fun typeScriptReferenceClient() {
    val server = listen()
    val script = java.io.File("testdata/client.ts").absoluteFile
    val process = ProcessBuilder("bun", script.path, server.port().toString(), referenceRoot.resolve("tests/quic/cert.pem").toString())
      .start()
    val output = StringBuilder()
    val errors = StringBuilder()
    val reader = thread(isDaemon = true) { output.append(process.inputStream.bufferedReader().readText()) }
    val errorReader = thread(isDaemon = true) { errors.append(process.errorStream.bufferedReader().readText()) }
    try {
      pumpUntil(30_000, "the TypeScript client didn't finish") { !process.isAlive }
    } catch (error: Throwable) {
      process.destroyForcibly()
      errorReader.join(2_000)
      throw AssertionError("${error.message}\nstdout: $output\nstderr: $errors", error)
    }
    reader.join(5_000)
    errorReader.join(5_000)
    assertEquals(0, process.exitValue(), "bun exited with ${process.exitValue()}: $output\n$errors")
    val expected = """{"hello":"world","echo":{"a":[1,"b"]},"name":"ts","mirror":"ts world","values":[0,1,2],"event":"tick"}"""
    assertEquals(defaultJsonCodec().decode(expected), defaultJsonCodec().decode(output.toString().trim()))
    pumpUntil { connected == 1 && disconnected == 1 }
  }

  /** Dials the server without the Channel client, to check the wire rules. */
  private fun rawSession(port: Int): Pair<QuicClientConnection, QuicStream> {
    val connection = QuicClientConnection.newBuilder()
      .version(QuicVersion.V1)
      .applicationProtocol(ALPN)
      .host("127.0.0.1")
      .port(port)
      .noServerCertificateCheck()
      .maxIdleTimeout(Duration.ofSeconds(30))
      .logger(NullLogger())
      .build()
    raw += connection
    connection.connect()
    return connection to connection.createStream(true)
  }

  /** Every frame the server sends on [stream], read on another thread. */
  private fun readFrames(stream: QuicStream): LinkedBlockingQueue<String> {
    val frames = LinkedBlockingQueue<String>()
    thread(isDaemon = true) {
      runCatching {
        val reader = FrameReader(stream.inputStream)
        while (true) {
          frames.add((reader.next() ?: break).toString(Charsets.UTF_8))
        }
      }
    }
    return frames
  }
}
