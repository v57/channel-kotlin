package channel.quic

import channel.CallResult
import channel.ChannelTransports
import channel.StreamNext
import channel.SubscribeResult
import channel.SubscriptionHandle
import java.io.File
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.test.fail

/** The TypeScript reference's checkout next to this one (`../channel`). */
internal val referenceRoot: Path by lazy {
  var cursor = Path.of("").toAbsolutePath()
  while (cursor.parent != null) {
    if (Files.isRegularFile(cursor.resolve("channel/tests/server.ts"))) {
      return@lazy cursor.resolve("channel")
    }
    cursor = cursor.parent
  }
  error("the TypeScript reference (../channel) isn't next to this checkout (from ${File(".").absolutePath})")
}

/** The reference's test certificate: localhost, 127.0.0.1 and ::1. */
internal val testCert: String by lazy { referenceRoot.resolve("tests/quic/cert.pem").toFile().readText() }
internal val testKey: String by lazy { referenceRoot.resolve("tests/quic/key.pem").toFile().readText() }
internal const val TEST_FINGERPRINT =
  "DD:D2:37:00:BD:8A:BD:29:14:15:D8:65:84:BE:C2:80:10:FF:21:DA:A1:C5:9D:B3:4A:7F:36:63:EF:4B:55:9B"

internal fun <T> CallResult<T>.requireOk(): T? = when (this) {
  is CallResult.Ok -> value
  is CallResult.RemoteErr -> fail("expected ok result, got remote error '$error'")
}

internal fun <T> CallResult<T>.requireRemoteErr(): String = when (this) {
  is CallResult.Ok -> fail("expected a remote error, got ok '$value'")
  is CallResult.RemoteErr -> error
}

internal fun SubscribeResult.requireOk(): SubscriptionHandle = when (this) {
  is SubscribeResult.Ok -> handle
  is SubscribeResult.RemoteErr -> fail("expected subscription handle, got '$error'")
}

internal fun <T> StreamNext<T>.item(): T = when (this) {
  is StreamNext.Item -> value
  StreamNext.Done -> fail("expected an item, got done")
  is StreamNext.RemoteErr -> fail("expected an item, got remote error '$error'")
}

/** Pumps every transport in this process until [condition] holds. */
internal fun pumpUntil(timeoutMs: Long = 5_000L, message: String = "timed out", condition: () -> Boolean) {
  val deadline = System.nanoTime() + timeoutMs * 1_000_000L
  while (!condition()) {
    if (System.nanoTime() > deadline) {
      fail(message)
    }
    if (!ChannelTransports.pumpAll()) {
      Thread.sleep(1)
    }
  }
}

/** Pumps for [millis], for checks that something does *not* happen. */
internal fun pumpFor(millis: Long) {
  val deadline = System.nanoTime() + millis * 1_000_000L
  while (System.nanoTime() < deadline) {
    if (!ChannelTransports.pumpAll()) {
      Thread.sleep(1)
    }
  }
}

/**
 * The reference fixture (`channel/tests/server.ts`) on `CHANNEL_QUIC_TEST_PORT` (default 2106): QUIC on UDP and
 * WebSocket on TCP. Started here, inside the toolchain container, unless one already runs there.
 */
internal object QuicTsFixture {
  val port: Int = System.getenv("CHANNEL_QUIC_TEST_PORT")?.toIntOrNull() ?: 2106
  private var process: Process? = null
  private val logs = StringBuilder()

  @Synchronized
  fun ensureStarted() {
    if (isPortOpen()) {
      return
    }
    if (process?.isAlive != true) {
      val started = ProcessBuilder("bun", "tests/server.ts")
        .directory(referenceRoot.toFile())
        .redirectErrorStream(true)
        .apply { environment()["CHANNEL_TEST_PORT"] = port.toString() }
        .start()
      process = started
      thread(isDaemon = true, name = "channel-quic-ts-fixture") {
        started.inputStream.bufferedReader().forEachLine { line -> synchronized(logs) { logs.append(line).append('\n') } }
      }
      Runtime.getRuntime().addShutdownHook(Thread { stop() })
    }
    val deadline = System.nanoTime() + 20_000_000_000L
    while (!isPortOpen()) {
      if (System.nanoTime() > deadline || process?.isAlive != true) {
        error("the TypeScript fixture didn't start on $port\n${synchronized(logs) { logs.toString() }}")
      }
      Thread.sleep(100)
    }
  }

  @Synchronized
  fun stop() {
    val running = process ?: return
    running.destroy()
    if (!running.waitFor(2, TimeUnit.SECONDS)) {
      running.destroyForcibly()
    }
    process = null
  }

  private fun isPortOpen(): Boolean = try {
    Socket().use { socket ->
      socket.connect(InetSocketAddress("127.0.0.1", port), 100)
      true
    }
  } catch (_: Exception) {
    false
  }
}
