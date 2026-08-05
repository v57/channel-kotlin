package channel.benchmark

import channel.CallResult
import channel.Channel
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.LockSupport
import kotlin.math.max
import kotlin.system.exitProcess

private const val CONNECT_TIMEOUT_MS = 120_000L

private data class Totals(
  val requestsSent: AtomicLong = AtomicLong(0),
  val responsesReceived: AtomicLong = AtomicLong(0),
  val errors: AtomicLong = AtomicLong(0),
)

fun main() {
  val host = envOrDefault("BENCH_HOST", "127.0.0.1")
  val port = requiredIntEnv("BENCH_PORT")
  val clientsCount = requiredIntEnv("BENCH_CLIENTS")
  val totalRequestsRaw = System.getenv("BENCH_TOTAL_REQUESTS")
  val totalRequests = when {
    totalRequestsRaw == null -> null
    else -> totalRequestsRaw.trim().toLongOrNull() ?: fail("BENCH_TOTAL_REQUESTS must be a valid non-negative integer")
  }
  if (totalRequests != null && totalRequests < 0L) {
    fail("BENCH_TOTAL_REQUESTS must be a valid non-negative integer")
  }

  val requestMode = envOrDefault("BENCH_REQUEST_MODE", "burst").trim().lowercase()
  if (requestMode != "burst" && requestMode != "stream") {
    fail("BENCH_REQUEST_MODE must be either burst or stream")
  }

  val maxInFlight = optionalLongEnv("BENCH_MAX_IN_FLIGHT", 4096L)
  if (totalRequests != null && maxInFlight <= 0L) {
    fail("BENCH_MAX_IN_FLIGHT must be a valid positive integer")
  }

  val runDurationMs = if (totalRequests == null) optionalLongEnv("BENCH_DURATION_MS", 0L) else 0L
  val targetRpsPerClient = if (totalRequests == null) requiredDoubleEnv("BENCH_RPS_PER_CLIENT") else 0.0
  val totalTargetRps = targetRpsPerClient * clientsCount.toDouble()
  val sendIntervalNanos = if (totalRequests == null && totalTargetRps > 0.0) {
    max(1L, (1_000_000_000.0 / totalTargetRps).toLong())
  } else {
    0L
  }

  val channel = Channel<BenchState>()
  val address = "ws://$host:$port/"
  val connectDeadline = System.nanoTime() + CONNECT_TIMEOUT_MS * 1_000_000L
  val clients = MutableList(clientsCount) {
    connectWithRetry(channel, address, connectDeadline)
  }
  val totals = Totals()
  val stopped = AtomicBoolean(false)
  val startedAt = System.nanoTime()

  fun shutdownAndEmit() {
    if (!stopped.compareAndSet(false, true)) {
      return
    }
    clients.forEach { client ->
      runCatching { client.stop() }
    }
    emitOut(
      mapOf(
        "type" to "result",
        "pid" to ProcessHandle.current().pid(),
        "requestsSent" to totals.requestsSent.get(),
        "responsesReceived" to totals.responsesReceived.get(),
        "errors" to totals.errors.get(),
        "elapsedSeconds" to ((System.nanoTime() - startedAt) / 1_000_000_000.0),
      ),
    )
  }

  Runtime.getRuntime().addShutdownHook(Thread(::shutdownAndEmit))

  emitOut(
    mapOf(
      "type" to "ready",
      "pid" to ProcessHandle.current().pid(),
      "host" to host,
      "port" to port,
      "clients" to clientsCount,
    ),
  )

  if (totalRequests == 0L) {
    shutdownAndEmit()
    exitProcess(0)
  }

  try {
    var nextClient = 0
    var nextSendAt = System.nanoTime()
    while (!stopped.get()) {
      if (runDurationMs > 0L && (System.nanoTime() - startedAt) >= runDurationMs * 1_000_000L) {
        break
      }
      if (totalRequests != null && totals.requestsSent.get() >= totalRequests) {
        break
      }

      if (sendIntervalNanos > 0L) {
        val now = System.nanoTime()
        if (now < nextSendAt) {
          LockSupport.parkNanos(nextSendAt - now)
          continue
        }
        nextSendAt = max(nextSendAt + sendIntervalNanos, now)
      }

      val client = clients[nextClient]
      nextClient = (nextClient + 1) % clients.size
      totals.requestsSent.incrementAndGet()

      when (val result = client.send<Int>("text/count", textCountBody)) {
        is CallResult.Ok -> {
          if (result.value == textCountBody.text.length) {
            totals.responsesReceived.incrementAndGet()
          } else {
            totals.errors.incrementAndGet()
          }
        }

        is CallResult.RemoteErr -> totals.errors.incrementAndGet()
      }
    }
  } finally {
    shutdownAndEmit()
  }
}

private fun connectWithRetry(
  channel: Channel<BenchState>,
  address: String,
  deadlineNanos: Long,
): channel.Client<BenchState> {
  var lastError: Throwable? = null
  while (System.nanoTime() < deadlineNanos) {
    try {
      return channel.connect(address)
    } catch (error: Throwable) {
      lastError = error
      LockSupport.parkNanos(10_000_000L)
    }
  }
  fail("Timed out waiting to connect to $address${lastError?.message?.let { ": $it" } ?: ""}")
}
