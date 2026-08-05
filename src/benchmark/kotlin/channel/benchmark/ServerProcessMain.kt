package channel.benchmark

import channel.CallResult
import channel.Channel
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.LockSupport

private const val DEFAULT_WARMUP_ENABLED = true
private const val DEFAULT_WARMUP_REQUESTS = 200_000L

fun main() {
  val port = requiredIntEnv("BENCH_PORT")
  val runDurationMs = optionalLongEnv("BENCH_DURATION_MS", 0L)
  val requests = AtomicLong(0)
  val emitted = AtomicBoolean(false)
  val stopRequested = AtomicBoolean(false)

  val channel = Channel<BenchState>()
    .post<TextCountBody, Int, Unit>("text/count") { request ->
      requests.incrementAndGet()
      request.body.text.length
    }
    .post<Unit, String, Unit>("bench/shutdown") {
      stopRequested.set(true)
      "ok"
    }

  runWarmup(channel)
  requests.set(0)

  val server = channel.listen(port)
  val startedAt = System.nanoTime()

  fun shutdownAndEmit() {
    stopRequested.set(true)
    if (!emitted.compareAndSet(false, true)) {
      return
    }
    runCatching { server.stop() }
    emitOut(
      mapOf(
        "type" to "result",
        "pid" to ProcessHandle.current().pid(),
        "requests" to requests.get(),
        "elapsedSeconds" to ((System.nanoTime() - startedAt) / 1_000_000_000.0),
      ),
    )
  }

  Runtime.getRuntime().addShutdownHook(Thread(::shutdownAndEmit))

  emitOut(
    mapOf(
      "type" to "ready",
      "pid" to ProcessHandle.current().pid(),
      "port" to server.port(),
    ),
  )

  try {
    val deadline = if (runDurationMs > 0L) System.nanoTime() + runDurationMs * 1_000_000L else Long.MAX_VALUE
    while (!stopRequested.get()) {
      if (System.nanoTime() >= deadline) {
        break
      }
      if (!server.poll()) {
        LockSupport.parkNanos(100_000L)
      }
    }
  } finally {
    shutdownAndEmit()
  }
}

private fun runWarmup(channel: Channel<BenchState>) {
  if (!optionalBooleanEnv("BENCH_WARMUP_ENABLED", DEFAULT_WARMUP_ENABLED)) {
    return
  }

  val warmupRequests = optionalLongEnv("BENCH_WARMUP_REQUESTS", DEFAULT_WARMUP_REQUESTS)
  if (warmupRequests <= 0L) {
    return
  }

  val process = channel.process(linkedMapOf())
  try {
    var index = 0L
    while (index < warmupRequests) {
      when (val result = process.send<Int>("text/count", textCountBody)) {
        is CallResult.Ok -> {
          if (result.value != textCountBody.text.length) {
            fail("Warmup returned invalid response")
          }
        }

        is CallResult.RemoteErr -> fail(result.error)
      }
      index += 1
    }
  } finally {
    process.stop()
  }
}
