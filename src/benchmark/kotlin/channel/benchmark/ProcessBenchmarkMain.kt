package channel.benchmark

import channel.CallResult
import channel.Channel
import kotlin.system.exitProcess

fun main() {
  val totalRequestsRaw = System.getenv("BENCH_TOTAL_REQUESTS")
  val totalRequests = when {
    totalRequestsRaw.isNullOrBlank() -> null
    else -> totalRequestsRaw.trim().toLongOrNull() ?: fail("BENCH_TOTAL_REQUESTS must be a valid non-negative integer")
  }
  if (totalRequests != null && totalRequests < 0L) {
    fail("BENCH_TOTAL_REQUESTS must be a valid non-negative integer")
  }

  val durationMs = if (totalRequests == null) requiredPositiveLongEnv("BENCH_DURATION_MS") else 0L

  val sender = Channel<BenchState>()
    .post<TextCountBody, Int, Unit>("text/count") { request ->
      request.body.text.length
    }
    .process(linkedMapOf())

  emitOut(
    mapOf(
      "type" to "ready",
      "pid" to ProcessHandle.current().pid(),
    ),
  )

  try {
    var completed = 0L
    var errors = 0L
    val startedAt = System.nanoTime()

    if (totalRequests != null) {
      while ((completed + errors) < totalRequests) {
        when (val result = sender.send<Int>("text/count", textCountBody)) {
          is CallResult.Ok -> {
            if (result.value == textCountBody.text.length) {
              completed += 1
            } else {
              errors += 1
            }
          }

          is CallResult.RemoteErr -> errors += 1
        }
      }

      emitOut(
        mapOf(
          "type" to "result",
          "pid" to ProcessHandle.current().pid(),
          "completed" to completed,
          "requestsSent" to (completed + errors),
          "responsesReceived" to completed,
          "errors" to errors,
          "elapsedSeconds" to ((System.nanoTime() - startedAt) / 1_000_000_000.0),
        ),
      )
      exitProcess(0)
    }

    val deadline = startedAt + durationMs * 1_000_000L
    while (System.nanoTime() < deadline) {
      when (val result = sender.send<Int>("text/count", textCountBody)) {
        is CallResult.Ok -> {
          if (result.value == textCountBody.text.length) {
            completed += 1
          } else {
            errors += 1
          }
        }

        is CallResult.RemoteErr -> errors += 1
      }
    }

    emitOut(
      mapOf(
        "type" to "result",
        "pid" to ProcessHandle.current().pid(),
        "completed" to completed,
        "requestsSent" to (completed + errors),
        "responsesReceived" to completed,
        "errors" to errors,
        "elapsedSeconds" to ((System.nanoTime() - startedAt) / 1_000_000_000.0),
      ),
    )
  } finally {
    sender.stop()
  }
}
