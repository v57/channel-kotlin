package channel.benchmark

import channel.CallResult
import channel.Channel
import java.io.File
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.nio.file.Path
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.LockSupport

private const val DEFAULT_STAGE_DURATION_MS = 5_000L
private const val DEFAULT_TARGET_RPS_PER_CLIENT = 1_000
private const val READY_TIMEOUT_MS = 120_000L
private const val STOP_TIMEOUT_MS = 5_000L
private const val EXIT_GRACE_MS = 2_000L
private const val SHUTDOWN_TIMEOUT_MS = 10_000L
private const val BENCHMARK_PATH = "benchmark.json"
private val STAGE_CLIENTS = listOf(1, 10, 100, 1000)
private const val SERVER_MAIN_CLASS = "channel.benchmark.ServerProcessMainKt"
private const val CLIENT_MAIN_CLASS = "channel.benchmark.ClientProcessMainKt"

private data class ProcessUsage(
  val cpu: Double,
  val mem: Double,
)

private data class StageResult(
  val rps: Double,
  val clients: Int,
  val client: ProcessUsage,
  val server: ProcessUsage,
)

fun main() {
  val stageDurationMs = optionalLongEnv("BENCH_STAGE_MS", DEFAULT_STAGE_DURATION_MS)
  val targetRpsPerClient = optionalIntEnv("BENCH_RPS_PER_CLIENT", DEFAULT_TARGET_RPS_PER_CLIENT)
  val results = mutableListOf<StageResult>()

  stageClients().forEach { clients ->
    results += runStage(clients, stageDurationMs, targetRpsPerClient)
  }

  val output = resolveOutputPath()
  writeBenchmark(output, results)
  println("benchmark written to ${output.toAbsolutePath()}")
}

private fun runStage(
  clients: Int,
  stageDurationMs: Long,
  targetRpsPerClient: Int,
): StageResult {
  val port = randomPort()
  val server = ManagedWorker.start(
    name = "server",
    mainClass = SERVER_MAIN_CLASS,
    env = mapOf(
      "BENCH_PORT" to port.toString(),
    ),
  )
  val client = ManagedWorker.start(
    name = "client",
    mainClass = CLIENT_MAIN_CLASS,
    env = mapOf(
      "BENCH_HOST" to "127.0.0.1",
      "BENCH_PORT" to port.toString(),
      "BENCH_CLIENTS" to clients.toString(),
      "BENCH_RPS_PER_CLIENT" to targetRpsPerClient.toString(),
      "BENCH_DURATION_MS" to stageDurationMs.toString(),
    ),
  )

  try {
    server.awaitJsonType("ready", READY_TIMEOUT_MS)
    client.awaitJsonType("ready", READY_TIMEOUT_MS)

    Thread.sleep(stageDurationMs)

    val clientUsage = sampleUsage(client.process.pid())
    val serverUsage = sampleUsage(server.process.pid())

    client.awaitJsonType("result", READY_TIMEOUT_MS)
    requestServerShutdown(port)
    val serverResult = server.awaitJsonType("result", READY_TIMEOUT_MS)
    val handledRequests = asLong(serverResult["requests"])
    val rps = round2(handledRequests.toDouble() / (stageDurationMs.toDouble() / 1000.0))

    client.awaitExit(STOP_TIMEOUT_MS)
    server.awaitExit(STOP_TIMEOUT_MS)

    return StageResult(
      rps = rps,
      clients = clients,
      client = clientUsage,
      server = serverUsage,
    )
  } finally {
    client.stopForcibly()
    server.stopForcibly()
  }
}

private fun writeBenchmark(output: Path, results: List<StageResult>) {
  val payload = linkedMapOf<String, Any?>(
    "language" to "kotlin",
    "results" to results.map { result ->
      linkedMapOf(
        "rps" to result.rps,
        "clients" to result.clients,
        "client" to linkedMapOf(
          "cpu" to result.client.cpu,
          "mem" to result.client.mem,
        ),
        "server" to linkedMapOf(
          "cpu" to result.server.cpu,
          "mem" to result.server.mem,
        ),
      )
    },
  )
  File(output.toUri()).writeText(benchmarkJson.encode(payload))
}

private fun requestServerShutdown(port: Int) {
  val deadline = System.nanoTime() + SHUTDOWN_TIMEOUT_MS * 1_000_000L
  while (System.nanoTime() < deadline) {
    try {
      val client = Channel<BenchState>().connect("ws://127.0.0.1:$port/")
      try {
        when (val result = client.send<String>("bench/shutdown")) {
          is CallResult.Ok -> return
          is CallResult.RemoteErr -> Unit
        }
      } finally {
        client.stop()
      }
    } catch (error: Throwable) {
      // The server may already be exiting; the result message is the authoritative signal.
    }
    LockSupport.parkNanos(10_000_000L)
  }
}

private fun stageClients(): List<Int> {
  val raw = System.getenv("BENCH_STAGE_CLIENTS")?.trim().orEmpty()
  if (raw.isEmpty()) {
    return STAGE_CLIENTS
  }

  val parsed = raw
    .split(',')
    .map { token -> token.trim() }
    .filter { it.isNotEmpty() }
    .map { token ->
      token.toIntOrNull()?.takeIf { it > 0 } ?: fail("BENCH_STAGE_CLIENTS must be a comma-separated list of positive integers")
    }
  if (parsed.isEmpty()) {
    fail("BENCH_STAGE_CLIENTS must include at least one stage")
  }
  return parsed
}

private fun resolveOutputPath(): Path = Path.of("").toAbsolutePath().normalize().resolve(BENCHMARK_PATH)

private fun randomPort(): Int {
  ServerSocket().use { socket ->
    socket.bind(InetSocketAddress("127.0.0.1", 0))
    return socket.localPort
  }
}

private fun sampleUsage(pid: Long): ProcessUsage {
  return try {
    val process = ProcessBuilder(
      "ps",
      "-p",
      pid.toString(),
      "-o",
      "%cpu=",
      "-o",
      "rss=",
    )
      .redirectErrorStream(false)
      .start()
    val stdout = process.inputStream.bufferedReader().use { it.readText() }
    process.waitFor(2, TimeUnit.SECONDS)
    val parts = stdout.trim().split(Regex("\\s+"))
    val cpu = parts.getOrNull(0)?.toDoubleOrNull() ?: 0.0
    val rssKb = parts.getOrNull(1)?.toDoubleOrNull() ?: 0.0
    ProcessUsage(cpu = round2(cpu), mem = round2(rssKb / 1024.0))
  } catch (_: Throwable) {
    ProcessUsage(cpu = 0.0, mem = 0.0)
  }
}

private class ManagedWorker private constructor(
  private val name: String,
  val process: Process,
  private val messages: LinkedBlockingQueue<Map<String, Any?>>,
) {
  companion object {
    fun start(
      name: String,
      mainClass: String,
      env: Map<String, String>,
    ): ManagedWorker {
      val javaBin = File(
        File(System.getProperty("java.home"), "bin"),
        if (System.getProperty("os.name").lowercase().contains("win")) "java.exe" else "java",
      ).absolutePath
      val classpath = System.getProperty("java.class.path")
      val process = ProcessBuilder(
        javaBin,
        "-cp",
        classpath,
        mainClass,
      )
        .directory(File(System.getProperty("user.dir")))
        .apply {
          environment().putAll(env)
        }
        .start()

      val messages = LinkedBlockingQueue<Map<String, Any?>>()

      Thread({
        process.inputStream.bufferedReader().useLines { lines ->
          lines.forEach { line ->
            parseLine(line)?.let { messages.offer(it) }
          }
        }
      }, "benchmark-$name-stdout").apply {
        isDaemon = true
        start()
      }

      Thread({
        process.errorStream.bufferedReader().useLines { lines ->
          lines.forEach { line ->
            if (line.isNotBlank()) {
              System.err.println("[$name] $line")
            }
          }
        }
      }, "benchmark-$name-stderr").apply {
        isDaemon = true
        start()
      }

      return ManagedWorker(name, process, messages)
    }
  }

  fun awaitJsonType(type: String, timeoutMs: Long): Map<String, Any?> {
    val deadline = System.nanoTime() + timeoutMs * 1_000_000L
    var exitedAt: Long? = null
    while (System.nanoTime() < deadline) {
      val message = messages.poll(100, TimeUnit.MILLISECONDS)
      if (message != null) {
        when (message["type"]) {
          "error" -> fail(message["message"]?.toString() ?: "worker error")
          type -> return message
        }
      }
      if (!process.isAlive) {
        if (exitedAt == null) {
          exitedAt = System.nanoTime()
        }
        if (System.nanoTime() - exitedAt > EXIT_GRACE_MS * 1_000_000L) {
          fail("$name exited with code ${process.exitValue()} before '$type'")
        }
      }
    }
    fail("Timed out waiting for $name message '$type'")
  }

  fun stopGracefully(timeoutMs: Long) {
    runCatching { process.destroy() }
    if (!process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
      stopForcibly()
    }
  }

  fun stopForcibly() {
    if (!process.isAlive) {
      return
    }
    runCatching { process.destroyForcibly() }
  }

  fun awaitExit(timeoutMs: Long) {
    if (!process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
      stopForcibly()
    }
  }
}
