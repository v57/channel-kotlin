package channel.integration

import java.io.File
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import kotlin.concurrent.thread

internal object TsFixture {
  private val lock = Any()
  private var process: Process? = null
  private val logs = StringBuilder()

  fun ensureStarted() {
    synchronized(lock) {
      if (isPortOpen()) {
        return
      }

      val existing = process
      if (existing != null && existing.isAlive) {
        waitForPort()
        return
      }

      logs.clear()
      val root = locateWorkspaceRoot()
      val builder = ProcessBuilder("bun", "tests/server.ts")
        .directory(root.resolve("channel").toFile())
        .redirectErrorStream(true)

      val started = builder.start()
      process = started

      thread(name = "channel-kotlin-ts-fixture", isDaemon = true) {
        started.inputStream.bufferedReader(StandardCharsets.UTF_8).useLines { lines ->
          lines.forEach { line ->
            synchronized(logs) {
              logs.append(line).append('\n')
            }
          }
        }
      }

      waitForPort()
    }
  }

  fun stop() {
    synchronized(lock) {
      val running = process ?: return
      if (running.isAlive) {
        running.destroy()
        if (!running.waitFor(2, java.util.concurrent.TimeUnit.SECONDS)) {
          running.destroyForcibly()
          running.waitFor()
        }
      }
      process = null
    }
  }

  private fun waitForPort() {
    val deadline = System.nanoTime() + 20_000_000_000L
    while (System.nanoTime() < deadline) {
      if (isPortOpen()) {
        return
      }
      Thread.sleep(100)
    }
    error("Timed out waiting for TS fixture on 127.0.0.1:2049\n${snapshotLogs()}")
  }

  private fun isPortOpen(): Boolean {
    return try {
      Socket().use { socket ->
        socket.connect(InetSocketAddress("127.0.0.1", 2049), 100)
        true
      }
    } catch (_: Throwable) {
      false
    }
  }

  private fun snapshotLogs(): String = synchronized(logs) { logs.toString() }

  private fun locateWorkspaceRoot(): Path {
    var cursor = Path.of("").toAbsolutePath()
    while (cursor.parent != null) {
      if (Files.isDirectory(cursor.resolve("channel")) && Files.isRegularFile(cursor.resolve("channel/tests/server.ts"))) {
        return cursor
      }
      cursor = cursor.parent
    }
    error("Unable to locate workspace root from ${File(".").absolutePath}")
  }
}
