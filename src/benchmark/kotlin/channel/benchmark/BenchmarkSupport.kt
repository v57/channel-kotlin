package channel.benchmark

import channel.defaultJsonCodec
import java.io.FileDescriptor
import java.io.FileOutputStream
import java.nio.charset.StandardCharsets
import kotlin.math.round
import kotlin.system.exitProcess

internal typealias BenchState = MutableMap<String, Any?>

/**
 * Benchmark request body.
 *
 * NOTE: must be `open` with non-final getters/setters — the websocket server
 * decodes request bodies with a runtime reflection codec (dsl-json 1.10
 * `Settings.withRuntime()`), whose ObjectAnalyzer rejects `final` methods
 * (canWrite checks `modifiers & FINAL`). Kotlin data-class/`var` properties
 * compile to final setters, so dsl-json saw zero properties, encoded `{}`,
 * and every cross-language request to the kotlin server failed with
 * "Unable to create an instance" / "Expecting '}' while decoding".
 */
open class TextCountBody {
  open var text: String = ""

  constructor()

  constructor(text: String) {
    this.text = text
  }
}

internal val benchmarkJson = defaultJsonCodec()
internal val textCountBody = TextCountBody("hello")

internal fun emitOut(payload: Map<String, Any?>) {
  emit(payload, FileDescriptor.out)
}

internal fun emitErr(payload: Map<String, Any?>) {
  emit(payload, FileDescriptor.err)
}

internal fun fail(message: String): Nothing {
  emitErr(
    mapOf(
      "type" to "error",
      "message" to message,
    ),
  )
  exitProcess(1)
}

internal fun requiredIntEnv(name: String): Int {
  val raw = System.getenv(name) ?: fail("$name must be a valid positive integer")
  val value = raw.trim().toIntOrNull() ?: fail("$name must be a valid positive integer")
  if (value <= 0) {
    fail("$name must be a valid positive integer")
  }
  return value
}

internal fun requiredPositiveLongEnv(name: String): Long {
  val raw = System.getenv(name) ?: fail("$name must be a valid positive integer")
  val value = raw.trim().toLongOrNull() ?: fail("$name must be a valid positive integer")
  if (value <= 0L) {
    fail("$name must be a valid positive integer")
  }
  return value
}

internal fun requiredDoubleEnv(name: String): Double {
  val raw = System.getenv(name) ?: fail("$name must be a positive number")
  val value = raw.trim().toDoubleOrNull() ?: fail("$name must be a positive number")
  if (!value.isFinite() || value <= 0.0) {
    fail("$name must be a positive number")
  }
  return value
}

internal fun optionalLongEnv(name: String, fallback: Long): Long {
  val raw = System.getenv(name) ?: return fallback
  return raw.trim().toLongOrNull() ?: fallback
}

internal fun optionalIntEnv(name: String, fallback: Int): Int {
  val raw = System.getenv(name) ?: return fallback
  return raw.trim().toIntOrNull() ?: fallback
}

internal fun optionalBooleanEnv(name: String, fallback: Boolean): Boolean {
  val raw = System.getenv(name) ?: return fallback
  return when (raw.trim().lowercase()) {
    "1", "true", "yes", "on" -> true
    "0", "false", "no", "off" -> false
    else -> fallback
  }
}

internal fun envOrDefault(name: String, fallback: String): String {
  val raw = System.getenv(name) ?: return fallback
  return raw.ifEmpty { fallback }
}

internal fun round2(value: Double): Double = round(value * 100.0) / 100.0

internal fun parseLine(line: String): Map<String, Any?>? {
  val trimmed = line.trim()
  if (trimmed.isEmpty()) {
    return null
  }
  @Suppress("UNCHECKED_CAST")
  return runCatching { benchmarkJson.decode(trimmed) as? Map<String, Any?> }.getOrNull()
}

internal fun asLong(value: Any?): Long = when (value) {
  is Long -> value
  is Int -> value.toLong()
  is Number -> value.toLong()
  is String -> value.toLongOrNull() ?: 0L
  else -> 0L
}

internal fun asDouble(value: Any?): Double = when (value) {
  is Double -> value
  is Float -> value.toDouble()
  is Number -> value.toDouble()
  is String -> value.toDoubleOrNull() ?: 0.0
  else -> 0.0
}

private fun emit(payload: Map<String, Any?>, descriptor: FileDescriptor) {
  runCatching {
    val out = FileOutputStream(descriptor)
    out.write((benchmarkJson.encode(payload) + System.lineSeparator()).toByteArray(StandardCharsets.UTF_8))
    out.flush()
  }
}
