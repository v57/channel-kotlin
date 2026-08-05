package channel

import com.dslplatform.json.DslJson
import com.dslplatform.json.runtime.Settings
import java.io.ByteArrayOutputStream

interface JsonCodec {
  fun encode(value: Any?): String
  fun decode(text: String): Any?
  fun <T> decode(text: String, type: Class<T>): T
}

private class DslJsonCodec : JsonCodec {
  private val dslJson = DslJson<Any>(
    Settings.withRuntime<Any>().includeServiceLoader(),
  )

  override fun encode(value: Any?): String {
    val output = ByteArrayOutputStream(256)
    dslJson.serialize(value, output)
    return output.toString(Charsets.UTF_8)
  }

  override fun decode(text: String): Any? {
    val bytes = text.toByteArray(Charsets.UTF_8)
    return dslJson.deserialize(Any::class.java, bytes, bytes.size)
  }

  override fun <T> decode(text: String, type: Class<T>): T {
    if (type == Unit::class.java) {
      @Suppress("UNCHECKED_CAST")
      return Unit as T
    }

    val normalized = normalizeClass(type)
    val bytes = text.toByteArray(Charsets.UTF_8)

    @Suppress("UNCHECKED_CAST")
    return when {
      normalized == Any::class.java || normalized == Object::class.java -> dslJson.deserialize(Any::class.java, bytes, bytes.size) as T
      Map::class.java.isAssignableFrom(normalized) -> dslJson.deserialize(Any::class.java, bytes, bytes.size) as T
      List::class.java.isAssignableFrom(normalized) -> dslJson.deserialize(Any::class.java, bytes, bytes.size) as T
      else -> dslJson.deserialize(normalized as Class<T>, bytes, bytes.size) as T
    }
  }
}

fun defaultJsonCodec(): JsonCodec = DslJsonCodec()

internal fun normalizeClass(type: Class<*>): Class<*> = when (type) {
  java.lang.Integer.TYPE -> java.lang.Integer::class.java
  java.lang.Long.TYPE -> java.lang.Long::class.java
  java.lang.Boolean.TYPE -> java.lang.Boolean::class.java
  java.lang.Double.TYPE -> java.lang.Double::class.java
  java.lang.Float.TYPE -> java.lang.Float::class.java
  java.lang.Short.TYPE -> java.lang.Short::class.java
  java.lang.Byte.TYPE -> java.lang.Byte::class.java
  java.lang.Character.TYPE -> java.lang.Character::class.java
  else -> type
}

internal fun encodeOptionalJson(codec: JsonCodec, value: Any?): String? {
  if (value == null || value == Unit) {
    return null
  }
  return codec.encode(value)
}

internal fun <T> decodeRequiredJson(codec: JsonCodec, json: String?, type: Class<T>): T {
  if (normalizeClass(type) == Unit::class.java) {
    @Suppress("UNCHECKED_CAST")
    return Unit as T
  }

  if (json == null) {
    throw ChannelError("missing required value for ${type.simpleName}")
  }
  return codec.decode(json, type)
}

@PublishedApi
internal inline fun <reified T> typeClass(): Class<T> {
  @Suppress("UNCHECKED_CAST")
  return when (T::class) {
    Int::class -> java.lang.Integer::class.java
    Long::class -> java.lang.Long::class.java
    Boolean::class -> java.lang.Boolean::class.java
    Double::class -> java.lang.Double::class.java
    Float::class -> java.lang.Float::class.java
    Short::class -> java.lang.Short::class.java
    Byte::class -> java.lang.Byte::class.java
    Unit::class -> Unit::class.java
    else -> T::class.java
  } as Class<T>
}

internal fun dynamicClass(value: Any?): Class<*> = when (value) {
  null,
  Unit,
  is Unit -> Unit::class.java
  is Int -> java.lang.Integer::class.java
  is Long -> java.lang.Long::class.java
  is Boolean -> java.lang.Boolean::class.java
  is Double -> java.lang.Double::class.java
  is Float -> java.lang.Float::class.java
  is Short -> java.lang.Short::class.java
  is Byte -> java.lang.Byte::class.java
  else -> value::class.java
}
