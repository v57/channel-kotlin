package channel.benchmark

import channel.defaultJsonCodec

class PlainBeanProbe {
  var text: String = ""
}

class PlainBeanNoDefault {
  var text: String = "x"
}

fun main() {
  val codec = defaultJsonCodec()
  val cases = listOf(
    "map" to { codec.decode("{\"text\":\"hello\"}", Map::class.java) },
    "plainBean" to { codec.decode("{\"text\":\"hello\"}", PlainBeanProbe::class.java) },
    "dataClass" to { codec.decode("{\"text\":\"hello\"}", TextCountBody::class.java) },
  )
  for ((name, fn) in cases) {
    try {
      val v = fn()
      println("$name OK: $v (${v?.javaClass?.name})")
    } catch (t: Throwable) {
      println("$name FAIL: ${t.message}")
    }
  }
  // encode side
  try {
    println("encode dataClass: " + codec.encode(TextCountBody("hello")))
  } catch (t: Throwable) {
    println("encode dataClass FAIL: ${t.message}")
  }
}
