package channel.benchmark

import channel.defaultJsonCodec

fun main() {
    val codec = defaultJsonCodec()
    println("decode result: " + codec.decode("{\"text\":\"hello\"}", TextCountBody::class.java))
}
