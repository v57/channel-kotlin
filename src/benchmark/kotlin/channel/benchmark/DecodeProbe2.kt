package channel.benchmark
import channel.defaultJsonCodec
fun main() {
    val codec = defaultJsonCodec()
    val r = codec.decode("{\"text\":\"hello\"}", TextCountBody::class.java)
    println("DECODED: ${(r as TextCountBody).text}")
}
