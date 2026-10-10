package channel

import java.lang.StringBuilder

internal data class ParsedMessage(
  val id: Int? = null,
  val path: String? = null,
  val stream: String? = null,
  val sub: String? = null,
  val unsub: String? = null,
  val cancel: Int? = null,
  val topic: String? = null,
  val bodyJson: String? = null,
  val contextJson: String? = null,
  val errorText: String? = null,
  val done: Boolean? = null,
)

internal data class OutgoingMessage(
  val id: Int? = null,
  val path: String? = null,
  val stream: String? = null,
  val sub: String? = null,
  val unsub: String? = null,
  val cancel: Int? = null,
  val topic: String? = null,
  val bodyJson: String? = null,
  val contextJson: String? = null,
  val errorText: String? = null,
  val done: Boolean? = null,
)

internal object WireCodec {
  fun parseMessages(raw: String): List<ParsedMessage> {
    val parser = JsonEnvelopeParser(raw)
    return parser.parse()
  }

  fun encodeMessage(message: OutgoingMessage): String = encodeMessages(listOf(message))

  fun encodeMessages(messages: List<OutgoingMessage>): String {
    if (messages.size == 1) {
      return buildObject(messages[0])
    }

    val out = StringBuilder(messages.size * 64)
    out.append('[')
    messages.forEachIndexed { index, message ->
      if (index != 0) {
        out.append(',')
      }
      out.append(buildObject(message))
    }
    out.append(']')
    return out.toString()
  }

  private fun buildObject(message: OutgoingMessage): String {
    val out = StringBuilder(128)
    out.append('{')
    var fieldCount = 0

    fun prefix(name: String) {
      if (fieldCount != 0) {
        out.append(',')
      }
      fieldCount += 1
      writeJsonString(out, name)
      out.append(':')
    }

    message.id?.let {
      prefix("id")
      out.append(it)
    }
    message.path?.let {
      prefix("path")
      writeJsonString(out, it)
    }
    message.stream?.let {
      prefix("stream")
      writeJsonString(out, it)
    }
    message.sub?.let {
      prefix("sub")
      writeJsonString(out, it)
    }
    message.unsub?.let {
      prefix("unsub")
      writeJsonString(out, it)
    }
    message.cancel?.let {
      prefix("cancel")
      out.append(it)
    }
    message.topic?.let {
      prefix("topic")
      writeJsonString(out, it)
    }
    message.bodyJson?.let {
      prefix("body")
      out.append(it)
    }
    message.contextJson?.let {
      prefix("context")
      out.append(it)
    }
    message.errorText?.let {
      prefix("error")
      writeJsonString(out, it)
    }
    message.done?.let {
      prefix("done")
      out.append(if (it) "true" else "false")
    }

    out.append('}')
    return out.toString()
  }
}

internal fun writeJsonString(builder: StringBuilder, value: String) {
  builder.append('"')
  value.forEach { ch ->
    when (ch) {
      '\\' -> builder.append("\\\\")
      '"' -> builder.append("\\\"")
      '\b' -> builder.append("\\b")
      '\u000C' -> builder.append("\\f")
      '\n' -> builder.append("\\n")
      '\r' -> builder.append("\\r")
      '\t' -> builder.append("\\t")
      else -> {
        if (ch.code < 0x20) {
          builder.append("\\u")
          builder.append(ch.code.toString(16).padStart(4, '0'))
        } else {
          builder.append(ch)
        }
      }
    }
  }
  builder.append('"')
}

private class JsonEnvelopeParser(private val input: String) {
  private var index = 0

  fun parse(): List<ParsedMessage> {
    skipWhitespace()
    if (index >= input.length) {
      throw ChannelError("empty json payload")
    }

    return when (input[index]) {
      '{' -> listOf(parseObject())
      '[' -> parseArray()
      else -> throw ChannelError("invalid json payload")
    }
  }

  private fun parseArray(): List<ParsedMessage> {
    expect('[')
    skipWhitespace()
    val messages = mutableListOf<ParsedMessage>()
    if (peek(']')) {
      index += 1
      return messages
    }

    while (true) {
      messages += parseObject()
      skipWhitespace()
      when {
        peek(',') -> {
          index += 1
          skipWhitespace()
        }

        peek(']') -> {
          index += 1
          return messages
        }

        else -> throw ChannelError("invalid json array")
      }
    }
  }

  private fun parseObject(): ParsedMessage {
    expect('{')
    skipWhitespace()

    var id: Int? = null
    var path: String? = null
    var stream: String? = null
    var sub: String? = null
    var unsub: String? = null
    var cancel: Int? = null
    var topic: String? = null
    var bodyJson: String? = null
    var contextJson: String? = null
    var errorText: String? = null
    var done: Boolean? = null

    if (peek('}')) {
      index += 1
      return ParsedMessage()
    }

    while (true) {
      val field = parseString()
      skipWhitespace()
      expect(':')
      skipWhitespace()
      when (field) {
        "id" -> id = parseInt()
        "path" -> path = parseNullableString()
        "stream" -> stream = parseNullableString()
        "sub" -> sub = parseNullableString()
        "unsub" -> unsub = parseNullableString()
        "cancel" -> cancel = parseInt()
        "topic" -> topic = parseNullableString()
        "body" -> bodyJson = parseRawValue()
        "context" -> contextJson = parseRawValue()
        // A non-string error (e.g. an object from a non-conforming peer) is kept as its raw JSON text.
        "error" -> errorText = if (peek('"') || input.startsWith("null", index)) parseNullableString() else parseRawValue()
        "done" -> done = parseBoolean()
        else -> parseRawValue()
      }

      skipWhitespace()
      when {
        peek(',') -> {
          index += 1
          skipWhitespace()
        }

        peek('}') -> {
          index += 1
          return ParsedMessage(
            id = id,
            path = path,
            stream = stream,
            sub = sub,
            unsub = unsub,
            cancel = cancel,
            topic = topic,
            bodyJson = bodyJson,
            contextJson = contextJson,
            errorText = errorText,
            done = done,
          )
        }

        else -> throw ChannelError("invalid json object")
      }
    }
  }

  private fun parseNullableString(): String? {
    if (matchLiteral("null")) {
      return null
    }
    return parseString()
  }

  private fun parseInt(): Int {
    val start = index
    if (peek('-')) {
      index += 1
    }
    while (index < input.length && input[index].isDigit()) {
      index += 1
    }
    if (start == index) {
      throw ChannelError("invalid integer")
    }
    return input.substring(start, index).toInt()
  }

  private fun parseBoolean(): Boolean {
    return when {
      matchLiteral("true") -> true
      matchLiteral("false") -> false
      else -> throw ChannelError("invalid boolean")
    }
  }

  private fun parseString(): String {
    expect('"')
    val out = StringBuilder()
    while (index < input.length) {
      val ch = input[index++]
      when (ch) {
        '"' -> return out.toString()
        '\\' -> {
          val escaped = input.getOrNull(index++) ?: throw ChannelError("invalid escape")
          when (escaped) {
            '"', '\\', '/' -> out.append(escaped)
            'b' -> out.append('\b')
            'f' -> out.append('\u000C')
            'n' -> out.append('\n')
            'r' -> out.append('\r')
            't' -> out.append('\t')
            'u' -> {
              val hex = input.substring(index, index + 4)
              out.append(hex.toInt(16).toChar())
              index += 4
            }

            else -> throw ChannelError("invalid escape")
          }
        }

        else -> out.append(ch)
      }
    }
    throw ChannelError("unterminated string")
  }

  private fun parseRawValue(): String? {
    val start = index
    when (input.getOrNull(index)) {
      '{' -> {
        index += 1
        skipNested('{', '}')
      }

      '[' -> {
        index += 1
        skipNested('[', ']')
      }

      '"' -> {
        index += 1
        while (index < input.length) {
          val ch = input[index++]
          if (ch == '\\') {
            index += 1
            if (input.getOrNull(index - 1) == 'u') {
              index += 4
            }
            continue
          }
          if (ch == '"') {
            break
          }
        }
      }

      't' -> matchLiteral("true")
      'f' -> matchLiteral("false")
      'n' -> {
        matchLiteral("null")
        return null
      }

      else -> {
        while (index < input.length) {
          val ch = input[index]
          if (ch == ',' || ch == '}' || ch == ']' || ch.isWhitespace()) {
            break
          }
          index += 1
        }
      }
    }

    return input.substring(start, index)
  }

  private fun skipNested(open: Char, close: Char) {
    var depth = 1
    while (index < input.length) {
      when (val ch = input[index++]) {
        '"' -> skipStringContents()
        open -> depth += 1
        close -> {
          depth -= 1
          if (depth == 0) {
            return
          }
        }
      }
    }
    throw ChannelError("unterminated json structure")
  }

  private fun skipStringContents() {
    while (index < input.length) {
      val ch = input[index++]
      if (ch == '\\') {
        val escaped = input.getOrNull(index++) ?: throw ChannelError("invalid escape")
        if (escaped == 'u') {
          index += 4
        }
        continue
      }
      if (ch == '"') {
        return
      }
    }
    throw ChannelError("unterminated string")
  }

  private fun matchLiteral(value: String): Boolean {
    if (!input.regionMatches(index, value, 0, value.length)) {
      return false
    }
    index += value.length
    return true
  }

  private fun skipWhitespace() {
    while (index < input.length && input[index].isWhitespace()) {
      index += 1
    }
  }

  private fun expect(ch: Char) {
    if (!peek(ch)) {
      throw ChannelError("expected '$ch'")
    }
    index += 1
  }

  private fun peek(ch: Char): Boolean = index < input.length && input[index] == ch
}
