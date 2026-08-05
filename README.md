<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;channel
</h1>

Powerful and lightweight communication built for modern language. Kotlin implementation of Channel: run a websocket server, a websocket client, or communicate inside your process. JVM-only, single-threaded, with explicit `poll()`.

# Installation

Gradle (project `verification`, jar `channel-kotlin`):

```kotlin
implementation("verification:channel-kotlin:VERSION")
```

# Usage

```kotlin
// Server
import channel.Channel

val server = Channel<MutableMap<String, Any?>>()
  .post<Unit, String, Unit>("hello") { "world" }
  .listen(2049)

while (true) {
  server.poll()
}
```

```kotlin
// Client
import channel.CallResult
import channel.Channel

val client = Channel<MutableMap<String, Any?>>().connect(2049)

when (val result = client.send<String>("hello")) {
  is CallResult.Ok -> println(result.value)
  is CallResult.RemoteErr -> error(result.error)
}

client.stop()
```

```kotlin
// Process transport (in-memory)
import channel.CallResult
import channel.Channel

val process = Channel<MutableMap<String, Any?>>()
  .post<Int, Int, Unit>("double") { request -> request.body * 2 }
  .process()

when (val result = process.send<Int>("double", 21)) {
  is CallResult.Ok -> println(result.value)
  is CallResult.RemoteErr -> error(result.error)
}

process.stop()
```

# Design

JVM-only Channel runtime aligned to `channel-zig` semantics:
- single-threaded ownership
- explicit `poll()` on websocket client/server handles
- blocking `send()`, `values().next()`, and `subscribe()`
- manual websocket transport, no Netty, no coroutines

# Requirements

- JDK 21
- Bun
- lcov

# Layout

- `src/main/kotlin/channel`: core runtime, wire codec, websocket client/server transport
- `src/benchmark/kotlin/channel/benchmark`: benchmark workers and staged benchmark runner
- `src/test/kotlin/channel`: JVM parity + TS interop tests

# Scripts

- `scripts/test`: runs the JVM test suite, including the TS fixture interop checks
- `scripts/benchmark`: runs the staged websocket benchmark and writes `benchmark.json`
- `scripts/coverage`: runs tests and writes `coverage/lcov.info`

# Notes

- `ConnectOptions.headers` is a raw HTTP header string.
- `ListenOptions.state` receives the accepted headers as a `Map<String, String>`.
- Blocking calls pump the global single-threaded transport registry internally; `poll()` is still required for idle servers, passive subscriptions, and externally driven progress.
