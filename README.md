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

# Dependencies

[dsl-json](https://github.com/ngs-doo/dsl-json) (`dsl-json-java8`) for JSON, plus the Kotlin standard library and
`kotlin-reflect`. The WebSocket client and server are written on Java NIO. The optional `channel-kotlin-quic` module
adds [kwik](https://github.com/ptrd/kwik) (`tech.kwik:kwik`, pure Java, with its TLS library `agent15` and two small
helpers).

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

# QUIC transport (experimental)

The same messages, requests, streams and events over QUIC instead of WebSocket (wire format: `channel/docs/quic.md`
in the TypeScript reference). It lives in its own Gradle module and jar, `channel-kotlin-quic`, so a WebSocket-only
install pulls no QUIC library:

```kotlin
implementation("verification:channel-kotlin:VERSION")
implementation("verification:channel-kotlin-quic:VERSION") // only for QUIC
```

It runs on [kwik](https://github.com/ptrd/kwik) (`tech.kwik:kwik`, pure Java, client and server, no native code).
Netty's `codec-quic` was the other option; it would bring Netty's event loops and a native quiche build per
platform into a port that is single-threaded NIO without Netty. JVM only, like the rest of this port.

```kotlin
import channel.quic.QuicConnectOptions
import channel.quic.QuicListenOptions
import channel.quic.connectQuic
import channel.quic.listenQuic

// Server: QUIC needs a key and certificate (PEM). listenQuic(4433, …) binds 127.0.0.1, like listen(port).
val server = Channel<MutableMap<String, Any?>>()
  .post<Unit, String, Unit>("hello") { "world" }
  .listenQuic("0.0.0.0:4433", QuicListenOptions(key = File("key.pem").readText(), cert = File("cert.pem").readText()))
while (true) server.poll()

// Client: blocks until the connection is open, throws ChannelError when it can't be
val client = Channel<MutableMap<String, Any?>>()
  .connectQuic("quic://example.com:4433", QuicConnectOptions(headers = mapOf("Authorization" to "…")))
```

`QuicListenOptions` takes the same `state`, `onConnect`, `onDisconnect` and `maxPayload` as `ListenOptions`; `state`
receives the client's hello headers with lowercased names. `QuicConnectOptions.headers` is a map (sent in the first
frame, where the WebSocket client sends upgrade headers). `connect("quic://…")` without the module fails with a
hint to use `connectQuic`.

Certificates (`QuicConnectOptions`):
- nothing set: the server certificate must chain to the system roots and match the address host;
- `ca`: a PEM certificate (or CA) to trust, for self-signed servers. The host must be in its subject alternative
  names, as a DNS name or an IP address;
- `fingerprint`: the certificate's SHA-256 fingerprint (hex, colons optional) to pin; nothing else is checked then;
- `servername`: the name sent (SNI) and checked instead of the address host.

Each client connection dials from its own UDP socket, never a shared one: Network.framework servers (HubServer)
take one connection per client address and port.

Behaviour: ALPN `channel/1`, 0-RTT refused, one bidirectional stream per connection (the server allows no other
stream), 30 s idle timeout, a keepalive from the client every 15 s, 16 MiB frame limit. The server speaks QUIC v1 and
also accepts v2, which Bun's client switches to through compatible version negotiation. As with this port's
WebSocket client there is no reconnect: a dropped connection fails pending calls with `"disconnected"`.

Languages with QUIC: TypeScript, Go, Swift, C#, Rust, Python, Elixir, Java, Kotlin, C and Zig; Dart, Ruby and PHP stay
on WebSocket (libraries and status in `channel/docs/quic.md`).

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
- `src/main/kotlin/channel/TransportApi.kt`: the opt-in API (`@ChannelTransportApi`) transport modules plug into
- `quic/`: the `channel-kotlin-quic` module (kwik), its tests and `testdata/client.ts` (the TS reference's QUIC client)

# Scripts

- `scripts/test`: runs the JVM test suites of both modules, including the TS fixture interop checks (WebSocket on
  2049; QUIC on `CHANNEL_QUIC_TEST_PORT`, default 2106, started by the tests when nothing runs there)
- `scripts/benchmark`: runs the staged websocket benchmark and writes `benchmark.json`
- `scripts/coverage`: runs tests and writes `coverage/lcov.info`

# Notes

- `ConnectOptions.headers` is a raw HTTP header string.
- `ListenOptions.state` receives the accepted headers as a `Map<String, String>`.
- Blocking calls pump the global single-threaded transport registry internally; `poll()` is still required for idle servers, passive subscriptions, and externally driven progress.
- Blocking calls made from inside a handler (a server handler calling back into the client that is waiting on it)
  pump the transports re-entrantly, so nested calls complete instead of deadlocking.
- `Channel.requestTimeout(millis)` fails blocking `send`/`subscribe`/`values().next()` with `"timeout"`; the default
  `0` waits forever, like TS.
- Limits: websocket messages (single or fragmented) above `maxPayload` (default 16 MiB, settable on
  `ListenOptions`/`ConnectOptions`) and upgrade heads above 64 KiB drop the connection. A peer that sends a malformed
  frame or invalid JSON is disconnected (client calls then fail with `"disconnected"`); JSON nested deeper than 512
  levels is rejected.
