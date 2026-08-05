package channel.client

import channel.Channel
import channel.Client
import channel.ConnectOptions

fun <State> Channel<State>.connect(port: Int, options: ConnectOptions<State> = ConnectOptions()): Client<State> {
  return this.connect("ws://127.0.0.1:$port", options)
}

fun <State> Channel<State>.connect(address: String, options: ConnectOptions<State> = ConnectOptions()): Client<State> {
  return this.connect(address, options)
}
