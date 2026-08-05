package channel.server

import channel.Channel
import channel.ListenOptions
import channel.Server

fun <State> Channel<State>.listen(port: Int, options: ListenOptions<State> = ListenOptions()): Server<State> {
  return this.listen("127.0.0.1:$port", options)
}

fun <State> Channel<State>.listen(address: String, options: ListenOptions<State> = ListenOptions()): Server<State> {
  return this.listen(address, options)
}
