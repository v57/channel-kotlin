package channel.process

import channel.Channel
import channel.Process

fun <State> Channel<State>.process(state: State = this.makeDefaultState()): Process<State> {
  return this.process(state)
}
