package channel

import kotlin.test.fail

internal fun <T> CallResult<T>.requireOk(): T? = when (this) {
  is CallResult.Ok -> value
  is CallResult.RemoteErr -> fail("expected ok result, got remote error '$error'")
}

internal fun <T> CallResult<T>.requireRemoteErr(expected: String) {
  when (this) {
    is CallResult.Ok -> fail("expected remote error '$expected', got ok '$value'")
    is CallResult.RemoteErr -> kotlin.test.assertEquals(expected, error)
  }
}

internal fun <T> StreamNext<T>.requireItem(expected: T) {
  when (this) {
    is StreamNext.Item -> kotlin.test.assertEquals(expected, value)
    is StreamNext.Done -> fail("expected stream item '$expected', got done")
    is StreamNext.RemoteErr -> fail("expected stream item '$expected', got remote error '$error'")
  }
}

internal fun <T> StreamNext<T>.requireDone() {
  when (this) {
    StreamNext.Done -> Unit
    is StreamNext.Item -> fail("expected done, got item '$value'")
    is StreamNext.RemoteErr -> fail("expected done, got remote error '$error'")
  }
}

internal fun SubscribeResult.requireOk(): SubscriptionHandle = when (this) {
  is SubscribeResult.Ok -> handle
  is SubscribeResult.RemoteErr -> fail("expected subscription handle, got '$error'")
}

internal fun waitUntil(timeoutMs: Long = 2_000L, stepMs: Long = 1L, condition: () -> Boolean) {
  val deadline = System.nanoTime() + (timeoutMs * 1_000_000L)
  while (System.nanoTime() < deadline) {
    if (condition()) {
      return
    }
    Thread.sleep(stepMs)
  }
  fail("timed out waiting for condition")
}
