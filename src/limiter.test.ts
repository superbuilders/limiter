import assert from "node:assert/strict"
import { afterEach, beforeEach, describe, it, mock, test } from "node:test"

import * as limiter from "#limiter.ts"

/**
 * Tracks a promise's settlement without awaiting it, so tests can assert
 * "not settled yet" between fake-timer ticks.
 */
function observe<T>(promise: Promise<T>): { settled: () => boolean; rejected: () => boolean } {
	let isSettled = false
	let isRejected = false
	promise.then(
		() => {
			isSettled = true
		},
		() => {
			isSettled = true
			isRejected = true
		}
	)
	return {
		settled: () => isSettled,
		rejected: () => isRejected
	}
}

/** Drains the microtask queue so settled promises observe as settled. */
async function flush(): Promise<void> {
	for (let i = 0; i < 10; i++) {
		await Promise.resolve()
	}
}

describe("limiter.rate", () => {
	beforeEach(() => {
		mock.timers.enable({ apis: ["setTimeout", "Date"] })
	})

	afterEach(() => {
		mock.timers.reset()
	})

	test("first take resolves immediately", async () => {
		const rl = limiter.rate(10)
		const first = observe(rl.take(new AbortController().signal))
		await flush()
		assert.strictEqual(first.settled(), true)
	})

	test("consecutive takes are spaced per/rate apart", async () => {
		const rl = limiter.rate(10, { slack: 0 })
		const signal = new AbortController().signal
		await rl.take(signal)
		const second = observe(rl.take(signal))
		const third = observe(rl.take(signal))
		await flush()
		assert.strictEqual(second.settled(), false)
		assert.strictEqual(third.settled(), false)

		mock.timers.tick(100)
		await flush()
		assert.strictEqual(second.settled(), true)
		assert.strictEqual(third.settled(), false)

		mock.timers.tick(100)
		await flush()
		assert.strictEqual(third.settled(), true)
	})

	test("strict mode (slack: 0) never rewards idle time with a burst", async () => {
		const rl = limiter.rate(10, { slack: 0 })
		const signal = new AbortController().signal
		await rl.take(signal)
		mock.timers.tick(5000)
		const first = observe(rl.take(signal))
		const second = observe(rl.take(signal))
		await flush()
		assert.strictEqual(first.settled(), true)
		assert.strictEqual(second.settled(), false)
		mock.timers.tick(100)
		await flush()
		assert.strictEqual(second.settled(), true)
	})

	test("slack accumulates idle time into a bounded burst", async () => {
		const rl = limiter.rate(10, { slack: 2 })
		const signal = new AbortController().signal
		await rl.take(signal)
		mock.timers.tick(5000)

		const burst1 = observe(rl.take(signal))
		const burst2 = observe(rl.take(signal))
		const burst3 = observe(rl.take(signal))
		const throttled = observe(rl.take(signal))
		await flush()
		assert.strictEqual(burst1.settled(), true)
		assert.strictEqual(burst2.settled(), true)
		assert.strictEqual(burst3.settled(), true)
		assert.strictEqual(throttled.settled(), false)

		mock.timers.tick(100)
		await flush()
		assert.strictEqual(throttled.settled(), true)
	})

	test("per configures the window like uber's Per option", async () => {
		const rl = limiter.rate(2, { per: 60_000, slack: 0 })
		const signal = new AbortController().signal
		await rl.take(signal)
		const second = observe(rl.take(signal))
		mock.timers.tick(29_999)
		await flush()
		assert.strictEqual(second.settled(), false)
		mock.timers.tick(1)
		await flush()
		assert.strictEqual(second.settled(), true)
	})

	test("pre-aborted signal rejects with the abort reason", async () => {
		const rl = limiter.rate(10)
		const controller = new AbortController()
		const reason = new Error("caller gave up")
		controller.abort(reason)
		const result = await rl.take(controller.signal).then(
			() => undefined,
			(err: Error) => err
		)
		assert.strictEqual(result, reason)
	})

	test("abort during the sleep rejects but the slot stays consumed", async () => {
		const rl = limiter.rate(10, { slack: 0 })
		const steady = new AbortController().signal
		await rl.take(steady)

		const controller = new AbortController()
		const aborted = observe(rl.take(controller.signal))
		const behind = observe(rl.take(steady))
		await flush()
		controller.abort(new Error("gave up waiting"))
		await flush()
		assert.strictEqual(aborted.rejected(), true)

		mock.timers.tick(100)
		await flush()
		assert.strictEqual(behind.settled(), false)
		mock.timers.tick(100)
		await flush()
		assert.strictEqual(behind.settled(), true)
	})

	test("rejects invalid configuration", () => {
		assert.throws(() => limiter.rate(0))
		assert.throws(() => limiter.rate(1.5))
		assert.throws(() => limiter.rate(10, { per: 0 }))
		assert.throws(() => limiter.rate(10, { slack: -1 }))
		assert.throws(() => limiter.rate(10, { slack: 2.5 }))
	})
})

describe("limiter.concurrency", () => {
	/** A work item the test starts and finishes by hand. */
	function deferredWork(): { work: () => Promise<string>; finish: (value: string) => void } {
		let resolveWork: (value: string) => void = () => {}
		const promise = new Promise<string>((resolve) => {
			resolveWork = resolve
		})
		return {
			work: () => promise,
			finish: (value) => {
				resolveWork(value)
			}
		}
	}

	it("runs up to max concurrently and queues the rest FIFO", async () => {
		const cl = limiter.concurrency(2)
		const signal = new AbortController().signal
		const a = deferredWork()
		const b = deferredWork()
		const c = deferredWork()

		const order: string[] = []
		const runA = cl.run({ signal, work: a.work }).then((v) => order.push(v))
		const runB = cl.run({ signal, work: b.work }).then((v) => order.push(v))
		const runC = cl.run({ signal, work: c.work }).then((v) => order.push(v))
		await flush()

		assert.strictEqual(cl.active, 2)
		assert.strictEqual(cl.pending, 1)
		assert.strictEqual(cl.max, 2)

		a.finish("a")
		await flush()
		assert.strictEqual(cl.active, 2)
		assert.strictEqual(cl.pending, 0)

		b.finish("b")
		c.finish("c")
		await Promise.all([runA, runB, runC])
		assert.deepStrictEqual(order, ["a", "b", "c"])
		assert.strictEqual(cl.active, 0)
	})

	it("a rejecting work releases its slot and propagates", async () => {
		const cl = limiter.concurrency(1)
		const signal = new AbortController().signal
		const boom = new Error("work exploded")
		const failing = cl.run({
			signal,
			work: () => Promise.reject(boom)
		})
		const after = cl.run({
			signal,
			work: () => Promise.resolve("survivor")
		})
		const failure = await failing.then(
			() => undefined,
			(err: Error) => err
		)
		assert.strictEqual(failure, boom)
		assert.strictEqual(await after, "survivor")
		assert.strictEqual(cl.active, 0)
	})

	it("aborting a queued run rejects with the reason and never starts the work", async () => {
		const cl = limiter.concurrency(1)
		const blocker = deferredWork()
		void cl.run({ signal: new AbortController().signal, work: blocker.work })

		const controller = new AbortController()
		let ran = false
		const queued = cl.run({
			signal: controller.signal,
			work: () => {
				ran = true
				return Promise.resolve("never")
			}
		})
		await flush()
		assert.strictEqual(cl.pending, 1)

		const reason = new Error("shed load")
		controller.abort(reason)
		const rejection = await queued.then(
			() => undefined,
			(err: Error) => err
		)
		assert.strictEqual(rejection, reason)
		assert.strictEqual(ran, false)
		assert.strictEqual(cl.pending, 0)
		blocker.finish("done")
	})

	it("a pre-aborted signal rejects immediately", async () => {
		const cl = limiter.concurrency(1)
		const controller = new AbortController()
		const reason = new Error("already dead")
		controller.abort(reason)
		const rejection = await cl.run({ signal: controller.signal, work: () => Promise.resolve("no") }).then(
			() => undefined,
			(err: Error) => err
		)
		assert.strictEqual(rejection, reason)
	})

	it("aborting after start has no effect — the work settles the promise", async () => {
		const cl = limiter.concurrency(1)
		const controller = new AbortController()
		const work = deferredWork()
		const running = cl.run({ signal: controller.signal, work: work.work })
		await flush()
		controller.abort(new Error("too late"))
		work.finish("completed anyway")
		assert.strictEqual(await running, "completed anyway")
	})

	it("onStart reports wait telemetry and limiter state", async () => {
		const cl = limiter.concurrency(1)
		const signal = new AbortController().signal
		const blocker = deferredWork()
		void cl.run({ signal, work: blocker.work })

		let seen: limiter.Start | undefined
		const queued = cl.run({
			signal,
			work: () => Promise.resolve("ok"),
			onStart: (start) => {
				seen = start
			}
		})
		await flush()
		blocker.finish("done")
		await queued
		assert.ok(seen)
		assert.strictEqual(seen.active, 1)
		assert.strictEqual(seen.pending, 0)
		assert.strictEqual(seen.max, 1)
		assert.strictEqual(typeof seen.waitMs, "number")
	})

	it("a throwing onStart rejects the run, releases the slot, and the queue advances", async () => {
		const cl = limiter.concurrency(1)
		const signal = new AbortController().signal
		const boom = new Error("telemetry exploded")
		const failing = cl.run({
			signal,
			work: () => Promise.resolve("unreached"),
			onStart: () => {
				throw boom
			}
		})
		const after = cl.run({ signal, work: () => Promise.resolve("survivor") })
		const rejection = await failing.then(
			() => undefined,
			(err: Error) => err
		)
		assert.strictEqual(rejection, boom)
		assert.strictEqual(await after, "survivor")
		assert.strictEqual(cl.active, 0)
	})

	it("rejects invalid configuration", () => {
		assert.throws(() => limiter.concurrency(0))
		assert.throws(() => limiter.concurrency(2.5))
	})
})
