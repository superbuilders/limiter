import * as errors from "@superbuilders/errors"

/**
 * Converts an abort into the error a limiter rejects with: the signal's own
 * reason when it is an Error (so callers can match the value they aborted
 * with via errors.is), otherwise an Error carrying its string form.
 */
function abortError(signal: AbortSignal): Error {
	if (signal.reason instanceof Error) {
		return signal.reason
	}
	return errors.new(String(signal.reason))
}

/**
 * An abortable sleep on the GLOBAL setTimeout — resolved at call time, so
 * fake-timer harnesses (node:test mock.timers) intercept it, and no Node
 * timers module is needed at all.
 */
function abortableSleep(ms: number, signal: AbortSignal): Promise<void> {
	return new Promise(function sleepUntil(resolve, reject) {
		if (signal.aborted) {
			reject(abortError(signal))
			return
		}
		function onAbort(): void {
			clearTimeout(timer)
			reject(abortError(signal))
		}
		const timer = setTimeout(function wake() {
			signal.removeEventListener("abort", onAbort)
			resolve()
		}, ms)
		signal.addEventListener("abort", onAbort, { once: true })
	})
}

type RateOptions = {
	per?: number
	slack?: number
}

type RateLimiter = {
	take(signal: AbortSignal): Promise<void>
}

/**
 * A leaky-bucket rate limiter — a port of go.uber.org/ratelimit's
 * atomicInt64Limiter. State is one number: the time the next permission
 * issues. Each take advances it by per/rate and sleeps until its slot;
 * idle time may accumulate up to slack unspent permissions for bursts
 * (slack: 0 enforces strict spacing, matching WithoutSlack). The Go
 * original's CAS loop and cache-line padding exist purely for goroutine
 * contention — in single-threaded JavaScript the whole algorithm collapses
 * to a straight-line state update followed by one abortable sleep.
 *
 * An aborted take rejects but has already consumed its slot: state cannot
 * be rolled back once later takes have built on it.
 */
function rate(ratePerWindow: number, options?: RateOptions): RateLimiter {
	if (!Number.isInteger(ratePerWindow) || ratePerWindow < 1) {
		throw errors.new("limiter rate must be a positive integer")
	}
	const per = options === undefined || options.per === undefined ? 1000 : options.per
	if (!Number.isFinite(per) || per <= 0) {
		throw errors.new("limiter per must be a positive number of milliseconds")
	}
	const slack = options === undefined || options.slack === undefined ? 10 : options.slack
	if (!Number.isInteger(slack) || slack < 0) {
		throw errors.new("limiter slack must be a non-negative integer")
	}

	const perRequest = per / ratePerWindow
	const maxSlack = slack * perRequest
	let nextPermissionAt: number | undefined

	async function take(signal: AbortSignal): Promise<void> {
		if (signal.aborted) {
			throw abortError(signal)
		}
		const now = Date.now()
		if (nextPermissionAt === undefined || (maxSlack === 0 && now - nextPermissionAt > perRequest)) {
			nextPermissionAt = now
		} else if (maxSlack > 0 && now - nextPermissionAt > maxSlack + perRequest) {
			nextPermissionAt = now - maxSlack
		} else {
			nextPermissionAt = nextPermissionAt + perRequest
		}
		const sleepMs = nextPermissionAt - now
		if (sleepMs <= 0) {
			return
		}
		await abortableSleep(sleepMs, signal)
	}

	return { take }
}

type Start = {
	waitMs: number
	active: number
	pending: number
	max: number
}

type RunInput<T> = {
	signal: AbortSignal
	work: () => Promise<T>
	onStart?: (start: Start) => void
}

type ConcurrencyLimiter = {
	readonly active: number
	readonly pending: number
	readonly max: number
	run<T>(input: RunInput<T>): Promise<T>
}

type QueueEntry = {
	start: () => void
	reject: (error: Error) => void
	signal: AbortSignal
	onAbort: () => void
}

/**
 * A concurrency limiter: at most `max` works run at once; excess run calls
 * queue FIFO. Aborting a queued call rejects it with the signal's reason
 * without ever starting the work; aborting after start has no effect — a
 * running work settles the promise itself. `onStart` fires as the work
 * leaves the queue, carrying how long it waited and the limiter's state at
 * that moment — the hook for backpressure telemetry.
 */
function concurrency(max: number): ConcurrencyLimiter {
	if (!Number.isInteger(max) || max < 1) {
		throw errors.new("limiter concurrency must be a positive integer")
	}
	let active = 0
	const queue: QueueEntry[] = []

	function pump(): void {
		while (active < max && queue.length > 0) {
			const entry = queue.shift()
			if (entry === undefined) {
				return
			}
			entry.signal.removeEventListener("abort", entry.onAbort)
			if (entry.signal.aborted) {
				entry.reject(abortError(entry.signal))
				continue
			}
			active += 1
			entry.start()
		}
	}

	function release(): void {
		active -= 1
		pump()
	}

	async function runStarted<T>(input: RunInput<T>, queuedAt: number): Promise<T> {
		if (input.onStart !== undefined) {
			const onStart = input.onStart
			const started = errors.trySync(function notifyStart() {
				onStart({
					waitMs: Math.round(Date.now() - queuedAt),
					active,
					pending: queue.length,
					max
				})
			})
			if (started.error) {
				release()
				throw started.error
			}
		}
		const result = await errors.try(input.work())
		release()
		if (result.error) {
			throw result.error
		}
		return result.data
	}

	function run<T>(input: RunInput<T>): Promise<T> {
		if (input.signal.aborted) {
			return Promise.reject(abortError(input.signal))
		}
		const queuedAt = Date.now()
		return new Promise<T>(function enqueue(resolve, reject) {
			const entry: QueueEntry = {
				reject,
				signal: input.signal,
				onAbort() {
					const index = queue.indexOf(entry)
					if (index >= 0) {
						queue.splice(index, 1)
					}
					reject(abortError(input.signal))
				},
				start() {
					void runStarted(input, queuedAt).then(resolve, reject)
				}
			}
			input.signal.addEventListener("abort", entry.onAbort, { once: true })
			queue.push(entry)
			pump()
		})
	}

	return {
		get active() {
			return active
		},
		get pending() {
			return queue.length
		},
		get max() {
			return max
		},
		run
	}
}

export type { ConcurrencyLimiter, RateLimiter, RateOptions, Start }
export { concurrency, rate }
