import * as errors from "@superbuilders/errors"

type Start = {
	waitMs: number
	activeCount: number
	pendingCount: number
	concurrency: number
}

type RunInput<T> = {
	signal: AbortSignal
	work: () => Promise<T>
	onStart?: (start: Start) => void
}

type Limiter = {
	readonly activeCount: number
	readonly pendingCount: number
	readonly concurrency: number
	run<T>(input: RunInput<T>): Promise<T>
}

type QueueEntry = {
	run: () => void
	reject: (error: Error) => void
	signal: AbortSignal
	onAbort: () => void
}

type Input = {
	concurrency: number
	abortMessage: string
}

function validateConcurrency(concurrency: number): void {
	if (!Number.isInteger(concurrency) || concurrency < 1) {
		throw errors.new("limiter concurrency must be a positive integer")
	}
}

function create(input: Input): Limiter {
	validateConcurrency(input.concurrency)
	let activeCount = 0
	const queue: QueueEntry[] = []

	function removeQueuedEntry(entry: QueueEntry): void {
		const index = queue.indexOf(entry)
		if (index >= 0) {
			queue.splice(index, 1)
		}
	}

	function pumpQueue(): void {
		while (activeCount < input.concurrency && queue.length > 0) {
			const entry = queue.shift()
			if (entry === undefined) {
				return
			}
			if (entry.signal.aborted) {
				entry.signal.removeEventListener("abort", entry.onAbort)
				entry.reject(errors.new(input.abortMessage))
				continue
			}
			activeCount += 1
			entry.signal.removeEventListener("abort", entry.onAbort)
			entry.run()
		}
	}

	function releaseActive(): void {
		activeCount -= 1
		pumpQueue()
	}

	function notifyStart<T>(runInput: RunInput<T>, queuedAt: number): void {
		const onStart = runInput.onStart
		if (onStart === undefined) {
			return
		}
		onStart({
			waitMs: Math.round(performance.now() - queuedAt),
			activeCount,
			pendingCount: queue.length,
			concurrency: input.concurrency
		})
	}

	async function runActive<T>(runInput: RunInput<T>, queuedAt: number): Promise<T> {
		const started = errors.trySync(function notifyLimitStart() {
			notifyStart(runInput, queuedAt)
		})
		if (started.error) {
			releaseActive()
			throw started.error
		}
		const result = await errors.try(runInput.work())
		releaseActive()
		if (result.error) {
			throw result.error
		}
		return result.data
	}

	function run<T>(runInput: RunInput<T>): Promise<T> {
		if (runInput.signal.aborted) {
			return Promise.reject(errors.new(input.abortMessage))
		}
		const queuedAt = performance.now()
		return new Promise<T>(function enqueue(resolve, reject) {
			const entry: QueueEntry = {
				reject,
				signal: runInput.signal,
				onAbort() {
					removeQueuedEntry(entry)
					runInput.signal.removeEventListener("abort", entry.onAbort)
					reject(errors.new(input.abortMessage))
					pumpQueue()
				},
				run() {
					void runActive(runInput, queuedAt).then(resolve, reject)
				}
			}
			runInput.signal.addEventListener("abort", entry.onAbort, { once: true })
			queue.push(entry)
			pumpQueue()
		})
	}

	return {
		get activeCount() {
			return activeCount
		},
		get pendingCount() {
			return queue.length
		},
		get concurrency() {
			return input.concurrency
		},
		run
	}
}

export { create }
