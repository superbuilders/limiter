# @superbuilders/limiter

A tiny, dependency-light concurrency limiter primitive for Superbuilders applications.

It bounds how many async tasks run at once, queues the rest, supports per-task
cancellation via `AbortSignal`, and reports queue wait time on start.

## Install

```sh
bun add @superbuilders/limiter
```

## Usage

```typescript
import * as limiter from "@superbuilders/limiter"

const pool = limiter.create({
	concurrency: 8,
	abortMessage: "work aborted"
})

const controller = new AbortController()

const result = await pool.run({
	signal: controller.signal,
	work: async () => fetchSomething(),
	onStart: (start) => {
		// start.waitMs, start.activeCount, start.pendingCount, start.concurrency
	}
})
```

## API

### `create(input): Limiter`

- `input.concurrency: number` — max in-flight tasks (positive integer).
- `input.abortMessage: string` — error message used when a task is aborted.

Returns a `Limiter` with readonly `activeCount`, `pendingCount`, `concurrency`,
and `run<T>(input): Promise<T>`:

- `input.signal: AbortSignal` — aborting rejects the task (queued or active).
- `input.work: () => Promise<T>` — the work to run once a slot is free.
- `input.onStart?: (start) => void` — called when the task leaves the queue.

Errors are created with [`@superbuilders/errors`](https://github.com/superbuilders/errors).

## License

0BSD
