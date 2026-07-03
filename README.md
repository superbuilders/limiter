# @superbuilders/limiter

Two throttling primitives for TypeScript, one per failure mode:

- **`limiter.rate`** — a leaky-bucket **rate** limiter: operations are *spaced in time*, on average `per / rate` apart. A faithful port of [go.uber.org/ratelimit](https://github.com/uber-go/ratelimit).
- **`limiter.concurrency`** — a **concurrency** limiter: at most N operations *in flight*, the rest queue FIFO.

Rate protects a target from your call *frequency*; concurrency protects it (and you) from your call *volume in flight*. They compose — take a rate slot, then run under a concurrency cap.

```typescript
import * as limiter from "@superbuilders/limiter"

const apiRate = limiter.rate(100)            // 100 per second, uber semantics
const apiConcurrency = limiter.concurrency(8) // at most 8 in flight

async function callApi<T>(signal: AbortSignal, work: () => Promise<T>): Promise<T> {
	await apiRate.take(signal)
	return apiConcurrency.run({ signal, work })
}
```

## Install

```
pnpm add @superbuilders/limiter
```

ESM only. No runtime dependencies beyond [@superbuilders/errors](https://github.com/superbuilders/errors); uses only the global `setTimeout`, so it runs anywhere (Node, browsers, Bun, Deno) and fake-timer test harnesses intercept it naturally.

## `limiter.rate(rate, options?)`

```typescript
const rl = limiter.rate(10)                          // 10 per second
const strict = limiter.rate(10, { slack: 0 })        // strict spacing, no bursts
const perMinute = limiter.rate(2, { per: 60_000 })   // 2 per minute

await rl.take(signal) // resolves when your slot arrives
doTheThing()
```

`take(signal)` resolves when the operation may proceed, sleeping if necessary so that calls are spaced `per / rate` milliseconds apart on average. Call it before every iteration — the direct translation of the Go library's *"the process is expected to call Take() before every iteration"*.

**Slack** is the burst allowance: idle time accumulates up to `slack` unspent permissions (default 10, exactly Uber's default), so a limiter that sat quiet can absorb a small burst at full speed before throttling resumes. `slack: 0` is Uber's `WithoutSlack` — strict spacing regardless of idle time.

**Abort**: a pre-aborted signal rejects immediately; aborting mid-sleep rejects the waiting `take`. Either way the rejection is **the signal's own `reason`** when it's an `Error` (so `errors.is(err, myReason)` works), and an aborted take's time slot stays consumed — the state can't roll back once later takes have built on it.

### Fidelity to the Go original

The algorithm is line-for-line [uber-go/ratelimit's `atomicInt64Limiter.Take`](https://github.com/uber-go/ratelimit/blob/main/limiter_atomic_int64.go): state is a single number — *the time the next permission issues* — advanced through the same three cases (first call / strict-idle reset, slack-capped idle reset, normal `+= perRequest`). What's deliberately absent:

| uber-go/ratelimit | here | why |
| --- | --- | --- |
| CAS loop over atomic int64 | straight-line update | the loop exists for goroutine contention; JavaScript is single-threaded |
| cache-line padding | — | same |
| `WithClock(clock)` | — | fake-timer harnesses mock the global `setTimeout`/`Date` directly |
| `Per(duration)` / `WithSlack(n)` / `WithoutSlack` | `{ per, slack }` options | options object instead of functional options |
| `Take() time.Time` returns the permission time | `take(signal): Promise<void>` | JS callers need abortability more than the timestamp |
| `NewUnlimited()` | — | pass no limiter |

## `limiter.concurrency(max)`

```typescript
const cl = limiter.concurrency(12)

const result = await cl.run({
	signal: opts.signal,
	work: () => fetchRoster(classId),
	onStart(start) {
		if (start.waitMs > 1_000) {
			logger.info({ waitMs: start.waitMs, active: start.active, pending: start.pending }, "roster call delayed by limiter")
		}
	}
})
```

At most `max` works run at once; excess `run` calls queue FIFO. The returned promise settles with the work's own result or rejection.

- **`onStart`** fires as the work leaves the queue, with `{ waitMs, active, pending, max }` — the hook for backpressure telemetry. A throwing `onStart` rejects that run and releases the slot; the queue keeps moving.
- **Abort** while queued rejects with the signal's `reason` and the work never starts. Abort after start does nothing — a running work settles its own promise (this library bounds admission, it does not cancel work; pass the same signal to the work for that).
- **Counters**: `cl.active`, `cl.pending`, `cl.max` are live reads.

## Migrating from 0.x

0.x exported one primitive, `create({ concurrency, abortMessage })`. In 1.0:

- `limiter.create({ concurrency: n, abortMessage })` → `limiter.concurrency(n)` — aborts now reject with the **signal's reason** instead of a configured message.
- `Start` fields renamed: `activeCount`/`pendingCount`/`concurrency` → `active`/`pending`/`max`.
- New: `limiter.rate` (the Uber port that gave the package its name).

## License

[0BSD](./LICENSE) © Bjorn Pagen
