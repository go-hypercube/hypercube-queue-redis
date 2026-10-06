# Hypercube Redis Queue

A Redis implementation of the [go-hypercube queue contract](https://pkg.go.dev/github.com/go-hypercube/go-hypercube/queue), using [`github.com/redis/go-redis/v9`](https://github.com/redis/go-redis).

Implements the core `queue.Queue` interface and all optional capabilities:

- `queue.VisibilityExtender`
- `queue.RedriveProvider`
- `queue.StatsProvider`

## Features

- Atomic enqueue, lease, settlement, visibility extension, and dead-letter redrive using Lua
- Concurrent workers across processes sharing the same Redis database and key prefix
- Delayed push and retry, with per-message/default visibility timeouts
- At-least-once redelivery after worker crashes or expired visibility leases
- Delivery-specific tokens that reject stale settlement, including before redelivery
- Independent snapshots of messages, payload, and metadata
- Duplicate ID detection across all queues and retained states in a prefix
- Batch operations with partial failure reporting and original input indices
- Explicit dead-letter storage, programmatic redrive, queue discovery, and stats
- No application retry limits or automatic dead-lettering

## Installation

The standalone module path follows the other Hypercube drivers:

```sh
go get github.com/go-hypercube/hypercube-queue-redis
```

## Usage

```go
package main

import (
    "context"
    "errors"
    "log"
    "time"

    "github.com/go-hypercube/go-hypercube/queue"
    redisqueue "github.com/go-hypercube/hypercube-queue-redis"
    "github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()
    client := redis.NewClient(&redis.Options{
        Addr:                  "localhost:6379",
        ContextTimeoutEnabled: true,
        ReadTimeout:           3 * time.Second,
        WriteTimeout:          3 * time.Second,
    })
    defer client.Close()

    q := redisqueue.New(client,
        redisqueue.WithKeyPrefix("myapp:queue"),
        redisqueue.WithPollingTimeout(2*time.Second),
        redisqueue.WithPollingInterval(25*time.Millisecond),
        redisqueue.WithDefaultVisibilityTimeout(30*time.Second),
    )

    msg := &queue.Message{
        QueueName: "emails",
        Namespace: "app",
        JobName:   "send-welcome-email",
        Payload:   []byte(`{"user_id":42}`),
    }
    if err := q.Push(ctx, msg); err != nil {
        log.Fatal(err)
    }

    messages, err := q.Pop(ctx, "emails", 10)
    if errors.Is(err, queue.ErrEmpty) {
        return
    }
    if err != nil {
        log.Fatal(err)
    }
    for _, delivery := range messages {
        // Process delivery.Payload; handlers must be idempotent.
        if err := q.Ack(ctx, delivery); err != nil {
            log.Fatal(err)
        }
    }
}
```

`New` accepts a `redis.UniversalClient`, including a `*redis.Client`, `*redis.ClusterClient`, or Sentinel failover client. It does not contact Redis. The caller owns and closes the client; the queue does not create a connection pool or persistent background workers.

## Configuration

| Option | Default | Behavior |
| --- | --- | --- |
| `WithKeyPrefix` | `hypercube:queue` | Storage isolation namespace. Instances using the same prefix and database share logical IDs, messages, and delivery leases. |
| `WithPollingTimeout` | `1s` | Maximum empty `Pop` wait. Non-positive values check once and return `queue.ErrEmpty` if nothing is visible. |
| `WithPollingInterval` | `25ms` | Interval between empty `Pop` checks. Non-positive values are ignored; waits are capped by the remaining polling timeout. |
| `WithDefaultVisibilityTimeout` | `30s` | Lease used when a message's `VisibilityTimeout` is zero. Non-positive option values are ignored. |
| `WithDeadLetterRetention` | `7 days` | Terminal retention measured from successful dead-lettering using server time. |
| `WithMaxRetainedMessages` | `100,000` | Maximum ready, delayed, in-flight, and dead messages combined across the prefix. |
| `WithMaxMessageBytes` | `1 MiB` | Maximum entire serialized JSON snapshot, including routing metadata and base64 expansion. |
| `WithMaxRetainedBytes` | `64 MiB` | Maximum sum of retained serialized message blobs across the prefix, excluding server/index overhead. |

Use the same default visibility configuration across workers. A message with an explicit nonzero `VisibilityTimeout` overrides the worker's default.

Delays and visibility deadlines use **Redis server time**, not worker clocks. Positive durations are rounded upward to milliseconds, including sub-millisecond leases. Original `VisibilityTimeout` values round-trip without losing nanosecond precision. Non-positive message delays are immediately eligible. A negative per-message visibility timeout creates an already-expired delivery, as in the memory driver.

`Pop` polls rather than using a blocking list or Pub/Sub subscription. It keeps polling after an empty response until the timeout expires or its context ends. Message discovery latency is normally bounded by the polling interval plus Redis latency. There is no FIFO guarantee.

## Capacity, retention, and maintenance

Safeguards are finite by default. Non-positive retention/capacity option values are ignored; they cannot disable the limits. The first backend operation atomically persists a versioned policy for the prefix. Every producer, consumer, and maintenance instance sharing that prefix must use the same four retention/capacity settings; incompatible settings return `ErrConfigurationMismatch`. Polling and default visibility settings are not part of the persisted policy.

Capacity is reserved atomically across processes and queues. A new push that would exceed the count or serialized-byte budget fails with an item-level `ErrCapacityExceeded`; an oversized snapshot fails with `ErrMessageTooLarge`. Retry and dead-letter updates also check message size and any increase in stored bytes. A failed update leaves that delivery and its previous stored snapshot intact. Shrinking a snapshot, acknowledging it, or pruning a dead letter releases the corresponding budget. Ack does not serialize caller-modified payload/metadata, so oversized annotations do not prevent discarding the delivery.

**Active messages have no TTL.** Ready, delayed, and in-flight work is never silently discarded by retention or capacity limits. Backpressure belongs to producers; application expiry decisions belong to workers/managers calling Ack or DeadLetter.

Dead letters expire at their stored absolute deadline. Expired messages cannot be redriven and are excluded from dead statistics. `PruneDeadLetters(ctx)` physically removes them across the prefix, atomically clearing snapshots, all message fields/indexes, ID reservations, queue counts, and byte accounting. It returns the known completed count with an error if a later chunk fails. Scripts handle at most 128 IDs at a time, rechecking current dead state to protect redriven or reused IDs.

Push prunes once before processing valid items in a batch, recovering expired IDs and capacity across queues. Queues prunes before discovery; Stats and Redrive opportunistically prune up to 128 expired messages in the named queue. **Run periodic maintenance to reclaim memory in idle queues**—logical expiry alone is not a native key TTL:

```go
maintenanceCtx, stopMaintenance := context.WithCancel(ctx)
maintenanceDone := make(chan error, 1)
go func() {
    maintenanceDone <- q.RunMaintenance(maintenanceCtx, redisqueue.DefaultMaintenanceInterval)
}()

// Run producers/workers here, and monitor maintenanceDone for backend failures.
// At shutdown, stop and join maintenance before closing the Redis client:
stopMaintenance()
if err := <-maintenanceDone; err != nil && !errors.Is(err, context.Canceled) {
    log.Printf("queue maintenance stopped: %v", err)
}
```

`RunMaintenance(ctx, interval)` prunes immediately, then periodically in the caller's goroutine until cancellation or a backend failure. The default suggested interval is one minute; interval must be positive. It does not silently retry failures or start hidden goroutines in New. One loop per prefix is sufficient; multiple instances can prune safely.

These budgets are **not a literal Redis RAM limit**: indexes, ID fields, delivery tokens, script/client overhead, and temporary nonzero redrive receipts use additional memory. Configure server `maxmemory` with `noeviction` and leave headroom. Ack, pruning, expiry discovery, and statistics/discovery scripts carry Redis 7 `allow-oom` metadata so freeing/read paths are not denied merely because the server's memory limit has been reached.

This is storage layout **version 2**. Earlier retained queues without policy/accounting metadata return `ErrIncompatibleStorage`; use a fresh prefix rather than resetting counters over existing work. Retention/capacity policy stays fixed for an initialized prefix, including after it is drained. Use a new prefix when intentionally changing that policy.

## Lifecycle and ownership

```text
Push             -> pending
Pop              -> pending -> in-flight
Ack              -> in-flight -> removed
Retry            -> in-flight -> pending
DeadLetter       -> in-flight -> dead
ExtendVisibility -> in-flight -> in-flight
Redrive          -> unexpired dead -> pending
PruneDeadLetters -> expired dead -> removed
```

- `Push` generates a collision-resistant ID if needed, snapshots message data, ignores caller-supplied `DriverData`, and starts the stored attempt count at zero.
- IDs stay reserved while pending, delayed, in-flight, or retained dead letters. An acknowledged or pruned ID can be reused.
- Every `Pop` returns independently allocated data, increments `Attempt`, resets `Delay`, and attaches a new opaque delivery token.
- An expired delivery is stale immediately. `Ack`, `Retry`, `DeadLetter`, and `ExtendVisibility` reject it with `queue.ErrNotFound`, whether or not another worker has popped it yet.
- `Retry` snapshots current `Payload`, `Extra`, and `Delay`. Set a new `Delay` explicitly; leaving it zero retries immediately.
- `DeadLetter` snapshots the latest caller-modifiable state, retaining the logical ID and attempt count for inspection/redrive until retention expires. Dead letters are never returned to ordinary workers unless explicitly redriven before expiry.
- `ExtendVisibility` replaces the deadline with server time plus the supplied positive extension; it does not add to the previous deadline.
- `Redrive` preserves ID, routing fields, payload, metadata, and visibility timeout. It resets attempts and delay and clears the old lease. The next delivery has `Attempt == 1`.

Preserve protected fields and `DriverData` unchanged between `Pop` and settlement. After successful `Ack`, `Retry`, or `DeadLetter`, relinquish that delivery and all memory reachable from it, as required by the portable contract.

## Batch operations and cancellation

`Push`, `Ack`, `Retry`, and `DeadLetter` process items independently, including batches spanning multiple queues. A failure does not prevent other valid items from completing. Each message uses its own atomic script; a whole batch is not one transaction.

```go
if err := q.Retry(ctx, messages...); err != nil {
    var batch *queue.BatchError
    if errors.As(err, &batch) {
        for _, item := range batch.Failed {
            log.Printf("item %d: %v", item.Index, item.Err)
        }
    }
}
```

Item errors include `queue.ErrInvalidArgument` for nil messages, `queue.ErrAlreadyExists` for duplicate pushed IDs, and `queue.ErrNotFound` for stale or already-settled deliveries. Infrastructure failures are retained as item errors too. Empty batches return nil without contacting Redis, even with an already-canceled context.

The driver observes cancellation during polling and while waiting for backend calls, even if the client's context-based socket deadlines are disabled. Use **finite socket timeouts**, preferably with `ContextTimeoutEnabled: true`, so canceled requests eventually release their resources. A submitted Redis command may still complete after cancellation: a context error is an **unknown outcome**, not evidence of rollback.

`Pop` returns at most 128 messages per invocation to bound server-side Lua work. Returning fewer than `maxMessages` is allowed by the contract. `Redrive` splits larger requests into chunks of up to 128 and returns the known completed count if a later chunk fails. It uses temporary five-minute request receipts to avoid moving extra messages if go-redis replays a chunk after a lost response; configure client retry/I/O budgets shorter than that retention window.

## Observability

```go
names, err := q.Queues(ctx)
if err != nil {
    return err
}
for _, name := range names {
    stats, err := q.Stats(ctx, name)
    if err != nil {
        return err
    }
    log.Printf("%s: ready=%d delayed=%d in_flight=%d dead=%d",
        stats.Name, stats.Ready, stats.Delayed, stats.InFlight, stats.Dead)
}
```

Each queue's stats are read atomically. Expired leases count as `Ready`, not `InFlight`. Unknown or empty queues return `queue.QueueStats{Name: queueName}`. Discovery includes queues with only unexpired dead letters; queues disappear after their last retained message is acknowledged or pruned. As with all queue drivers, statistics are for observability, not correctness decisions.

## Redis storage and deployment requirements

Keys use this layout:

```text
{sha256(prefix)}:prefix:messages             # ID -> serialized message data
{sha256(prefix)}:prefix:attempts             # ID -> delivery count
{sha256(prefix)}:prefix:visibility           # ID -> configured lease duration
{sha256(prefix)}:prefix:delays               # ID -> current Delay in nanoseconds
{sha256(prefix)}:prefix:tokens               # ID -> current delivery token
{sha256(prefix)}:prefix:queues               # queue name -> retained message count
{sha256(prefix)}:prefix:policy               # schema version and immutable safeguard settings
{sha256(prefix)}:prefix:retained-bytes       # sum of serialized message blob lengths
{sha256(prefix)}:prefix:dead-expiry          # expiry -> base64(queue name):ID
{sha256(prefix)}:prefix:q:base64(name):pending
{sha256(prefix)}:prefix:q:base64(name):inflight
{sha256(prefix)}:prefix:q:base64(name):dead    # sorted by absolute expiry deadline
{sha256(prefix)}:prefix:redrive:request-id    # temporary redrive receipt
```

`sha256(prefix)` represents the full hexadecimal digest, and `base64(name)` uses raw URL-safe base64. The controlled hash tag comes before the user prefix, so embedded braces or separators cannot cause collisions or select a different Cluster slot. All keys are explicitly declared to Lua.

Message blobs are JSON; byte slices **and opaque string fields** are base64-encoded to preserve arbitrary bytes, including invalid UTF-8. Attempts, current delay, visibility scheduling, and delivery state live separately. Caller-provided `DriverData` is never serialized. The storage format is driver-owned, not a stable public API; use queue methods for transitions, and do not modify its Redis keys manually.

Deployment requirements and limitations:

- Use a Redis primary capable of running Lua and the hash/sorted-set/string commands above. Redis 7+ is required for Lua script metadata. Read-only replicas cannot serve queue operations.
- Configure appropriate **AOF/RDB persistence, replication, backups, and failover** for your durability needs. Redis without persistence loses messages on restart; asynchronous replication can lose acknowledged writes during failover. This driver does not strengthen those guarantees with `WAIT` or `WAITAOF`.
- Use **`maxmemory-policy noeviction`** or a dedicated Redis instance with no applicable eviction policy. Do not expire, evict, flush, or externally mutate queue state: removing only some of these related keys can lose messages or break indexes.
- Every prefix occupies **one Redis Cluster hash slot**, deliberately, to preserve atomic global ID checks and transitions. Use separate prefixes for independent tenants/workloads if you need to distribute them across slots; message ID uniqueness is scoped to each prefix.
- Active messages have no TTL or application attempt limit. Dead letters expire after the configured retention. Plan capacity, handle producer backpressure, and run maintenance for idle queues.
- Ordering is unspecified. At-least-once delivery requires idempotent handlers.

## Testing

The test suite always uses isolated [`miniredis`](https://github.com/alicebob/miniredis) test doubles. It does not connect to an external Redis server and needs no Redis installation or Docker daemon.

```sh
make test
make race
make lint
# Or:
go test -race -count=1 ./...
go vet ./...
```

Tests cover ownership boundaries, binary-safe data, duplicates and batch errors, leases, redrive, prefix isolation, concurrency, polling/cancellation, capacity reservation, exact serialized-size boundaries, byte accounting, retention deadlines, stale expiry/ID reuse, schema/policy mismatches, and idle maintenance.

Miniredis does not understand Redis 7 Lua headers. Test-only client adapters strip that metadata while executing the unchanged script bodies. Real-server Lua flag handling, memory-pressure behavior, and Cluster integration are not validated by this suite.
