# Node.js Background Job Queue Scheduled Retry Past a 7-Day Delay

A background job queue should deliver work that is ready, not serve as the calendar for work due weeks from now. **For a delayed retry that exceeds the queue's maximum scheduled delay, persist the due time in durable storage and use a periodic worker to release due records into the queue.** The release path must tolerate repeated execution, because a scheduler can run twice and delivery is normally at-least-once.

This matters after the obvious workaround appears to work. A process-local timer vanishes on restart; a broker-held delay eventually reaches its documented ceiling; and a cron tick can overlap during a slow batch. None of those events is exotic. They are ordinary operating conditions.

## How should a Node.js background job queue schedule a retry after the delayed-message limit?

Separate scheduling from delivery. On a failed attempt, write one durable retry record containing a stable identifier, payload reference, attempt number, and `due_at`. A small scheduler runs on a cadence appropriate to the service, claims records whose due time has passed, and enqueues them without a long delay. The queue then does its narrow job: getting ready work to consumers.

That shape handles a seven-day maximum delay and a ninety-day follow-up with the same mechanism. More importantly, it makes overdue work visible. An operator can query the pending set, find the oldest `due_at`, and repair a specific record without searching opaque delayed-message state.

Don't make `setTimeout` the source of truth. It is useful for a short in-process pause, but a deployed Node.js worker has no durable obligation to remember a callback after a replacement, restart, or scale-down. A database row, by contrast, gives the retry a lifecycle that deployment does not erase.

## The failure mode is between claim and enqueue

The difficult boundary is not the clock. It is the interval after a scheduler selects a due record and before the consumer has completed the business action. If a scheduler claims a row, submits a message, and stops before recording success, the next scheduler must be allowed to try again. If it is not allowed, work can remain stranded. If it is allowed, the downstream consumer can receive the same logical attempt again.

So treat duplicate delivery as normal and make the consumer idempotent. Give each scheduled attempt a key derived from the retry record and attempt count. Store that key under a unique constraint in the same transaction as the business effect. A repeated message then becomes a no-op instead of a second email, charge, or state transition.

Run it twice.

Picture the scheduler tick as a very small handoff protocol rather than a timer callback. It first selects a bounded batch of rows that are due, and the selection must exclude rows leased by another active tick. It changes each selected row to a claimed state before calling the queue, so another scheduler cannot send the same row during the normal handoff. The queue call happens outside the database transaction because holding row locks across a network request would turn a slow queue into database contention. After each successful enqueue, the scheduler records that the attempt is queued. There remains an unavoidable narrow interval between those two calls: a repeat tick might submit the same attempt after the lease expires. This is why the idempotency key has to reach the consumer and protect the business write itself; a producer-side flag alone cannot prove that a prior message was not delivered. Conversely, a queue submission that never happened must be eligible for a later attempt, which is why a claimed row cannot stay claimed forever. The lease is the explicit recovery rule. It lets an operator inspect an in-flight batch, lets the system resume after a stopped process, and avoids pretending that an external enqueue can participate in the same atomic transaction as a database update.

Here is the essential part of a generic release worker. The SQL shown assumes PostgreSQL; `SKIP LOCKED` lets concurrent scheduler instances claim different due rows without waiting on one another. The queue interface is deliberately generic, since the correctness boundary belongs in the database claim and consumer transaction.

```go
package scheduler

import (
	"context"
	"database/sql"
	"fmt"
)

type DueRetry struct {
	ID      int64
	Attempt int
	Payload []byte
}

type Queue interface {
	Enqueue(context.Context, string, []byte) error
}

const claimDue = `
WITH candidates AS (
    SELECT id
      FROM scheduled_retry
     WHERE state = 'pending'
       AND due_at <= now()
     ORDER BY due_at
     FOR UPDATE SKIP LOCKED
     LIMIT $1
)
UPDATE scheduled_retry AS r
   SET state = 'claimed',
       attempt = attempt + 1,
       lease_until = now() + interval '2 minutes'
  FROM candidates
 WHERE r.id = candidates.id
RETURNING r.id, r.attempt, r.payload`

func Sweep(ctx context.Context, db *sql.DB, q Queue, batch int) error {
	rows, err := db.QueryContext(ctx, claimDue, batch)
	if err != nil {
		return fmt.Errorf("claim due retries: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var item DueRetry
		if err := rows.Scan(&item.ID, &item.Attempt, &item.Payload); err != nil {
			return err
		}
		key := fmt.Sprintf("scheduled-retry-%d-%d", item.ID, item.Attempt)
		if err := q.Enqueue(ctx, key, item.Payload); err != nil {
			return fmt.Errorf("enqueue %s: %w", key, err)
		}
		if _, err := db.ExecContext(ctx, `
UPDATE scheduled_retry
   SET state = 'queued', lease_until = NULL
 WHERE id = $1`, item.ID); err != nil {
			return err
		}
	}
	return rows.Err()
}
```

A lease is needed for recovery. A later sweep can reclaim a `claimed` row only after its lease expires, and it should use the same idempotency key for the already-created attempt. The code sample omits that recovery predicate to keep the claim query readable; it is a required part of the production schema and query.

The scheduler is not an exactly-once machine. Neither is a typical queue. The useful guarantee is that every due record is eventually released and every business effect accepts the attempt key once. Small distinction. Big outcome.

## Operate the scheduler as a control loop

Use database time for the due comparison so every scheduler instance evaluates one authority. Index the active state and `due_at`; otherwise a growing history table turns each tick into a scan. Cap batches so a burst of overdue records does not crowd out fresh traffic, then keep looping until the normal backlog is drained.

The runbook needs signals for the oldest pending due time, the count of expired leases, claim-to-enqueue latency, and consumer idempotency conflicts. The last metric is especially useful: some duplicates are expected, while a sudden rise tells the on-call engineer that the scheduler may be restarting or timing out around the boundary. Alert on lateness, not merely on whether the cron invocation returned. A scheduler can complete successfully while doing no useful work.

Test the dangerous paths directly. Seed records just before and just after `due_at`; run two scheduler instances; inject a repeat release; and verify that the consumer's unique key preserves one business result. Also test a lease expiry with a mocked database clock or a controlled test timestamp. Waiting seven days in a test proves patience, not scheduling behavior.

For deployment, make the scheduler safe to overlap before increasing its frequency. A one-minute cadence is often easier to reason about than a clever dynamic timer, and the right interval depends on the allowed lateness and expected batch size. Your mileage may vary if the underlying database is geographically distributed or the workload has sharp daily spikes.

## When should you choose a different scheduling mechanism?

Use native delayed delivery when the required delay fits comfortably inside the queue's documented bound. It has fewer components and fewer alarms. Use a periodic cron trigger for truly periodic work, such as a nightly aggregation, where there is no per-record due date to manage. Vercel documents cron jobs as scheduled invocations, which illustrates that distinction: a cron trigger starts work on a timetable; it does not replace a durable per-entity retry ledger.

A durable workflow runtime can be a better fit when a wait is embedded in a long process with multiple branches, human approvals, or several sleeps. Inngest documents durable execution and step-based workflows, but the general trade-off is independent of any implementation: adopting a workflow model changes how the team writes, tests, and observes application logic.

The catch is operational ownership. A retry table plus scheduler is not suitable when the team cannot operate the database state, cannot make consumers idempotent, or only needs a delay well within the queue's supported window. In those cases, stick with the queue's native delay or a managed workflow approach whose model matches the process. A row-and-sweep design earns its extra machinery when long waits, auditability, and targeted repair matter more than having fewer moving parts.

## Sources

- https://www.postgresql.org/docs/current/sql-select.html
- https://vercel.com/docs/cron-jobs
- https://www.inngest.com/docs
