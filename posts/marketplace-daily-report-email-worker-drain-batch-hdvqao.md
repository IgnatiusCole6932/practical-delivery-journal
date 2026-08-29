# Marketplace Daily Report Email: Worker Drain, Batch Size, and 256 KB Queue Limits

Short answer: make the daily report job a set of durable send intents, put references rather than report content on the queue, and size every serialized message before publishing. Use a delayed message for a bounded deferral of up to seven days; use the daily scheduler to create work. The worker should be safe to run twice.

That decision is about delivery guarantees, not payload aesthetics. A marketplace report can be late because the worker pool is rate-limited, but the recovery path must still distinguish “not attempted” from “sent and not acknowledged.”

## How should a daily email worker drain a queue when batch size and message limits collide?

Start with one invariant: one report date, one recipient, one send intent. A batch is an optimization over that unit, never the identity of the work. Store the rendered report in durable storage and put `report_id`, `send_intent_id`, recipient identifiers, and a schema version in the queue message. The worker can then reload the same artifact after a retry instead of trusting a large mutable payload.

The 256 KB limit is a ceiling on the serialized message, not a target for the report. Escaping, metadata, and envelope fields consume bytes. A recipient count that fits today can fail after someone adds a long subject, a localization field, or a new diagnostic attribute. Measure the encoded representation at the publish boundary.

I use a conservative application budget below the wire limit. It is a guardrail, not a promise from a queue service. The exact margin belongs in configuration and should be revisited when the envelope changes.

```go
package main

import (
	"encoding/json"
	"fmt"
)

const (
	wireLimit = 256 * 1024
	// Keep room for the publish envelope and future fields.
	applicationBudget = 240 * 1024
)

type SendIntent struct {
	SchemaVersion int    `json:"schema_version"`
	ReportID      string `json:"report_id"`
	IntentID      string `json:"send_intent_id"`
	RecipientID   string `json:"recipient_id"`
}

func encodedSize(v any) (int, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func batchesWithinBudget(intents []SendIntent) ([][]SendIntent, error) {
	var result [][]SendIntent
	var batch []SendIntent

	for _, intent := range intents {
		candidate := append(append([]SendIntent(nil), batch...), intent)
		size, err := encodedSize(candidate)
		if err != nil {
			return nil, err
		}
		if size > applicationBudget {
			if len(batch) == 0 {
				return nil, fmt.Errorf("intent %q exceeds the application budget", intent.IntentID)
			}
			result = append(result, batch)
			batch = []SendIntent{intent}
			continue
		}
		batch = candidate
	}

	if len(batch) > 0 {
		result = append(result, batch)
	}
	return result, nil
}

func main() {
	intents := []SendIntent{
		{SchemaVersion: 1, ReportID: "report-2026-08-11", IntentID: "send-1842", RecipientID: "buyer-1842"},
		{SchemaVersion: 1, ReportID: "report-2026-08-11", IntentID: "send-1843", RecipientID: "buyer-1843"},
	}
	batches, err := batchesWithinBudget(intents)
	if err != nil {
		panic(err)
	}
	fmt.Println(len(batches))
}
```

One recipient per message is the easiest retry contract. If a measured batch is useful, persist each recipient's outcome separately and retry only the unfinished intents. A batch that cannot explain its partial failure is too large, even when its bytes are acceptable.

## What does a duplicate delivery prove about the report pipeline?

Usually, it proves less than the alert suggests. The worker may have sent the email and died before acknowledgment. A timeout may have hidden a successful provider response. A redelivery is therefore not evidence that the first attempt did nothing; it is evidence that the queue could not observe a durable completion.

I've been paged for both missed jobs and duplicate deliveries. The useful postmortem question was not “why did the queue resend?” It was “which state transition was durable before the process disappeared?”

The worker should claim the send intent with a uniqueness constraint, perform the send under an explicit idempotency policy, record the result, and acknowledge only after the record is durable. If the claim says `sent`, a later delivery can acknowledge without sending again. If it says `pending`, the retry needs a defined rule for whether the external email operation can be repeated safely. A 429 should trigger backoff, not a tight loop; preserve the distinction between throttling and a permanent recipient failure. In practice, the claim record is where the report date, recipient, attempt number, provider result, and next-attempt time meet. That record lets an operator answer a duplicate complaint without guessing from queue depth. It also gives reconciliation a precise unit: find pending intents for one date, compare them with the sent set, and enqueue only the missing work. The report body is irrelevant to that decision, which is why putting it on the queue creates noise without improving recovery.

This sequence also makes the queue drain measurable. Track queued intents, claimed intents, sent intents, retry age, and the oldest unclaimed report date. A worker-pool graph that shows only throughput can look healthy while a small set of recipients is being retried forever.

Three words: acknowledge last.

Idempotency is the feature.

## Where does seven-day delay belong in the design?

Delay is a timing tool. It is useful when a business rule says “send this intent later,” including a postponement no longer than seven days. It should not be the only representation of tomorrow's report, and it should not be treated as an audit log.

The scheduler creates the report job for each report date. A delay can move an existing send intent into a later delivery window. A separate durable record retains the report date, intended recipient, current state, and deletion deadline. This separation matters when a schedule is paused, a worker pool is drained, or an operator has to replay one day's missing intents without recreating every email.

Retention and privacy are part of the same design. A queue message is an operational handoff; it is a poor place to keep the full report or personal data longer than required. A deletion request should have a direct path through the report artifact, send-intent record, and logs, with identifiers that do not require searching email HTML. The right-to-erasure obligation makes this easier to reason about when references are short and records have owners.

The small operational rule is worth writing into the runbook: a seven-day deferral changes when an intent is attempted; it does not change what the intent is.

## Which delivery contract is suitable for this worker pool?

Choose at-least-once delivery with idempotent send intents when the system must tolerate rate limits and process restarts. It gives the worker a recoverable path, but it requires durable state and a clear duplicate policy. An at-most-once path can reduce duplicate risk by acknowledging earlier, yet it can turn a transient crash into a silent missed email. For a daily marketplace report, that is a poor trade when the recipient list is part of an operational commitment.

| Contract | Recovery behavior | Operational cost |
|---|---|---|
| At-least-once | A redelivery can recover after a crash, provided the send intent is idempotent | Requires durable claims, result records, and duplicate handling |
| At-most-once | Acknowledgment can prevent a repeat, but a crash can lose the email | Simpler state, weaker protection against missed reports |

The catch is that reference-only messages are not suitable when the report artifact cannot be stored durably or fetched by workers in the drain environment. A single huge message may be simpler for a short-lived prototype, but it makes byte limits, privacy deletion, and retry inspection harder. Stick with a larger batch only when the team has measured serialized size, isolated partial failures, and tested a process death after the external send.

I am not sure a fixed batch size can survive every template and recipient population. Your mileage may vary. The durable decision is to enforce bytes and state transitions, then let observed worker latency determine the batch size.

Before launch, test a message just under 256 KB, one over it, a seven-day deferral, a request beyond seven days, a duplicate delivery, a worker crash after send, and a throttling response. Verify that a missed daily trigger is found by a reconciliation sweep and that a completed intent is not sent again. Those tests exercise the delivery guarantee directly.

## References

- [Wikipedia: Cron](https://en.wikipedia.org/wiki/Cron)
- [GDPR Article 17: Right to erasure](https://gdpr-info.eu/art-17-gdpr/)
