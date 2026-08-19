# Routing 10-Minute SMS Alerts to US and EU Support Queues (Cancellation and Status Polling)

Short answer: for an edtech contact form, choose the SMS API that fits behind a small, durable scheduling boundary with idempotent sends, explicit cancellation, and status polling; keep consent, regional routing, and support ownership in your own application.

The page says a high-priority enrollment request has waited ten minutes without an owner. On-call sees the form ID, the US or EU queue, the scheduled alert time, the current delivery state, and one action: inspect the unassigned case. They should not have to infer whether the scheduler ran, whether a cancellation raced the send, or whether a provider accepted the message.

I've been paged by missed jobs and duplicate deliveries. The provider logo has never been the useful clue. The useful clues are a stable message key, a timestamp for every state transition, and evidence that the support queue still needed the reminder when the worker claimed it.

## Run the failure drill before choosing an API

Start with integration effort, but define it as the work required to operate the path, not the time needed to produce the first successful request. A thin integration accepts one internal command: “remind the correct support queue if form `frm_7429` is still unassigned at 14:10 UTC.” Everything specific to an SMS provider stays behind an adapter. The domain record retains the contact-form ID, region, due time, consent evidence, destination class, deduplication key, provider reference, and internal status.

The decision table is short:

| Requirement | Acceptance test | Reason to reject an integration |
| --- | --- | --- |
| Scheduling | A durable job survives a worker restart and runs no earlier than its due time | Scheduling exists only in process memory |
| Idempotency | Replaying the same form and reminder key produces at most one send command | Retries can create a second user-visible alert |
| Cancellation | Assigning the case before the due time leaves a terminal canceled record | Cancellation is only a best-effort flag with no observable result |
| Status polling | The adapter maps remote states into a small documented internal state machine | Application code must understand provider-specific states everywhere |
| Regional routing | Policy selects the configured US or EU path before dispatch | Region is guessed from a phone prefix inside the worker |
| Operations | A page links one form, job, attempt, and provider reference | Investigation requires searching unrelated logs by phone number |

This boundary also makes a later provider change finite. It won't erase migration work — templates, consent handling, regional configuration, and state mappings still need review — but it prevents those details from spreading through the contact-form handler, queue service, scheduler, and on-call tooling.

Don't use WebOTP as a general reminder design. MDN describes the WebOTP API as a way for a web app to receive a specially formatted SMS and pass a one-time password to the origin, with user consent. That is a different job from notifying a support queue about an unassigned contact form. Likewise, RFC 8058 defines one-click unsubscribe for email through list headers; it is useful evidence that channel-specific standards matter, not a specification to paste onto an SMS workflow.

## Trace the page backward

The late-contact page is the last signal in the chain, so work backward. It fires because a reminder deadline passed while the case remained unassigned. That condition depends on a scheduled job. The job depends on successful persistence after form validation and queue selection. Queue selection depends on an explicit region and topic policy. Each dependency needs a signal that can fail earlier and more precisely than “no one answered the learner.”

Instrument four moments: form accepted, reminder scheduled, dispatch claimed, and result observed. Record cancellation as a state transition too. A useful event carries `form_id`, `queue_id`, `region`, `reminder_key`, `scheduled_at`, `attempt`, and `state`; keep the phone number out of routine logs. The page can then say “dispatch claim absent” or “remote result pending” instead of collapsing every failure mode into “SMS late.”

The first alert should usually be about a stuck state, not a single old case. For example, page when the oldest due reminder exceeds the service objective and the worker has made no forward progress; create a lower-urgency ticket when one delivery remains pending beyond the polling window. The exact window is a local policy choice. I'm not sure there is a defensible universal threshold: contact urgency, staffing hours, traffic volume, and the selected provider's documented status model should decide it.

One detail matters during deployment. Emit both a counter of terminal transitions and a gauge of the oldest nonterminal due job. A healthy-looking send rate can hide one partition that stopped moving, while an age gauge exposes it. Conversely, age without transition counts makes a brief backlog look like total failure. The pair gives on-call a rate and a queueing symptom.

That is the earlier signal the original page was missing.

## Put cancellation next to the send decision

Cancellation races are ordinary. A support agent can claim a form while a worker is waking up for its ten-minute reminder. Checking assignment in the web handler and then trusting that old result in the worker leaves a gap large enough for a duplicate or irrelevant alert. Put the final eligibility check and the job claim in one transactional boundary; make the outward send depend on the claimed record and a stable idempotency key.

The following Go sketch keeps provider behavior behind a narrow interface. It is deliberately a domain example, not a vendor setup guide:

```go
package reminders

import (
	"context"
	"errors"
	"time"
)

var ErrNotDue = errors.New("reminder is not due")

type Status string

const (
	Pending  Status = "pending"
	Claimed  Status = "claimed"
	Canceled Status = "canceled"
	Accepted Status = "accepted"
)

type Reminder struct {
	FormID         string
	QueueID        string
	Region         string
	IdempotencyKey string
	DueAt          time.Time
	Assigned       bool
	Status         Status
}

type Store interface {
	ClaimIfDueAndUnassigned(context.Context, string, time.Time) (Reminder, error)
	MarkAccepted(context.Context, string, string) error
	MarkCanceled(context.Context, string) error
}

type Sender interface {
	SendAlert(context.Context, Reminder) (providerReference string, err error)
}

func Dispatch(ctx context.Context, store Store, sender Sender, key string, now time.Time) error {
	r, err := store.ClaimIfDueAndUnassigned(ctx, key, now)
	if err != nil {
		return err
	}

	if r.Assigned || r.Status == Canceled {
		return store.MarkCanceled(ctx, key)
	}
	if r.DueAt.After(now) {
		return ErrNotDue
	}

	ref, err := sender.SendAlert(ctx, r)
	if err != nil {
		return err
	}
	return store.MarkAccepted(ctx, key, ref)
}
```

In production, define what happens when the process stops after the provider accepts the request but before `MarkAccepted` commits. The adapter should reuse `IdempotencyKey` when the selected API supports an equivalent mechanism; the reconciliation worker should poll by the stored provider reference once it exists. If the external contract cannot make retries safe or expose a queryable reference, integration effort moves out of the happy-path code and into recovery operations. Count that work during selection.

Status polling should normalize only what the application uses. `pending`, `accepted`, `delivered`, `failed`, and `canceled` may be enough for the runbook even when an external API has more states. Preserve the raw state in restricted diagnostic data, map it once in the adapter, and make transitions monotonic. A late poll must not move a terminal record back to pending.

## Make recovery ownership explicit

A successful API response proves little about the whole reminder. Test the clock boundary just before and exactly at ten minutes; concurrent assignment and dispatch; a repeated job claim; a restart after the send command; delayed status observations; a cancellation replay; and a US/EU routing-policy change. Use fake time and a fake sender for deterministic tests, then run a small contract suite against each configured adapter.

Make deployment boring. Shadow the routing decision without sending, compare expected queue and region, enable one internal destination, then widen by queue. A rollback should stop new claims without deleting durable jobs, because deletion destroys the evidence needed to reconcile what was scheduled and what was sent.

The catch is that a polling design is not suitable when the provider offers no stable message reference or when the required status freshness would force wasteful, high-frequency reads. Prefer an event callback when its authenticity can be verified and delivery can be retried into your durable inbox; keep periodic polling as reconciliation rather than the only signal. Stick with application-managed scheduling when cancellation must share a transaction with contact assignment. Provider-managed scheduling can reduce worker code, but it creates more integration work if cancel and status semantics differ across regions or adapters.

No single choice removes the need for consent and channel policy. The contact form should make the intended follow-up clear, and the system should retain the evidence required by its policy. Avoid turning an operational support reminder into an authentication message just because both use SMS.

## How should a transactional app backend choose SMS alerts, reminders, cancellation, and status polling?

After adding the oldest-due-job signal, replay known workload shapes in a staging environment and review production distributions before selecting the page threshold. Separate business-hours queue latency from scheduler health. One slow contact may deserve a support escalation without implying that the delivery system is unhealthy; a flat transition rate across many due jobs is the stronger infrastructure symptom.

False positives have a real cost. A threshold set too close to the ten-minute business reminder will page during harmless scheduling jitter, shift changes, or a brief burst, and repeated noise teaches responders to treat the alert as background. Set a sustained condition, include a minimum affected-job count where traffic supports it, and route isolated delivery outcomes to a ticket or dashboard. Your mileage may vary for a low-volume EU queue, where even one missed high-priority contact matters and aggregate thresholds have almost no statistical value.

The final selection rule is operational: choose the API and scheduling location that make every reminder explainable from form acceptance through cancellation or a terminal delivery state, with the least adapter-specific code. Integration effort belongs in the failure path too.

## References

- [RFC 8058: One-Click Unsubscribe](https://datatracker.ietf.org/doc/html/rfc8058)
- [MDN: WebOTP API](https://developer.mozilla.org/en-US/docs/Web/API/WebOTP_API)
