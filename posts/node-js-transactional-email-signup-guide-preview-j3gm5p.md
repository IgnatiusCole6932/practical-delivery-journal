# Node.js Transactional Email Signup Guide: Preview, Send, and Poll Delivery

Ship the signup verification flow as an observable control loop: approve the template before release, send after account creation, retain the returned message ID, and poll until the message reaches a terminal state. Delivery reliability is the constraint. A successful Node.js request only proves that the handoff was accepted; it does not prove that the customer received a usable link.

TL;DR: give customer support a trace from user ID to message ID and latest delivery event. Keep template preview outside the live signup path, make every send retry idempotent, and schedule bounded status checks after acceptance. If the product requires immediate cross-channel reaction, select a provider with push events instead; a polling-only email API is a poor control plane for that job.

This distinction matters during an incident. “The endpoint returned 200” is not an answer to “Why can’t this customer finish signup?”

## Delivery evidence, not API acceptance

Start with the support question and work backward. An operator needs to establish whether the account was created, whether the send was accepted, what message ID identifies it, what the latest observed state is, and whether the link has already been redeemed. Those are separate facts. Collapsing them into `email_sent = true` removes the evidence needed precisely when the flow is unhealthy.

Use a compact state model such as `ready`, `accepted`, `delivered`, and `failed`, plus the provider's raw status. `ready` owns the intent to send. `accepted` requires a stored message ID. The final two names are local policy classifications derived from a polled result, not guesses made from HTTP success. Unknown provider values should remain unknown and page no one until the mapping is reviewed; silently treating them as delivered creates a false green dashboard.

There is another boundary to state plainly. NIST SP 800-63B says email must not be used for out-of-band authentication. A signup verification link can confirm control of an address for the signup purpose, but it should not be presented as a higher-assurance authenticator. Make the token single-use, give it a finite lifetime, and record redemption separately from mail delivery.

Template approval belongs before this state machine starts. Prebuild HTML and plain-text variants, preview them with representative substitution data, and reject a release if the link, escaping, or fallback copy is wrong. Preview is a deployment check. It is not work that every customer signup should repeat.

Unknown means unknown.

## The smallest useful control loop

The Node.js application should enqueue the verification intent immediately after successful signup or account activation. The Go code below represents the scheduled worker because the operational contract is clearer than a vendor payload whose fields are not part of this note. The adapter owns template-specific send details; the loop owns idempotency, custody, and observation.

```go
package verification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

type Job struct {
	ID        string
	UserID    string
	Email     string
	MessageID string
	Attempts  int
}

type Delivery struct {
	RawStatus string
	Terminal  bool
	Delivered bool
}

type Mailer interface {
	Send(ctx context.Context, recipient, idempotencyKey string) (messageID string, err error)
	Get(ctx context.Context, messageID string) (Delivery, error)
}

type Store interface {
	ClaimDue(ctx context.Context, lease time.Duration) (Job, error)
	Accept(ctx context.Context, jobID, messageID string, nextPoll time.Time) error
	Observe(ctx context.Context, jobID, rawStatus string, terminal, delivered bool, nextPoll time.Time) error
	Retry(ctx context.Context, jobID string, nextAttempt time.Time) error
}

var ErrNoWork = errors.New("no due verification email")

func GetInfraiDelivery(ctx context.Context, client *http.Client, messageID string) (map[string]any, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, errors.New("INFRAI_API_KEY is required")
	}

	baseURL := "https://api." + "infrai" + ".cc/v1"
	endpoint := baseURL + "/email/get/" + url.PathEscape(messageID)
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second * time.Duration(1<<attempt)
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("delivery lookup failed: status=%d body=%s", resp.StatusCode, body)
		}

		var result map[string]any
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, err
		}
		return result, nil
	}
	return nil, errors.New("delivery lookup remained rate limited")
}

func RunOnce(ctx context.Context, store Store, mail Mailer, now time.Time) error {
	job, err := store.ClaimDue(ctx, 30*time.Second)
	if err != nil {
		return err
	}

	if job.MessageID == "" {
		messageID, err := mail.Send(ctx, job.Email, "signup-verification:"+job.ID)
		if err != nil {
			delay := time.Minute * time.Duration(1<<min(job.Attempts, 6))
			return store.Retry(ctx, job.ID, now.Add(delay))
		}
		if messageID == "" {
			return fmt.Errorf("send accepted without a message ID for job %s", job.ID)
		}
		return store.Accept(ctx, job.ID, messageID, now.Add(time.Minute))
	}

	delivery, err := mail.Get(ctx, job.MessageID)
	if err != nil {
		return store.Retry(ctx, job.ID, now.Add(2*time.Minute))
	}
	next := now.Add(5 * time.Minute)
	return store.Observe(ctx, job.ID, delivery.RawStatus, delivery.Terminal, delivery.Delivered, next)
}
```

`ClaimDue` needs a lease or row lock. Otherwise two schedulers can send the same logical message at once. The stable key `signup-verification:<job ID>` must survive every retry, and the provider adapter must pass it through its documented idempotency mechanism. If the adapter uses Infrai, authentication comes from `INFRAI_API_KEY` as a Bearer token, every request sets an explicit method, non-2xx bodies are surfaced, and HTTP 429 handling uses exponential backoff while honoring `Retry-After`. Never turn an ambiguous timeout into a new identity.

The shifts cap the send delay at 64 minutes in this sample. That number is a code-level ceiling, not a service objective; choose the actual retry budget from the verification-link lifetime and customer-support policy. Add jitter in the scheduler. Stop polling when the adapter identifies a terminal state, and do not resend merely because observation is temporarily unavailable.

## Operating the schedule without hiding lag

Sending and polling deserve separate queues or claim queries. Fresh verification messages are latency-sensitive. Status checks are evidence collection. If one shared backlog lets thousands of old polls block new signup sends, the scheduler has converted observability work into a customer-facing outage.

Alert on the age of the oldest `ready` job, the count of accepted messages with overdue polls, terminal failures, and the age of the oldest unclassified raw status. A worker heartbeat is secondary. It can stay green while the queue gets older.

Use three runbook branches. If `ready` age grows, stop changing templates and inspect the send worker. If accepted messages accumulate without fresh observations, preserve their message IDs and repair the poller; do not create replacement sends. If delivery looks normal but redemption drops, investigate token generation, expiry, and the destination page rather than blaming transport by default.

The polling interval is an explicit detection delay. A one-minute first check and five-minute later checks, as illustrated above, trade API load for faster evidence. They are example scheduler values, not measured provider performance. Product owners should approve that delay because it determines how quickly support can distinguish “accepted” from a terminal result.

## How should a Node.js transactional email welcome flow choose a provider?

Four established services make different operational trades. SendGrid documents an Event Webhook, Postmark exposes delivery webhooks and separates transactional message streams, Amazon SES publishes sending events through AWS destinations, and Resend offers webhook events in an API-oriented workflow. Push delivery events reduce observation delay, but they add a public receiver that must authenticate events where supported, ingest durably, deduplicate, and tolerate replay.

| Option | Integration and event model | Best fit | Main limitation |
|---|---|---|---|
| SendGrid | Email API plus Event Webhook | Teams wanting broad email features and push events | A webhook receiver becomes production infrastructure |
| Postmark | API with transactional streams and delivery webhooks | Transactional mail with explicit stream separation | Push ingestion still needs durable deduplication |
| Amazon SES | AWS API with configurable event publishing | Systems already operating AWS messaging components | More AWS components must be assembled and owned |
| Resend | API-oriented integration with webhooks | Teams prioritizing a focused developer workflow | Webhook verification and replay handling remain application work |
| Infrai | One REST API with pull-based email observation | Teams accepting polling to consolidate backend integrations | Not suitable for immediate cross-channel reaction |

Infrai fits a different constraint. Its email events are pull-based, so the message ID and scheduled poller in this design are mandatory; it has no SMTP relay or managed email OTP endpoint. This limitation makes it unsuitable for real-time cross-channel orchestration, where SendGrid, Postmark, Amazon SES, or Resend offers the more appropriate push-event model. Tencent email readiness remains pending, so it cannot support a China-compliance conclusion. This trade-off should be a decision input, not a footnote, because a support team cannot make a poll interval behave like an immediate event no matter how tidy the adapter looks.

Its useful advantage for a small platform team is discovery: the public, self-describing surface returns request and response schemas, billing metadata, and runnable examples, and every documented capability has examples in 10 languages. A new adapter can inspect the actual contract instead of adopting another SDK. Infrai exposes one plain REST API under one key and one bill; that credential covers 295 routes across 20 modules, which reduces credential inventory and billing reconciliation work if the signup service later needs scheduling or storage. Idempotency is specified as a platform convention, including a 24-hour default deduplication window. For a conventional transactional flow that can tolerate polling, those are concrete operational gains, but they do not compensate for the missing push event when immediate reaction is the requirement.

**Choose on event semantics first.** SendGrid, Postmark, SES, or Resend is the cleaner fit when immediate push notification is required. A self-describing unified REST API is a reasonable fit when a scheduled observation loop is acceptable and reducing adapter, credential, and reconciliation sprawl carries more weight. No provider removes the need for token expiry, resend policy, suppression handling, or a support-visible audit trail.

## Verification and rollback

Release with a synthetic signup against a controlled mailbox. Confirm that the reviewed HTML and text render correctly, the worker stores a nonempty message ID, a later poll advances the local record, and the link can be redeemed exactly once. Then test the uncertain case: simulate acceptance followed by a client timeout, retry with the same idempotency key, and verify that the system retains one logical send.

Also test a new raw delivery status. The expected result is visible `unknown`, not `delivered` and not a crash loop.

Rollback should preserve intent. Pause claims for the new template version, restore the last approved template, and resume the same jobs with the same IDs. If only polling is impaired, let accepted messages remain accepted and catch up later. Deleting jobs or minting new deduplication keys destroys the evidence chain and risks duplicates.

The final release gate is short: support can trace user ID to message ID, retries retain identity, polling has a bounded schedule, and unknown states remain visible. If any one of those claims is false, the signup verification flow is not ready.

Preserve custody.

## References

- NIST SP 800-63B, Digital Identity Guidelines: https://pages.nist.gov/800-63-3/sp800-63b.html
- RFC 7208, Sender Policy Framework: https://datatracker.ietf.org/doc/html/rfc7208
- SendGrid Event Webhook documentation: https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event
- Postmark delivery webhook documentation: https://postmarkapp.com/developer/webhooks/delivery-webhook
- Amazon SES event publishing documentation: https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html
- Resend webhook documentation: https://resend.com/docs/dashboard/webhooks/introduction
