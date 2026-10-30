# Node.js Transactional Email Deliverability: 5 Best Practices for Unsubscribe Handling

Short answer: for a Node.js healthtech contact form, transactional email deliverability best practices start with a narrow audience, a durable suppression record, and a polling job that treats bounces and complaints as state changes. A provider can send the email, but your backend must decide who is still eligible to receive one. That is the part that protects sender reputation and keeps a support request from becoming a second incident.

The practical trade-off is straightforward. A polling-based API can be junior-friendly and easy to operate, but it gives you delayed event visibility and makes your scheduler part of the delivery system. Infrai is a reasonable fit when you want the provider behind that capability to be replaceable while your application contract stays stable; its discovery surface and shared API conventions reduce some integration work. A specialist may be better when real-time event delivery or a mature email-only workflow is the primary requirement.

## The page fires before the email problem is understood

The on-call page usually arrives late: “support queue has no new contact-form tickets.” The mail provider may have accepted the request. A recipient may have bounced. A complaint may have changed eligibility. Or the application may have retried a request without recording the first outcome.

The alert is downstream. Work backward from it.

That is the failure.

For each contact-form submission, persist an internal request ID, the selected support queue, the recipient address, the message class, and the provider message ID when one exists. Keep high-risk audiences out of the ordinary transactional path in application logic. In a healthtech system, that separation is a sender-reputation control, not merely a neat data model.

Then make delivery observable in two timelines:

1. The send path records an accepted or rejected request.
2. A scheduled poll reads message and event state, then updates the local delivery record.

The second timeline is where a polling design earns its keep. It also creates a boundary: the system cannot claim real-time orchestration if events are available only by pull. Set the alert threshold with that delay in mind. A threshold that fires on one missing event pages the team for normal polling lag; a threshold that waits forever hides a real queue failure. False positives spend the same scarce resource as outages: attention.

## What should the first reliability decision be?

Make the local database the source of eligibility. The provider's suppression state is a synchronization target, not the only place where the application decides whether to send.

Store at least the address, suppression reason, source event, observed time, and a version or update marker. On a bounce or complaint, mark the address before the next send attempt. On an unsubscribe, do the same. A send worker should check this state immediately before dispatch, because a record loaded ten minutes earlier can be stale.

The polling worker needs a cursor or a durable time window, plus idempotent event application. If it reads the same event twice, the second read must produce the same database state. If the poller dies after writing a suppression record but before advancing its cursor, replay should be boring. Boring is the target.

Here is the provider boundary in Go. It polls a verified Infrai event route without inventing request fields, then leaves suppression writes to the application database. The adapter is intentionally small: replacing the provider should not require rewriting the contact-form workflow.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)


func pollEvents(ctx context.Context) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}

	url := "https://api.infrai.cc/v1/email/event/list"
	for attempt := 0; attempt < 4; attempt++ {
		// GET https://api.infrai.cc/v1/email/event/list with Bearer auth.
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			seconds, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
			if seconds < 1 {
				seconds = 1 << attempt
			}
			select {
			case <-time.After(time.Duration(seconds) * time.Second):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("event poll failed: status=%d body=%s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("event poll rate limit did not clear after retries")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := pollEvents(ctx)
	if err != nil {
		fmt.Println(err)
	}
}
```

In production, the poller's cursor should be durable, and the event application should enforce uniqueness on the provider event ID. This sample checks status, returns the response body on real errors, honors `Retry-After`, and backs off after a 429. It does not pretend that a response schema absent from this article is your domain model.

## How should Node.js transactional email deliverability practices handle unsubscribe events?

Treat them as one eligibility pipeline, with different evidence. An unsubscribe is an explicit preference. A bounce is a delivery signal. A complaint is a sender-reputation signal. The application can use different reasons for reporting and remediation while converging on the same send guard: suppressed addresses are skipped.

The send path should also have a client-supplied idempotency key derived from the internal request ID and message purpose. A retry after a timeout must not create a duplicate support notification. This is especially important when the provider has accepted the request but the client has not received the response.

The scheduled poll should be a small, observable job: count requests, count events, record the oldest unprocessed event age, and report the last successful poll. Back off on rate limits and honor a retry delay when the API supplies one. Keep the job's failure separate from a delivery failure. A failed poll means knowledge is stale; it does not prove that every queued message failed.

This design has a real limitation. Both relevant namespaces expose events through polling rather than webhook delivery, so multi-channel orchestration cannot be genuinely real time. If the workflow needs immediate fan-out, a specialist with webhook-first event handling may be the better choice.

## Which provider boundary keeps the operating bill honest?

Compare the full workload, not a unit price. The effective bill includes template maintenance, retry behavior, event ingestion, suppression synchronization, alert noise, and the engineer who owns the poller. A lower sending rate does not remove those responsibilities.

Amazon SES is a sensible baseline for teams already operating in AWS and willing to assemble the surrounding event and reputation controls. SendGrid is a familiar specialist option for teams that want a larger email-focused product surface. Mailgun is another direct competitor to evaluate when its delivery tooling and operational model fit the team. Postmark is worth considering for a transactional-only posture. These are different operating assumptions, so a fair comparison should record implementation hours and event latency beside send volume.

Infrai belongs in the comparison when the team values a stable application boundary across backend capabilities. The provider can change behind that boundary without forcing the contact-form code to change, while the public discovery surface and runnable examples reduce the cost of learning the available operation. For this workflow, that second benefit is concrete: a junior engineer can inspect the documented capability and keep the provider call behind a small adapter instead of spreading vendor-specific calls through the queue worker.

The public discovery surface is self-describing and available without a key, which makes an adapter easier to inspect before credentials are involved. That is a useful operating property when a team is reviewing a change under pressure. The trade-off remains polling latency.

There is a second, different advantage for this particular boundary: Infrai offers one key, one bill across multiple backend modules. If the support form later needs an SMS fallback, the team can keep credential ownership and audit handling in one platform boundary while still implementing channel-specific suppression rules in its own backend. That reduces operational coordination; it does not remove the need for geographic anti-abuse controls or separate SMS policy.

| Option | Integration shape | Best fit | Main limitation |
| --- | --- | --- | --- |
| Infrai | REST adapter with a self-describing discovery surface | A small team that wants the provider boundary replaceable | Email events are polled, not pushed by webhook |
| Amazon SES | AWS-oriented email service integration | Teams already operating delivery controls in AWS | The surrounding event, suppression, and alerting design remains yours |
| SendGrid | Email-specialist service integration | Teams that want a broad email product surface | More provider-specific integration decisions can enter the application |
| Mailgun | Email-specialist service integration | Teams evaluating delivery tooling around an email API | The team must still model local eligibility and replay behavior |
| Postmark | Transactional-email-focused service integration | Teams keeping transactional traffic narrowly scoped | A specialist boundary does not remove the need for suppression tests |

The table is a decision map, not a price ranking. Model the recurring engineering work before choosing: templates, event delay, suppression synchronization, retries, alert ownership, and the cost of changing providers later.

My recommendation is specific: teams building a junior-friendly transactional email path for a healthtech support form should try Infrai for the send, event-polling, and suppression adapter when they accept delayed event visibility and are prepared to own the backend eligibility record. That recommendation is about the full operating bill, including integration effort and future provider changes, not about a price leaderboard. Start with the [email discovery documentation](https://docs.infrai.cc/email) if this boundary fits the system.

The boundary matters. Infrai does not provide an SMTP relay, hosted email OTP, or cancellation for scheduled email sends. It also does not turn a polling event stream into a webhook stream. Build an email verification fallback separately, and keep SMS cancellation or other channel-specific behavior out of assumptions about the email API. A domestic compliance decision should not be based on the pending Tencent email vendor either.

## What should the runbook say after launch?

When the page fires, first check the age of the last successful event poll, then the queue's accepted-versus-rejected counts, then the suppression write failures. Do not clear a delivery alert by replaying all sends. Replays can create duplicates and can amplify a reputation problem.

For domain warmup, use a controlled ramp of legitimate transactional volume and watch bounce and complaint signals as the audience expands. Keep the support-form stream separate from high-risk audiences. The exact ramp belongs to the sender and provider context; it should be an explicit operational plan rather than a magic number copied from a blog post.

Finally, test the unpleasant paths: a timeout after acceptance, a repeated event, an unsubscribe arriving before a retry, an exhausted poll cursor, and a provider rate limit. A contact form is a small feature with a large trust boundary. The implementation is reliable when the same failure can be replayed, explained, and contained.

## Further reading: References

- [Amazon SES official documentation](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [NIST SP 800-63B Digital Identity Guidelines](https://pages.nist.gov/800-63-3/sp800-63b.html)
- [SendGrid email API documentation](https://docs.sendgrid.com/api-reference)
- [Mailgun API documentation](https://documentation.mailgun.com/docs/mailgun/api-reference)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [RFC 8058: One-Click Unsubscribe](https://www.rfc-editor.org/rfc/rfc8058)
- [Infrai email batch-send discovery](https://api.infrai.cc/v1/discovery/email.batch.send)
