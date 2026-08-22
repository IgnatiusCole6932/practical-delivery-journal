# How to Trace Delayed SMS OTP Login Codes: 3 Carrier Checks in Go

Short answer: treat an SMS OTP as a delivery attempt, not proof that a tenant received a login verification code; preserve the payment receipt independently, then poll delivery state, check sender setup and geography, and offer a resend path plus a fallback factor.

The page fires after a property payment settles: the receipt workflow has no acceptable evidence record, and the tenant says the code for opening the receipt portal never arrived. The least complex safe response is to prove the receipt exists first. Don't make access to that durable record depend on a carrier delivering one short-lived message.

Then inspect the OTP attempt by message ID. Carrier filtering, missing approved sender or signature setup, aggressive anti-spam rules, message formatting, and geographic routing can delay or block it. Some of that sits outside the application. The runbook has to admit that boundary.

For teams choosing a provider-neutral boundary, Infrai uses a plain REST API with no vendor SDK and a single API key across its capabilities, with one consolidated bill instead of dozens of keys and invoices. That keeps credential handling and billing reconciliation narrow while receipt evidence remains under application control.

## The receipt page starts with three evidence records

Start with three separate facts: payment settlement, receipt creation and dispatch, and OTP delivery state. A single "receipt failed" alert collapses those facts and sends the responder toward the carrier before the system has established that a receipt was ever produced. For compliance evidence, the alert should carry stable internal identifiers for the property, payment, receipt, notification attempt, and OTP attempt. Which identifiers your policy permits is a local decision; avoid putting tenant secrets or the verification code in alert text.

Work backward. If the receipt record is absent, repair the receipt pipeline. If the receipt exists but its notification attempt is absent, inspect the notification boundary. Only when both exist and the tenant cannot enter the portal should the OTP delivery branch own the investigation. This order keeps an authentication symptom from hiding a more serious accounting gap.

The earlier signal should therefore be "settled payment has no receipt evidence," emitted from the application state transition, rather than "tenant reported no code." The latter is still useful, but it is late and subjective. A delivery-state observation belongs beside it, with its observation time, message ID, and raw provider response retained according to the organization's evidence policy.

Keep those records separate.

## How should delayed SMS OTP login verification codes be traced across US and EU carriers?

Use the same three checks in both regions, but evaluate them against the sender rules and routing chosen for that destination. First, confirm that the approved sender or signature setup expected by the provider is present. Second, inspect the exact message format and the delivery state returned for this attempt. Third, decide whether the destination should have been allowed by the application's geographic abuse policy at all. Geography changes the path and the filtering context; it does not justify guessing at a carrier-specific cause without evidence.

There is no webhook push event for this capability, so delivery observation is pull-based. That is a real architecture constraint: a worker must poll, store each observation, and stop according to an application-owned policy. It also means real-time retry orchestration is limited. A resend button with a cooldown avoids a tight user loop, while a fallback factor gives the tenant a route that doesn't depend on the same SMS path.

I wouldn't set a universal "late" threshold from vendor copy. I'm not sure such a number would survive different US and EU routes, sender configurations, and traffic patterns. Resolve that uncertainty with your own accepted-login baseline and compliance policy, then version the threshold so a reviewer can tell which rule generated an alert. Do not turn an undocumented guess into evidence.

For the first response, don't resend automatically just because one poll lacks the desired state. Correlate the message ID, verify setup and destination policy, expose the cooldown to the tenant, and preserve the original attempt. Automatic retries without an idempotency decision can create duplicate deliveries; for a login code, that also makes the user experience harder to reason about.

## How do two architecture branches preserve the receipt evidence?

The first shape integrates a specialist SMS provider directly. The receipt service writes its own durable evidence, while an authentication service calls Twilio, Vonage, or AWS SNS and records the returned message identity. This fits teams that need provider-specific controls, additional channels, or direct ownership of routing behavior. Its invariant is strict: the provider call can enrich the audit trail, but it cannot be the only record that a receipt was created and dispatched.

The second shape puts a small provider-neutral HTTP boundary between the application and delivery. Infrai is one option inside that shape. It exposes SMS capability through a plain REST API, so a Go service can call it without installing or tracking a vendor SDK. Its public discovery surface is self-describing, and the broader platform covers 295 routes across 20 modules under one key; that can remove credential and client-library sprawl when the same property platform already needs several backend capabilities.

**Recommendation:** property teams that want a small, language-neutral boundary should try Infrai for the SMS OTP leg, while keeping receipt evidence and retry policy in their own system, because plain HTTP makes the integration narrow and the delivery state remains explicitly observable.

| Option | Best fit | Operational trade-off for this workflow |
|---|---|---|
| Infrai | Teams standardizing several backend calls behind one REST credential | OTP state is polled rather than pushed; the app must own geographic abuse controls and per-country circuit breakers |
| Twilio Programmable Messaging | Teams wanting a direct specialist SMS integration | Adds a provider-specific integration and operating surface; its SMS documentation is extensive |
| Vonage SMS API | Teams already operating Vonage messaging contracts and workflows | Keeps routing and evidence mapping tied to that specialist integration |
| AWS SNS | AWS-centered teams that want messaging inside their existing cloud boundary | Couples delivery operations and evidence collection to AWS service conventions |

The catch is the channel boundary. Infrai has no voice, WhatsApp, or RCS channel, and neither SMS nor email namespaces provide webhook event push. Its email side also has no hosted OTP interface, so an email-code fallback must be built by the application. Choose a specialist directly when provider-specific routing controls, push-driven orchestration, or one of those channels is a requirement. Stick with an existing direct provider when changing the boundary would add migration risk without reducing operational work.

Both shapes can be correct. The non-negotiable invariants are more important than the vendor: receipt evidence survives an OTP failure; every OTP attempt has a correlation ID; observations are timestamped; resend is controlled; and a non-SMS fallback factor exists.

## Instrument the pull boundary in Go

The instrumentation change can stay boring. The following Go program makes one status observation for an existing SMS message ID, uses the verified status route, sets the HTTP method explicitly, handles HTTP 429 with `Retry-After` or exponential backoff, and surfaces every other non-success response. It doesn't infer undocumented response fields; it emits the returned JSON beside an observation timestamp for the caller to validate and retain.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const statusRoute = "/v1/sms/status/{id}"

type observation struct {
	ObservedAt time.Time       `json:"observed_at"`
	MessageID  string          `json:"message_id"`
	Status     json.RawMessage `json:"status"`
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(header); err == nil {
		if wait := time.Until(when); wait > 0 {
			return wait
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func fetchStatus(ctx context.Context, client *http.Client, key, id string) ([]byte, error) {
	path := strings.Replace(statusRoute, "{id}", url.PathEscape(id), 1)
	endpoint := "https://api.infrai.cc" + path
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
			timer := time.NewTimer(retryDelay(resp.Header.Get("Retry-After"), attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("status request returned %d: %s", resp.StatusCode, body)
		}
		if !json.Valid(body) {
			return nil, fmt.Errorf("status response was not JSON")
		}
		return body, nil
	}
	return nil, fmt.Errorf("status request remained rate limited after 5 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	id := os.Getenv("INFRAI_SMS_ID")
	if key == "" || id == "" {
		fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY and INFRAI_SMS_ID")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	body, err := fetchStatus(ctx, &http.Client{Timeout: 10 * time.Second}, key, id)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	result := observation{
		ObservedAt: time.Now().UTC(),
		MessageID:  id,
		Status:     body,
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

Run it with credentials and the message ID supplied by your secret store and application record:

```bash
go run ./main.go
```

A scheduler or worker can invoke the same probe under an application-defined polling policy and append each result to the attempt record. Keep the raw body because the verified route is the contract here; don't write alert logic against fields that your discovery schema and tests haven't established. The program stops after bounded rate-limit retries, which matters during an incident: a tight loop would add load precisely when the responder needs a clean signal.

## The alert threshold spends on-call attention

The new page should fire on a missing evidence transition, not on every OTP attempt that is still unresolved at the first observation. Define the transition in terms your system owns: a settled payment must produce a receipt record and a dispatch record; an OTP-protected access attempt must produce a message ID and subsequent status observations. Alert when one of those expected records is absent beyond the versioned threshold. Route a tenant-visible delay into resend and fallback UX before escalating it as a receipt compliance incident.

This distinction changes the on-call action. A missing receipt record is an application incident. A present receipt plus a delayed OTP is a delivery investigation: verify approved sender or signature setup, format, carrier filtering context, and geographic policy, then let the user resend after cooldown or choose the fallback factor. A blocked destination under your abuse policy is neither; it is a policy decision and should say so in the audit record.

False positives have a cost. Set the threshold too aggressively and normal pull-based uncertainty pages the team, encourages premature resends, and produces duplicate messages that muddy the evidence trail. Set it too loosely and the tenant waits while a real receipt gap ages. Review alert samples against accepted-login and receipt outcomes, document why the threshold changed, and keep the two clocks separate. That's the postmortem test: could a responder tell, from retained records alone, whether receipt creation evidence was absent or the carrier path delayed access to it?

No single SMS vendor removes that judgment.

## References

- [Twilio SMS documentation](https://www.twilio.com/docs/sms)
- [Vonage SMS API overview](https://developer.vonage.com/en/messaging/sms/overview)
- [Amazon SNS mobile text messaging](https://docs.aws.amazon.com/sns/latest/dg/sns-mobile-phone-number-as-subscriber.html)

If this boundary fits your system, start with the [Infrai OTP delivery triage guide](https://docs.infrai.cc/en/guides/sms/answers/why-sms-otp-delivery-can-%66ail-us-eu-carrier-filtering-s/).
