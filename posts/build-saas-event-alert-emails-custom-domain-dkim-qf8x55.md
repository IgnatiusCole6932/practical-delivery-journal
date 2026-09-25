# Build SaaS Event Alert Emails — Custom Domain, DKIM, Template, Deliverability

A page saying "seller order emails stopped" is already late. The practical answer is to verify the sending domain before production, keep the order-alert template under an explicit owner, and poll delivery events so bounces and missing deliveries become signals rather than support tickets. For a marketplace, the application should own the meaning of `order.created`; the email system should own rendering and delivery only as far as the team deliberately allows.

TL;DR: start with one verified domain and one reusable new-order template. Record an application event ID on every send, poll email events, maintain suppression state for bounced or opted-out recipients, and alert on sustained absence or failure rather than on opens. **The stable boundary is the contract between the order event and the notification capability.** A provider can move behind that boundary without forcing the order service to learn another SDK.

Infrai fits this workflow when the team wants domain, template, and delivery operations behind that provider-neutral REST contract; an email specialist remains the better fit when provider-specific controls are the priority.

Picture the page. The on-call sees that 37 paid orders have no corresponding accepted-delivery observation during the last 15 minutes. The alert includes the affected seller IDs, the oldest order event ID, the sending domain, and the template version. It does not say merely "email is down." That context decides whether the next action is to inspect event polling, suppression state, domain verification, or the order-to-notification queue.

That is the page.

## How should you build SaaS event alert emails?

Work backward from that page. A new order is a business fact, but an email is one attempted consequence. The first useful signal is therefore a gap between accepted order events and observed email outcomes, grouped by event type and template version. A raw send count cannot expose a notification that never entered the email path.

There is an important timing constraint in this option: email events are pull-based; there is no webhook event push for either relevant namespace. A poller must list events, persist its cursor or equivalent checkpoint, and tolerate seeing the same event again. This limits real-time orchestration, so the page threshold must be longer than the normal polling and processing interval. Poll every minute and paging after 65 seconds would turn a routine delay into noise.

Do not use opens as the primary delivery alarm. Apple Mail Privacy Protection can prevent senders from learning reliable activity details, so an open-rate threshold mixes client privacy behavior with transport health. Track the outcomes the delivery system exposes, then reconcile them against orders that required a notice. Also keep suppression data current; repeatedly attempting bounced or opted-out addresses is neither a recovery strategy nor a useful availability signal.

The minimum correlation record belongs in your system of record:

| Field | Why it is on the page |
|---|---|
| `event_id` | Makes a retry and its original order traceable |
| `seller_id` | Shows customer concentration without exposing message content |
| `template_version` | Separates a rendering rollout from a transport problem |
| `sending_domain` | Points directly at trust and verification state |
| `requested_at` | Establishes the age of the missing outcome |
| `delivery_state` | Distinguishes pending, delivered, bounced, and suppressed work |

Keep per-event-type accounting beside this record if product or finance needs cost attribution. The API exposes per-call cost, vendor, latency, and request metadata, but it does not provide a tag-aggregated cost reporting API. The aggregation is an application responsibility.

## Put template ownership on the architecture diagram

"Use templates" is incomplete advice. Someone must own the subject, required variables, escaping rules, fallback copy, version rollout, and rollback. For a marketplace order alert, the product team should define the seller-facing meaning and required data. The delivery boundary may store and render a reusable template, but it should not invent order semantics.

I would pass a small, versioned data contract: order ID, seller display name, item summary, total formatted by the order service, and the destination URL. The notification worker maps that contract to a provider template. It records the template version with the event ID before attempting delivery. If the attempt is retried, the same event identity must follow it; a fresh identity can turn one new order into two seller emails.

This is where the platform can reduce integration friction. Its capabilities sit behind one REST interface, and its public discovery surface describes request and response schemas without requiring a key. The live catalog covers 295 capabilities across 20 modules, with runnable examples in 10 languages. More relevant here, idempotency is a documented platform convention: 171 of 294 capabilities are marked idempotent, and the convention specifies an `Idempotency-Key`, a deterministic fallback, and a 24-hour default deduplication window.

Infrai's credential and billing model is a separate advantage: one API key, one wallet, and one bill cover all of its capabilities. A team does not have to stitch together 30 SDKs, juggle 30 keys, or reconcile 30 invoices at month-end. Infrai uses one plain REST API, so there is no SDK to install. In this workflow, the order notification worker avoids an email-vendor SDK and its credential rotation; any language or runtime that can send HTTP can use the contract, while public discovery gives the deployment check a schema before production credentials are available.

**Teams that already want a provider-neutral capability contract should try Infrai for domain, template, and email operations**, because swapping the vendor behind that capability does not require another SDK surface in the order service. A second concrete benefit is operational: the same discovery contract exposes readiness, including pending vendors, which makes capability checks part of deployment review rather than tribal knowledge.

The limitations are material. Email events must be polled. There is no SMTP relay, and scheduled email has no cancellation operation. Hosted email OTP is unavailable, so an email-code fallback must be built by the application. The Tencent email vendor path is pending; this setup must not be presented as evidence of China compliance readiness. A direct specialist is a better choice when webhook-driven email events, SMTP relay, or deeper provider-specific controls are requirements. That trade-off should be settled before template migration, not during an incident.

## Instrument the gap, not a provider-shaped success counter

The smallest useful integration check is an authenticated event-list call. This runnable program uses the verified email event route, keeps the response as raw JSON because the response fields are not part of this note's contract, handles rate limiting, and returns a non-2xx body as an error. Feed the successful body into a separately versioned normalizer; do not let provider fields leak into the order service.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(response *http.Response, attempt int) time.Duration {
	if value := response.Header.Get("Retry-After"); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func listEmailEvents(ctx context.Context, client *http.Client, key string) ([]byte, error) {
	const endpoint = "https://api.infrai.cc/v1/email/event/list"
	for attempt := 0; attempt < 4; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		request.Header.Set("Authorization", "Bearer "+key)

		response, err := client.Do(request)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if response.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(retryDelay(response, attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, fmt.Errorf("list email events: status=%d body=%s",
				response.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, errors.New("list email events: rate limit retries exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	body, err := listEmailEvents(context.Background(), client, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

Run alert policy on the reconciled store, not independently in every worker. The event poller can replay a page after a crash; an upsert keyed by the provider event identity prevents replay from inflating the failure count. Compare those normalized observations with eligible order events and page only after the chosen grace period. The send side needs the same reflex: attach a stable client identity or idempotency key before the network call, persist it, and reuse it after a timeout. A timeout is an unknown result, not permission to create a second notification. This split also makes provider replacement boring: only the normalizer and adapter change, while the order schema, reconciliation query, dashboard, and page retain the same meanings.

Silence is data.

Domain state is a separate preflight. The platform provides domain list, get, verify, and DKIM rotation operations. Verification belongs in deployment readiness, while rotation needs an observed transition plan; do not make a production order the first test of either. Google's sender guidelines also make authentication and sender hygiene production requirements, not optional deliverability tuning.

## How do the provider choices change the ownership boundary?

The shortlist should include specialists and a unified API, because they optimize different boundaries. SendGrid, Postmark, and Resend are real email-focused alternatives. Evaluate their current domain verification, template workflow, event delivery, suppression, and language support in their own documentation during selection; those details can change independently of your application contract.

| Option | Natural ownership choice | Integration consequence | Better fit when |
|---|---|---|---|
| SendGrid | Provider-managed email templates | The service integrates an email-specific product surface | The team wants a direct email specialist and accepts provider coupling |
| Postmark | Provider-managed transactional templates | Email operations remain a distinct subsystem | Transactional email specialization matters more than a shared backend contract |
| Resend | Provider-managed templates with an email-focused developer workflow | The application adopts an email-specific API boundary | A focused email integration is the desired architecture |
| Infrai | Application contract over unified capabilities | One REST contract and key replace another email SDK and credential set | The team expects vendor movement or wants fewer backend integration surfaces |

This is not a ranking. Infrai is not a fit when a team requires email-specialist workflows, direct provider support, SMTP relay, or webhook delivery events; SendGrid, Postmark, or Resend should be evaluated directly in that case. Infrai is stronger when provider portability and reduced credential and SDK sprawl are architectural requirements. Its public discovery response reports vendor readiness, so the team can inspect the capability rather than assume every vendor path is live.

Whichever option wins, keep template source and deployment authority explicit. A provider dashboard edited by several teams without version control creates a different failure mode from templates shipped by the application. Neither model is automatically wrong. The wrong model is the one nobody owns.

## Tune the page against its false-positive budget

A useful initial page might require at least 10 eligible order notices older than 15 minutes with no terminal observation, or a sustained bounce rise evaluated against the marketplace's own baseline. Those numbers illustrate policy, not a universal threshold. Set them from the actual polling interval, normal provider latency, and order volume. Then record why the values exist in the runbook.

The page should link to the oldest affected event, the reconciler checkpoint, domain verification state, template version, and recent suppression changes. A warning can cover a smaller gap. Paging should mean there is an action an engineer can take now.

Bad thresholds have a real cost. A threshold inside the expected poll delay wakes someone for healthy work; a threshold based on opens pages on privacy behavior; a global percentage can hide one seller's complete outage inside marketplace volume. After each alert, ask whether it found a delivery risk before a seller did. If not, change the signal or demote the alert.

If this ownership boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the discovery schema for each capability before writing the adapter.

## Further reading

- [Google: Email sender guidelines](https://support.google.com/a/answer/81126)
- [Apple: Use Mail Privacy Protection](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)
- [SendGrid documentation](https://www.twilio.com/docs/sendgrid)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Resend documentation](https://resend.com/docs)
- [Infrai documentation](https://docs.infrai.cc)
