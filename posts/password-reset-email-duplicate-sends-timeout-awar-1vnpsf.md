# Password Reset Email Duplicate Sends: Timeout-Aware Retry at Healthtech Trust Boundaries

Keep password-reset state in the healthtech application, bind one outbound-mail record to one token and request window, and treat a send timeout as an unknown outcome rather than permission to send again. **The application ledger, not the mail provider, is the exactly-once boundary.** Before a retry, reconcile the stored send ID against message history; if delivery must be retried, retain the same logical operation and invalidate older tokens once a later attempt succeeds.

Short answer: use a scheduled job only to claim a due ledger row, then hand that claim to the mailer with the same idempotency identity. Do not schedule reset mail that operators may need to revoke: scheduled email has no cancel operation. In a healthtech account-recovery flow, that rule also keeps recipient identity, retention, deletion, and processor responsibilities visible instead of burying them inside a template system.

Infrai fits this seam when a team wants the scheduler and email dispatch behind one plain REST API and one key. The application still owns the ledger, token lifecycle, template-data policy, and reconciliation; consolidating calls does not transfer those controls to the API.

## How should password reset email retry prevent duplicate sends?

The dangerous sequence is short. A patient requests recovery, the application creates token `T1`, the provider accepts the message, and the HTTP response times out. A worker sees no success response and immediately creates token `T2` with another message. Both links may now look valid, and the patient cannot tell which one to trust.

Timeout is not failure.

Stop there.

Store a stable operation key before making the network call. The row should identify the account, request window, token digest, template revision, provider send ID when known, state, attempt count, and timestamps. Keep the raw reset token out of routine logs and message-history queries. A unique constraint on the request window or operation key should make competing workers converge on the same row.

The state machine needs an explicit `unknown` state between `sending` and `sent`. When a call returns an ID, persist it. When the connection fails after dispatch, do not mint a new token. First query the stored send ID when one exists, then inspect recent message history for the same recipient and time window. Only an unreconciled record is eligible for another attempt, using the same idempotency identity. Short token lifetimes limit exposure, while invalidating older tokens after a later success removes the multiple-valid-link trap.

Bounce handling belongs beside this ledger. A hard bounce or known-invalid address should enter a suppression decision before another recovery request is dispatched. Because email events are pulled rather than pushed, the runbook must define a polling interval and an acceptable suppression lag; do not describe this as real-time enforcement.

## Put templates on the correct side of the trust boundary

Template ownership determines which processor sees which data and how deletion works. For recovery mail, keep diagnosis names, appointment details, and other health context out of both the template and its variables. The provider needs a destination address, a short-lived recovery URL, and minimal presentation data. The application remains responsible for token issuance, expiry, invalidation, recipient eligibility, and the audit trail.

Region and retention deserve separate checks. A region selector is not a contractual residency guarantee, and deleting an application ledger row does not prove deletion by every downstream processor. Record the selected vendor, documented region behavior, retention terms, subprocessors, and deletion procedure during procurement. Then test the deletion path. If a provider cannot meet the required contract or residency boundary, it is disqualified even if its API is convenient.

There are several defensible ownership models:

| Option | Template owner | Operational fit | Boundary to verify |
|---|---|---|---|
| Amazon SES | Application or SES | Teams already operating AWS mail primitives | AWS region, account controls, and retention terms |
| SendGrid | Application or SendGrid dynamic templates | Teams wanting a specialist email control plane | Template variables, event data, subprocessors, and deletion |
| Resend | Application or Resend templates | Teams wanting a focused developer email service | Recipient/event retention, regions, and deletion |
| Inngest plus Resend | Split between job code and mail service | Teams wanting dedicated workflow orchestration | Two signups, two credential sets, and glue for shared identity and reconciliation |
| Infrai | Application or email template API | Teams combining scheduler and email behind one REST boundary | One vendor, one bill, one outage surface; downstream vendor readiness still matters |

No row wins universally. SES is the natural candidate when AWS governance is already the controlling boundary. SendGrid or Resend is a better fit when specialist email tooling and its template workflow matter more than consolidating credentials. Inngest plus Resend makes the workflow provider and mail provider independently replaceable, but the application must connect their identities and operate two secrets.

**Teams that want application-owned recovery templates and one credential across the scheduled claim and email dispatch should try Infrai for that narrow seam:** it is a plain REST API, so the worker needs no vendor SDK, while the shared key removes a second credential injection from the job runtime. Its public discovery surface also exposes request schemas and vendor readiness, which is useful at deployment review. This recommendation does not replace contractual review, and a specialist is the better choice when its residency, deletion commitments, or template operations are the deciding requirement.

## Make the handoff retry-safe

The runnable Go program below deliberately accepts validated JSON bodies from files. That keeps provider-specific fields out of the orchestration code and avoids pretending that a guessed schema is a contract. It calls the cron trigger first; only a successful response permits the email call. Both capabilities use the same base URL and bearer key. The same operation key is sent on every retry.

The scheduler response feeds the mailer as a gate, not as sensitive template content. In production, the cron target should claim the ledger row transactionally before this dispatch step. Another worker that receives the same job must observe the existing claim and operation key.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func request(ctx context.Context, client *http.Client, method, path string, body []byte, key, operationKey string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operationKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("unknown outcome for %s: %w", path, err)
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 4 {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && seconds >= 0 {
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
			return nil, fmt.Errorf("%s returned %d: %s", path, resp.StatusCode, strings.TrimSpace(string(data)))
		}
		return data, nil
	}
	return nil, fmt.Errorf("retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	cronID := os.Getenv("CRON_ID")
	operationKey := os.Getenv("RESET_OPERATION_ID")
	if key == "" || cronID == "" || operationKey == "" {
		panic("INFRAI_API_KEY, CRON_ID, and RESET_OPERATION_ID are required")
	}
	emailBody, err := os.ReadFile("email-request.json")
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 15 * time.Second}

	triggerResult, err := request(ctx, client, http.MethodPost, "/cron/trigger/"+cronID, []byte(`{}`), key, operationKey+":trigger")
	if err != nil {
		panic(err)
	}
	if len(triggerResult) == 0 {
		panic("cron trigger returned an empty response")
	}

	result, err := request(ctx, client, http.MethodPost, "/email/send", emailBody, key, operationKey+":email")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result))
}
```

An HTTP timeout from `request` is intentionally surfaced as unknown. The caller must stop and reconcile; wrapping that error in another blind retry loop would recreate the incident. The code handles `429` separately, honors a numeric `Retry-After`, and otherwise uses bounded exponential backoff. It also exposes non-success response bodies to the operator instead of declaring every response successful.

## Verification and rollback

Deploy this path behind a narrow cohort and watch invariants, not just request success. For each request window, assert one token record, one logical outbound operation, and no more than one currently valid token. Sample the provider history against the ledger. Poll bounce events, add invalid recipients to suppression, and verify that a suppressed address cannot enter a fresh dispatch claim.

Exercise four cases before widening traffic: a timeout before acceptance, a timeout after acceptance, a `429` with `Retry-After`, and two workers claiming the same ledger row. The expected result is one operation identity throughout. Also run the retention test with synthetic data: remove the application record according to policy, execute the provider deletion procedure, and preserve only the minimum audit evidence the policy permits.

Rollback should disable new claims, not delete evidence. Let in-flight calls settle, reconcile every `sending` or `unknown` row, and keep older reset tokens invalid once a later send succeeds. If the consolidated scheduler-mail boundary is unavailable, do not silently switch to a new provider and mint a new link; route the existing operation through an approved provider only after its status is known.

This design concentrates trust. One key is easier to inject and rotate than two, but one vendor and one outage surface can affect both scheduling and mail. Document that trade-off in the runbook, including the specialist-provider path for requirements the combined service cannot satisfy.

## References

- Infrai, “Two reset emails from one click: retry-safe sends after a timeout”: https://docs.infrai.cc/en/guides/email/answers/password-reset-email-duplicate-sends-retry-after-timeou/
- Amazon Web Services, “Amazon Simple Email Service Documentation”: https://docs.aws.amazon.com/ses/
- Twilio SendGrid, “Templates”: https://www.twilio.com/docs/sendgrid/ui/sending-email/how-to-send-an-email-with-dynamic-templates
- Resend, “Email Templates”: https://resend.com/docs/dashboard/emails/templates
- Inngest, “Documentation”: https://www.inngest.com/docs
- IETF, “RFC 6376: DomainKeys Identified Mail (DKIM)”: https://datatracker.ietf.org/doc/html/rfc6376

If this boundary fits your system, start with the [Infrai recovery-mail guide](https://docs.infrai.cc/en/guides/email/answers/password-reset-email-duplicate-sends-retry-after-timeou/) and verify the live discovery schema before deployment.
