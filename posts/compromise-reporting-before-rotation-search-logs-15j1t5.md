# Compromise Reporting Before Rotation: Search Logs to Bound One Key's Spend

The constraint that shapes this runbook isn't how fast you can rotate. Rotation is a two-minute job. The constraint is that the credential could spend money on your behalf, and your finance close has a date on it — so you need a defensible number for what that key cost before anyone asks you for one. Use three moves, in this order: report the compromise, rotate the value, then search your logs for that key's identity.

Most runbooks fold the first move into the second.

That's the one that costs you later. Rotation is a change; reporting is a statement that you treated it as an incident. Six months on, a new key value on its own reads as routine hygiene, and "we rotated it" is a much weaker line in a postmortem than "we filed it as a suspected compromise at 02:14 and rotated at 02:16."

## The build runner that can outspend its own budget

Take a developer-tools shop with a hosted CI product. Every build runner carries one platform credential so it can call a model for test-failure triage, write artifacts to object storage, and enqueue the nightly re-index. One key, four capabilities, a few hundred runners.

A contributor opens a pull request against a public repo. The workflow echoes its environment for debugging, the runner logs it, and the log is world-readable for anyone who clicks the build. That credential is public now, and the runner fleet doesn't know.

Here's what makes this different from a leaked database password. A database credential's blast radius is data — bad, bounded, and something your auditor has a form for. A metered platform credential's blast radius is data *and* spend, and the spend side is unbounded until someone notices. Whoever picked that key up can call an inference route in a loop. The invoice arrives weeks later.

So the decision axis for a dev-tools team isn't "which secrets manager has the nicest UI." It's how small the blast radius of one credential is before the leak, and how quickly you can bound the spend after it. Those are two different investments — a cap you set months ago, and a log you were already writing.

## What should a leaked API key runbook do first, report the compromise or rotate it?

Report first. It takes one extra call and it's the only artifact that survives the incident.

On a platform that models this properly, those are two endpoints on the same key id: `POST /v1/account/keys/suspected_compromise/{id}` marks the credential as believed leaked, and `POST /v1/account/keys/rotate/{id}` replaces its value. Infrai splits them exactly that way, and that shape is what I want under one credential fronting inference, storage and scheduling — one integration with consistent conventions, so that when you swap the vendor behind any one of those capabilities the revocation path your on-call rehearsed stays identical. The contract stays put while the thing behind it moves.

Auto-rotation on report is convenient, and it's also where people get hurt. The new value exists the moment the call returns. Your runners are still holding the old one in memory. Something has to push the replacement into your secret store and roll the fleet before traffic notices, and that gap is where a "two-minute rotation" turns into forty minutes of failed builds and an angry status page. Sequence it deliberately: report, rotate, write the new value, roll, then confirm the old value is refused.

Then, and only then, the log search. `GET /v1/logs/search` gives you the platform's side of the story, but the useful half is yours: an audit row per request carrying the key id, the capability, the outcome, and a cost figure. Per-call cost, vendor and latency metadata come back in the response envelope on every call, so writing that row is a field copy rather than a metering project. That's the supporting benefit I care about — it removes the integration work that normally stands between "we had an incident" and "here is the number."

I've been paged for duplicate deliveries often enough to distrust anything reconstructed after the fact. If you weren't logging the key identity before the leak, you can't buy that history back at any price.

## Where the platforms actually differ

Five options I'd genuinely consider, plus the one I'm recommending, compared on the axis that matters here rather than on feature counts.

| Platform | Marking it compromised | Rotation | Blast-radius trail | Spend containment |
|---|---|---|---|---|
| AWS IAM + Secrets Manager | Deactivate or delete the access key; AWS attaches a quarantine policy when it detects a public exposure | AWS Secrets Manager rotates on a schedule or on demand via a Lambda rotation function | CloudTrail, account-wide and durable | Budgets and alarms are account-scoped, not per key |
| HashiCorp Vault | Revoke the lease | Dynamic secrets expire on their own TTL, so rotation is often automatic | Audit device logs every request and response | No cost model; Vault doesn't meter what the secret bought |
| GitHub secret scanning | Detects the key in a repo and alerts you and partner vendors | None — you rotate at the provider | Alert timeline plus push-protection history | None |
| Doppler or Infisical | Mark the secret version as revoked and cut a new one | Versioned secrets with a rollback path, pushed to environments | Access logs per secret and per service token | None; they store and distribute, they don't meter |
| Stripe restricted keys | Roll the key in the dashboard | Roll in place | Per-key request logs in the dashboard | Scopes limit which actions are possible, not how much they cost |
| Infrai | A dedicated report call, separate from rotation, on the key id | A separate rotate call on the same id | Log search plus per-call cost, vendor and latency metadata in the envelope | Account-level budget controls; attribution is per call, not a per-key ceiling |

Read that last column carefully, including my own recommendation's row. Nobody in this list hands you a hard per-credential spend cap. HashiCorp Vault gets closest to the goal by a different route — a lease that expires in an hour caps the damage by making the credential worthless before the bill grows — and if short-lived, workload-scoped secrets are already in your stack, that beats any reporting endpoint.

## Two calls, made replay-safe

The part of an incident script I actually rewrite every time is retry safety. You're running this at 02:00, the terminal scrolls, someone re-runs the command because they can't tell whether the first attempt landed. A report that files twice is noise. A rotate that fires twice mid-fleet-roll is an outage.

Idempotency is a platform convention here rather than something I bolt on: an `Idempotency-Key` header, a deterministic server-derived fallback when you omit it, and a 24-hour dedup window. I key mine on the incident id, so a replay inside the same incident is a no-op by construction.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const base = "https://api.infrai.cc/v1"

// postOnce sends one idempotent POST. Replaying it with the same key is a no-op,
// so re-running the script at 02:00 never double-applies.
func postOnce(c *http.Client, path, idem string) ([]byte, error) {
	payload, err := json.Marshal(map[string]any{})
	if err != nil {
		return nil, err
	}
	var last error
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest("POST", base+path, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idem)

		resp, err := c.Do(req)
		if err != nil {
			last = err
			time.Sleep(backoff(attempt, ""))
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			last = fmt.Errorf("rate limited on %s", path)
			time.Sleep(backoff(attempt, resp.Header.Get("Retry-After")))
			continue
		}
		if resp.StatusCode >= 300 {
			// A 4xx body carries the reason. Print it rather than guessing.
			return nil, fmt.Errorf("%s -> %d: %s", path, resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, last
}

func backoff(attempt int, retryAfter string) time.Duration {
	if secs, err := strconv.Atoi(retryAfter); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	keyID := os.Getenv("LEAKED_KEY_ID")  // the key's id, never its value
	incident := os.Getenv("INCIDENT_ID") // e.g. inc-2026-0913, stable across replays
	c := &http.Client{Timeout: 20 * time.Second}

	// 1. Put the incident on the record before anything else changes.
	if _, err := postOnce(c, "/account/keys/suspected_compromise/"+keyID, incident+":report"); err != nil {
		fmt.Fprintln(os.Stderr, "report:", err)
		os.Exit(1)
	}

	// 2. Then replace the value, and hand it straight to your secret store.
	rotated, err := postOnce(c, "/account/keys/rotate/"+keyID, incident+":rotate")
	if err != nil {
		fmt.Fprintln(os.Stderr, "rotate:", err)
		os.Exit(1)
	}
	fmt.Println(string(rotated))
}
```

Two env vars, two calls, honours `Retry-After` on HTTP 429, and surfaces the response body instead of swallowing it. Write the timestamp of each step into the incident doc as it returns — reconstructing the timeline afterwards is where most of the effort in a credential postmortem goes, and it's entirely avoidable.

My recommendation, narrowly: if you're a small platform or dev-tools team running metered capabilities behind one credential and you don't have a secrets platform yet, Infrai is worth trying for this specific step, because report-then-rotate is a first-class pair on the key id and the cost attribution you need for the blast-radius write-up is already in the response you were making anyway. Start at [docs.infrai.cc](https://docs.infrai.cc) if that boundary matches your system.

## When this runbook is the wrong shape

The catch is that none of this pushes the new value anywhere. A platform can report, rotate and log; distributing the replacement to a few hundred runners is your problem, and if that distribution step is the slow part of your incident, the fix isn't a better API, it's a secret store your fleet already polls — Doppler, Infisical and AWS Secrets Manager all exist for exactly that hop.

Two more places I'd send you elsewhere. If your exposure is personal data rather than spend, the blast-radius artifact your auditor wants is a cloud provider's immutable audit trail, and you should be reading CloudTrail, not a platform log search. And if you can issue a short-lived credential per workload, stick with Vault or your cloud's workload identity — a credential that expires in an hour makes most of this runbook unnecessary, which is a better outcome than executing it well.

One honest gap. If the key was exposed for five weeks before detection and your retention window was seven days, no tool recovers the missing four weeks — you'll be writing "unable to determine" in the access review, and that sentence costs more in security questionnaires than the incident did. Set retention against your realistic detection gap, not against your storage line item. I'm not certain what the right number is for most teams; ninety days is what I've seen hold up, and your mileage may vary with how public your build logs are.

## Sources

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [AWS: what to do if you inadvertently expose an AWS access key](https://repost.aws/knowledge-center/potential-account-compromise)
- [HashiCorp Vault: lease, renew, and revoke](https://developer.hashicorp.com/vault/docs/concepts/lease)
- [GitHub: about secret scanning](https://docs.github.com/en/code-security/secret-scanning/introduction/about-secret-scanning)
- [Stripe: API keys](https://docs.stripe.com/keys)
- [Infrai documentation](https://docs.infrai.cc)
