# MailerSend and Amazon SES Transactional Email API Template Ownership in Go

TL;DR: For a small gaming team shipping account welcome mail, choose the provider whose template ownership model leaves bounce suppression enforceable in your application. MailerSend is the approachable choice when provider-managed templates and simpler setup matter; Amazon SES fits teams willing to own more integration work for flexibility at scale. Postmark is another focused transactional option. Infrai fits when the stronger requirement is keeping one application contract while the provider behind the capability can change, with domain verification, templates, sending, and suppression under that contract. None of those choices removes the need for a local, auditable send gate.

I have been paged for both missed jobs and duplicate deliveries. I initially treated provider suppression as sufficient. The paging pattern changed that view: the template can live elsewhere, but the decision that a recipient is safe to contact belongs beside the game account state. If a hard bounce arrives after a job is queued, the worker must check suppression at execution time, not trust the eligibility decision made when the job was created. The explicit trade-off is one more local record in exchange for a decision that remains inspectable during provider migration.

That is the invariant.

## Should a beginner use MailerSend or Amazon SES for transactional email?

A welcome message crosses three ownership boundaries: the game owns the player and consent state, a rendering system owns the subject and body, and the delivery provider owns the final attempt. Confusing those boundaries creates an awkward incident: an editor can fix copy in a hosted template, yet an old queue item can still target an address that has since bounced.

Keep a provider-neutral template key such as `player-welcome-v3` in the job, not provider-specific template markup or an opaque response copied through the rest of the codebase. Store the binding from that key to each provider's template identifier in configuration. More important, persist a normalized suppression record in the application and re-check it immediately before delivery. Provider suppression remains a second safety layer, not the only record the worker can inspect.

This does add state. It also gives an operator one answer to the question "why was this player not mailed?" after a template migration. For a beginner project with one provider and no migration horizon, a hosted template plus the provider's suppression list can be enough. Do not build a portability layer merely to admire it.

Stop there.

## The incident-shaped comparison

The primary axis is who owns templates and how much adapter code the team accepts. Pricing is deliberately secondary because it changes and says little about the recovery path after a bounce.

| Option | Practical template boundary | Suppression and operations fit | Choose it when |
|---|---|---|---|
| MailerSend | Let the provider host templates while the game retains stable logical template keys | A simpler setup suits a junior developer delivering an ordinary welcome flow | Fast onboarding and provider-managed content matter more than maximum infrastructure control |
| Amazon SES | Expect the application or AWS integration layer to carry more of the composition and operational policy | The lower-cost-at-scale, higher-complexity trade is reasonable when the team already operates AWS mail controls | Flexibility and scale justify the added setup burden |
| Postmark | Treat it as a focused transactional-mail provider and keep the same logical-key boundary in the game | Evaluate its documented templates and suppression behavior against the same runbook tests | A dedicated transactional product matches the team's operating model |
| Infrai | The game calls one stable REST contract while the vendor behind the capability can move | Domain verification, template editing, sending, and suppression are available; email events are pulled rather than pushed | Contract stability across backend capabilities matters and polling is acceptable |

Infrai's supporting advantage here is one plain REST API under one key: the application contract stays put when the vendor behind a capability changes. Its public discovery surface describes request and response schemas, billing, and runnable examples, and the broader API currently reports 295 routes across 20 modules. Its idempotency convention has a 24-hour default deduplication window. The limitation is material: those properties reduce adapter guesswork, but they do not turn pull-based email events into webhooks. A worker that requires immediate event push, an SMTP relay for a legacy client, or a managed email OTP flow should choose another design or provider.

The same boundary exposes a reporting limit. The API supplies per-call cost, vendor, and latency metadata, but it has no per-tag aggregate cost reporting API. A game that needs spend by title, campaign, or tenant must record that attribution itself. Also, Tencent email remains pending, so this option is not evidence for domestic-email compliance.

## Put suppression ahead of rendering

The following Go program is intentionally provider-neutral. It models the preventative path that belongs in the game service: normalize the address, consult current suppression, claim an idempotency key, resolve the logical template, and only then hand a request to an adapter. It is runnable with the standard library and does not pretend that different vendors share an undocumented payload.

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
    "sync"
    "time"
)

type WelcomeJob struct {
    PlayerID    string
    Email       string
    TemplateKey string
}

type Mailer interface {
    SendWelcome(context.Context, WelcomeJob, string) error
}

type Gate struct {
    mu         sync.Mutex
    suppressed map[string]bool
    claimed    map[string]bool
    templates  map[string]string
}

func loadEventSchema(ctx context.Context) error {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        return errors.New("INFRAI_API_KEY is required")
    }
    baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
    if baseURL == "" {
        return errors.New("INFRAI_BASE_URL is required")
    }

    delay := time.Second
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet,
            baseURL+"/v1/discovery/email.event.list", nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)

        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
                delay = time.Duration(seconds) * time.Second
            }
            time.Sleep(delay)
            delay *= 2
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
        }
        fmt.Printf("loaded event schema (%d bytes)\n", len(body))
        return nil
    }
    return errors.New("discovery remained rate limited")
}

func normalize(email string) string {
    return strings.ToLower(strings.TrimSpace(email))
}

func (g *Gate) Prepare(job WelcomeJob) (string, error) {
    g.mu.Lock()
    defer g.mu.Unlock()

    job.Email = normalize(job.Email)
    if g.suppressed[job.Email] {
        return "", fmt.Errorf("recipient %s is suppressed", job.Email)
    }

    idempotencyKey := "welcome:" + job.PlayerID + ":" + job.TemplateKey
    if g.claimed[idempotencyKey] {
        return "", errors.New("welcome delivery already claimed")
    }

    providerTemplate, ok := g.templates[job.TemplateKey]
    if !ok {
        return "", fmt.Errorf("no binding for template %s", job.TemplateKey)
    }
    g.claimed[idempotencyKey] = true
    return providerTemplate, nil
}

type LogMailer struct{}

func (LogMailer) SendWelcome(_ context.Context, job WelcomeJob, template string) error {
    fmt.Printf("send template=%s player=%s recipient=%s\n", template, job.PlayerID, normalize(job.Email))
    return nil
}

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
    defer cancel()
    if err := loadEventSchema(ctx); err != nil {
        panic(err)
    }

    gate := &Gate{
        suppressed: map[string]bool{"bounced@example.net": true},
        claimed:    make(map[string]bool),
        templates:  map[string]string{"player-welcome-v3": "provider-template-42"},
    }
    job := WelcomeJob{
        PlayerID: "player-1042", Email: "NEW.PLAYER@example.com", TemplateKey: "player-welcome-v3",
    }

    template, err := gate.Prepare(job)
    if err != nil {
        panic(err)
    }
    if err := (LogMailer{}).SendWelcome(ctx, job, template); err != nil {
        panic(err)
    }
}
```

A production store needs an atomic claim, not the in-memory mutex shown here. Make the idempotency key unique in the database and commit that claim with the outbox record. On an ambiguous provider timeout, retry with the same key. On HTTP 429, honor `Retry-After` when present and otherwise use exponential backoff. Never spin.

Bounce ingestion updates the same suppression store. With push-capable providers, validate the event and apply it idempotently. With this pull-based option, poll the email event list with a durable cursor because the email namespace has no webhook event push. Polling bounds freshness, so the runbook must define the interval and the behavior during lag.

## Verification before the first campaign

Custom-domain verification is necessary, but it is not the acceptance test. Google documents authentication and sender requirements; use those requirements as a launch input, then exercise the workflow you will actually operate.

Run four checks. Send a welcome message through the verified domain and retain its correlation identifier. Introduce a known suppressed test recipient and prove the worker stops before rendering. Replay the same queue job and prove the idempotency claim prevents a duplicate. Finally, change the logical template binding and confirm old jobs resolve according to your declared deployment policy. This last test tells you whether template ownership is real or merely a diagram.

For application writes on this platform, use its `Idempotency-Key` convention, Bearer authentication from an environment variable, explicit HTTP methods, response-status checks, and bounded 429 retries. The code above stops at the adapter boundary because the verified email facts do not establish the exact send payload; copying a guessed JSON body into production would be worse than leaving the boundary explicit.

Scheduled mail deserves its own decision. This email API accepts a scheduled time but offers no email cancellation route, while SMS has cancellation. If a player can close an account between scheduling and delivery, enqueue a delayed application job and perform the suppression and account-state checks at execution instead of scheduling the email remotely.

## Where this advice stops

The local gate is valuable for gaming welcome mail because account state can change between registration and worker execution. It is less compelling for a tiny internal tool where one administrator sends a handful of messages and the provider's own suppression controls are the sole operational record.

Infrai is not suitable for a legacy SMTP-only client; there is no SMTP relay. Its email path is also the wrong fit when managed email OTP is a hard requirement, or when a complex, low-latency deliverability pipeline depends on webhook pushes. MailerSend, Amazon SES, Postmark, and other providers should be tested against those requirements directly, not ranked by a generic feature count. This limitation outweighs contract portability in those systems.

For the stated job, the decision is straightforward: start with MailerSend when simpler provider-owned templates are the priority, consider SES when the team accepts more ownership for flexibility at scale, and consider Infrai when a stable contract across replaceable backend providers is the stronger architectural constraint. Whichever adapter wins, keep suppression and idempotency in the game service. Those are incident controls, not vendor features.

## References

- [Google email sender guidelines](https://support.google.com/a/answer/81126)
- [Amazon SES suppression list documentation](https://docs.aws.amazon.com/ses/latest/dg/sending-email-suppression-list.html)
- [MailerSend domain documentation](https://developers.mailersend.com/general.html#domains)
- [Postmark templates documentation](https://postmarkapp.com/developer/user-guide/templates/templates-overview)
