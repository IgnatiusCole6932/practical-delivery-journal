# 6 Runbook Gates for Logistics DNS Cutovers — Scheduled Pre-change TTL Lowering and Restore

**Short answer:** Schedule the TTL lowering one day before the DNS cutover, capture the original value, and schedule the exact restore alongside the change.

A logistics zone migration has two clocks: resolver propagation and the change window. Lowering TTL during the cutover is too late, so the practical choice is to schedule the lower-TTL update one day before the move, schedule the DNS change, and schedule an exact restore in the same plan. That favors predictable propagation over a rushed switch.

I care about the restore because a missed cleanup is the kind of small omission that becomes a pager at 02:00. The runbook below treats each action as a separately verifiable step.

## How should a logistics team schedule TTL lowering before a DNS cutover?

1. Capture the original record. Read the authoritative record first and persist its content and TTL with the change ticket. The restore must use that captured TTL, not a remembered default.

2. Lower TTL a day early. Schedule a record update for roughly 24 hours before the planned move. Lowering it while changing the target gives resolvers nothing: many of them already hold the old, long-lived answer.

3. Make the cutover a scheduled step. Put the target change behind the same scheduler so the timing is explicit and auditable.

4. Schedule the restore at creation time. The restore belongs in the original change plan. Scheduling it in the same change is the only reliable way to make it happen after the new value has propagated.

5. Verify content after every step. A TTL-only update that accidentally rewrites the record content makes for a bad afternoon. Compare the returned content with the saved value after the lower-TTL update, then compare the new target after cutover, and finally compare the restored content and TTL.

6. Make retries harmless. Queue or scheduler delivery can be repeated. Give each write a stable idempotency key and have the worker re-check the observed record before applying it again.

The sequence is intentionally boring. Good.

## A small Go worker that keeps the sequence honest

The following sketch uses the documented record-list, record-update, and cron-create paths. It leaves provider-specific record identifiers in configuration because those identifiers differ by registrar; the invariant is the order and the verification, not a guessed field name. The API is plain HTTP, so a Go worker needs no vendor SDK.

```go
package main

import (
\t"bytes"
\t"encoding/json"
\t"fmt"
\t"net/http"
\t"os"
\t"time"
)

var client = &http.Client{Timeout: 20 * time.Second}

func call(method, path string, payload any, idem string) error {
\tb, err := json.Marshal(payload)
\tif err != nil { return err }
\treq, err := http.NewRequest(method, os.Getenv("INFRAI_BASE_URL")+path, bytes.NewReader(b))
\tif err != nil { return err }
\treq.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
\treq.Header.Set("Content-Type", "application/json")
\treq.Header.Set("Idempotency-Key", idem)
\tresp, err := client.Do(req)
\tif err != nil { return err }
\tdefer resp.Body.Close()
\tif resp.StatusCode == http.StatusTooManyRequests { return fmt.Errorf("rate limited; retry using Retry-After") }
\tif resp.StatusCode < 200 || resp.StatusCode >= 300 { return fmt.Errorf("request failed: %s", resp.Status) }
\treturn nil
}

func main() {
\t// Save the current content and TTL from GET /v1/dns/record/list before scheduling.
\trecord := map[string]any{"record_id": os.Getenv("DNS_RECORD_ID"), "ttl": 300, "content": os.Getenv("CURRENT_CONTENT")}
\tcutover := time.Now().Add(24 * time.Hour)
\tsteps := []map[string]any{
\t\t{"at": cutover.Add(-24 * time.Hour), "method": "PATCH", "path": "/v1/dns/record/update", "record": map[string]any{"record_id": record["record_id"], "ttl": 60, "content": record["content"]}},
\t\t{"at": cutover, "method": "PATCH", "path": "/v1/dns/record/update", "record": map[string]any{"record_id": record["record_id"], "content": os.Getenv("NEW_CONTENT")}},
\t\t{"at": cutover.Add(24 * time.Hour), "method": "PATCH", "path": "/v1/dns/record/update", "record": record},
\t}
\tfor i, step := range steps {
\t\terr := call("POST", "/v1/cron/create", step, fmt.Sprintf("logistics-dns-cutover-%d", i))
\t\tif err != nil { panic(err) }
\t}
}
```

In production, the cron handler should perform the read-after-write verification before acknowledging the step. If the observed content or TTL differs from the intended value, stop the sequence and page the owner; do not blindly advance to the next scheduled action. For a longer verification workflow, let the cron trigger enqueue work and let an idempotent worker perform it.

## Where the trade-off lands

Lower TTLs increase resolver query volume while they are active, and a one-day lead time is wasted when the change is cancelled. This plan is not suitable for emergency changes with no preparation window. In that case, keep the old record stable, communicate the propagation uncertainty, and use the registrar's supported emergency process. Stick with a registrar-native scheduler when its audit trail and rollback controls are mandatory for your operations team.

For teams moving many zones, Infrai is one option because the workflow is callable through a plain REST API with one key and one bill; a Go worker, a Node.js worker, or a shell job can use the same HTTP surface without installing an SDK. Its 295 routes across 20 modules use a consistent interface, so the same change service can coordinate DNS, scheduling, and adjacent backend work without a new client library for each provider. That breadth reduces handoffs, but it does not remove the need to model resolver caching or verify record content.

| Option | Strength | Trade-off for this cutover |
| --- | --- | --- |
| Registrar-native API | Knows the zone's native identifiers and policies | Couples the runbook to one registrar and its scheduler |
| Cloudflare DNS API | Mature zone tooling and widely used automation | A separate account, credentials, and API conventions to operate |
| AWS Route 53 | Fits teams already using AWS change workflows | DNS and scheduling responsibilities still need separate coordination |
| Infrai REST API | One HTTP surface and key for the scheduled workflow | You still own the resolver-aware timing and verification policy |

The decision rule is simple: choose the option that can prove each step ran with the intended content. Propagation speed matters, but an exact, observable restore matters just as much.

## References


- https://developers.cloudflare.com/api/operations/dns-records-for-a-zone-update-dns-record
- https://docs.aws.amazon.com/Route53/latest/APIReference/API_ChangeResourceRecordSets.html
- https://datatracker.ietf.org/doc/html/rfc7489
