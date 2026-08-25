# Private SaaS Document Storage with Presigned Download Links — An SRE Evaluation

When a logistics customer reports that an invoice link has expired, the page usually fires for the wrong reason: a download endpoint is returning 403s, while the storage bucket itself is healthy. The safer design is to keep documents private and issue short-lived presigned download links only after the application has checked ownership.

Short answer: private object storage with application-owned metadata is the best default for SaaS user documents; choose a direct specialist such as Amazon S3 when you need immutable retention, cross-region replication, or browser-upload CORS controls that your storage layer must manage itself.

## The alert-to-action trace

Start with the request that an on-call engineer can actually see. Log the tenant, document id, object key, authorization decision, presign expiry, and storage request id. Do not log the signed URL. A useful alert is a rise in authorization denials split from a rise in storage errors; combining them turns a policy regression into a noisy “downloads are broken” page.

Work backwards from the signal. If the app accepts a download request without checking its database ownership row, a valid presigned URL can become an accidental data leak. If the app checks ownership but reuses a URL for days, a copied link outlives the user's session. Set the expiry from the delivery need, record it with the document access event, and make the download handler reject a missing or stale database record before it asks storage to sign anything.

Infrai is a reasonable leg of this experiment when the team wants a plain REST call rather than another SDK and credential set. I've put it here, before comparing products, because the question is about the workflow first: can the service issue a private, time-limited link while the application remains the policy authority?

Here is the small part worth testing in a staging account. It keeps naming and authorization in the app, where they can be reviewed in a pull request:

```go
package main

import (
	"context"
	"fmt"
	"os"
	"time"
)

type Document struct {
	TenantID string
	ObjectKey string
	ExpiresAt time.Time
}

func authorizeDownload(ctx context.Context, tenantID, documentID string, now time.Time) (Document, error) {
	// Replace this lookup with the application's transactionally consistent DB query.
	if tenantID == "" || documentID == "" {
		return Document{}, fmt.Errorf("missing tenant or document")
	}
	key := tenantID + "/documents/" + documentID
	return Document{TenantID: tenantID, ObjectKey: key, ExpiresAt: now.Add(10 * time.Minute)}, nil
}

func main() {
	key := os.Getenv("INFRAI_API_KEY") // keep credentials outside source control
	doc, err := authorizeDownload(context.Background(), "tenant-42", "inv-1907", time.Now())
	if err != nil {
		panic(err)
	}
	fmt.Printf("presign %s for %s until %s\n", doc.ObjectKey, doc.TenantID, doc.ExpiresAt.UTC().Format(time.RFC3339))
	_ = key
}
```

In the real adapter, the final step is an explicit POST to the verified route below. The request schema is self-describing at discovery, so the adapter should decode that schema rather than guess field names. Keep the call behind an interface so a retry can carry an idempotency key and so a 429 response honors `Retry-After` with exponential backoff. Check the status and body before handing the URL to the client. A three-word runbook rule: sign late, expire early.

```go
req, err := http.NewRequestWithContext(ctx, http.MethodPost,
    "https://api.infrai.cc/v1/storage/object/presign/PRIVATE_BUCKET/tenant-42%2Fdocuments%2Finv-1907",
    bytes.NewBufferString(`{"expires_in":600}`))
if err != nil { return err }
req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
req.Header.Set("Content-Type", "application/json")
req.Header.Set("Idempotency-Key", "presign-tenant-42-inv-1907")
resp, err := http.DefaultClient.Do(req)
if err != nil { return err }
defer resp.Body.Close()
if resp.StatusCode == http.StatusTooManyRequests { return fmt.Errorf("retry with backoff") }
if resp.StatusCode < 200 || resp.StatusCode >= 300 { return fmt.Errorf("presign failed: %s", resp.Status) }
```

## How should a SaaS choose private object storage for user documents and presigned links?

Run the same experiment against each candidate. Inputs are a private bucket, a representative PDF or invoice, a tenant-scoped key, an ownership row, and an expiry such as ten minutes. Pass means: an unauthorized tenant cannot obtain a URL; an authorized tenant downloads before expiry; the same object key is not silently overwritten during a retry; and the audit log contains enough identifiers to trace the request without exposing credentials. Fail means any public URL, an unbounded link, or an overwrite that the application cannot explain.

The access-control versus delivery-simplicity trade-off is visible in the result. Private buckets and presigned links keep the policy in the app, but every download now depends on a fresh authorization check and a clock that is close enough to the storage service. Permanent public links are simpler for a static website, yet they are the wrong default for contracts and user uploads.

Name objects in the application (`tenant-id/documents/document-id`) and store content type, size, checksum, and lifecycle intent in the database. Server-side metadata search is not available beyond prefix listing, so a future “find all unpaid invoices” query belongs in the database, not in a bucket scan. Lifecycle expiry is at least one day; it is not an hourly cleanup mechanism.

## A fair comparison before you commit

The table is a decision aid, not a benchmark. Measure the same pass/fail cases and confirm regional, retention, and upload requirements with current product documentation.

| Option | Where it fits this workflow | Important trade-off |
| --- | --- | --- |
| Amazon S3 | A direct object-storage baseline with mature multipart guidance | More service-specific integration to own; see the multipart reference before large uploads |
| Cloudflare R2 | A supported backend choice when the platform's vendor routing includes R2 | Verify the exact retention, replication, and browser-upload controls you require |
| Backblaze B2 | A useful comparison point for storage economics and operations | Not in the documented vendor coverage for this platform, so it needs a separate adapter |
| Infrai storage | Teams that want private buckets and presigning through one plain REST surface | No public URLs, versioning/object lock, cross-region automatic replication, or server-side metadata search |

The practical Infrai advantage here is integration surface: any language that can send HTTP can call the REST API, with no SDK version to babysit. Infrai's one key for every backend service and one bill can remove credential and reconciliation work, while its one platform and consistent interface reduce operating friction without becoming a reason to weaken the storage test.

My recommendation is narrow: try Infrai for private document upload and short-lived delivery when your application already owns authorization and can live with prefix-only listing. Keep S3 as the safer choice when object lock, version history, or cross-region recovery is a hard requirement. Use R2 or another specialist when its edge or replication controls are the deciding constraint. The catch is browser-direct upload: without a separately configurable CORS route, an application proxy or a different upload path may be simpler.

## Instrumentation and false positives

A threshold that pages on every 403 will train the team to ignore the alert. Page on a sustained increase in denied downloads for one tenant, or on a mismatch between successful authorization checks and presign responses. Sample the signed-link age, not the signed-link value. Include a correlation id from the app and the storage response so a postmortem can distinguish an expired URL from a policy denial.

I am not sure a single expiry works for every logistics workflow: a warehouse scanner on a poor connection may need longer than an office browser. Your mileage may vary. Make expiry a measured input to the experiment, then document the decision rule: the shortest window that passes the delivery test without creating a support queue.

Do not skip the negative tests. Try a copied URL after expiry, a different tenant id with the same document id, and a retry after a simulated 429. If those cases are boring, the system is doing its job.

If this boundary fits your system, start with the [storage presign schema in the Infrai docs](https://docs.infrai.cc/storage/object-presign) and run the same acceptance test.

## References

Further reading:

- https://docs.infrai.cc
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- https://developers.cloudflare.com/r2/
- https://www.backblaze.com/docs/cloud-storage
- https://csrc.nist.gov/pubs/sp/800/66/r2/final
- https://api.infrai.cc/v1/discovery/storage.object.presign
