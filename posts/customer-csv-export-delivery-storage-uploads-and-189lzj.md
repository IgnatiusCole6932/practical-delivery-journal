# Customer CSV Export Delivery: Storage Uploads and Signed Download Links

Generate the report outside the Express request, stream it once into private S3-compatible storage, and return a signed download link only after an independent readiness check succeeds. For large e-commerce exports, the deciding constraint is throughput across the whole path, not how quickly a handler can begin writing CSV rows.

Short answer: accept an export request, claim an idempotency key, enqueue one job, stream rows into a private object, verify the stored object, mark the export ready, and mint a short-lived signed link when the authenticated customer asks for the result.

Don't keep the original HTTP request open while a large orders report is generated. A client retry can then create duplicate work, an application restart can strand a half-finished response, and slow downloads can occupy capacity needed by normal traffic. The safer contract is asynchronous: the create call returns an export identifier; a status call reports `queued`, `running`, `ready`, or `failed`; and the download action returns a fresh signed URL only for the customer who owns that identifier.

## Reliability failures when report generation shares the request lifecycle

An export has three different throughput stages: reading order data, encoding CSV, and uploading bytes. Treat the slowest stage as the system limit. Adding Express workers won't help if database scans saturate the replica, and increasing upload concurrency won't help if CSV encoding is waiting on queries. Measure rows per second and bytes per second separately, then record queue delay, generation duration, upload duration, object size, and time to readiness. One end-to-end timer hides the bottleneck.

Memory is the first predictable failure mode. Building one giant string or `Buffer` makes peak memory proportional to report size, then often creates another copy during upload. Streaming keeps memory bounded, but it introduces backpressure: the row producer must slow down when the storage client can't accept bytes. Any adapter that silently buffers the entire body defeats the design, so verify that behavior under a report much larger than the process memory limit. Retries are the next trap. I've been paged by missed jobs and duplicate deliveries, and the useful reflex is the same here: assume a message may run more than once. Use a stable key derived from tenant ID plus export ID, and make one durable record the authority for state transitions. A worker must claim that record before doing expensive work. A second delivery should observe `running` or `ready`, not create a second object under a random name. If the first worker stops after upload but before the database commit, the deterministic key gives a reconciler something concrete to inspect; a random key leaves an unowned object and no reliable way to distinguish complete output from abandoned work.

Backpressure is capacity control.

Keep partial work invisible.

## Customer data governance before export bytes move

The object key is an internal identifier, never an authorization decision. Store the object privately, bind the export record to the authenticated tenant and customer, and authorize every status or download request against that record before signing. A signed URL is a bearer credential until it expires, so don't put it in routine application logs, queue payloads, or analytics events.

Define the report's data boundary before the worker runs. Record the tenant, requester, selected columns, filter, and cutoff with the export job. Either the file reflects a stated cutoff or it is allowed to include concurrent updates; leaving that choice implicit produces totals that cannot be explained later. Retention is another explicit state transition: expire the export record, delete its object under policy, and prevent the API from signing a key after that transition.

Small detail, large blast radius.

## How can a service implement CSV generation, upload it to compatible storage, and return a signed download link?

Model the workflow as a state machine rather than a long Express callback. The web tier validates the requested date range and columns, creates or reuses the export record, and enqueues its ID. The worker reads rows in stable pages, encodes them through a CSV writer, and feeds an upload stream. After upload, it checks object metadata, commits `ready`, and only then allows the API to sign a download. This ordering prevents a fast poller from receiving a link to an object that isn't yet complete.

The following Go example is deliberately storage-neutral. It shows the control points an Express/Node.js implementation needs even though the concrete stream and SDK calls will differ. The `Store` adapter is responsible for a private upload and for applying the provider's signing rules; the repository owns idempotency and state transitions.

```go
package exports

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"time"
)

type ObjectInfo struct {
	Size int64
}

type Store interface {
	Put(context.Context, string, io.Reader, string) error
	Head(context.Context, string) (ObjectInfo, error)
	SignGet(context.Context, string, time.Duration, string) (string, error)
	Delete(context.Context, string) error
}

type Repository interface {
	Claim(context.Context, string) (bool, error)
	MarkReady(context.Context, string, string, int64) error
	MarkFailed(context.Context, string, error) error
}

type Rows interface {
	Stream(context.Context, string, func([]string) error) error
}

type Worker struct {
	store Store
	repo  Repository
	rows  Rows
}

func (w Worker) Run(ctx context.Context, exportID, tenantID string) error {
	claimed, err := w.repo.Claim(ctx, exportID)
	if err != nil || !claimed {
		return err
	}

	key := fmt.Sprintf("tenants/%s/exports/%s.csv", tenantID, exportID)
	reader, writer := io.Pipe()
	generated := make(chan error, 1)

	go func() {
		csvWriter := csv.NewWriter(writer)
		err := w.rows.Stream(ctx, exportID, csvWriter.Write)
		csvWriter.Flush()
		if err == nil {
			err = csvWriter.Error()
		}
		_ = writer.CloseWithError(err)
		generated <- err
	}()

	uploadErr := w.store.Put(ctx, key, reader, "text/csv")
	if uploadErr != nil {
		_ = reader.CloseWithError(uploadErr)
		_ = <-generated
		_ = w.repo.MarkFailed(ctx, exportID, uploadErr)
		return uploadErr
	}
	if err := <-generated; err != nil {
		_ = w.repo.MarkFailed(ctx, exportID, err)
		return err
	}

	info, err := w.store.Head(ctx, key)
	if err != nil || info.Size == 0 {
		if err == nil {
			err = errors.New("uploaded export is empty")
		}
		_ = w.repo.MarkFailed(ctx, exportID, err)
		return err
	}
	if err := w.repo.MarkReady(ctx, exportID, key, info.Size); err != nil {
		return err
	}
	return nil
}
```

In the API layer, look up the ready record by export ID and tenant, then call `SignGet` with a configured lifetime and a sanitized filename such as `orders-2026-08.csv`. Keep signing out of the worker: links can expire while a job waits in a queue or while a customer is away. Minting on demand also lets the application recheck authorization each time.

For browser downloads, set the intended filename through the response or signed request's `Content-Disposition` behavior. MDN documents the `attachment` disposition and `filename` parameters, including browser handling considerations. Treat filenames as presentation data: remove control characters and path separators rather than placing raw customer input into the header.

## Measure the throughput envelope before scaling workers

Start with a concurrency budget, not an unbounded worker pool. One export consumes database read capacity, CPU for encoding, network bandwidth, and storage request capacity at the same time. Set a per-tenant limit to prevent one large merchant from monopolizing the queue, plus a global limit tied to the weakest shared dependency. Your mileage may vary because row width, quoting frequency, indexes, and network distance all change the result; I'm not sure what the right concurrency is for a given system until a production-shaped load test identifies saturation.

| Stage | Early saturation signal | First control to adjust |
| --- | --- | --- |
| Row scan | Query latency rises while upload waits | Reduce worker concurrency or page size |
| CSV encoding | CPU saturates while reads stay healthy | Cap parallel encoders |
| Object upload | Upload duration rises and producers block | Limit bytes in flight |
| Queue | Oldest-job age rises across tenants | Reserve fair per-tenant capacity |

Use bounded database pages and a deterministic ordering key. Offset pagination can become slower deep into a large table and can produce surprising results while orders change; keyset-style progression over a stable snapshot or defined cutoff gives the exporter a clearer resume boundary. Decide what the report means before coding it: either it reflects data as of a recorded cutoff, or it is allowed to include concurrent updates. Customers notice inconsistent totals long before they care which storage adapter produced the URL.

Queue age needs an alert because it is the earliest signal that promised reports are falling behind. Also alert on the ratio of failed jobs, repeated claims, upload duration, and ready objects whose database transition never completed. Logs should carry export ID, tenant ID, attempt number, row count, byte count, and state transition, but no signed URL. A trace should span the enqueue operation, worker claim, row scan, upload, metadata check, and ready commit.

Test the invariants, not only the happy-path response. Deliver the same queue message twice and confirm there is one authoritative export. Cancel the worker midway through generation and confirm no record becomes `ready`. Make the upload consumer slower than CSV production and watch process memory remain bounded. Use fields containing commas, quotes, newlines, and non-ASCII customer names, then parse the resulting file with an independent CSV reader. Finally, request a link as another tenant and require an authorization denial before the signer is called.

## Rollout and rollback without corrupting export state

Deployment should begin with low worker concurrency and a small tenant cohort. Compare generated row counts and object sizes with the source query, watch queue age and database load, and raise concurrency one step at a time. The rollback switch stops new claims while allowing in-flight uploads either to finish or to be cancelled by context. Old application versions must still understand every durable state written by the new worker; otherwise a code rollback becomes a data migration under pressure.

Ship slowly.

## Cost boundaries and the point where streaming stops fitting

Cost enters capacity planning, though it shouldn't select the architecture by itself. Estimate retained bytes, storage duration, write and metadata operations, download operations, and outbound transfer using the current terms for each candidate service. Public pricing pages change and provider billing dimensions differ, so keep those inputs in configuration or a planning sheet rather than baking a dollar claim into the runbook.

If upload succeeds but `MarkReady` does not, a reconciler can inspect records left in `running`, check the deterministic object key, validate its metadata, and complete the transition. If validation cannot prove completeness, delete the private object and requeue the same export ID. The catch is that pure streaming cannot resume from an arbitrary byte without a durable multipart plan and deterministic boundaries. For extremely large reports that must survive process loss without restarting, use staged parts plus a manifest, or a storage adapter with explicit resumable-upload state. Stick with the simpler one-pass stream when regeneration is acceptable and operational complexity matters more than avoiding repeated reads.

The final acceptance check is blunt: an authenticated customer can request an orders export, disconnect, return later, and receive a fresh signed download link to exactly one complete private CSV; duplicate delivery, worker cancellation, and a slow storage sink do not violate that contract.

## References

- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Disposition
- https://aws.amazon.com/s3/pricing/
