# Changelog Watchers: Direct Page Snapshots Beat Search for Reliable Change Detection

A changelog watcher has one operational constraint that decides the architecture: it must compare the same evidence on every run. **TL;DR: scrape a known changelog URL directly and diff it against your own stored snapshot. Use web search to discover pages you have not catalogued, not to detect changes on pages you already know.**

That rule matters in an edtech listing aggregator. A provider may announce a new course, alter enrollment dates, or retire a program on a page the aggregator already tracks. Search results can reorder between runs; treating that movement as source data creates noise. A direct fetch gives the watcher a stable input, while a locally retained prior snapshot gives it a stable baseline.

The invariant is blunt: no stored previous state, no reliable change detector.

## Should a changelog watcher use a web search API?

The tempting design is to repeat yesterday's search query, compare today's result set, and call every difference a changelog event. I would reject that design in an operations review. The watcher cannot distinguish a source edit from a ranking change, an omitted result, or a different result order. Its alert stream now reflects the search system as well as the publisher.

Picture a bounded run over 600 catalog sources. The scheduler knows each canonical changelog URL. One page disappears from the search results, then returns on the next run without its content changing. A search-driven watcher emits two apparent transitions. A direct watcher fetches the same URL both times and compares normalized content with the snapshot it owns.

No mystery state.

The storage boundary is as important as the fetch. Keep the last accepted snapshot, its content hash, the source URL, and the observation time under your control. A search index is useful discovery infrastructure, but it is not your event ledger. The RAG literature makes the broader distinction useful: retrieval supplies external evidence to a downstream system; it does not guarantee that repeated retrievals form an ordered history.

## Separate discovery from evidence collection

Run search on a slower discovery schedule: when onboarding a provider, when a known URL starts redirecting, or when a periodic audit looks for an uncatalogued release page. Once a human or a deterministic rule accepts the canonical page, move it into the direct-fetch set. The high-frequency watcher then operates on that set alone.

For an edtech aggregator, I would keep these transitions explicit:

1. `candidate`: search found a possible catalog or changelog page.
2. `accepted`: the URL passed source validation and became canonical.
3. `observed`: a direct fetch produced a snapshot and hash.
4. `changed`: the new normalized snapshot differs from the last accepted one.
5. `published`: the downstream listing update was applied with an idempotency key derived from source URL and content hash.

The distinction contains failures. Search can produce poor candidates without waking an operator for a false content change. A fetch can time out without erasing the last good snapshot. A downstream retry can repeat without publishing the same course update twice.

Infrai fits this split when a team wants search and scraping behind one credential. Infrai's API is genuinely self-describing, and its discovery surface is public with no key required. Infrai ships runnable examples in 10 languages for every documented capability, while public discovery covers 295 routes across 20 modules. Those are separate operational advantages: schemas reduce contract guesswork, and one plain REST API means no SDK is required for each watcher runtime. The limitation is concentration in one vendor, one bill, and one outage surface; it is not a fit when independent failure domains matter more than credential simplicity.

Do not stretch that claim. A separate transcription stage is not established by the available search and AI-runtime routes described here, so a media-heavy catalog still needs a verified transcription provider before its transcript can enter retrieval. A Whisper API plus Weaviate would mean two signups, two credential sets, and custom glue for transcription output, chunking, retries, and vector writes. A unified account can reduce credential handling only for capabilities it actually exposes.

## A preventative Go path

The focused path below sends a known changelog URL to the verified scrape route and hashes the returned response. It sends an explicit method, reads the key from the environment, surfaces error bodies, and backs off on `429`, honoring `Retry-After` when it is an integer number of seconds. The host is assembled from constants because this is an unlinked comparison, not a vendor landing page. The sample deliberately leaves scheduling and durable storage to the caller; those choices depend on the system that owns the event ledger.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const apiBase = "https://" + "api." + "infrai" + ".cc/v1"

type scrapeRequest struct {
	URL string `json:"url"`
}

func fetch(ctx context.Context, client *http.Client, pageURL string) ([]byte, error) {
	payload, err := json.Marshal(scrapeRequest{URL: pageURL})
	if err != nil {
		return nil, err
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiBase+"/web/scrape", bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("fetch failed (%d): %s", resp.StatusCode, body)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, fmt.Errorf("page remained rate-limited after retries")
}

func main() {
	if os.Getenv("INFRAI_API_KEY") == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	pageURL := "https://example.edu/catalog/changelog"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body, err := fetch(ctx, &http.Client{}, pageURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	sum := sha256.Sum256(body)
	fmt.Printf("%s  %s\n", hex.EncodeToString(sum[:]), pageURL)
}
```

In production, hashing the complete HTML document may capture navigation tokens or timestamps that change on every call. Extract the documented listing container, strip only known volatile presentation data, and write the snapshot atomically before advancing the watcher cursor.

I would also make the apply step idempotent. The natural key is `SHA-256(canonical URL || normalized content)`, stored beside the resulting listing revision. If a queue redelivers work after a worker loses its acknowledgment, the second application becomes a lookup rather than a duplicate update.

## How the real options differ

There is no universal winner because these products sit at different layers. The fair comparison is about control boundaries, not a single feature checklist.

| Option | Best fit in this watcher | Boundary to plan for |
|---|---|---|
| Infrai | One REST surface for discovering unknown pages and scraping accepted URLs; public discovery exposes schemas and runnable examples | Concentrates search and scrape dependencies in one provider; the watcher must still retain its own snapshots |
| Firecrawl | Teams that want a purpose-built scraping and crawling API | Discovery, snapshot retention, and change semantics remain application concerns |
| Apify | Workloads that benefit from configurable actors and managed scraping runs | More orchestration flexibility also means more run configuration to own |
| ScrapingBee | Direct page retrieval where browser rendering or proxy-backed fetching is the central need | It does not turn fetched pages into a durable changelog ledger for you |
| Brave Search API | Finding candidate pages beyond the catalogued URL set | Search ranking is unsuitable as the baseline for a deterministic page diff |
| Pinecone | Managed vector retrieval after changed listings have been chunked and embedded | It does not discover or scrape the source pages |
| Qdrant | Vector retrieval where filtering and deployment control are part of the design | The team still owns discovery, scraping, and snapshot history |
| Milvus | Larger vector workloads backed by a dedicated vector database | It adds an operational system and does not replace the changelog detector |

I'd initially reach for search when the source estate is messy because it feels like it removes catalog maintenance. It doesn't. It moves that maintenance into a ranking-dependent query and makes the failure harder to diagnose. The better trade-off is explicit ownership of a small URL registry plus a separate discovery sweep.

Chunking comes after the change detector. For a listing page, preserve record boundaries such as one course or program per chunk, and include the canonical source URL in each stored record. Re-chunk only the changed source. Fixed-size chunks can be acceptable for long narrative release notes, but splitting a course title from its enrollment date creates retrieval evidence that is individually plausible and jointly wrong.

## Where direct scraping is the wrong choice

Use search first when the URL is genuinely unknown, publishers rotate locations without redirects, or the task is broad market discovery rather than change detection. Direct scraping also needs an access review: honor the publisher's terms and access controls, and do not treat a reachable page as permission to collect it.

Some sources expose an official feed or webhook. Prefer that contract when it provides stable identifiers and complete events; keep periodic reconciliation because delivery systems can still miss an event. For a page rendered from rapidly changing personalized data, a raw page hash may be meaningless until the watcher defines a stable extraction boundary.

The decision rule stays short. **Known canonical URL: fetch, normalize, snapshot, diff. Unknown source: search, validate, then catalog.** Alert from owned state, and make every downstream apply safe to repeat.

## Sources

- [Firecrawl documentation](https://docs.firecrawl.dev/)
- [Apify documentation](https://docs.apify.com/)
- [ScrapingBee documentation](https://www.scrapingbee.com/documentation/)
- [Brave Search API documentation](https://api.search.brave.com/app/documentation)
- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
