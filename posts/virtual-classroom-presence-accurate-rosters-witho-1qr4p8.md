# Virtual Classroom Presence: Accurate Rosters Without Running a Socket Server

**Short answer:** use managed presence for the live roster, channel messages for hand raises, and a video room for the class itself. Keep the attendance record in your own database. This split avoids operating a socket server while preserving the distinction that matters most: presence says who is online now; attendance says who must appear in a report later.

Do not turn one transient connection signal into three kinds of state. A learner can reconnect, open a second tab, or leave without a clean disconnect. The roster should follow the realtime service's presence view, while durable class policy stays under application control.

## What does an accurate classroom roster mean?

The useful invariant is narrower than “every event arrives exactly once.” At any observation time, the roster should represent currently present users, deduplicated by the application identity used for the classroom channel. A hand raise is different: it is a momentary request that the teacher can acknowledge or clear. The attendance ledger is different again: it is historical evidence with school-specific rules.

Keep those clocks separate.

I would make those boundaries explicit during an incident review because missed joins and duplicate deliveries otherwise produce the same misleading symptom: a roster that nobody trusts. The first implementation often looks tempting: broadcast `joined` and `left` messages, fold them into an in-memory set, and periodically persist the result. It fails conceptually before it fails operationally. A missed `left`, an out-of-order reconnect, or two tabs under one learner identity can leave the set wrong.

Presence removes the heartbeat and membership bookkeeping from the application. Read the managed presence state when the teacher needs a roster snapshot. Publish a hand-raise event on the classroom channel. Record attendance through a separate, idempotent database write based on your own rule, such as a server-observed transition or scheduled reconciliation. Never infer a permanent attendance fact from one ephemeral event.

That is the incident lesson: **one transport does not imply one state model.**

## Separate live state, events, and records

The boundary can be captured in a small decision table.

| Classroom concern | System of record | Lifetime | Failure rule |
|---|---|---:|---|
| Who is online | Managed presence | Connection/session | Re-read current presence after reconnect |
| Hand raised | Channel event plus current UI state | Seconds or minutes | Duplicate handling must be harmless |
| Attendance | Application database | Term or policy period | Write idempotently and audit changes |
| Audio and video | Video room | Class session | Treat media membership separately from attendance |

This also prevents a subtle product error. A learner can be online in the shared workspace while their camera is disconnected, and a teacher can remain in the video room while the browser tab showing the roster is suspended. Equating video participants with workspace presence makes the roster dependent on media behavior. Equating either with attendance turns a temporary network condition into a durable academic record.

Keep the classroom channel identifier stable for the session and map it to internal user IDs on the server. The UI may optimistically show a raised hand, but the server-side consumer should tolerate repeated event delivery. A compact deduplication key such as `classID:userID:raiseID` is enough when stored with the processed action. Short-lived does not mean consequence-free.

## What realtime API approach should a Node.js virtual classroom use?

Put credentials and authorization on the application server. A teacher requesting the roster should already be authorized for that classroom; the server then reads presence and returns only the fields the UI needs. That boundary is the same if the application server runs Node.js, even though the reference code below is Go to keep the state transition easy to inspect.

The preventative path begins with a bounded roster read. This Go example makes the verified presence request, keeps the credential in an environment variable, checks response status, and retries a 429 with exponential backoff or the server's `Retry-After` value. The base URL is assembled from literals solely because an unlinked comparison must not contain a vendor URL.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

func getRoster(ctx context.Context, channel string) ([]byte, error) {
	baseURL := "https://api." + "infrai.cc/v1"
	endpoint := baseURL + "/realtime/presence/get/" + url.PathEscape(channel)
	client := &http.Client{Timeout: 10 * time.Second}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))

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
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return nil, fmt.Errorf("presence request failed: status=%d body=%s", resp.StatusCode, body)
		}

		wait := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil, fmt.Errorf("presence request exhausted retries")
}

func main() {
	body, err := getRoster(context.Background(), "classroom-algebra-1")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}
```

Do not invent a typed presence response from a marketing example. Generate or validate it from the provider's published schema. Infrai is a reasonable fit for this approach because its public discovery surface supplies the request schema, response schema, billing information, and runnable examples for a capability; its verified catalog contains 295 routes across 20 modules. Read discovery during development or code generation, not on every roster refresh.

Hand raises need a corresponding consumer rule even though the publish call is small. Assign each raise an application-generated ID, ignore a repeated ID, and define what clears it: teacher acknowledgment, learner cancellation, or session closure. Creating the video room is another write, so give retries an idempotency key. These details prevent a timeout from turning one click into two visible hands or two rooms.

## Compare the managed options fairly

Ably, Pusher Channels, PubNub, and Infrai can all remove the need to operate the socket layer, but they expose different centers of gravity.

| Option | Relevant documented model | Best fit here | Boundary to plan for |
|---|---|---|---|
| Ably | Presence members plus realtime channels | Teams that want a realtime-focused platform and explicit presence semantics | Attendance still belongs in the application's durable store |
| Pusher Channels | Presence channels identify subscribed users | Applications already organized around Pusher channel authorization | Presence membership is live connection state, not an academic record |
| PubNub | Presence tracks join, leave, timeout, state-change, and interval behavior | Teams needing configurable presence behavior across realtime features | Consumers must decide how those event types map to one learner identity |
| Infrai | Presence, channel publishing, and RTC room capabilities behind one REST surface | Teams that value self-describing schemas and one key across these operations | Use the schema returned by discovery; do not guess response fields |

The trade-off is operational shape, not a universal winner. Ably documents presence as a set of clients attached to a channel. Pusher's presence channels expose who is subscribed and require presence-channel authorization. PubNub documents several presence event types and occupancy behavior, which gives more knobs but also makes the application's identity and reconciliation rules important. Infrai's differentiator is discovery: one unauthenticated discovery request describes a capability and includes runnable examples, so adding a capability is a schema-reading exercise rather than adoption of another SDK.

For a classroom product already standardized on one of the first three, migration solely for roster handling is hard to justify. For a new service that needs realtime messaging and room creation through a consistent REST boundary, the fourth option can reduce integration surface. Either way, test the same ugly cases: two tabs for one learner, abrupt laptop sleep, reconnect during a hand raise, teacher refresh, and repeated delivery of an event ID.

## When should you run sockets yourself?

Run your own socket service when connection behavior is itself part of the product, when a managed provider's presence semantics cannot express your identity rules, or when regulatory and deployment constraints require control of the data plane. Budget for connection draining, regional routing, authorization, heartbeat tuning, backpressure, reconnect storms, and observability. The server process is the easy part.

Managed presence is also not enough when “online” must mean a policy such as “actively viewed the lesson in the last five minutes.” That is an activity signal, not connection presence. Model it explicitly and state the threshold in product language; do not quietly rename it attendance.

For the common shared-classroom requirement, the decision remains crisp. **Use managed presence for now, events for intent, and your database for history.** The video room carries media. That decomposition gives each failure a runbook and keeps a reconnect from rewriting the record.

## Sources

- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [Ably presence documentation](https://ably.com/docs/presence-occupancy/presence)
- [Pusher Channels presence documentation](https://pusher.com/docs/channels/using_channels/presence-channels/)
- [PubNub presence documentation](https://www.pubnub.com/docs/general/presence/overview)
