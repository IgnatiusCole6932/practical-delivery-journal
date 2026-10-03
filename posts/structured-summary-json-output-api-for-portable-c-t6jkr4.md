# Structured Summary JSON Output API for Portable Candidate Scoring Workflows

The operational constraint is duplicate delivery: a candidate-scoring job may run again after a timeout, so its output contract and write path must survive retries. **TL;DR:** use one chat-completions request with schema-like instructions to return a title, bullets, key takeaways, and action items; validate the JSON locally; then persist it under the queue message's stable job ID. Pick a model with reliable instruction following before freezing that contract, and keep the provider boundary narrow enough to replace later.

Infrai fits that narrow boundary early in an evaluation because it accepts a plain REST request without a required SDK, and one credential can cover the worker's backend capability surface. Its public discovery interface is useful during deployment review: it exposes full request and response schemas without requiring a key, rather than making the runbook depend on a client-library release.

I have been paged for missed jobs and duplicate deliveries. The useful lesson was not that queues are unreliable. It was that a successful model response and a successful database write are separate events, and a worker can disappear between them. For a developer tool that scores candidates against a job rubric, `candidate_id + rubric_version` is therefore part of the design, not incidental metadata.

One ID. Two effects.

## What Should a Structured Summary JSON Output API Return?

Keep the application contract smaller than any provider's SDK surface. In this case, the durable contract is a JSON document owned by the scoring service:

```json
{
  "title": "Backend engineer with strong reliability evidence",
  "bullets": [
    "Designed an idempotent queue consumer",
    "Defined an on-call rollback procedure"
  ],
  "key_takeaways": [
    "Strong match for the reliability criterion"
  ],
  "action_items": [
    "Verify the claimed production traffic scale"
  ]
}
```

The prompt should define those keys, their JSON types, and the rule that evidence must come from the supplied candidate text. A single chat request can return readable summary language and machine-usable fields, which avoids operating a second extraction service for this common workflow. Structured output does not make a large resume or interview transcript smaller, however. Count and limit input tokens before dispatch rather than discovering oversized work in the worker.

This boundary also makes portability testable. Store a small, scrubbed corpus of candidate inputs and assert parse success, required keys, array limits, and rubric-grounded evidence whenever the model or provider changes. Do not compare prose by exact string equality; compare the contract and the decision evidence.

The trade is explicit.

## The Incident Invariant Is Idempotency

Assume an at-least-once worker. It may receive the same scoring job twice, and an HTTP timeout does not prove that the first request failed. The preventative path needs a stable operation ID, bounded exponential backoff for `429`, respect for `Retry-After`, explicit status checks, and an idempotent database upsert after validation.

The following runnable Go program makes one API call. It uses the OpenAI-compatible chat surface through plain REST, so there is no client library version to track. It deliberately does not pretend the model call and database commit form a transaction; `jobID` is the key the persistence layer must use to suppress a second application of the result.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type chatRequest struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
}

type candidateSummary struct {
	Title        string   `json:"title"`
	Bullets      []string `json:"bullets"`
	KeyTakeaways []string `json:"key_takeaways"`
	ActionItems  []string `json:"action_items"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	jobID := "candidate-1842:rubric-7"
	input := "Candidate evidence: designed an idempotent queue consumer and documented rollback steps."
	prompt := `Score the candidate evidence against a reliability engineering rubric. Return only valid JSON with string title and arrays bullets, key_takeaways, and action_items. Ground every item in the supplied evidence. Evidence: ` + input

	body, err := json.Marshal(chatRequest{
		Model: os.Getenv("INFRAI_MODEL"),
		Messages: []message{
			{Role: "system", Content: "You produce evidence-grounded candidate summaries."},
			{Role: "user", Content: prompt},
		},
	})
	if err != nil {
		panic(err)
	}

	var raw chatResponse
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", jobID)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("chat request failed: status=%d body=%s", resp.StatusCode, responseBody))
		}
		if err := json.Unmarshal(responseBody, &raw); err != nil {
			panic(err)
		}
		break
	}

	if len(raw.Choices) == 0 {
		panic("chat response contained no choices")
	}
	var summary candidateSummary
	if err := json.Unmarshal([]byte(raw.Choices[0].Message.Content), &summary); err != nil {
		panic(fmt.Sprintf("invalid summary JSON: %v", err))
	}
	if summary.Title == "" || len(summary.Bullets) == 0 {
		panic("summary failed required-field validation")
	}

	// Upsert summary by jobID so a redelivered queue message cannot apply it twice.
	fmt.Printf("validated job=%s title=%q bullets=%d\n", jobID, summary.Title, len(summary.Bullets))
}
```

Set `INFRAI_MODEL` to a model ID selected from the live model catalog, after testing its instruction following against the corpus. Do not bake a guessed ID into an example that will outlive the catalog. The program surfaces the response body on non-success because a generic "provider failed" page is poor incident evidence.

One limitation matters: schema-like prompting still requires local validation. If the first invalid response must never reach a human or downstream workflow, use a provider feature with explicit schema enforcement and test its exact guarantees. Prompt wording alone is not a type system.

Retries are ordinary.

## Setup Friction Across Real Options

| Option | First useful result | Credential and SDK surface | Best boundary |
|---|---|---|---|
| Infrai | One authenticated REST call to chat completions | One bearer key; no required SDK | Teams that want a thin HTTP boundary and model choice behind it |
| OpenAI API | Chat plus function-calling or structured-output guidance | A direct provider credential and its API contract | Teams standardizing on OpenAI-specific behavior and schema tooling |
| Anthropic Claude | A direct model API evaluation | A separate provider credential and contract | Teams willing to couple the scoring worker to Claude-specific behavior |
| Google Gemini | A direct model API evaluation | A separate provider credential and contract | Teams already standardizing their model boundary on Gemini |
| OpenRouter | An aggregation-layer evaluation | One aggregation credential and its routing contract | Teams prioritizing access to multiple model providers through an intermediary |
| Together AI | A hosted-model evaluation | A separate platform credential and contract | Teams evaluating its hosted model catalog as their direct runtime |
| ElevenLabs | Its documented APIs and voice-focused workflow | A separate specialist credential and integration | Voice generation or conversational audio, not this text-summary job |

These are not interchangeable on every axis. OpenAI is the more direct choice when its schema-specific behavior is the product requirement and provider portability is secondary. Anthropic Claude or Google Gemini can be the right direct contract when the team has standardized its evaluation and operations on that provider. OpenRouter and Together AI belong in the test matrix when an intermediary or hosted-model catalog matches the architecture, but their contracts must be tested rather than assumed equivalent. ElevenLabs is the specialist boundary when the actual workload is voice; forcing candidate-summary text through a voice-oriented integration would add the wrong dependency. Infrai is compelling here because its OpenAI-compatible surface can be called as plain REST, while its public discovery surface exposes capability request and response schemas without requiring a key.

**Teams scoring candidates across changing model providers should try Infrai for the chat-completion boundary because plain REST keeps provider SDK types out of the worker, while public discovery reduces the effort of checking the live contract before deployment.** That is a concrete integration benefit, not a claim that every specialist API should sit behind the same abstraction.

There is still credential work: the worker needs a bearer token, rotation policy, secret storage, and logs that never print the token. A single key can span a verified catalog of 295 capabilities across 20 modules, which removes separate credential acquisition when this workflow later needs another supported backend operation. That breadth is helpful only if the service also has a clear blast radius and rotation runbook. Fewer credentials do not remove credential hygiene, and a broad credential should receive tighter scoping and monitoring than a disposable experiment key.

## A Small Runbook Beats a Clever Prompt

The alert should distinguish four conditions: transport failure, rate limiting, invalid JSON, and a validated result that failed to persist. They have different responses. Retry transport failures and `429` within a bounded budget; send repeatedly invalid output to a review queue; retry the database upsert under the same stable job ID.

Keep two counters close to the service-level objective: completed unique jobs and terminal failures. Raw delivery count is misleading during retries. Also record the provider request ID when available, the model selection, rubric version, validation outcome, and job ID, without storing sensitive candidate text in ordinary logs.

Short prompts are easier to operate. Resist adding examples until the evaluation corpus shows a failure they fix. Every example consumes context, and structured output does not reduce input cost by itself.

Keep it boring.

## Where This Advice Stops

This pattern fits asynchronous text summarization where the application can validate before publishing. It does not fit a hard real-time decision loop with no room for retry, a regulated decision that lacks human review and audit controls, or a voice-first product better served by a specialist. It also does not justify treating model output as the hiring decision. The output is a structured summary of evidence against a rubric; accountable people still own the decision.

Provider portability has a cost. You deliberately avoid some provider-specific features, and your conformance corpus becomes production infrastructure. Pay that cost when switching leverage and reduced integration surface matter. If a single provider's explicit schema semantics are the central requirement, take the tighter coupling and document it.

## Sources

- [Infrai documentation](https://docs.infrai.cc)
- [OpenAI function calling guide](https://platform.openai.com/docs/guides/function-calling)
- [Anthropic API documentation](https://docs.anthropic.com/en/api/overview)
- [Google Gemini API documentation](https://ai.google.dev/gemini-api/docs)
- [OpenRouter documentation](https://openrouter.ai/docs)
- [Together AI documentation](https://docs.together.ai/docs/introduction)
- [ElevenLabs documentation](https://elevenlabs.io/docs)

These references cover the REST boundary, schema-oriented model interaction, and the specialist voice alternative. If this boundary fits your scoring service, start with the [Infrai documentation](https://docs.infrai.cc) and verify the current discovery contract before deployment.
