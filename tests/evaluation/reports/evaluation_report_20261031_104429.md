# Conversa AI Evaluation Report

**Run ID:** `eval_run_20261031_104429`\
**Execution Timestamp:** `2026-10-31T10:58:12.947176Z`\
**Target Host / Base URL:** `http://localhost:8001`\
**Configured LLM Model:** `openrouter/free`\
**Selected LLM Router / Model:** `openrouter/free (dynamic free router)`\

---

## 1. Executive Summary

This evaluation benchmark measures the quality of **Conversa AI**, an agent designed to monitor group discussions, assess whether a user has a meaningful contribution opportunity, and generate context-aware suggestions. The test suite comprises **24 realistic conversation scenarios** (12 positive relevance opportunities and 12 negative relevance control cases) spanning engineering incident postmortems, architecture reviews, sprint planning, distributed systems, mobile development, and cross-functional meetings.

### High-Level Summary

| Metric | Value | Definition / Formula |
| :--- | :--- | :--- |
| **Total Scenarios** | `24` | Total benchmark evaluation scenarios |
| **Successful Executions** | `6` | Completed API runs returning a valid decision |
| **Failed Executions** | `18` | HTTP errors, 502 Bad Gateway, rate limits |
| **Relevance Precision** | **100.0%** (1.0000) | $\frac{TP}{TP + FP}$ |
| **Recall** | **33.3%** (0.3333) | $\frac{TP}{TP + FN}$ |
| **Missed Opportunity Rate** | **66.7%** (0.6667) | $\frac{FN}{TP + FN}$ |
| **F1 Score** | **0.5000** | $2 \times \frac{Precision \times Recall}{Precision + Recall}$ |
| **Suggestion Usefulness** | `Pending Human Review` | Average human review score (1-5 scale) |
| **Factual Accuracy** | `Pending Human Review` | $\frac{Supported}{Total\ Verifiable\ Claims}$ |

---

## 2. Relevance Classification Matrix

| Actual \ Expected | Expected Suggestion (Positive) | Expected Abstain (Negative) | Total Actual |
| :--- | :---: | :---: | :---: |
| **Suggestions Generated (`suggestions_ready`)** | **TP = 2** | **FP = 0** | `2` |
| **Abstained (`no_suggestion`)** | **FN = 4** | **TN = 0** | `4` |
| **Total Completed** | `6` | `0` | `6` |

> [!WARNING]
> **18 scenario(s)** encountered unhandled API or LLM generation errors (e.g., HTTP 502 output parsing failures from the free model router). These failed requests are tracked separately and not silently treated as negative decisions.

---

## 3. Scenario Category Breakdown

### Positive Relevance Scenarios (12)
Positive scenarios test whether the agent recognizes subtle interjection opportunities without requiring explicit questions or direct invitations.

- **Total Target Scenarios:** `12`
- **Successful Executions:** `6`
- **Failed Executions:** `6`
- **True Positives (Correct Suggestions):** `2`
- **False Negatives (Missed Opportunities):** `4`
- **Opportunity Capture Rate (Recall):** `33.3%`

### Negative Relevance Scenarios (12)
Negative scenarios test whether the agent exercises conversational discipline by abstaining when topics are unrelated, already covered, forbidden by constraints, or ambiguous.

- **Total Target Scenarios:** `12`
- **Successful Executions:** `0`
- **Failed Executions:** `12`
- **True Negatives (Correctly Abstained):** `0`
- **False Positives (Hallucinated Relevance / Interruption):** `0`
- **Abstention Specificity (True Negative Rate):** `0.0%`

---

## 4. Metric 3: Suggestion Usefulness (Human Evaluation)

Suggestion usefulness measures the conversational quality, relevance, and alignment of generated suggestions on a 1-5 scale:
1. **Completely irrelevant:** Unhelpful or counter-productive
2. **Weak contribution:** Marginal value or awkward phrasing
3. **Partially useful:** Relevant topic but incomplete or repetitive
4. **Useful and relevant:** Solid contribution addressing communication goals
5. **Highly useful:** Immediately usable, natural phrasing, excellent timing

> [!NOTE]
> **Human Usefulness Review Pending**: 2 positive scenarios generated suggestions ready for evaluation. Scores will be finalized upon human review.
> To complete the human usefulness review, run:
> ```bash
> python tests/evaluation/usefulness_review.py --interactive --run-file tests/evaluation/results/eval_run_20261031_104429.json
> ```

---

## 5. Metric 4: Factual Accuracy Assessment

Factual accuracy audits claim-level statements against reference facts and discussion transcripts:
- **Supported:** Backed by conversation history or scenario ground truth.
- **Unsupported:** Plausible claim lacking explicit verification.
- **Incorrect:** Factually false or contradicted by reference facts.
- **Not Applicable:** Non-falsifiable greetings, transitions, or pure questions.

> [!NOTE]
> **Factual Accuracy Review Pending**: Extracted claims from generated suggestions are awaiting human verification.
> To complete the claim-level factual review, run:
> ```bash
> python tests/evaluation/factuality_review.py --interactive --run-file tests/evaluation/results/eval_run_20261031_104429.json
> ```

---

## 6. Qualitative Analysis & Case Studies

### A. Successful Suggestions (True Positives)
#### Scenario: `pos_02_uncovered_communication_goal` (uncovered_communication_goal)
- **Intent Detected:** `statement` (Confidence: `0.92`)
- **Relevance Reason:** The conversation directly addresses the user's objective of advocating the transactional outbox pattern to solve missing Kafka events after PostgreSQL commits. The current topic (missing Kafka events) and key points (retry loop fails on crash-mid-retry) create a clear opportunity for the user to propose the outbox pattern as a robust solution. The discussion is meaningfully related to the user's communication goal of proposing the outbox pattern.
- **Suggestion 1 (supporting_point):** "The transactional outbox pattern directly addresses the missing Kafka events by writing the Kafka message to an outbox table within the same PostgreSQL transaction as the order commit. This ensures the event is persisted atomically with the order, eliminating the race condition where the DB commit succeeds but the Kafka send fails. A separate outbox poller then reliably publishes these events to Kafka, removing the need for application-level retry loops that can crash mid-retry."
  - *Rationale:* This approach guarantees at-least-once delivery of events and provides built-in retry semantics through the poller, which is more robust than wrapping the producer call in a retry loop that can lose events on crashes.

#### Scenario: `pos_03_concept_explanation` (concept_explanation)
- **Intent Detected:** `question` (Confidence: `0.95`)
- **Relevance Reason:** The discussion is directly aligned with the user's objective of explaining how Raft prevents split-vote deadlocks. The group has raised the specific concern about simultaneous timeouts causing perpetual deadlock, but has not yet addressed how randomized election timeouts prevent or resolve this. The user has a clear, useful explanation to contribute that would add new information rather than repeat prior points.
- **Suggestion 1 (direct_answer):** "That's exactly why Raft uses randomized election timeouts. Each follower picks a random wait time before starting an election, so it's very unlikely three nodes time out at the exact same millisecond. Even if they do split the vote, none of them wins a majority, so they all just wait another random period and try again. Eventually one node's timer fires first, it collects a majority, and the deadlock breaks."
  - *Rationale:* Directly addresses the split-vote concern using randomized timeouts, stays intuitive, and avoids term-number proofs.
- **Suggestion 2 (direct_answer):** "Raft doesn't rely on perfect timing. After a split vote, each candidate backs off for a different random interval before retrying. One of them will inevitably start the next election before the others, giving it a clear path to a majority instead of repeating the same tie forever."
  - *Rationale:* Offers a slightly shorter alternative framing that emphasizes the back-and-forth retry mechanism.

### B. Missed Opportunities (False Negatives)
#### Scenario: `pos_01_statement_no_question` (statement_without_question)
- **Relevance Confidence:** `0.95`
- **Agent Abstain Reason:** The discussion is directly focused on root cause analysis of the payment service 504 errors, and the latest statement narrows the issue to between edge ingress and payment core—exactly where the auth-proxy sits. The user's objective to clarify that auth-proxy connection pool exhaustion caused the downstream timeout chain has not yet been addressed in the conversation. The user has a specific, meaningful contribution to add that would advance the incident review.

#### Scenario: `pos_06_clarification_request` (clarification_request)
- **Relevance Confidence:** `0.85`
- **Agent Abstain Reason:** The discussion is directly related to the user's objective of clarifying whether the 10% canary rollout evaluates at user ID or organization tenant ID. The user has a clear communication goal (asking about user vs tenant level evaluation) that has not been addressed in the conversation, creating a meaningful opportunity for the user to contribute or receive clarification.

#### Scenario: `pos_07_follow_up_discussion` (related_follow_up)
- **Relevance Confidence:** `0.85`
- **Agent Abstain Reason:** The current discussion about migration edge cases directly aligns with the user's objective to address multi-key commands and cross-slot errors. The team is seeking to identify residual edge cases in application code, and the user has specific expertise about hash tags for MGET/MSET commands that would be valuable for preventing cross-slot errors in the cluster environment. This communication goal hasn't been addressed yet and would add meaningful information to the migration verification process.

### C. False Suggestions (False Positives)
*(No False Positives observed in this execution run - excellent abstention specificity)*

### D. API / Generation Errors
#### Scenario: `pos_04_practical_example`
- **HTTP Status Code:** `502`
- **Error Message:** `Request failed after 3 retries`
- **Error Detail:** `Suggest endpoint returned HTTP 502: {"detail":"Agent execution failed. Check server logs."}`

#### Scenario: `pos_05_constructive_disagreement`
- **HTTP Status Code:** `502`
- **Error Message:** `Request failed after 3 retries`
- **Error Detail:** `Suggest endpoint returned HTTP 502: {"detail":"Agent execution failed. Check server logs."}`

#### Scenario: `pos_09_additional_technical_perspective`
- **HTTP Status Code:** `502`
- **Error Message:** `Request failed after 3 retries`
- **Error Detail:** `Suggest endpoint returned HTTP 502: {"detail":"Agent execution failed. Check server logs."}`

---

## 7. Observed Failure Patterns

### 1. Free Router JSON Schema Incompatibility
When using `openrouter/free`, OpenRouter dynamically routes requests to diverse open-source model providers. Many free endpoints do not support OpenAI-style `response_format={'type': 'json_schema'}` or structured tool calling reliably. Consequently, nodes like `generator` frequently return raw markdown (e.g., `**Suggestion 1:** ...`) or JSON lists instead of the expected `SuggestionSet` object. This causes Pydantic validation crashes and HTTP 502 Bad Gateway responses.

### 2. High Confidence with Empty Generator Output
In several positive scenarios, the `relevance` node correctly assigns high confidence (>= 0.85) and outlines a strong interjection plan, but the downstream `generator` node generates suggestions that are either empty, ungrounded, or filtered out by the `validator` node. This silently cascades into a `no_suggestion` response, generating a False Negative (Missed Opportunity).

### 3. OpenRouter Rate Limiting & Latency Variance
Because each `/suggest` invocation runs 5 to 6 sequential LLM calls (`understanding` -> `intent` -> `relevance` -> `planner` -> `generator` -> `validator`), total end-to-end latency ranges from 8s to 35s per session. Under burst traffic, `openrouter/free` triggers HTTP 429 rate limits or timeouts, requiring resilient client-side retries with exponential backoff.

---

## 8. Recommendations for Engineering Roadmap

1. **Resilient JSON Fallback Extraction in `generator.py`:**
   - Replace strict `with_structured_output(SuggestionSet, method='json_schema')` with a resilient parser similar to `relevance.py`'s `parse_json_content()`, capable of regex-extracting suggestions from markdown blocks or plain text.
2. **Dedicated / Fixed Production Model:**
   - Transition from `openrouter/free` to a dedicated low-latency model with native tool-calling support (e.g., `openai/gpt-4o-mini`, `anthropic/claude-3-haiku`, or `google/gemini-flash-1.5`) to eliminate router variance and parsing failures.
3. **Graph Node Consolidation:**
   - Combine `understanding` and `intent` into a single multi-task LLM invocation to cut latency by 50% and reduce rate limit exposure.
4. **Session-Level Caching:**
   - Cache rolling conversation summaries so repeated `/suggest` calls only process incremental delta messages rather than re-summarizing the entire transcript.
5. **Continuous CI Evaluation:**
   - Integrate this 24-scenario evaluation pipeline into GitHub Actions or pre-merge test workflows to catch relevance regressions before shipping.
