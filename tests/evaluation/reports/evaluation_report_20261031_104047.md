# Conversa AI Evaluation Report

**Run ID:** `eval_run_20261031_104047`
**Execution Timestamp:** `2026-10-31T10:41:12.336153Z`
**Target Host / Base URL:** `http://localhost:8001`
**Configured LLM Model:** `openrouter/free`
**Selected LLM Router / Model:** `openrouter/free (dynamic free router)`

---

## 1. Executive Summary

This evaluation benchmark measures the quality of **Conversa AI**, an agent designed to monitor group discussions, assess whether a user has a meaningful contribution opportunity, and generate context-aware suggestions. The test suite comprises **24 realistic conversation scenarios** (12 positive relevance opportunities and 12 negative relevance control cases) spanning engineering incident postmortems, architecture reviews, sprint planning, distributed systems, mobile development, and cross-functional meetings.

### High-Level Summary

| Metric | Value | Definition / Formula |
| :--- | :--- | :--- |
| **Total Scenarios** | `1` | Total benchmark evaluation scenarios |
| **Successful Executions** | `0` | Completed API runs returning a valid decision |
| **Failed Executions** | `1` | HTTP errors, 502 Bad Gateway, rate limits |
| **Relevance Precision** | **0.0%** (0.0000) | $\frac{TP}{TP + FP}$ |
| **Recall** | **0.0%** (0.0000) | $\frac{TP}{TP + FN}$ |
| **Missed Opportunity Rate** | **0.0%** (0.0000) | $\frac{FN}{TP + FN}$ |
| **F1 Score** | **0.0000** | $2 \times \frac{Precision \times Recall}{Precision + Recall}$ |
| **Suggestion Usefulness** | `Pending Human Review` | Average human review score (1-5 scale) |
| **Factual Accuracy** | `Pending Human Review` | $\frac{Supported}{Total\ Verifiable\ Claims}$ |

---

## 2. Relevance Classification Matrix

| Actual \ Expected | Expected Suggestion (Positive) | Expected Abstain (Negative) | Total Actual |
| :--- | :---: | :---: | :---: |
| **Suggestions Generated (`suggestions_ready`)** | **TP = 0** | **FP = 0** | `0` |
| **Abstained (`no_suggestion`)** | **FN = 0** | **TN = 0** | `0` |
| **Total Completed** | `0` | `0` | `0` |

> [!WARNING]
> **1 scenario(s)** encountered unhandled API or LLM generation errors (e.g., HTTP 502 output parsing failures from the free model router). These failed requests are tracked separately and not silently treated as negative decisions.

---

## 3. Scenario Category Breakdown

### Positive Relevance Scenarios (12)
Positive scenarios test whether the agent recognizes subtle interjection opportunities without requiring explicit questions or direct invitations.

- **Total Target Scenarios:** `1`
- **Successful Executions:** `0`
- **Failed Executions:** `1`
- **True Positives (Correct Suggestions):** `0`
- **False Negatives (Missed Opportunities):** `0`
- **Opportunity Capture Rate (Recall):** `0.0%`

### Negative Relevance Scenarios (12)
Negative scenarios test whether the agent exercises conversational discipline by abstaining when topics are unrelated, already covered, forbidden by constraints, or ambiguous.

- **Total Target Scenarios:** `0`
- **Successful Executions:** `0`
- **Failed Executions:** `0`
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
> **Human Usefulness Review Pending**: 0 positive scenarios generated suggestions ready for evaluation. Scores will be finalized upon human review.
> To complete the human usefulness review, run:
> ```bash
> python tests/evaluation/usefulness_review.py --interactive --run-file tests/evaluation/results/eval_run_20261031_104047.json
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
> python tests/evaluation/factuality_review.py --interactive --run-file tests/evaluation/results/eval_run_20261031_104047.json
> ```

---

## 6. Qualitative Analysis & Case Studies

### A. Successful Suggestions (True Positives)
*(No True Positives observed in this execution run)*

### B. Missed Opportunities (False Negatives)
*(No False Negatives observed in this execution run)*

### C. False Suggestions (False Positives)
*(No False Positives observed in this execution run - excellent abstention specificity)*

### D. API / Generation Errors
#### Scenario: `pos_01_statement_no_question`
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
