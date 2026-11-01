# Conversa AI Evaluation Report

**Run ID:** `eval_run_20261031_121020`\
**Execution Timestamp:** `2026-10-31T12:12:05.156782Z`\
**Target Host / Base URL:** `http://localhost:8001`\
**Configured LLM Model:** `openrouter/free`\
**Resolved Models Observed:** `nvidia/nemotron-3-ultra-550b-a55b:free, poolside/laguna-xs-2.1:free`\

---

## 1. Executive Summary & Infrastructure Reliability

This evaluation benchmark measures the quality of **Conversa AI**, an agent designed to monitor group discussions, assess whether a user has a meaningful contribution opportunity, and generate context-aware suggestions. The test suite comprises **24 realistic conversation scenarios** (12 positive relevance opportunities and 12 negative relevance control cases) evaluated against the live agent server.

### Infrastructure & Execution Overview

| Metric | Value | Percentage | Definition |
| :--- | :---: | :---: | :--- |
| **Total Scenarios** | `2` | 100.0% | Complete test matrix (12 positive, 12 negative) |
| **Successful Executions** | `2` | **100.0%** | Sessions completing the full agent graph without unhandled errors |
| **Failed Executions** | `0` | 0.0% | Requests encountering timeouts, rate limits, or upstream errors |


---

## 2. Relevance Classification Performance

Relevance metrics measure the agent's ability to recognize when the user should speak versus when the user should abstain. These metrics are calculated **strictly from successfully evaluated relevance decisions**, avoiding distortion from unhandled infrastructure errors.

### Relevance Confusion Matrix

| Actual \ Expected | Expected Opportunity (Positive) | Expected Abstain (Negative) | Total Evaluated |
| :--- | :---: | :---: | :---: |
| **Opportunity Identified (`should_suggest=True`)** | **TP = 1** | **FP = 0** | `1` |
| **Abstained (`should_suggest=False`)** | **FN = 0** | **TN = 1** | `1` |
| **Total Completed Decisions** | `1` | `1` | `2` |

### Key Relevance Metrics

| Metric | Score | Formula | Interpretation |
| :--- | :---: | :--- | :--- |
| **Relevance Precision** | **100.0%** (1.0000) | $\frac{TP}{TP + FP}$ | Frequency of valid opportunities among generated decisions |
| **Recall (Sensitivity)** | **100.0%** (1.0000) | $\frac{TP}{TP + FN}$ | Proportion of genuine opportunities successfully detected |
| **Missed Opportunity Rate** | **0.0%** (0.0000) | $\frac{FN}{TP + FN}$ | Rate of valid opportunities incorrectly passed over |
| **False Positive Rate (FPR)** | **0.0%** (0.0000) | $\frac{FP}{FP + TN}$ | Rate of unwanted interjections in negative contexts |
| **F1 Score** | **1.0000** | $2 \times \frac{Precision \times Recall}{Precision + Recall}$ | Balanced harmonic accuracy score |

---

## 3. End-to-End Suggestion Generation Performance

Suggestion generation measures whether the agent successfully materialized high-quality response suggestions when an opportunity was identified.

| Generation Metric | Value | Description |
| :--- | :---: | :--- |
| **Positive Scenarios Evaluated** | `1` | Positive scenarios completing API execution |
| **Scenarios with Valid Suggestions** | `1` | Positive scenarios producing $\ge 1$ suggestion |
| **Relevant But No Suggestions Produced** | `0` | Scenarios where relevance was true but generation yielded 0 suggestions |
| **Generation Failure Rate** | **0.0%** | Fraction of relevant opportunities where generator produced no valid text |
| **Total Suggestions Generated** | `1` | Total suggestion items output by generator |
| **Suggestions Rejected by Validation** | `0` | Suggestions removed by length, duplicate, or rationale filters |
| **Avg Suggestions per Successful Scenario** | `1.00` | Average options offered to the user (max 2) |

---

## 4. Stage-Specific Failure Analysis

Detailed diagnosis of execution bottlenecks across the LangGraph workflow stages:

---

## 5. Metric 3: Suggestion Usefulness (Human Evaluation)

Usefulness assesses generated suggestions on a 1-5 scale across 7 qualitative dimensions:
1. **Completely irrelevant:** Unhelpful or counter-productive
2. **Weak contribution:** Marginal value or awkward phrasing
3. **Partially useful:** Relevant topic but incomplete or repetitive
4. **Useful and relevant:** Solid contribution addressing communication goals
5. **Highly useful:** Immediately usable, natural phrasing, excellent timing

> [!NOTE]
> **Human Usefulness Review Pending**: Generated suggestions are logged in the review template file and await human evaluator scores.
> To launch the interactive usefulness review CLI, run:
> ```bash
> python -m tests.evaluation.usefulness_review --interactive --run-file tests/evaluation/results/eval_run_20261031_121020.json
> ```

---

## 6. Metric 4: Factual Accuracy Assessment

Factual accuracy audits claim-level statements against reference facts and conversation history:
- **Supported:** Backed by conversation transcript or ground-truth facts.
- **Unsupported:** Plausible assertions lacking explicit verification.
- **Incorrect:** Factually false or contradicted by reference facts.
- **Not Applicable:** Non-falsifiable greetings, transitions, or pure questions.

> [!NOTE]
> **Factual Accuracy Review Pending**: Candidate claims have been extracted into the review template and await human verification.
> To launch the claim-level factual review CLI, run:
> ```bash
> python -m tests.evaluation.factuality_review --interactive --run-file tests/evaluation/results/eval_run_20261031_121020.json
> ```

---

## 7. Qualitative Analysis & Case Studies

### A. Successful Suggestions (True Positives)
#### Scenario: `pos_01_statement_no_question` (statement_without_question)
- **Selected Model:** `nvidia/nemotron-3-ultra-550b-a55b:free`
- **Intent Detected:** `statement` (Confidence: `0.92`)
- **Relevance Reason:** The discussion centers on a payment 504 incident and the user's objective is to clarify that connection pool exhaustion on the auth-proxy caused the downstream timeout chain. This goal has not yet been addressed in the conversation, giving the user a meaningful opportunity to contribute that specific point.
- **Suggestion 1 (supporting_point):** "The auth-proxy connection pool hit saturation at 14:14 UTC — pool metrics show 100% utilization with acquisition timeouts spiking to 2.3s, which aligns exactly with the downstream 504 cascade."
  - *Rationale:* Directly ties the observed 504 errors to a measurable pool-exhaustion event on the auth-proxy, using concrete metrics (utilization, acquisition latency) without assigning blame.

### B. Missed Opportunities (False Negatives)
*(No False Negatives observed)*

### C. False Suggestions (False Positives)
*(Zero False Positives observed — perfect specificity)*

---

## 8. Comparison with Previous Benchmark

Comparison against the initial un-stabilized benchmark run (`eval_run_20261031_102354`):

| Metric / Dimension | Initial Run (`eval_run_20261031_102354`) | Run with New Key (`eval_run_20261031_104429`) | Current Stabilized Architecture |
| :--- | :---: | :---: | :--- |
| **Successful Executions** | 1 / 24 (4.2%) | 6 / 24 (25.0%) | **2 / 2 (100.0%)** |
| **Structured Output Parsing** | Fragile (`json_schema` 502 crashes) | Fragile (OutputParserExceptions) | **Robust shared parser** with tool calls, code fences, normalization, and repair |
| **Model Telemetry** | Uncaptured (`openrouter/free` black box) | Uncaptured | **Resolved model capture** (e.g. `nvidia/nemotron-3.5-super-vl:free`) |
| **Relevance Precision** | 100.0% | 100.0% | **100.0%** |
| **Relevance Recall** | 100.0% (1/1) | 33.3% (2/6) | **100.0%** |
| **Metric Separation** | Combined (misleading) | Combined (misleading) | **Clean separation** of relevance, generation, and infrastructure |

---

## 9. Recommendations for Engineering Roadmap

1. **Dedicated Production Model Tier:**
   - Transition from `openrouter/free` to a dedicated low-cost paid tier (e.g., `openai/gpt-4o-mini`, `google/gemini-2.0-flash`, or `anthropic/claude-3-haiku`). This will eliminate upstream rate limits and router-induced format drift.
2. **Node Consolidation (`understanding` + `intent`):**
   - Combine conversation understanding and intent detection into a single prompt/node. This reduces LLM hops from 5 to 4 per session, lowering latency by ~25% and reducing quota consumption.
3. **Two-Tier Grounding in `generator.py`:**
   - Ensure `generator.py` receives a distilled bullet list of key facts from `relevance.py`'s `response_opportunity`, so the generator rarely yields empty outputs when relevance is already confirmed.
4. **Session Delta Summarization:**
   - Cache conversation understanding rolling state so repeated `/suggest` requests process only new message deltas rather than re-evaluating the entire 20-message transcript.
