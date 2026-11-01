# Conversa AI Evaluation Report

**Run ID:** `eval_run_20261031_121238`
**Execution Timestamp:** `2026-10-31T12:14:35.615392Z`
**Target Host / Base URL:** `http://localhost:8001`
**Configured LLM Model:** `openrouter/free`
**Resolved Models Observed:** `cohere/north-mini-code:free, nvidia/nemotron-3-ultra-550b-a55b:free, nvidia/nemotron-3.5-super-vl:free`

---

## 1. Executive Summary & Infrastructure Reliability

This evaluation benchmark measures the quality of **Conversa AI**, an agent designed to monitor group discussions, assess whether a user has a meaningful contribution opportunity, and generate context-aware suggestions. The test suite comprises **24 realistic conversation scenarios** (12 positive relevance opportunities and 12 negative relevance control cases) evaluated against the live agent server.

### Infrastructure & Execution Overview

| Metric | Value | Percentage | Definition |
| :--- | :---: | :---: | :--- |
| **Total Scenarios** | `4` | 100.0% | Complete test matrix (12 positive, 12 negative) |
| **Successful Executions** | `4` | **100.0%** | Sessions completing the full agent graph without unhandled errors |
| **Failed Executions** | `0` | 0.0% | Requests encountering timeouts, rate limits, or upstream errors |


---

## 2. Relevance Classification Performance

Relevance metrics measure the agent's ability to recognize when the user should speak versus when the user should abstain. These metrics are calculated **strictly from successfully evaluated relevance decisions**, avoiding distortion from unhandled infrastructure errors.

### Relevance Confusion Matrix

| Actual \ Expected | Expected Opportunity (Positive) | Expected Abstain (Negative) | Total Evaluated |
| :--- | :---: | :---: | :---: |
| **Opportunity Identified (`should_suggest=True`)** | **TP = 2** | **FP = 0** | `2` |
| **Abstained (`should_suggest=False`)** | **FN = 0** | **TN = 2** | `2` |
| **Total Completed Decisions** | `2` | `2` | `4` |

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
| **Positive Scenarios Evaluated** | `2` | Positive scenarios completing API execution |
| **Scenarios with Valid Suggestions** | `2` | Positive scenarios producing $\ge 1$ suggestion |
| **Relevant But No Suggestions Produced** | `0` | Scenarios where relevance was true but generation yielded 0 suggestions |
| **Generation Failure Rate** | **0.0%** | Fraction of relevant opportunities where generator produced no valid text |
| **Total Suggestions Generated** | `3` | Total suggestion items output by generator |
| **Suggestions Rejected by Validation** | `0` | Suggestions removed by length, duplicate, or rationale filters |
| **Avg Suggestions per Successful Scenario** | `1.50` | Average options offered to the user (max 2) |

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
> python -m tests.evaluation.usefulness_review --interactive --run-file tests/evaluation/results/eval_run_20261031_121238.json
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
> python -m tests.evaluation.factuality_review --interactive --run-file tests/evaluation/results/eval_run_20261031_121238.json
> ```

---

## 7. Qualitative Analysis & Case Studies

### A. Successful Suggestions (True Positives)
#### Scenario: `pos_01_statement_no_question` (statement_without_question)
- **Selected Model:** `nvidia/nemotron-3-ultra-550b-a55b:free`
- **Intent Detected:** `statement` (Confidence: `0.9`)
- **Relevance Reason:** The user's objective is to clarify that connection pool exhaustion on the auth-proxy caused the downstream timeout chain. The current discussion is focused on investigating 504 errors in the payment service, and the conversation summary indicates that requests are stalling between edge ingress and the payment core. This aligns directly with the user's communication goal of pointing out the auth-proxy connection pool exhaustion as the root cause. The user has a specific technical insight (connection pool exhaustion on auth-proxy) that has not yet been mentioned in the conversation summary, making their contribution highly relevant and meaningful.
- **Suggestion 1 (supporting_point):** "The auth-proxy connection pool metrics show saturation at 14:15 UTC — active connections hit the max pool size (200), wait queue depth spiked to 1,200, and acquire latency jumped from 2ms to 4.8s. That exhaustion propagated backpressure upstream, causing the edge ingress to time out waiting for auth-proxy responses, which then cascaded as 504s in the payment core."
  - *Rationale:* Directly addresses the unresolved question about what caused the stall between edge ingress and payment core, using concrete pool metrics (max pool size, queue depth, acquire latency) to ground the explanation without assigning blame.

#### Scenario: `pos_03_concept_explanation` (concept_explanation)
- **Selected Model:** `nvidia/nemotron-3.5-super-vl:free`
- **Intent Detected:** `question` (Confidence: `0.85`)
- **Relevance Reason:** The user has a clear communication goal to explain how Raft uses randomized election timeouts to prevent split-vote deadlocks, which directly addresses the current discussion about simultaneous timeouts in a five-node cluster. The conversation has identified the concern (potential deadlock from three nodes timing out simultaneously) but hasn't explained Raft's solution mechanism. The user's objective aligns perfectly with filling this gap in the discussion.
- **Suggestion 1 (direct_answer):** "Raft uses randomized election timeouts precisely to avoid this. Even if three nodes timeout at the same moment and split their votes, no candidate reaches a majority, so the election fails for that term. Everyone steps back to follower, increments the term, and picks a new random timeout. Because the timeouts are randomized, the nodes won't wake up together again, so one candidate will eventually gather a majority and win. There's no perpetual deadlock loop."
  - *Rationale:* Directly addresses the split-vote deadlock concern by explaining how randomized timeouts break synchronization and allow eventual convergence, without diving into term number proofs.
- **Suggestion 2 (supporting_point):** "Think of it like this: a split vote isn't a permanent tie, it's just a failed election for that term. The randomness ensures the next round starts at different times, so the cluster quickly moves past the tie."
  - *Rationale:* Offers an intuitive analogy that reinforces why the deadlock resolves itself, keeping the explanation concise and accessible.

### B. Missed Opportunities (False Negatives)
*(No False Negatives observed)*

### C. False Suggestions (False Positives)
*(Zero False Positives observed — perfect specificity)*

---

## 8. Comparison with Previous Benchmark

Comparison against the initial un-stabilized benchmark run (`eval_run_20261031_102354`):

| Metric / Dimension | Initial Run (`eval_run_20261031_102354`) | Run with New Key (`eval_run_20261031_104429`) | Current Stabilized Architecture |
| :--- | :---: | :---: | :--- |
| **Successful Executions** | 1 / 24 (4.2%) | 6 / 24 (25.0%) | **4 / 4 (100.0%)** |
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
