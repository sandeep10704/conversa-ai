# Conversa AI Evaluation Framework

A comprehensive, production-grade evaluation pipeline for **Conversa AI**—an intelligent agent that listens to ongoing group discussions, analyzes conversation momentum, determines whether the user has a meaningful contribution opportunity, and generates context-aware suggestions.

---

## 1. Overview & Evaluation Philosophy

Conversa AI is designed as a non-intrusive participant in group conversations. Unlike conventional chat bots that respond unconditionally to every prompt, Conversa AI must exercise **conversational discipline**:
- **Intervene when appropriate:** Proactively recognize relevant opportunities—even when no explicit question or invitation is directed at the user.
- **Abstain when prudent:** Remain silent when the discussion is off-topic, when goals have already been covered, when constraints forbid intervention, or when context is ambiguous.

This evaluation framework establishes a rigorous, reproducible quality baseline across **24 realistic conversation scenarios** (12 positive relevance opportunities and 12 negative relevance control cases) without modifying the underlying agent architecture or fabricating human evaluation scores.

---

## 2. Benchmark Dataset Structure

The benchmark dataset is located at [`tests/evaluation/dataset.jsonl`](file:///Users/saivenkatasandeepgarlapati/Documents/projects/01/conversa-ai/tests/evaluation/dataset.jsonl). It contains 24 distinct scenarios spanning distributed systems, cloud infrastructure, incident response, mobile engineering, web performance, security reviews, and sprint planning.

> [!IMPORTANT]
> In accordance with Conversa AI's data model, transcripts are strictly chronological utterances **without speaker identities or diarization prefixes**.

### A. Positive Relevance Scenarios (12)
The agent is expected to identify a meaningful contribution opportunity (`status: "suggestions_ready"`):

| Scenario ID | Subtype | Core Challenge |
| :--- | :--- | :--- |
| `pos_01_statement_no_question` | Relevant statement without question | Interjecting on auth-proxy pool exhaustion root cause without an explicit question. |
| `pos_02_uncovered_communication_goal` | Uncovered communication goal | Introducing the transactional outbox pattern to address dual-write Kafka message loss. |
| `pos_03_concept_explanation` | Concept explanation opportunity | Explaining Raft randomized election timeouts to resolve split votes. |
| `pos_04_practical_example` | Practical implementation example | Sharing a 3-step expand-and-contract column rename pattern for zero-downtime migrations. |
| `pos_05_constructive_disagreement` | Constructive disagreement | Disagreeing with 200ms HTTP polling for collaborative cursors and proposing WebSockets. |
| `pos_06_clarification_request` | Clarification question | Inquiring whether 10% canary rollout hashes on user ID or tenant organization. |
| `pos_07_follow_up_discussion` | Related follow-up | Advising developers on Redis hash tags `{...}` to prevent cross-slot errors in Redis Cluster. |
| `pos_08_objective_not_addressed` | Relevant objective not addressed | Raising API gateway token bucket rate limiting on a public API edge security checklist. |
| `pos_09_additional_technical_perspective`| Additional technical perspective | Introducing S3 Glacier retrieval fees and minimum retention rules into a storage cost review. |
| `pos_10_latest_message_not_question` | Latest message is not a question | Suggesting bitmap downsampling and Glide caching after an analytical statement about Android OOM crashes. |
| `pos_11_contribute_without_repetition` | Non-repetitive contribution | Pinpointing scheduled snapshot I/O saturation during an Elasticsearch postmortem without repeating known GC issues. |
| `pos_12_multi_message_evolving_topic` | Multi-message evolving topic | Transitioning from UI button layout to proposing ISO 8601 UTC timestamps and client `Intl.DateTimeFormat`. |

### B. Negative Relevance Scenarios (12)
The agent is expected to abstain from suggesting (`status: "no_suggestion"`):

| Scenario ID | Subtype | Reason for Abstaining |
| :--- | :--- | :--- |
| `neg_01_completely_unrelated` | Completely unrelated discussion | Social chit-chat about food trucks and lunch during a database optimization context. |
| `neg_02_all_goals_already_addressed` | All communication goals addressed | Team already enabled and verified HPA memory and custom Kafka metrics. |
| `neg_03_repeated_information` | Repeated information | Colleague announced they added Redis TTL jitter seconds before; repeating is redundant. |
| `neg_04_insufficient_context` | Insufficient conversation context | Audio check greetings ("can you hear me?") provide zero context for architectural guidance. |
| `neg_05_superficial_keyword_match` | Superficial keyword match | Discussion about novelist Franz Kafka matches keyword 'Kafka' but is about literature, not event streaming. |
| `neg_06_unrelated_to_objective` | Unrelated to objective | Office facility renovations and room reservation policies during a frontend Svelte migration goal. |
| `neg_07_unsupported_claims_required` | Unsupported claims required | Answering would require guessing unverified customer retention metrics, violating constraints. |
| `neg_08_no_useful_contribution` | No useful contribution apparent | Automated RDS failover already completed successfully in 28s; manual instructions are moot. |
| `neg_09_topic_shifted_away` | Topic shifted away | Protocol agenda item concluded; meeting officially shifted to holiday on-call scheduling. |
| `neg_10_contribution_already_present` | Contribution already delivered | Team already accepted and agreed on Vite migration proposal presented by another engineer. |
| `neg_11_constraints_prohibit_response` | Constraints prohibit response | User's strict NDA constraint forbids sharing confidential negotiated vendor discount percentages. |
| `neg_12_ambiguous_abstain_safest` | Ambiguous rumors / safest to abstain | Unverified corporate reorganization rumors; constraint requires abstaining from spreading speculation. |

---

## 3. Evaluation Metrics

### Metric 1: Relevance Precision
Measures the proportion of generated suggestions that correspond to valid contribution opportunities:
$$\text{Precision} = \frac{\text{TP}}{\text{TP} + \text{FP}}$$
- **TP (True Positive):** Agent correctly generated suggestions on a positive scenario.
- **FP (False Positive):** Agent incorrectly generated suggestions on a negative scenario (unwanted interjection).

### Metric 2: Missed Opportunity Rate & Recall
Measures how often valid contribution opportunities are missed:
$$\text{Missed Opportunity Rate} = \frac{\text{FN}}{\text{TP} + \text{FN}}$$
$$\text{Recall} = \frac{\text{TP}}{\text{TP} + \text{FN}}$$
- **FN (False Negative):** Agent abstained (`no_suggestion`) on a positive scenario where a contribution was warranted.

### F1 Score
Harmonic mean balancing precision and recall:
$$\text{F1} = 2 \times \frac{\text{Precision} \times \text{Recall}}{\text{Precision} + \text{Recall}}$$

### Metric 3: Suggestion Usefulness (Human Evaluation)
Rated on a 1–5 qualitative rubric:
1. **Completely irrelevant:** Unhelpful, confusing, or counter-productive.
2. **Weak contribution:** Marginal value or awkward conversational tone.
3. **Partially useful:** Addresses the topic but has notable omissions, slight repetition, or excessive verbosity.
4. **Useful and relevant:** Solid contribution aligned with the user's objective and communication goals.
5. **Highly useful:** Immediately usable, crisp conversational phrasing, excellent timing.

*Qualitative Dimensions Evaluated:*
- Relevance to ongoing conversation
- Alignment with user objective
- Alignment with communication goals
- Practical value
- Natural conversational language
- Conciseness
- Non-repetition

### Metric 4: Factual Accuracy
Assesses verifiable claims extracted from generated suggestions:
$$\text{Factual Accuracy} = \frac{\text{Supported Claims}}{\text{Total Verifiable Claims}}$$
- **Supported:** Grounded directly in conversation history or scenario `reference_facts`.
- **Unsupported:** Plausible-sounding assertions lacking factual verification.
- **Incorrect:** Factually false or contradicted by facts.
- **Not Applicable:** Non-falsifiable greetings or questions.

---

## 4. Directory Structure

```text
tests/evaluation/
├── dataset.jsonl             # 24 realistic test scenarios
├── schemas.py                # Pydantic data models for evaluation
├── dataset_loader.py         # Scenario loader and integrity validator
├── run_evaluation.py         # HTTP test runner against Conversa AI API
├── metrics.py                # Precision, Recall, Missed Opp Rate, F1, & Reviews
├── usefulness_review.py      # Human usefulness review CLI & management
├── factuality_review.py      # Factual accuracy claim extraction & review CLI
├── report_generator.py       # Markdown evaluation report generator
├── test_evaluation_framework.py # Automated pytest test suite
├── README.md                 # Framework documentation
├── results/                  # Raw execution JSON outputs
└── reports/                  # Generated Markdown evaluation reports
    └── evaluation_report.md  # Latest evaluation benchmark report
```

---

## 5. Execution Guide

### Prerequisites
Ensure the Conversa AI Agent Server is running at `http://localhost:8001`:
```bash
curl http://localhost:8001/health/live
# Expected: {"service":"conversa-agent-server","status":"alive"}
```

### Running the Full Evaluation Benchmark
Execute all 24 scenarios through the REST API:
```bash
python -m tests.evaluation.run_evaluation
```
Options:
- `--base-url <url>`: Specify custom agent server host (default: `http://localhost:8001`).
- `--scenario-id <id>`: Run a specific scenario (can be repeated).
- `--category <positive|negative>`: Run only positive or negative scenarios.
- `--retries <n>`: Number of retry attempts on network/rate limit errors (default: `3`).
- `--backoff <factor>`: Exponential backoff factor (default: `2.0`).

### Running a Specific Scenario
```bash
python -m tests.evaluation.run_evaluation --scenario-id pos_01_statement_no_question
```

### Conducting Human Reviews

#### Suggestion Usefulness Review
Launch the interactive human scoring terminal:
```bash
python -m tests.evaluation.usefulness_review --interactive --run-file tests/evaluation/results/<run_id>.json
```

#### Factual Accuracy Review
Audit claim-level statements against reference facts:
```bash
python -m tests.evaluation.factuality_review --interactive --run-file tests/evaluation/results/<run_id>.json
```

### Regenerating Reports
After completing reviews, refresh the final Markdown report:
```bash
python -m tests.evaluation.report_generator \
  --run-file tests/evaluation/results/<run_id>.json \
  --usefulness-file tests/evaluation/results/usefulness_<run_id>.json \
  --factuality-file tests/evaluation/results/factuality_<run_id>.json \
  --report-file tests/evaluation/reports/evaluation_report.md
```

### Running Framework Unit Tests
Verify that schemas, loader integrity, and metric computations operate flawlessly:
```bash
pytest tests/evaluation/test_evaluation_framework.py -v
```

---

## 6. Reproducibility & OpenRouter Free Router Notes

Conversa AI utilizes `openrouter/free` as its default language model router. Because the free router dynamically assigns queries to varying backend open-source providers:
1. **Model Variance:** Responses, latencies, and output adherence can fluctuate across runs.
2. **Output Parsing Vulnerability:** Some free models return raw Markdown instead of strict JSON schema objects, causing intermittent 502 Bad Gateway responses in the `generator` node.
3. **Execution Separation:** Failed requests are explicitly isolated under `Failed Executions` and are **never silently counted as negative relevance decisions**.
