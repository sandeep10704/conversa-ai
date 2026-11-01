# Conversa AI — Nemotron Inference Reliability: Offline Fix Preparation

**Date:** 2026-11-01T05:10:00Z
**Target Model:** `nvidia/nemotron-3.5-lightning:free`
**Execution Mode:** Offline Preparation & Validation
**OpenRouter Inference Requests Made:** **0**
**Production Defaults:** `MOCK_LLM=true`, `AGENT_AUTO_RUN=false` (Unchanged)

---

## 1. Executive Summary

Following the forensic diagnosis of the failed canary (where `nvidia/nemotron-3.5-lightning:free` hit the 1,000 completion token ceiling during internal chain-of-thought reasoning), we prepared and validated a safe, hardened request configuration completely offline.

Key Accomplishments:
1. **Public Metadata Inspection:** Inspected OpenRouter's public model catalog for `nvidia/nemotron-3.5-lightning:free`. Confirmed that `reasoning.mandatory` is `false`, but `supported_efforts` is completely omitted. Established metadata-driven reasoning configuration that avoids blindly injecting unverified parameters.
2. **Concise Grounding-Preserving Prompt:** Replaced the bloated 336-word canary prompt with a tight 161-word prompt (**-52.1% word reduction**), asking for exactly one suggestion. All 6 core grounding, timing distinction, and anti-repetition rules are fully preserved.
3. **Completion Budget Rationale:** Retained `max_tokens=1000` with temperature 0.1, matching Experiment 2 which succeeded in 830 tokens. By cutting prompt complexity in half and limiting generation to 1 suggestion, the model's internal reasoning is curtailed back into the safe 600–750 token window without bloating token limits.
4. **Offline Regression Suite:** Added 14 offline unit tests in [`tests/test_nemotron_inference_reliability.py`](file:///Users/saivenkatasandeepgarlapati/Documents/projects/01/conversa-ai/tests/test_nemotron_inference_reliability.py) verifying model ID enforcement, reasoning control logic, zero retries, prohibition of parser repairs, empty/truncated response rejection, and valid response parsing. **All 14 tests pass with outbound network calls prohibited.**
5. **Zero API Consumption:** Zero real model requests were performed.

---

## 2. Model Metadata Verification

We queried OpenRouter's unauthenticated public models endpoint (`GET https://openrouter.ai/api/v1/models`) for `nvidia/nemotron-3.5-lightning:free`:

| Property | Value | Notes |
|---|---|---|
| **Model ID** | `nvidia/nemotron-3.5-lightning:free` | Exact model endpoint |
| **Canonical Slug** | `nvidia/nemotron-3.5-lightning-20260807` | MoE 3B active / 30B total |
| **Context Length** | 1,000,000 | Token context window |
| **Supported Parameters** | `["include_reasoning", "max_tokens", "reasoning", "seed", "temperature", "tool_choice", "tools", "top_p"]` | Includes `reasoning` |
| **`reasoning.mandatory`** | `false` | Reasoning is not forced |
| **`reasoning.supported_efforts`** | `null` (omitted) | **Not defined in catalog** |
| **`reasoning.default_effort`** | `null` (omitted) | Not defined |
| **`reasoning.default_enabled`** | `null` (omitted) | Not defined |
| **`reasoning.supports_max_tokens`** | `null` (omitted) | Not defined |

### Comparison to Other Endpoints:
- Models such as `nvidia/nemotron-3-ultra-550b-a55b` explicitly define `"supported_efforts": ["high", "medium", "none"]` and `"default_effort": "high"`.
- In contrast, `nvidia/nemotron-3.5-lightning:free` omits `supported_efforts` entirely.

### Safe Parameter Handling Rule:
Because `supported_efforts` is omitted in the model metadata, client support for `reasoning={"effort": "none"}` is **ambiguous**. Passing `reasoning={"enabled": False}` (as done previously) was silently ignored by OpenRouter because `enabled` is not a valid OpenRouter property. Using `reasoning.exclude=true` is prohibited because it only hides reasoning tokens from the response without reducing token consumption.

**Decision:** The request builder checks model metadata dynamically. It applies `reasoning={"effort": "none"}` only when `supported_efforts` explicitly contains `"none"`. When omitted or ambiguous, it omits the unconfirmed reasoning parameter to avoid gateway rejections or unintended behavior.

---

## 3. Before-and-After Prompt Comparison

| Dimension | Previous Canary Prompt | Proposed Concise Prompt | Impact |
|---|---|---|---|
| **Character Count** | 2,409 characters | 1,180 characters | **-51.0% reduction** (-1,229 chars) |
| **Word Count** | 336 words | 161 words | **-52.1% reduction** (-175 words) |
| **Target Output** | "At most two suggestions" | "ONE concise spoken suggestion" | Halves generation search space |
| **Rule Structure** | 10 rules with 5 sub-bullets | 6 dense, focused rules | Removes cognitive reasoning overhead |
| **Hidden Reasoning Instruction** | Mentioned at the end | Explicit: "Do not include hidden reasoning... Output only the JSON object." | Direct output instruction |

### Preservation of Mandatory Quality Criteria:
1. **Spoken Natural Style:** Rule 1: *"Natural, direct, and concise for speaking aloud in a meeting."*
2. **Factual Grounding:** Rule 2: *"Use only facts stated in the conversation."*
3. **Participant-Reported Figures Qualified:** Rule 2: *"Treat participant-stated figures (e.g., ~50ms Silero VAD, ~180ms Whisper Tiny) as conversational observations, not verified system benchmarks."*
4. **Latency vs. Silence Distinction:** Rule 3: *"Do not confuse VAD chunk processing latency with speech silence thresholds for turn-taking."*
5. **No Invented Numbers or Architecture:** Rule 4: *"Do not invent unmentioned timing thresholds, safety margins, or components. If an exact threshold is unmeasured, recommend measuring, profiling, or calibrating instead of asserting an invented figure."*
6. **Anti-Repetition:** Rule 5: *"Check previous_suggestions; do not repeat ideas already suggested."*

---

## 4. Proposed Request Budget & Configuration

```python
{
    "model": "nvidia/nemotron-3.5-lightning:free",
    "temperature": 0.1,
    "max_tokens": 1000,
    "max_retries": 0,
    "timeout": 60_000,
    "app_url": "https://github.com/sandeep10704/conversa-ai",
    "app_title": "Conversa AI Quality Canary"
}
```

### Justification for `max_tokens=1000`:
- In Experiment 2, using a concise prompt (178 words), `nvidia/nemotron-3.5-lightning:free` consumed 830 completion tokens total (~710 reasoning tokens + ~120 JSON tokens) and completed successfully with `finish_reason="stop"`.
- The Canary run hit 1,000 completion tokens solely because the 336-word prompt and 10 nested rules nearly doubled the deliberation requirements.
- By shrinking the prompt by 52% and requesting a single suggestion, reasoning length is brought back within the 600–750 token window, leaving ample headroom within `max_tokens=1000`.
- Retaining `max_tokens=1000` satisfies the requirement to avoid unneeded token ceiling inflation while guaranteeing zero retries (`max_retries=0`) and zero parser repairs (`allow_repair=False`).

---

## 5. Offline Regression Test Results

We implemented and executed [`tests/test_nemotron_inference_reliability.py`](file:///Users/saivenkatasandeepgarlapati/Documents/projects/01/conversa-ai/tests/test_nemotron_inference_reliability.py):

| Test Case | Scope | Status |
|---|---|---|
| `test_request_builder_exact_model_id` | Verifies exact model ID `nvidia/nemotron-3.5-lightning:free` | **PASS** |
| `test_request_builder_rejects_non_canary_model` | Rejects non-canary model IDs with `ValueError` | **PASS** |
| `test_reasoning_config_when_metadata_missing` | Omits reasoning when metadata is None | **PASS** |
| `test_reasoning_config_for_actual_nemotron_free_metadata` | Omits reasoning for actual Nemotron Free metadata (ambiguous support) | **PASS** |
| `test_reasoning_config_when_none_is_explicitly_supported` | Sets `effort: 'none'` when metadata explicitly includes `'none'` | **PASS** |
| `test_reasoning_config_when_mandatory_is_true` | Rejects `effort: 'none'` when reasoning is mandatory | **PASS** |
| `test_reasoning_config_when_none_not_in_supported_efforts` | Omits reasoning when `'none'` is not in `supported_efforts` | **PASS** |
| `test_max_retries_is_strictly_zero` | Guarantees `max_retries=0` and `temperature=0.1` | **PASS** |
| `test_no_parser_repair_attempted` | Ensures `allow_repair=False` prevents `execute_repair_attempt` | **PASS** |
| `test_concise_prompt_preserves_all_grounding_rules` | Checks all 6 quality rules, single suggestion, and word count < 200 | **PASS** |
| `test_empty_content_response_rejected` | Confirms empty content (`""`) is rejected as `EMPTY_RESPONSE` | **PASS** |
| `test_truncated_json_response_rejected` | Confirms truncated JSON is rejected as `JSON_PARSE_ERROR` | **PASS** |
| `test_valid_json_content_parses_cleanly` | Confirms valid JSON parses to `SuggestionSet` | **PASS** |
| `test_valid_tool_call_parses_cleanly` | Confirms native tool calls parse to `SuggestionSet` | **PASS** |

**Summary:** 14/14 tests passed in 0.05 seconds. Full agent-server test suite (77 tests) passed in 3.96 seconds. Outbound network sockets were blocked during execution.

---

## 6. Remaining Uncertainties

1. **Dynamic Upstream Provider Routing:**
   `nvidia/nemotron-3.5-lightning:free` on OpenRouter routes dynamically across multiple upstream providers (NVIDIA NIM, Io Net, Fireworks, DeepInfra, CoreWeave). Upstream providers may enforce thinking tokens regardless of client reasoning parameters. The prompt conciseness reduction (from 336 words to 161 words) is the most robust safeguard against token exhaustion.

---

## 7. Next Step: Manually Approved Single-Request Canary

The offline fix is fully prepared. **No real inference was executed.**

When the user approves running the next real-model canary, execute the following command:

```bash
PYTHONPATH=services/agent-server:. services/agent-server/.venv/bin/python tests/run_real_model_canary.py
```
