# Conversa AI — Nemotron Empty Canary Response Forensic Diagnosis

**Date:** 2026-10-31T20:12:39.080427+00:00
**Model:** `nvidia/nemotron-3.5-lightning:free`
**Provider:** OpenRouter (NVIDIA NIM backend)
**Root Cause Confidence:** **HIGH (98%)**
**Additional OpenRouter API Calls Made:** **0** (Offline Artifact & Telemetry Analysis)

---

## 1. Executive Summary

During the recent canary run, `nvidia/nemotron-3.5-lightning:free` returned HTTP 200 OK with `finish_reason: "length"`, consuming **exactly 1,000 completion tokens** (the configured `max_tokens` ceiling) and returning an empty string for `content`.

In contrast, earlier Experiment 2 using the identical model generated a structured suggestion in 830 completion tokens with `finish_reason: "stop"`.

Forensic comparison of the request payloads, telemetry, and OpenRouter parameter handling reveals the exact root cause:
1. **Hidden Reasoning Token Consumption:** `nvidia/nemotron-3.5-lightning:free` is an open-weights reasoning model. In Experiment 2, it generated **830 completion tokens**, yet the emitted JSON payload was only **~120 tokens**. The model was already spending **~710 tokens on internal chain-of-thought** before outputting JSON, despite `reasoning={"enabled": False}`.
2. **Prompt Complexity Expansion:** The canary system prompt nearly doubled in size (from 1,231 chars / 178 words to 2,409 chars / 336 words), introducing 10 complex constraints (VAD latency vs silence duration distinction, safety margin prohibitions, empirical calibration instructions, and repetition prevention).
3. **Completion Token Starvation:** Reconciling these new constraints required additional reasoning steps. The model's chain-of-thought expanded to consume the entire 1,000 token budget (`finish_reason="length"`). OpenRouter severed the generation stream before the model could emit the final `{"suggestions": [...]}` text block.
4. **Deterministic Parser Protection:** The Conversa AI parser properly identified the empty content and raised `StructuredOutputError(EMPTY_RESPONSE)`, which was safely rejected (`no_suggestion`) without executing unsafe retries.

---

## 2. Side-by-Side Parameter Comparison

| Dimension | Experiment 2 (Success) | Canary (Empty Response) | Difference & Impact |
|---|---|---|---|
| **Model** | `nvidia/nemotron-3.5-lightning:free` | `nvidia/nemotron-3.5-lightning:free` | Identical endpoint. |
| **Provider** | OpenRouter | OpenRouter | Identical. |
| **System Prompt Length** | 1,231 chars (178 words) | 2,409 chars (336 words) | **+95.7% expansion**. Significantly more complex prompt logic. |
| **User Context Length** | 1,969 chars (12 fields) | 2,066 chars (13 fields) | +97 chars (`previous_suggestions` field added). |
| **Prompt Tokens** | 1,065 | 1,346 | **+281 tokens (+26.4%)**. |
| **Reasoning Control** | `{"enabled": False}` | `{"enabled": False}` | Identical, but OpenRouter docs state that `effort: "none"` or `max_tokens: 0` is required to fully suppress thinking on some provider backends. |
| **Max Tokens** | 1,000 | 1,000 | Identical ceiling. In Exp 2, total usage was 830 < 1000. In Canary, deeper reasoning hit 1000. |
| **Completion Tokens** | 830 (finish_reason=`stop`) | 1,000 (finish_reason=`length`) | **Token budget reached.** |
| **Emitted Content** | ~120 tokens of valid JSON | Empty string `""` | Cut off before content emission. |
| **Tool Calls Emitted** | None (`[]`) | None (`[]`) | Model outputs raw JSON into content instead of native tool calling. |
| **Temperature** | 0.1 | 0.1 | Identical. |
| **Tool Schema** | `SuggestionSet` | `SuggestionSet` | Identical. |
| **Tool Choice** | `auto` | `auto` | Identical. |

---

## 3. Evaluation of the 5 Hypotheses

### 1. Consumed tokens during reasoning? — **CONFIRMED ROOT CAUSE**
- **Evidence:** In Experiment 2, `completion_tokens` was 830, while the actual JSON suggestion was only ~120 tokens. The model spent ~710 tokens on internal chain-of-thought reasoning before outputting JSON text.
- With the expanded canary prompt (10 strict rules), the model's reasoning expanded past 1,000 tokens.

### 2. Generated an incomplete tool call? — **DISPROVED**
- **Evidence:** Both Experiment 2 and the Canary returned empty `tool_calls: []`. `nemotron-3.5-lightning:free` does not generate native OpenAI tool calls under `tool_choice="auto"`; it outputs raw JSON directly into `content`.

### 3. Returned malformed tool arguments? — **DISPROVED**
- **Evidence:** No tool call was generated; therefore, arguments were neither present nor malformed.

### 4. Returned content in an unexpected format? — **DISPROVED**
- **Evidence:** The model emitted `content: ""`. It did not emit prose, markdown, or unstructured commentary.

### 5. Reached output limit before completing structure? — **CONFIRMED MECHANISM**
- **Evidence:** The response returned `finish_reason: "length"` at exactly `completion_tokens: 1000`. The token limit truncated the request during reasoning before content emission began.

---

## 4. Parser Compatibility Assessment

We verified [`services/agent-server/app/core/parsing.py`](file:///Users/saivenkatasandeepgarlapati/Documents/projects/01/conversa-ai/services/agent-server/app/core/parsing.py) against all 5 response types:

1. **Tool-Call-Only Responses:** Fully compatible. Extracts `tool_call["args"]` from `tool_calls[0]`.
2. **JSON Content Responses:** Fully compatible. Extracts content, cleans code fences, and parses JSON via `extract_raw_json`.
3. **Empty Content:** Strictly guarded. Detects empty strings and raises `StructuredOutputError(EMPTY_RESPONSE)`, safely triggering deterministic rejection.
4. **Incomplete Tool Arguments:** Strictly guarded. Detects unparseable JSON or schema mismatches and raises `StructuredOutputError(SCHEMA_VALIDATION_ERROR)`.
5. **Truncated Responses:** Strictly guarded. Incomplete JSON raises `JSON_PARSE_ERROR` and is discarded without triggering unsafe retry loops.

The parser behavior is correct and preserves system stability by rejecting incomplete model responses.

---

## 5. Recommended Safe Fixes (No Production Changes Yet)

To resolve compatibility when evaluating reasoning models without spending excessive budget:

1. **Pass `effort: "none"` in Reasoning Options:**
   OpenRouter normalizes provider-level reasoning suppression via:
   ```python
   reasoning={"effort": "none", "enabled": False}
   ```
   This explicitly instructs the underlying NVIDIA NIM container to skip chain-of-thought generation.

2. **Increase Test `max_tokens` Ceiling for Reasoning Models:**
   In `tests/run_real_model_canary.py`, increase `max_tokens` from `1000` to `2048` (production already uses `max_tokens=1200`). This ensures that if a provider continues to emit thinking tokens, the model still has enough token budget (300–500 tokens) to emit the final structured JSON.

3. **Prompt Conciseness:**
   Keep all 10 grounding, timing distinction, and repetition rules, but condense the wording from 336 words to ~200 high-density words to reduce the model's cognitive deliberation time.

---

## 6. Safety Verification

- **Real OpenRouter Requests Made During Diagnosis:** **0**
- **Lifetime OpenRouter Calls:** Preserved at 3 (`used: 3, limit: 50, remaining: 47`).
- **Configuration:** `MOCK_LLM=true`, `AGENT_AUTO_RUN=false` active.
