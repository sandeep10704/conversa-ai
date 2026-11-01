# Conversa AI — Focused Quality & Grounding Evaluation Report

**Date:** 2026-10-31T19:15:43.099052+00:00
**Status:** ALL SCENARIOS PASSED (6/6)
**OpenRouter API Calls Made:** 0 (Deterministic Offline Evaluation)
**Configuration:** `MOCK_LLM=true`, `AGENT_AUTO_RUN=false`

---

## 1. Executive Summary

This report evaluates the context-aware suggestion quality, factual grounding, and repetition prevention improvements across six focused scenarios and regression test suites.

All evaluations ran completely offline with **zero OpenRouter API calls**, zero external HTTP requests, and deterministic state fixtures.

### Key Improvements Verified
1. **Factual Grounding & Numeric Protection:**
   - Participant-stated numbers (e.g., 50 ms Silero VAD, 180 ms Whisper Tiny) remain valid when cited directly.
   - Any invented metrics, unstated SLAs, or artificial safety margins (such as the ~100 ms margin from Experiment 2) are deterministically rejected.
2. **Timing Concept Disambiguation:**
   - VAD chunk processing latency, speech-end silence detection threshold, transcription latency, and LLM inference time are explicitly separated.
   - Suggestions recommend empirical calibration/profiling when required parameters are missing.
3. **Repetition Prevention:**
   - Evaluates underlying idea and token containment overlap (containment >= 0.50 or Jaccard >= 0.40) against `previous_suggestions`, preventing paraphrased duplicates.
4. **Natural Contribution Quality & Platitude Rejection:**
   - Rejects empty platitudes (e.g., *"Consider reviewing the asynchronous event pipeline latency metrics"*).
   - Enforces concise, natural phrasing ready to be spoken aloud.

---

## 2. Six Focused Scenarios Results

| # | Scenario | Intent | Should Suggest | Result Status | Grounding | Repetition Check |
|---|---|---|---|---|---|---|
| **1** | Direct technical question with enough context | question | Yes | `suggestions_ready` | Passed (LISTEN/NOTIFY suggested) | N/A (first suggestion) |
| **2** | Unresolved engineering decision (no direct question) | statement | Yes | `suggestions_ready` | Passed (Empirical benchmark advised) | N/A |
| **3** | Unrelated conversation (team lunch chat) | statement (social) | No | `no_suggestion` | Passed (Relevance routed to END) | N/A |
| **4** | All discussion goals already addressed | statement (resolved) | No | `no_suggestion` | Passed (Goal satisfaction detected) | N/A |
| **5** | Previous suggestion repeated in later response | statement | Yes | `no_suggestion` | Passed | **Detected & Rejected (overlap: 0.62)** |
| **6** | Technical recommendation with missing measurements | statement | Yes | `suggestions_ready` (Grounded) | **Flawed Rejected / Grounded Accepted** | N/A |

---

## 3. Regression Test: VAD Timing Confusion (Experiment 2 Before vs After)

### Before (Experiment 2 Output)
```text
"Silero VAD's 50 ms and Whisper Tiny's 180 ms leave about 270 ms for the LLM; add a brief safety margin of ~100 ms to ensure smooth delivery."
```
* **Issues:**
  1. Conflated chunk processing time with speech-end silence threshold.
  2. Invented a 270 ms budget without an explicit total latency constraint in the discussion.
  3. Invented an ungrounded `~100 ms` safety margin.
* **Deterministic Validator Action:** **REJECTED**
* **Warnings Emitted:**
  - `Unsupported numeric recommendation: ungrounded metric/threshold '270 ms' not present in conversation context.`
  - `Unsupported recommendation: proposes ungrounded safety margin without empirical calibration.`

### After (Grounded Output)
```text
"The 50 ms VAD and 180 ms Whisper times are participant estimates. We should measure the end-of-speech silence detection threshold before fixing an LLM latency budget."
```
* **Improvements:**
  1. Clearly qualifies participant-reported numbers as estimates, not global benchmarks.
  2. Recommends empirical testing for the unmeasured silence detection threshold.
  3. Uses only numbers supported by conversation (`50` and `180`).
* **Deterministic Validator Action:** **ACCEPTED** (`suggestions_ready`)

---

## 4. Before-and-After Comparisons

### A. Repetition Prevention
* **Before:** `We should enforce 16 kHz mono 16-bit PCM at the Audio Gateway to eliminate client-side transcoding issues.` followed by `We should require 16 kHz mono 16-bit PCM format on the Audio Gateway WebSocket to stop transcoding errors.` were both admitted because exact strings differed.
* **After:** Content tokenization and containment overlap (61.5%) flags the paraphrased duplicate as `Repeated suggestion: underlying idea already addressed in previous suggestion` and suppresses it.

### B. Generic Platitudes
* **Before:** `Consider reviewing the asynchronous event pipeline latency metrics.` was accepted as a valid suggestion.
* **After:** Regex and semantic patterns catch empty boilerplate (`consider reviewing .* metrics`) and reject it: `Generic platitude rejected: suggestion lacks substantive technical contribution.`

---

## 5. Remaining Limitations

1. **Semantic Embeddings for Paraphrases:** While token containment overlap effectively catches paraphrased engineering suggestions, complex metaphorical shifts without overlapping terminology would benefit from offline vector embeddings in future iterations.
2. **Context Window Boundary:** In extremely long multi-turn sessions (100+ turns), historical suggestion checks compare against the latest 10 suggestions (`limit=10`).
3. **Multilingual Conversations:** Current stop-word and tokenization logic is optimized for English technical discussions.

---

## 6. Safety and Budget Verification

- **Real OpenRouter Requests:** **0**
- **Docker Compose Status:** `MOCK_LLM=true`, `AGENT_AUTO_RUN=false` preserved.
- **Full Test Suite:** 77 passing tests (0 failures).
