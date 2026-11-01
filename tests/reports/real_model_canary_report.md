# Conversa AI — Controlled Real-Model Canary Report

**Timestamp:** 2026-11-01T05:15:12.530766+00:00
**Model:** `nvidia/nemotron-3.5-lightning:free` (routed to `nvidia/nemotron-3.5-lightning:free`)
**Provider:** OpenRouter (NVIDIA)
**Outbound Requests Count:** **1** (Strict Single-Request Guard Verified)
**Validator Verdict:** **ACCEPTED** (`suggestions_ready`)

---

## 1. Executive Summary

This canary experiment evaluated the newly deployed prompt grounding rules and deterministic validation engine against a real inference request using `nvidia/nemotron-3.5-lightning:free`.

The test executed **exactly ONE outbound OpenRouter request** with `max_retries=0`, `allow_repair=False`, and zero secondary LLM calls.

The canary experiment executed successfully. The model generated a structured suggestion in 640 completion tokens (finish_reason=`stop`), well within the 1,000 completion token ceiling. The deterministic validator evaluated all 6 quality and factual grounding criteria and accepted 1 suggestion into the session with zero validation warnings.

---

## 2. Telemetry and Latency

| Metric | Value |
|---|---|
| **Outbound Requests** | **1** (Physical Guard Verified) |
| **Duration** | 8.53 s |
| **Prompt Tokens** | 1087 |
| **Completion Tokens** | 640 |
| **Total Tokens** | 1727 |
| **Finish Reason** | `stop` |
| **Request ID** | `gen-1793510104-FTj7mgyhOIfoBOyp4hlA` |

---

## 3. Raw Model Output and Validation

### Raw Generated Suggestion
```json
[
  {
    "response_type": "direct_answer",
    "text": "After the VAD reports the user has stopped speaking, insert a brief, calibrated pause before starting LLM inference to ensure the user's turn is complete.",
    "rationale": "The conversation states Silero VAD processes in ~50 ms and Whisper Tiny in ~180 ms; a short, measured pause after VAD endpointing prevents the assistant from speaking over the user while keeping latency predictable.",
    "follow_up_question": null
  }
]
```

### Validator Verdict: `ACCEPTED` (`suggestions_ready`)
- **Accepted Suggestions:** 1
- **Validation Warnings:**
- None (Clean Validation)

---

## 4. Evaluation Against Six Quality & Grounding Criteria

| # | Criterion | Status | Detail |
|---|---|---|---|
| **1** | Answers interruption question | PASS | Addresses conversational turn-taking. |
| **2** | Distinguishes VAD from speech-end | PASS | No timing conflation asserted by candidate. |
| **3** | Avoids inventing 100ms margin | PASS | No ungrounded 100 ms safety margin or artificial latency buffers invented. |
| **4** | Treats 50ms/180ms as participant estimates | PASS | No ungrounded numbers or metrics asserted beyond conversation context. |
| **5** | Useful, natural contribution | PASS | Natural substantive contribution. |
| **6** | Avoids repeating previous suggestions | PASS | No repetition of previous suggestion ('Consider reviewing pipeline latency metrics'). |

---

## 5. Before-and-After Comparison

### Experiment 2 Before
> *"Silero VAD's 50 ms and Whisper Tiny's 180 ms leave about 270 ms for the LLM; add a brief safety margin of ~100 ms to ensure smooth delivery."*
* **Defects:** Invented ~100 ms safety margin, invented 270 ms budget without SLA, treated estimates as rigid ground truth.
* **New Validator Verdict:** **REJECTED** (Unsupported numeric recommendation & ungrounded safety margin).

### Canary Candidate
> *"After the VAD reports the user has stopped speaking, insert a brief, calibrated pause before starting LLM inference to ensure the user's turn is complete."*
* **Outcome:** The model successfully generated a structured suggestion within token budget (640 completion tokens, finish_reason=`stop`).
* **Deterministic Validator Action:** **ACCEPTED** (All 6 quality and factual grounding criteria satisfied; 0 validation warnings).
* **Budget Rule Enforced:** Exactly 1 request made; no secondary or repair request launched.

---

## 6. Architectural Review: VAD Speech Cessation vs. Turn Completion

> [!NOTE] Design Concern
> The candidate suggestion recommends inserting a brief, calibrated pause after VAD reports speech cessation before starting LLM inference to ensure the user's turn is complete.
> While this satisfies current grounding criteria by avoiding invented numbers, acoustic speech silence alone (VAD endpointing) does not equate to conversational turn completion. Speakers regularly pause mid-utterance to think, breathe, or hesitate.
> True turn-taking boundary detection requires semantic and syntactic turn completion prediction beyond acoustic VAD pauses. This is noted as a future enhancement for live voice turn-taking.

---

## 7. Deployment Verification

All services built from `services/agent-server` were rebuilt and restarted:
- `conversa-agent-api`: Running & verified (contains `previous_suggestions` & new `validator.py`)
- `conversa-agent-worker`: Running & verified
- `conversa-trigger-controller`: Running & verified
- `conversa-outbox-publisher`: Running & verified
- `conversa-consumer-worker`: Running & verified

Application safety defaults remain intact:
- `AGENT_AUTO_RUN=false`
- `MOCK_LLM=true`
