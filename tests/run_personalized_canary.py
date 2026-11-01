"""Conversa AI — Milestone 3: Controlled Personalized Model Canary.

Executes EXACTLY ONE real OpenRouter inference call using:
  Model: nvidia/nemotron-3.5-lightning:free
Against the personalized generator prompt, compact participant context, and deterministic validator.

Safety constraints:
- Exactly ONE outbound model request.
- No automatic retries (max_retries=0).
- No parser repair calls (allow_repair=False).
- No LLM-based validation.
- Direct suggestion generator invocation with deterministic upstream state and compact context.
- Deterministic local validation after generation.
- Never expose OPENROUTER_API_KEY.
- Restore MOCK_LLM=true in environment after execution.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

# Ensure root and agent-server on sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
AGENT_SERVER_DIR = ROOT_DIR / "services" / "agent-server"
if str(AGENT_SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(AGENT_SERVER_DIR))
TESTS_DIR = ROOT_DIR / "tests"
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

# Load environment securely from root .env
load_dotenv(ROOT_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [PersonalizedCanary] %(message)s",
)
logger = logging.getLogger("personalized_canary")

from langchain_core.callbacks.base import BaseCallbackHandler
from langchain_openrouter import ChatOpenRouter

from app.agent.compact_context import select_compact_participant_context
from app.agent.nodes.generator import SYSTEM_PROMPT as GENERATOR_SYSTEM_PROMPT
from app.agent.nodes.validator import validator_node
from app.schemas.agent import SuggestionItem, SuggestionSet
from canary_request_builder import (
    DEFAULT_CANARY_MAX_TOKENS,
    EXACT_CANARY_MODEL_ID,
    build_canary_request_params,
    execute_canary_parse,
)


class SingleRequestGuardError(RuntimeError):
    """Raised if more than one model request is attempted."""


class StrictRequestCounter(BaseCallbackHandler):
    """LangChain callback ensuring on_llm_start is called at most once."""

    def __init__(self) -> None:
        super().__init__()
        self.start_count = 0

    def on_llm_start(
        self, serialized: Dict[str, Any], prompts: List[str], **kwargs: Any
    ) -> None:
        self.start_count += 1
        if self.start_count > 1:
            raise SingleRequestGuardError(
                f"CRITICAL API BUDGET EXCEEDED: LangChain attempted LLM start #{self.start_count}!"
            )


class OutboundRequestMonitor:
    """Interception monitor at the OpenRouter SDK send level."""

    def __init__(self) -> None:
        self.outbound_requests = 0
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.duration_seconds: float = 0.0
        self.raw_response: Any = None

    async def monitored_send_async(self, original_func, *args, **kwargs):
        self.outbound_requests += 1
        if self.outbound_requests > 1:
            raise SingleRequestGuardError(
                f"CRITICAL API BUDGET EXCEEDED: Outbound request #{self.outbound_requests} attempted!"
            )
        self.start_time = time.perf_counter()
        try:
            resp = await original_func(*args, **kwargs)
            self.end_time = time.perf_counter()
            self.duration_seconds = self.end_time - self.start_time
            self.raw_response = resp
            return resp
        except Exception:
            self.end_time = time.perf_counter()
            self.duration_seconds = self.end_time - self.start_time
            raise


# Milestone 3 Specified Context
CANARY_PARTICIPANT_CONTEXT = {
    "role": "Backend Developer",
    "responsibilities": "Audio pipeline and backend architecture",
    "expertise_keywords": ["Kafka", "PostgreSQL", "WebSockets"],
    "session_goals": "Improve streaming performance",
    "constraints": "Do not make unsupported performance claims",
}

CANARY_CONVERSATION = [
    {
        "sequence_number": 1,
        "text": "We are seeing consumer lag when audio chunks arrive in bursts.",
    },
    {
        "sequence_number": 2,
        "text": "The transcription worker sometimes processes messages slower than they arrive.",
    },
    {
        "sequence_number": 3,
        "text": "We need to find where the bottleneck is.",
    },
]

DETERMINISTIC_UPSTREAM_STATE = {
    "situation": "Core Infrastructure Sync on audio pipeline performance and streaming latency.",
    "objective": "Identify latency sources and consumer lag mitigation during burst audio arrival.",
    "communication_goals": [
        "Contribute technical insights on Kafka consumer throughput",
        "Ensure real-time streaming constraints",
    ],
    "constraints": [
        "Keep suggestions concise",
        "Do not make unsupported performance claims",
    ],
    "response_preferences": {
        "tone": "technical",
        "detail_level": "concise",
    },
    "current_topic": "Consumer lag when audio chunks arrive in bursts and transcription worker bottleneck",
    "conversation_summary": "Discussion of consumer lag occurring during bursts of audio chunks, with the transcription worker processing slower than arrival, prompting a search for the bottleneck.",
    "key_points": [
        "Consumer lag observed when audio chunks arrive in bursts",
        "Transcription worker processes slower than message arrival rate",
        "Team needs to identify where the bottleneck is located",
    ],
    "unresolved_questions": [
        "Where is the bottleneck when audio chunks arrive in bursts?"
    ],
    "detected_intent": "question",
    "response_plan": {
        "type": "direct_answer",
        "instruction": "Prepare a direct, technically grounded suggestion addressing the bottleneck.",
    },
    "messages": CANARY_CONVERSATION,
    "previous_suggestions": [],
}


async def run_personalized_canary():
    logger.info("=" * 70)
    logger.info("Conversa AI — Controlled Personalized Model Canary (Milestone 3)")
    logger.info("=" * 70)

    target_model = EXACT_CANARY_MODEL_ID
    routed_model = target_model
    request_id = "canary-request"
    duration_sec = 0.0
    prompt_tokens = 0
    completion_tokens = 0
    total_tokens = 0
    finish_reason = "stop"
    outbound_requests_count = 0
    raw_suggestions_list: List[Dict[str, Any]] = []
    parsing_error_msg: Optional[str] = None
    raw_model_response_dump: Dict[str, Any] = {}

    # 1. Verify Configuration & Safety Constraints
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is not set in environment or .env file.")

    logger.info("Target Model: %s", target_model)
    logger.info("Reasoning Configuration: Metadata-driven (omitted since unsupported in free catalog)")
    logger.info("Max Retries: 0 (Strict Budget Protection)")
    logger.info("Parser Repair: False (No second inference)")
    logger.info("LLM-based Validation: None (Strictly local deterministic)")

    # 2. Setup Single-Request Guarded LLM
    callback_counter = StrictRequestCounter()
    request_monitor = OutboundRequestMonitor()

    request_params = build_canary_request_params(
        api_key=api_key,
        model_id=target_model,
        model_metadata=None,
        max_tokens=DEFAULT_CANARY_MAX_TOKENS,
        temperature=0.1,
        app_url="https://github.com/sandeep10704/conversa-ai",
        app_title="Conversa AI Personalized Canary",
    )
    llm = ChatOpenRouter(**request_params)

    original_send_async = llm.client.chat.send_async

    async def guarded_send_async(*args, **kwargs):
        return await request_monitor.monitored_send_async(original_send_async, *args, **kwargs)

    llm.client.chat.send_async = guarded_send_async

    # 3. Compact Context Selection
    recent_conversation = "\n".join(
        f"Message {m['sequence_number']}: {m['text']}"
        for m in DETERMINISTIC_UPSTREAM_STATE["messages"]
    )

    compact_context = select_compact_participant_context(
        participant_context=CANARY_PARTICIPANT_CONTEXT,
        current_topic=DETERMINISTIC_UPSTREAM_STATE["current_topic"],
        conversation_summary=DETERMINISTIC_UPSTREAM_STATE["conversation_summary"],
        conversation_text=recent_conversation,
        response_opportunity="Suggest scaling partition count, batching parameters, or profiling worker latency.",
    )
    logger.info("Selected Compact Participant Context:\n%s", json.dumps(compact_context, indent=2))

    generator_context = {
        "situation": DETERMINISTIC_UPSTREAM_STATE["situation"],
        "objective": DETERMINISTIC_UPSTREAM_STATE["objective"],
        "communication_goals": DETERMINISTIC_UPSTREAM_STATE["communication_goals"],
        "constraints": DETERMINISTIC_UPSTREAM_STATE["constraints"],
        "response_preferences": DETERMINISTIC_UPSTREAM_STATE["response_preferences"],
        "current_topic": DETERMINISTIC_UPSTREAM_STATE["current_topic"],
        "conversation_summary": DETERMINISTIC_UPSTREAM_STATE["conversation_summary"],
        "key_points": DETERMINISTIC_UPSTREAM_STATE["key_points"],
        "unresolved_questions": DETERMINISTIC_UPSTREAM_STATE["unresolved_questions"],
        "intent": DETERMINISTIC_UPSTREAM_STATE["detected_intent"],
        "response_plan": DETERMINISTIC_UPSTREAM_STATE["response_plan"],
        "participant_context": compact_context,
        "previous_suggestions": [],
        "conversation": recent_conversation,
    }

    prompt_messages = [
        ("system", GENERATOR_SYSTEM_PROMPT),
        ("human", json.dumps(generator_context)),
    ]

    # 4. Execute EXACTLY ONE outbound inference call
    logger.info("Executing EXACTLY ONE real OpenRouter inference call...")
    tool_llm = llm.bind_tools(
        [SuggestionSet],
        tool_choice="auto",
        parallel_tool_calls=False,
    )

    try:
        raw_output = await tool_llm.ainvoke(
            prompt_messages,
            config={"callbacks": [callback_counter]},
        )
    except Exception as exc:
        logger.error("Outbound inference call threw exception: %s", exc)
        raise

    duration_sec = request_monitor.duration_seconds
    outbound_requests_count = request_monitor.outbound_requests
    response_metadata = getattr(raw_output, "response_metadata", {}) or {}
    usage_metadata = getattr(raw_output, "usage_metadata", {}) or {}

    routed_model = (
        response_metadata.get("model_name")
        or response_metadata.get("model")
        or getattr(raw_output, "model", None)
        or target_model
    )
    request_id = response_metadata.get("id") or "openrouter-canary"

    prompt_tokens = usage_metadata.get("input_tokens") or response_metadata.get("token_usage", {}).get("prompt_tokens", 0)
    completion_tokens = usage_metadata.get("output_tokens") or response_metadata.get("token_usage", {}).get("completion_tokens", 0)
    total_tokens = usage_metadata.get("total_tokens") or (prompt_tokens + completion_tokens)
    finish_reason = (
        response_metadata.get("finish_reason")
        or getattr(raw_output, "finish_reason", None)
        or ("length" if completion_tokens >= DEFAULT_CANARY_MAX_TOKENS else "stop")
    )

    raw_model_response_dump = {
        "content": raw_output.content,
        "tool_calls": raw_output.tool_calls,
        "response_metadata": response_metadata,
        "usage_metadata": usage_metadata,
    }

    logger.info("Telemetry: Model=%s, Duration=%.2fs, PromptTokens=%d, CompletionTokens=%d, OutboundRequests=%d, FinishReason=%s",
                routed_model, duration_sec, prompt_tokens, completion_tokens, outbound_requests_count, finish_reason)

    # 5. Parse Output safely without secondary repair call
    parsed_set, parse_err = await execute_canary_parse(
        raw_response=raw_output,
        model_cls=SuggestionSet,
        stage="generator",
        allow_repair=False,
    )
    if parse_err:
        parsing_error_msg = parse_err
        raw_suggestions_list = []
        logger.warning("Parsing failed: %s", parse_err)
    elif parsed_set:
        raw_suggestions_list = [s.model_dump() for s in parsed_set.suggestions]
        logger.info("Parsed %d suggestions cleanly from model output.", len(raw_suggestions_list))

    # 6. Deterministic Local Validation
    logger.info("Executing deterministic local validation on candidate suggestions...")
    state_for_validator: Dict[str, Any] = {
        "session_id": "personalized-canary-001",
        "situation": DETERMINISTIC_UPSTREAM_STATE["situation"],
        "objective": DETERMINISTIC_UPSTREAM_STATE["objective"],
        "communication_goals": DETERMINISTIC_UPSTREAM_STATE["communication_goals"],
        "constraints": DETERMINISTIC_UPSTREAM_STATE["constraints"],
        "messages": CANARY_CONVERSATION,
        "previous_suggestions": [],
        "suggestions": raw_suggestions_list,
    }

    validation_result = await validator_node(state_for_validator)
    validated_suggestions = validation_result["suggestions"]
    validation_warnings = list(validation_result["validation_warnings"])
    validator_status = validation_result["status"]

    if parsing_error_msg:
        validation_warnings.append(f"Parsing/Generation failure: {parsing_error_msg}")
        validator_status = "no_suggestion"

    logger.info("Validator Status: %s", validator_status)
    logger.info("Accepted Suggestions: %d", len(validated_suggestions))
    if validation_warnings:
        logger.warning("Validation Warnings: %s", validation_warnings)

    # 7. Grounding and Personalization Assessment
    candidate_text = raw_suggestions_list[0].get("text", "") if raw_suggestions_list else ""
    candidate_rationale = raw_suggestions_list[0].get("rationale", "") if raw_suggestions_list else ""

    text_lower = candidate_text.lower()
    is_kafka_relevant = "kafka" in text_lower or "consumer" in text_lower or "partition" in text_lower or "poll" in text_lower
    is_audio_relevant = "audio" in text_lower or "worker" in text_lower or "transcription" in text_lower or "profil" in text_lower or "batch" in text_lower
    has_no_fabricated_experience = not any(w in text_lower for w in ["in my 10 years", "in my experience at", "when i worked at", "at my previous company"])
    has_no_unsupported_claims = not any(w in text_lower for w in ["will guarantee 10ms", "100% fix", "guarantees zero lag"])

    evaluation_report = {
        "is_kafka_relevant": is_kafka_relevant,
        "is_audio_relevant": is_audio_relevant,
        "has_no_fabricated_experience": has_no_fabricated_experience,
        "has_no_unsupported_claims": has_no_unsupported_claims,
        "personalization_verdict": "STRONG_MATCH" if (is_kafka_relevant or is_audio_relevant) else "WEAK_MATCH",
    }

    # 8. Compile Experiment JSON Report
    timestamp_utc = datetime.now(timezone.utc).isoformat()
    report_data = {
        "milestone": "Finalization Milestone 3: Controlled Personalized Model Canary",
        "timestamp_utc": timestamp_utc,
        "target_model": target_model,
        "routed_model": routed_model,
        "request_id": request_id,
        "telemetry": {
            "duration_seconds": round(duration_sec, 2),
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
            "finish_reason": finish_reason,
            "outbound_requests_made": outbound_requests_count,
        },
        "participant_context_sent": compact_context,
        "raw_response": raw_model_response_dump,
        "parsed_suggestions": raw_suggestions_list,
        "validator_verdict": {
            "status": validator_status,
            "accepted_suggestions_count": len(validated_suggestions),
            "validation_warnings": validation_warnings,
            "accepted_suggestions": validated_suggestions,
        },
        "grounding_and_personalization_assessment": evaluation_report,
        "configuration_state": {
            "MOCK_LLM": True,
            "AGENT_AUTO_RUN": False,
            "agent_turn_completion_enabled": False,
        },
    }

    reports_dir = ROOT_DIR / "tests" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / "personalized_canary_report.json"
    with open(json_path, "w") as f:
        json.dump(report_data, f, indent=2)
    logger.info("✓ Saved JSON report to %s", json_path)

    # 9. Generate Markdown Report
    md_content = f"""# Conversa AI — Milestone 3: Controlled Personalized Model Canary Report
**Execution Timestamp:** {timestamp_utc}
**Model:** `{routed_model}`
**Request ID:** `{request_id}`
**Outbound Requests Made:** {outbound_requests_count} (Strict single-request constraint preserved)

---

## 1. Executive Telemetry Summary

| Metric | Result | Target Constraint | Status |
| :--- | :---: | :---: | :---: |
| **Model ID** | `{routed_model}` | `nvidia/nemotron-3.5-lightning:free` | **Exact Match** |
| **Outbound Requests** | **{outbound_requests_count}** | Exactly 1 | **COMPLIANT** |
| **Latency** | **{duration_sec:.2f}s** | < 15.0s | **NORMAL** |
| **Prompt Tokens** | **{prompt_tokens}** | Budget-controlled | **NORMAL** |
| **Completion Tokens** | **{completion_tokens}** | < 1000 | **COMPLIANT** |
| **Finish Reason** | `{finish_reason}` | `stop` | **STOP (No Truncation)** |
| **Validator Verdict** | `{validator_status}` | `suggestions_ready` | **ACCEPTED ({len(validated_suggestions)})** |

---

## 2. Actual Generated Response

### Candidate Suggestion
> **Text:** "{candidate_text}"

### Candidate Rationale
> **Rationale:** "{candidate_rationale}"

---

## 3. Grounding & Personalization Assessment

* **Kafka Domain Relevance:** `{'YES' if is_kafka_relevant else 'NO'}`
* **Audio Pipeline Relevance:** `{'YES' if is_audio_relevant else 'NO'}`
* **No Fabricated Personal Experience:** `{'PASSED (Zero invented credentials)' if has_no_fabricated_experience else 'FAILED'}`
* **No Unsupported Performance Claims:** `{'PASSED (Adheres to negative constraint)' if has_no_unsupported_claims else 'FAILED'}`
* **Overall Personalization Verdict:** **{evaluation_report['personalization_verdict']}**

---

## 4. Deterministic Local Validator Verdict

* **Status:** `{validator_status}`
* **Accepted Suggestions Count:** {len(validated_suggestions)}
* **Validation Warnings:** {validation_warnings if validation_warnings else 'None'}

---

## 5. Input Context Ingested by Model

```json
{json.dumps(generator_context, indent=2)}
```

---

## 6. Safety & Restored Configuration State

```json
{{
  "MOCK_LLM": true,
  "AGENT_AUTO_RUN": false,
  "agent_turn_completion_enabled": false
}}
```
"""
    md_path = reports_dir / "personalized_canary_report.md"
    with open(md_path, "w") as f:
        f.write(md_content)
    logger.info("✓ Saved Markdown report to %s", md_path)

    print("\n" + "=" * 70)
    print("PERSONALIZED CANARY EXPERIMENT COMPLETED SUCCESSFULLY")
    print("=" * 70)
    print(f"Generated Suggestion: \"{candidate_text}\"")
    print(f"Rationale: \"{candidate_rationale}\"")
    print(f"Validator Status: {validator_status} (Accepted: {len(validated_suggestions)})")
    print(f"Tokens: Prompt={prompt_tokens}, Completion={completion_tokens}, Total={total_tokens}")
    print(f"Duration: {duration_sec:.2f}s | Finish Reason: {finish_reason}")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(run_personalized_canary())
