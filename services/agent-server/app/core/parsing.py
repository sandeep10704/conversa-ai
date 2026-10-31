"""Robust structured-output parsing utility for Conversa AI agent.

Handles varied response formats produced by diverse models across OpenRouter:
1. Tool call arguments (dict or JSON string)
2. Raw JSON text
3. Markdown-fenced JSON blocks (```json ... ```)
4. Text-block content structures (list of typed dicts)
5. Embedded JSON extraction via balanced-bracket matching
6. Pydantic schema validation with detailed field-error tracking
7. Normalization for collection wrappers and alternative key naming
8. Controlled single repair attempt for malformed responses
9. Clear classification of failure stages and exception categories
"""

from enum import Enum
import json
import logging
import re
from typing import Any, Type, TypeVar

from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class ParsingErrorCategory(str, Enum):
    """Categorization of failures during LLM invocation and parsing."""

    API_RATE_LIMIT = "rate_limit"
    API_TIMEOUT = "timeout"
    API_CONNECTION_ERROR = "connection_error"
    EMPTY_RESPONSE = "empty_response"
    JSON_PARSE_ERROR = "json_parse_error"
    SCHEMA_VALIDATION_ERROR = "validation_error"
    UNKNOWN_ERROR = "unknown_error"


class StructuredOutputError(RuntimeError):
    """Raised when structured output cannot be extracted or validated."""

    def __init__(
        self,
        message: str,
        stage: str,
        category: ParsingErrorCategory,
        raw_content: str | None = None,
        validation_errors: list[str] | None = None,
    ):
        super().__init__(message)
        self.stage = stage
        self.category = category
        self.raw_content = sanitize_preview(raw_content) if raw_content else None
        self.validation_errors = validation_errors or []


def sanitize_preview(text: str | None, max_length: int = 400) -> str:
    """Sanitize and truncate text preview to prevent logging credentials or bloated logs."""
    if not text:
        return ""
    # Redact common key patterns if present
    sanitized = re.sub(r"sk-[a-zA-Z0-9_\-]{20,}", "[REDACTED_API_KEY]", str(text))
    sanitized = sanitized.strip().replace("\r", "")
    if len(sanitized) > max_length:
        return sanitized[:max_length] + "... [TRUNCATED]"
    return sanitized


def extract_text_from_content(content: Any) -> str:
    """Extract plain text from string or structured content blocks."""
    if isinstance(content, str):
        return content.strip()

    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, dict):
                if block.get("type") == "text" and "text" in block:
                    parts.append(str(block["text"]))
                elif "text" in block:
                    parts.append(str(block["text"]))
            elif isinstance(block, str):
                parts.append(block)
        return "\n".join(parts).strip()

    if content is None:
        return ""

    return str(content).strip()


def extract_balanced_json(text: str) -> str | None:
    """Find the first balanced JSON object ({...}) or array ([...]) within text."""
    # Search for first opening brace or bracket
    start_curly = text.find("{")
    start_bracket = text.find("[")

    if start_curly == -1 and start_bracket == -1:
        return None

    # Determine which starts first
    if start_curly != -1 and (start_bracket == -1 or start_curly < start_bracket):
        start_char, end_char = "{", "}"
        start_pos = start_curly
    else:
        start_char, end_char = "[", "]"
        start_pos = start_bracket

    depth = 0
    in_string = False
    escape = False

    for idx in range(start_pos, len(text)):
        char = text[idx]

        if escape:
            escape = False
            continue

        if char == "\\":
            escape = True
            continue

        if char == '"':
            in_string = not in_string
            continue

        if not in_string:
            if char == start_char:
                depth += 1
            elif char == end_char:
                depth -= 1
                if depth == 0:
                    return text[start_pos : idx + 1]

    return None


def extract_raw_json(raw_text: str) -> Any:
    """Extract and parse JSON from raw text, code fences, or embedded text."""
    clean = raw_text.strip()
    if not clean:
        raise ValueError("Model response is empty.")

    # 1. Direct JSON parse
    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        pass

    # 2. Markdown code fences: ```json ... ``` or ``` ... ```
    fence_pattern = re.compile(r"```(?:json)?\s*([\s\S]*?)\s*```", re.IGNORECASE)
    matches = fence_pattern.findall(clean)
    for candidate in matches:
        candidate_clean = candidate.strip()
        try:
            return json.loads(candidate_clean)
        except json.JSONDecodeError:
            # Try balanced search within fence
            balanced = extract_balanced_json(candidate_clean)
            if balanced:
                try:
                    return json.loads(balanced)
                except json.JSONDecodeError:
                    pass

    # 3. Search for balanced brackets anywhere in text
    balanced = extract_balanced_json(clean)
    if balanced:
        try:
            return json.loads(balanced)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Extracted JSON block is invalid: {exc}") from exc

    raise ValueError(f"No valid JSON structure found in text: {clean[:200]!r}")


def normalize_payload_for_schema(payload: Any, model_cls: Type[BaseModel]) -> dict[str, Any]:
    """Normalize common structural variations before Pydantic validation."""
    schema_name = model_cls.__name__

    # If payload is a list and schema expects a wrapper object (like SuggestionSet)
    if isinstance(payload, list):
        if schema_name == "SuggestionSet" or "suggestions" in model_cls.model_fields:
            payload = {"suggestions": payload}
        else:
            raise ValueError(f"Expected dict for schema {schema_name}, got list")

    if not isinstance(payload, dict):
        raise ValueError(f"Expected dict for schema {schema_name}, got {type(payload).__name__}")

    # Specific normalization for SuggestionSet
    if schema_name == "SuggestionSet" or "suggestions" in payload:
        raw_items = payload.get("suggestions", [])
        if isinstance(raw_items, dict):
            raw_items = [raw_items]
        elif not isinstance(raw_items, list):
            raw_items = []

        valid_types = {
            "direct_answer",
            "technical_explanation",
            "supporting_point",
            "polite_disagreement",
            "example",
            "follow_up_question",
        }

        normalized_items = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue

            # Support alternative key naming
            text = (
                item.get("text")
                or item.get("suggestion")
                or item.get("response")
                or item.get("content")
                or ""
            )
            rationale = (
                item.get("rationale")
                or item.get("reason")
                or item.get("explanation")
                or "Aligned with communication goals and discussion context."
            )
            resp_type = item.get("response_type") or item.get("type") or "supporting_point"
            if resp_type not in valid_types:
                resp_type = "supporting_point"

            follow_up = item.get("follow_up_question")

            normalized_items.append(
                {
                    "text": str(text).strip(),
                    "rationale": str(rationale).strip(),
                    "response_type": resp_type,
                    "follow_up_question": str(follow_up).strip() if follow_up else None,
                }
            )

        payload["suggestions"] = normalized_items

    # Specific normalization for DiscussionAnalysis
    elif schema_name == "DiscussionAnalysis":
        if "summary" not in payload and "discussion_summary" in payload:
            payload["summary"] = payload.get("discussion_summary", "")
        if "key_points" not in payload or not isinstance(payload.get("key_points"), list):
            payload["key_points"] = []
        if "unresolved_questions" not in payload or not isinstance(
            payload.get("unresolved_questions"), list
        ):
            payload["unresolved_questions"] = []

        valid_intents = {
            "question",
            "opinion_request",
            "disagreement",
            "request_for_explanation",
            "statement",
            "topic_shift",
            "unclear",
        }
        intent = payload.get("detected_intent", "unclear")
        if intent not in valid_intents:
            payload["detected_intent"] = "unclear"
        if "intent_evidence" not in payload:
            payload["intent_evidence"] = ""

    # Specific normalization for RelevanceAssessment
    elif schema_name == "RelevanceAssessment":
        if "response_opportunity" not in payload:
            payload["response_opportunity"] = ""
        # Handle string booleans
        if isinstance(payload.get("should_suggest"), str):
            payload["should_suggest"] = payload["should_suggest"].lower() in ("true", "1", "yes")

    return payload


async def execute_repair_attempt(
    raw_text: str,
    error_detail: str,
    model_cls: Type[T],
    llm: Any,
    stage: str,
) -> T:
    """Execute one controlled repair invocation asking the LLM to format the response properly."""
    logger.info(f"[{stage}] Attempting controlled 1-shot repair for schema {model_cls.__name__}...")

    repair_system = (
        "You are a strict JSON formatting assistant. "
        "Your task is to fix malformed or invalid output and return a clean JSON object "
        "that strictly adheres to the requested JSON Schema. "
        "Output ONLY the valid JSON object. Do not include markdown code fences, thought process, or explanations."
    )

    schema_str = json.dumps(model_cls.model_json_schema(), indent=2)
    prompt = (
        f"The previous output failed validation for schema: {model_cls.__name__}\n\n"
        f"JSON Schema:\n{schema_str}\n\n"
        f"Validation Error:\n{error_detail}\n\n"
        f"Previous Output (to be corrected):\n{raw_text[:1200]}\n\n"
        f"Return ONLY the corrected JSON object:"
    )

    repair_raw = await llm.ainvoke([
        ("system", repair_system),
        ("human", prompt),
    ])

    return await parse_structured_output(
        raw=repair_raw,
        model_cls=model_cls,
        stage=stage,
        llm=None,
        allow_repair=False,
    )


async def parse_structured_output(
    raw: Any,
    model_cls: Type[T],
    stage: str,
    llm: Any = None,
    allow_repair: bool = True,
) -> T:
    """Parse, normalize, and validate LLM output into a Pydantic model.

    Args:
        raw: The raw response from ChatOpenRouter (AIMessage).
        model_cls: The expected Pydantic model class.
        stage: The agent workflow stage name (e.g. 'understanding', 'relevance', 'generator').
        llm: Optional LLM instance for performing 1-shot repair if needed.
        allow_repair: Whether to attempt one controlled repair if parsing/validation fails.

    Returns:
        Validated instance of model_cls.

    Raises:
        StructuredOutputError: If output cannot be extracted or validated.
    """
    payload: Any = None
    extracted_text: str = ""

    # 1. Check for tool calls first
    if hasattr(raw, "tool_calls") and raw.tool_calls:
        tool_call = raw.tool_calls[0]
        args = tool_call.get("args", {})
        if isinstance(args, str):
            try:
                payload = json.loads(args)
            except json.JSONDecodeError:
                extracted_text = args
        elif isinstance(args, dict):
            payload = args

    # 2. Check invalid_tool_calls fallback
    if payload is None and hasattr(raw, "invalid_tool_calls") and raw.invalid_tool_calls:
        args_str = raw.invalid_tool_calls[0].get("args", "")
        if isinstance(args_str, str):
            extracted_text = args_str

    # 3. Check message content if no tool call payload found
    if payload is None:
        content = getattr(raw, "content", "")
        extracted_text = extract_text_from_content(content)

        if not extracted_text:
            raise StructuredOutputError(
                message=f"Model returned empty content during {stage}.",
                stage=stage,
                category=ParsingErrorCategory.EMPTY_RESPONSE,
            )

        try:
            payload = extract_raw_json(extracted_text)
        except Exception as exc:
            # Check if repair is allowed
            if allow_repair and llm is not None:
                try:
                    return await execute_repair_attempt(
                        raw_text=extracted_text,
                        error_detail=str(exc),
                        model_cls=model_cls,
                        llm=llm,
                        stage=stage,
                    )
                except Exception as repair_exc:
                    logger.warning(f"[{stage}] Controlled repair failed: {repair_exc}")

            raise StructuredOutputError(
                message=f"Failed to parse JSON during {stage}: {exc}",
                stage=stage,
                category=ParsingErrorCategory.JSON_PARSE_ERROR,
                raw_content=extracted_text,
            ) from exc

    # 4. Normalize structure
    try:
        normalized_payload = normalize_payload_for_schema(payload, model_cls)
    except Exception as norm_exc:
        if allow_repair and llm is not None:
            try:
                return await execute_repair_attempt(
                    raw_text=extracted_text or json.dumps(payload),
                    error_detail=str(norm_exc),
                    model_cls=model_cls,
                    llm=llm,
                    stage=stage,
                )
            except Exception as repair_exc:
                logger.warning(f"[{stage}] Controlled repair failed after normalization error: {repair_exc}")

        raise StructuredOutputError(
            message=f"Payload structure invalid for {model_cls.__name__} during {stage}: {norm_exc}",
            stage=stage,
            category=ParsingErrorCategory.SCHEMA_VALIDATION_ERROR,
            raw_content=extracted_text or json.dumps(payload),
        ) from norm_exc

    # 5. Pydantic Model Validation
    try:
        return model_cls.model_validate(normalized_payload)
    except ValidationError as val_exc:
        field_errors = [
            f"{'.'.join(str(loc) for loc in err['loc'])}: {err['msg']} (type={err['type']})"
            for err in val_exc.errors()
        ]
        error_summary = "; ".join(field_errors)

        if allow_repair and llm is not None:
            try:
                return await execute_repair_attempt(
                    raw_text=extracted_text or json.dumps(payload),
                    error_detail=error_summary,
                    model_cls=model_cls,
                    llm=llm,
                    stage=stage,
                )
            except Exception as repair_exc:
                logger.warning(f"[{stage}] Controlled repair failed after validation error: {repair_exc}")

        raise StructuredOutputError(
            message=f"Validation failed for {model_cls.__name__} during {stage}: {error_summary}",
            stage=stage,
            category=ParsingErrorCategory.SCHEMA_VALIDATION_ERROR,
            raw_content=extracted_text or json.dumps(payload),
            validation_errors=field_errors,
        ) from val_exc
