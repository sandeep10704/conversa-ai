import json

from app.agent.compact_context import select_compact_participant_context
from app.agent.state import AgentState
from app.core.llm import get_llm
from app.core.parsing import parse_structured_output
from app.schemas.agent import SuggestionSet


SYSTEM_PROMPT = """
You are the response generation component of Conversa AI.

Your job is to help the user contribute naturally to a
group conversation.

Use:
- User's situation.
- User's objective.
- Communication goals.
- Conversation history.
- Current discussion topic.
- Response strategy.
- Response preferences.
- Participant context (if provided):
  * role: Professional specialty to guide tone and technical perspective.
  * responsibilities: Discussion areas the user owns in this session.
  * expertise_keywords: Technical domains for grounded contributions.
  * session_goals: What the user intends to contribute.
  * constraints: Boundaries and topics the user must avoid.

Generate at most two useful suggestions.

Suggestion 1: Best possible response.
Suggestion 2: Alternative response, only when useful.

Rules:

1. Write naturally, as if the user is speaking aloud in a real meeting.
2. Concise and easy to say aloud. Prefer one clear, substantive contribution per suggestion.
3. Follow the requested tone and response length.
4. Never claim that a question was directed at the user, and never attribute statements to specific speakers.
5. Do not invent things participants said or agreements not made.
6. Factual Grounding & Latency Concepts:
   - Use only information supported by the conversation.
   - Participant-reported figures (e.g., approximately 50 ms for Silero VAD, 180 ms for Whisper Tiny) are estimates/observations, NOT verified benchmarks.
   - Strictly DO NOT confuse:
     * VAD per-chunk processing latency
     * Silence duration threshold used for speech-end detection
     * Transcription/Whisper inference latency
     * LLM inference latency
     * Total end-to-end response latency
   - DO NOT invent unmentioned timing thresholds (such as 100 ms, 300 ms), safety margins, performance benchmarks, architectural components, database metrics, or unmentioned system behavior.
   - When a recommendation depends on missing measurements or parameters, recommend measuring, profiling, or calibrating instead of asserting an invented number.
7. Avoid generic empty platitudes such as "consider reviewing metrics" or "monitor the pipeline". Provide concrete technical substance.
8. Avoid unnecessary throat-clearing introductions (e.g., "I think that...", "As we know...").
9. Repetition Prevention:
   - Review previous_suggestions. Do not repeat an idea that has already been suggested or adequately covered.
10. Participant Context & Grounding Rules:
   - Ground suggestions in the user's role and expertise without ever fabricating personal experience, past projects, past employers, degrees, qualifications, or unstated responsibilities.
   - Never say "In my experience at [Company]..." or "When I built...", unless explicitly mentioned in the conversation.
   - Strictly adhere to participant constraints (e.g. if the user must avoid committing to deadlines, do not propose timeline commitments).
   - If participant_context is missing or null, generate general suggestions based on the situation and communication goals.
11. If a response cannot be grounded in the available context, return an empty suggestions list.

Do not include hidden reasoning or internal analysis.
Return only the structured suggestion fields in a valid JSON object matching the schema:
{"suggestions": [{"response_type": "...", "text": "...", "rationale": "...", "follow_up_question": null}]}
"""


async def generator_node(state: AgentState):

    messages = state.get("messages", [])

    recent_conversation = "\n".join(
        message["text"]
        for message in messages[-20:]
    )

    context = {

        "situation": state.get("situation"),

        "objective": state.get("objective"),

        "communication_goals": state.get(
            "communication_goals", []
        ),

        "constraints": state.get(
            "constraints", []
        ),

        "response_preferences": state.get(
            "response_preferences", {}
        ),

        "current_topic": state.get(
            "current_topic"
        ),

        "conversation_summary": state.get(
            "conversation_summary"
        ),

        "key_points": state.get(
            "key_points", []
        ),

        "unresolved_questions": state.get(
            "unresolved_questions", []
        ),

        "intent": state.get(
            "detected_intent"
        ),

        "response_plan": state.get(
            "response_plan"
        ),

        "previous_suggestions": [
            s.get("suggestion_text") or s.get("text", "")
            for s in state.get("previous_suggestions", [])
        ],

        "conversation": recent_conversation
    }

    relevance_opportunity = ""
    relevance_state = state.get("relevance")
    if isinstance(relevance_state, dict):
        relevance_opportunity = relevance_state.get("response_opportunity", "")

    compact_context = select_compact_participant_context(
        participant_context=state.get("participant_context"),
        current_topic=state.get("current_topic"),
        conversation_summary=state.get("conversation_summary"),
        conversation_text=recent_conversation,
        response_opportunity=relevance_opportunity,
    )
    if compact_context:
        context["participant_context"] = compact_context

    llm = get_llm()
    prompt_messages = [
        ("system", SYSTEM_PROMPT),
        ("human", json.dumps(context))
    ]

    try:
        tool_llm = llm.bind_tools(
            [SuggestionSet],
            tool_choice="auto",
            parallel_tool_calls=False,
        )
        raw = await tool_llm.ainvoke(prompt_messages)
    except Exception:
        raw = await llm.ainvoke(prompt_messages)

    selected_model = state.get("selected_model")
    if hasattr(raw, "response_metadata") and isinstance(raw.response_metadata, dict):
        model_name = raw.response_metadata.get("model_name") or raw.response_metadata.get("model")
        if model_name:
            selected_model = model_name

    result = await parse_structured_output(
        raw=raw,
        model_cls=SuggestionSet,
        stage="generator",
        llm=llm,
        allow_repair=True,
    )

    return {
        "suggestions": [
            item.model_dump()
            for item in result.suggestions
        ],
        "selected_model": selected_model,
        "stage_reached": "generator",
    }
