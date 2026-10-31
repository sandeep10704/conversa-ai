import json

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

Generate at most two useful suggestions.

Suggestion 1: Best possible response.
Suggestion 2: Alternative response, only when useful.

Rules:

1. Write naturally, as if the user is speaking.
2. Follow the requested tone and response length.
3. Never claim that a question was directed at the user.
4. Never attribute statements to specific speakers.
5. Do not invent things participants said.
6. Avoid unsupported statistics and numerical claims.
7. Do not invent agreements or commitments.
8. Avoid repeating already discussed points.
9. Clearly qualify uncertain information.
10. If a response cannot be grounded in the available context,
    return an empty suggestions list.

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

        "conversation": recent_conversation
    }

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