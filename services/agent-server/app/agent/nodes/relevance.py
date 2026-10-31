import json

from app.agent.state import AgentState
from app.core.llm import get_llm
from app.core.parsing import parse_structured_output
from app.schemas.agent import RelevanceAssessment


SYSTEM_PROMPT = """
You are the goal relevance evaluator of Conversa AI.

Your responsibility is to determine whether the ongoing
conversation creates a meaningful opportunity for the user
to contribute.

You are NOT a conventional question-answering assistant.
You are evaluating opportunities for useful participation.

Evaluate these factors:

1. User's situation.
2. User's objective.
3. User's communication goals.
4. Current discussion topic.
5. Conversation history and summary.
6. Detected intent and supporting evidence.
7. Whether the user's communication goals have already
   been sufficiently addressed in the discussion.

IMPORTANT DECISION PRINCIPLES:

- An explicit question is NOT required for a suggestion.
- An explicit invitation to speak is NOT required.
- Statements, opinions, disagreements and explanations
  can create useful contribution opportunities.
- Do not assume that any question is directed at the user.
- Do not identify or infer speaker identities.
- Do not generate an actual response or suggestion here.

POSITIVE RELEVANCE:

Set should_suggest=true when:
- The discussion is meaningfully related to the user's
  objective or communication goals.
- The user has a useful point, explanation, example,
  clarification or perspective to contribute.
- At least one communication goal remains insufficiently
  addressed in the conversation.
- A contribution would add meaningful information rather
  than simply repeat what was already discussed.

NEGATIVE RELEVANCE:

Set should_suggest=false when:
- The discussion is unrelated to the user's objective.
- The user's communication goals have already been
  sufficiently addressed.
- The user has no meaningful contribution to add.
- The available context is insufficient.
- A proposed contribution would require unsupported claims.

IMPORTANT:

Do not set should_suggest=false merely because the latest
message is a statement instead of a question.

Do not set should_suggest=true merely because a keyword
matches the user's objective.

Evaluate the entire context before deciding.

OUTPUT:

Return all four fields:
- should_suggest: boolean
- confidence: number between 0 and 1
- reason: concise explanation of the decision
- response_opportunity: specific opportunity for the user,
  or an empty string when there is none.
"""


async def relevance_node(state: AgentState):

    llm = get_llm()

    context = {
        "situation": state.get("situation"),
        "objective": state.get("objective"),
        "communication_goals": state.get("communication_goals", []),
        "current_topic": state.get("current_topic"),
        "conversation_summary": state.get("conversation_summary"),
        "key_points": state.get("key_points", []),
        "detected_intent": state.get("detected_intent"),
        "intent_evidence": state.get("intent_evidence"),
    }

    prompt_messages = [
        ("system", SYSTEM_PROMPT),
        ("human", json.dumps(context)),
    ]

    try:
        tool_llm = llm.bind_tools(
            [RelevanceAssessment],
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
        model_cls=RelevanceAssessment,
        stage="relevance",
        llm=llm,
        allow_repair=True,
    )

    should_suggest = (
        result.should_suggest
        and result.confidence >= 0.65
        and bool(result.response_opportunity.strip())
    )

    return {
        "relevance": result.model_dump(),
        "should_suggest": should_suggest,
        "selected_model": selected_model,
        "stage_reached": "relevance",
    }
