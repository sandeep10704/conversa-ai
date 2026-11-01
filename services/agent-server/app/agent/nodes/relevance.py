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
4. User's session-specific participant context (if provided):
   - preferred_name: The user's name.
   - role: Professional role or specialty.
   - responsibilities: What the user is responsible for in this session.
   - expertise_keywords: Technical domains where the user has competence.
   - session_goals: What the user hopes to contribute.
   - constraints: Topics or actions the user wants to avoid.
5. Current discussion topic.
6. Conversation history and summary.
7. Detected intent and supporting evidence.
8. Whether the user's communication goals have already
   been sufficiently addressed in the discussion.

IMPORTANT DECISION PRINCIPLES:

- An explicit question is NOT required for a suggestion.
- An explicit invitation to speak is NOT required.
- Statements, opinions, disagreements and explanations
  can create useful contribution opportunities.
- Do not identify or infer speaker identities.
- Do not generate an actual response or suggestion here.

PARTICIPANT CONTEXT RULES:

1. Direct Address:
   - When a speaker directly addresses the participant by their preferred_name
     (e.g., "Sandeep, what do you think about the latency?"), set should_suggest=true
     if the subject relates to their domain.
   - Do NOT assume the participant is speaking merely because their name is mentioned.

2. Relevant Discussion Without Name Mention:
   - When participants discuss topics matching the user's expertise_keywords,
     responsibilities, or role, set should_suggest=true if a meaningful technical
     contribution, clarification, or insight is possible, even if the user's name
     is not mentioned.

3. Same-Name Disambiguation:
   - If someone mentions the participant's name referring to a different colleague
     in another role or department (e.g., "Sandeep from Marketing" or "Sandeep in HR"
     when the user is a Backend Developer), do NOT treat it as an invitation to THIS user.
     Set should_suggest=false unless the underlying discussion is independently relevant.

4. Unrelated Discussion:
   - When the discussion is off-topic, social small talk, or unrelated to the user's
     role, responsibilities, and goals, set should_suggest=false.

5. Missing Participant Context:
   - When participant_context is null or not provided, fall back gracefully to the
     general situation, objective, and communication_goals.

6. Unsupported Expertise & Grounding:
   - Do NOT invent qualifications, roles, or work experience for the user.
   - Do NOT suggest contributions that require asserting unstated knowledge or skills.
   - Do NOT set should_suggest=true merely because a keyword matches. Evaluate whether
     a meaningful contribution is genuinely possible.

POSITIVE RELEVANCE:

Set should_suggest=true when:
- The discussion is meaningfully related to the user's objective, goals, or stated expertise.
- The user is directly addressed or invited to contribute on a relevant topic.
- The user has a useful technical point, explanation, example, or perspective to add.
- At least one communication goal remains insufficiently addressed in the conversation.
- A contribution would add meaningful information rather than simply repeat what was said.

NEGATIVE RELEVANCE:

Set should_suggest=false when:
- The discussion is unrelated to the user's objective/expertise or is purely social small talk.
- The name mention refers to another person (same-name disambiguation).
- All user's communication goals have already been sufficiently addressed.
- Previous suggestions have already addressed this topic and no new question has arisen.
- The user has no meaningful contribution to add.
- The available context is insufficient or requires unsupported claims.

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
        "participant_context": state.get("participant_context"),
        "current_topic": state.get("current_topic"),
        "conversation_summary": state.get("conversation_summary"),
        "key_points": state.get("key_points", []),
        "unresolved_questions": state.get("unresolved_questions", []),
        "detected_intent": state.get("detected_intent"),
        "intent_evidence": state.get("intent_evidence"),
        "previous_suggestions": [
            s.get("suggestion_text") or s.get("text", "")
            for s in state.get("previous_suggestions", [])
        ],
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
