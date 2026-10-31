from app.agent.state import AgentState


async def intent_node(state: AgentState):

    analysis = state.get("analysis", {})

    intent = analysis.get(
        "detected_intent",
        "unclear"
    )

    evidence = analysis.get(
        "intent_evidence",
        ""
    )

    return {
        "detected_intent": intent,
        "intent_evidence": evidence
    }