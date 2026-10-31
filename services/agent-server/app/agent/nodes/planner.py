from app.agent.state import AgentState


STRATEGY_MAP = {

    "question": {
        "type": "direct_answer",
        "instruction": "Prepare a direct and relevant answer."
    },

    "opinion_request": {
        "type": "supporting_point",
        "instruction": "Prepare a reasoned opinion."
    },

    "disagreement": {
        "type": "polite_disagreement",
        "instruction": "Prepare a respectful counterargument."
    },

    "request_for_explanation": {
        "type": "technical_explanation",
        "instruction": "Explain the relevant concept clearly."
    },

    "statement": {
        "type": "supporting_point",
        "instruction": "Add a useful insight to the discussion."
    },

    "topic_shift": {
        "type": "follow_up_question",
        "instruction": "Only contribute if the new topic remains relevant."
    },

    "unclear": {
        "type": "direct_answer",
        "instruction": "Do not make assumptions. Ask for clarification if needed."
    }
}


async def planner_node(state: AgentState):

    intent = state.get(
        "detected_intent",
        "unclear"
    )

    strategy = STRATEGY_MAP.get(
        intent,
        STRATEGY_MAP["unclear"]
    )

    return {
        "response_plan": strategy
    }