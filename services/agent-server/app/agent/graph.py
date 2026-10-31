from functools import lru_cache

from langgraph.graph import StateGraph, START, END

from app.agent.state import AgentState

from app.agent.nodes.understanding import (
    understanding_node
)

from app.agent.nodes.intent import (
    intent_node
)

from app.agent.nodes.relevance import (
    relevance_node
)

from app.agent.nodes.planner import (
    planner_node
)

from app.agent.nodes.generator import (
    generator_node
)

from app.agent.nodes.validator import (
    validator_node
)


def route_after_relevance(state: AgentState):

    if state.get("should_suggest", False):
        return "planner"

    return "finish"


def build_agent_graph():

    workflow = StateGraph(AgentState)

    # Register nodes
    workflow.add_node(
        "understanding",
        understanding_node
    )

    workflow.add_node(
        "intent",
        intent_node
    )

    workflow.add_node(
        "relevance",
        relevance_node
    )

    workflow.add_node(
        "planner",
        planner_node
    )

    workflow.add_node(
        "generator",
        generator_node
    )

    workflow.add_node(
        "validator",
        validator_node
    )

    # Define workflow
    workflow.add_edge(
        START,
        "understanding"
    )

    workflow.add_edge(
        "understanding",
        "intent"
    )

    workflow.add_edge(
        "intent",
        "relevance"
    )

    # Conditional routing
    workflow.add_conditional_edges(
        "relevance",
        route_after_relevance,
        {
            "planner": "planner",
            "finish": END
        }
    )

    workflow.add_edge(
        "planner",
        "generator"
    )

    workflow.add_edge(
        "generator",
        "validator"
    )

    workflow.add_edge(
        "validator",
        END
    )

    return workflow.compile()


@lru_cache
def get_agent_graph():

    return build_agent_graph()