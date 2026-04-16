"""
Dynamic Workflow Interpreter — DAG Executor on Restate

Demonstrates that Restate workflows are "just code" — you can read a workflow
definition (graph topology) at runtime and execute it durably. No special
workflow language needed; control flow is plain Python with durable replay.
"""

import time
import restate
from restate import Workflow, WorkflowContext, WorkflowSharedContext


# ─────────────────────────────────────────────────────────────────────────────
# Node Execution Functions
# ─────────────────────────────────────────────────────────────────────────────

def _execute_node(node_id: str, node_def: dict, node_input: str) -> str:
    node_type = node_def.get("type")
    config = node_def.get("config", {})
    if node_type == "llm":
        return _run_llm(node_id, config, node_input)
    elif node_type == "transform":
        return _run_transform(node_id, config, node_input)
    else:
        raise ValueError(f"Unknown node type: {node_type!r}")


def _run_llm(node_id: str, config: dict, node_input: str) -> str:
    prompt = config.get("prompt", "").replace("{input}", node_input)[:60]
    mock_response = config.get("mock", "")
    print(f"    [llm:{node_id}] prompt='{prompt}...' input='{node_input[:40]}'")
    time.sleep(0.3)  # simulates LLM latency — replace with real LLM call in prod
    return mock_response


def _run_transform(node_id: str, config: dict, node_input: str) -> str:
    mode = config.get("mode", "passthrough")
    print(f"    [transform:{node_id}] mode={mode}")
    if mode == "passthrough":
        print(f"      → {node_input[:60]}")
        return node_input
    elif mode == "aggregate":
        result = f"[aggregated] {node_input}"
        print(f"      → {result[:60]}")
        return result
    elif mode == "template":
        result = config.get("template", "{input}").replace("{input}", node_input)
        print(f"      → {result[:60]}")
        return result
    else:
        raise ValueError(f"Unknown transform mode: {mode!r}")


def _node_action(node_id: str, node_def: dict, node_input: str):
    """Factory that returns a zero-arg callable capturing this node's arguments.

    Defined at module level so it's never redefined inside a loop, which would
    confuse linters and potentially Restate's journal tracking.
    """
    def action() -> str:
        return _execute_node(node_id, node_def, node_input)
    return action


# ─────────────────────────────────────────────────────────────────────────────
# Graph Helper Functions
# ─────────────────────────────────────────────────────────────────────────────

def _build_predecessor_map(definition: dict) -> dict[str, list[dict]]:
    """Returns {node_id: [incoming edges]} for every node in the definition."""
    pred_map: dict[str, list[dict]] = {n: [] for n in definition.get("nodes", {})}
    for edge in definition.get("edges", []):
        to_node = edge["to"]
        if to_node in pred_map:
            pred_map[to_node].append(edge)
    return pred_map


def _is_ready(
    node_id: str,
    pred_map: dict,
    completed: set,
    results: dict,
    nodes: dict,
) -> bool:
    """
    Check if a node is ready to execute given the current completed set.

    Two fan-in modes (set via node_def["fan_in"]):
    - "all"  (default): all unconditional predecessors must be completed;
                        at least one conditional edge must fire.
    - "any": the node runs as soon as ANY predecessor fires (used for conditional
             fan-in, e.g. when only one of N branches will ever complete).
    """
    incoming = pred_map.get(node_id, [])
    if not incoming:
        return True  # start node — no predecessors

    fan_in = nodes.get(node_id, {}).get("fan_in", "all")

    if fan_in == "any":
        return any(
            e["from"] in completed
            and _check_condition(e.get("when"), results.get(e["from"], ""))
            for e in incoming
        )

    # "all" mode: split unconditional vs conditional edges
    unconditional = [e for e in incoming if "when" not in e]
    conditional = [e for e in incoming if "when" in e]

    if unconditional and not all(e["from"] in completed for e in unconditional):
        return False
    if conditional and not any(
        e["from"] in completed
        and _check_condition(e.get("when"), results.get(e["from"], ""))
        for e in conditional
    ):
        return False
    return True


def _check_condition(condition: str | None, value: str) -> bool:
    """
    Evaluate a simple string condition. No eval() — safe for untrusted input.

    Supported: "contains:X", "equals:X", "startswith:X". None → always True.
    """
    if not condition:
        return True
    v = value.lower()
    if condition.startswith("contains:"):
        return condition[len("contains:"):].lower() in v
    if condition.startswith("equals:"):
        return v == condition[len("equals:"):].lower()
    if condition.startswith("startswith:"):
        return v.startswith(condition[len("startswith:"):].lower())
    return True  # unknown operator — pass through


def _gather_input(
    node_id: str,
    pred_map: dict,
    results: dict,
    user_input: str,
) -> str:
    """
    Build the input string for a node from its predecessors' outputs.
    Falls back to the original user_input for root nodes.
    Aggregates multiple predecessor results with " | ".
    """
    incoming = pred_map.get(node_id, [])
    if not incoming:
        return user_input

    contributed = []
    for edge in incoming:
        pred = edge["from"]
        if pred in results:
            condition = edge.get("when")
            if condition is None or _check_condition(condition, results[pred]):
                contributed.append(results[pred])

    if len(contributed) == 1:
        return contributed[0]
    if contributed:
        return " | ".join(contributed)
    return user_input


# ─────────────────────────────────────────────────────────────────────────────
# Main Interpreter Workflow
# ─────────────────────────────────────────────────────────────────────────────

interpreter = Workflow("workflow")


async def _run_graph(ctx: WorkflowContext, definition: dict, user_input: str) -> str:
    """
    Execute a workflow DAG durably.

    Each node runs inside ctx.run_typed() — its result is journaled so it
    survives crashes and is never re-executed on replay. Parallel nodes run
    via restate.gather(), meaning they execute concurrently in Restate.
    """
    start_node = definition.get("start")
    end_node = definition.get("end")
    nodes = definition.get("nodes", {})
    pred_map = _build_predecessor_map(definition)

    results: dict[str, str] = {}
    completed: set[str] = set()
    frontier: set[str] = {start_node}

    while frontier:
        ready = [
            node_id for node_id in frontier
            if _is_ready(node_id, pred_map, completed, results, nodes)
        ]

        if not ready:
            break  # nothing can progress — deadlock or all blocked on conditions

        tasks: list[tuple[str, restate.RestateDurableFuture]] = []
        for node_id in ready:
            node_def = nodes[node_id]
            node_input = _gather_input(node_id, pred_map, results, user_input)
            task = ctx.run_typed(node_id, _node_action(node_id, node_def, node_input))
            tasks.append((node_id, task))

        # Execute all ready nodes in parallel, then collect results
        await restate.gather(*[t for _, t in tasks])

        for node_id, task in tasks:
            results[node_id] = await task
            completed.add(node_id)
            frontier.discard(node_id)
            print(f"  [graph] '{node_id}' done → {results[node_id][:60]}")

        if end_node in completed:
            break  # done — don't process stale frontier nodes

        # Advance frontier: follow edges from the nodes that just completed
        just_completed = {n for n, _ in tasks}
        for edge in definition.get("edges", []):
            if edge["from"] in just_completed:
                to_node = edge["to"]
                if to_node not in completed:
                    frontier.add(to_node)

    return results.get(end_node, "")


@interpreter.main()
async def run(ctx: WorkflowContext, payload: dict) -> dict:
    definition = payload.get("workflow", {})
    user_input = payload.get("input", "")
    workflow_id = ctx.key()

    ctx.set("status", "running")

    output = await _run_graph(ctx, definition, user_input)

    ctx.set("status", "completed")
    return {"output": output, "workflow_id": workflow_id, "status": "completed"}


@interpreter.handler()
async def status(ctx: WorkflowSharedContext) -> str:
    return await ctx.get("status", type_hint=str) or "pending"
