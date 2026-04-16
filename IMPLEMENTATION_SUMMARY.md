# Implementation Summary: Dynamic Workflow DAG Interpreter on Restate

## Overview

Successfully implemented a dynamic workflow DAG (Directed Acyclic Graph) interpreter on top of Restate, demonstrating that Restate workflows are "just code" — you can read a workflow definition at runtime and execute it durably without a special workflow language.

## Files Created/Modified

### 1. **interpreter.py** (NEW)

A complete DAG executor that:

- **Node Execution**: `_execute_node()`, `_run_llm()`, `_run_transform()`
  - LLM nodes: simulate LLM calls with mock responses
  - Transform nodes: support passthrough, aggregate, and template modes
- **Graph Helpers**:
  - `_build_predecessor_map()`: Build incoming edge map for each node
  - `_is_ready()`: Implement fan-in logic (wait all unconditional, any conditional)
  - `_check_condition()`: Evaluate simple conditions (contains, equals, startswith)
  - `_gather_input()`: Merge predecessor results into node input

- **Main Workflow**:
  - `_run_graph()`: Core algorithm that:
    1. Maintains frontier of ready nodes
    2. Executes all ready nodes in parallel via `restate.gather()`
    3. Durable journaling of each node's result via `ctx.run_typed()`
    4. Advances frontier by following edges
  - `run()` handler: Main entry point accepting workflow definition + user input
  - `status()` & `progress()` handlers: Query workflow state

### 2. **main.py** (MODIFIED)

- Added import: `from interpreter import interpreter`
- Registered interpreter in `restate.app(services=[order, interpreter])`

### 3. **scenarios.py** (MODIFIED)

Added two new demo scenarios:

#### Scenario 5: Support Ticket Triage (Conditional Routing)

```
classify(LLM) ──bug──────→ handle_bug(LLM) ──→ format_response(transform) → done
              ──feature──→ handle_feature(LLM) ─→
              ──question─→ handle_question(LLM) →
```

- Demonstrates conditional routing: edges with `when: "contains:X"` only fire if condition passes
- Shows that the same interpreter handles any routing topology dynamically
- Added `submit_workflow_sync()` helper for workflow submissions

#### Scenario 6: Parallel Text Analysis (Fan-Out / Fan-In)

```
sentiment ──→┐
key_points ─→├→ aggregate → done
summary ────→┘
```

- Demonstrates parallel execution: multiple nodes run simultaneously via `restate.gather()`
- Shows that each branch is durably retried independently
- Fan-in: `aggregate` waits for all predecessors to complete

## Key Features Implemented

### ✅ Durable Execution

- Each node's result is journaled via `ctx.run_typed()`
- On server crash/restart, node execution replays from journal
- No redundant re-execution of already-completed nodes

### ✅ Parallel Fan-Out

- Multiple node branches execute concurrently using `restate.gather()`
- Each branch has independent durability

### ✅ Conditional Routing

- Edges can specify `when: "condition_string"`
- Conditions: `contains:X`, `equals:X`, `startswith:X`
- Only edge fires if condition matches predecessor's result

### ✅ Fan-In Synchronization

- Nodes with multiple unconditional predecessors wait for ALL
- Nodes with only conditional predecessors ready when ANY fires
- Aggregate transforms merge multi-predecessor results

### ✅ Runtime Workflow Definition

- Workflow topology (nodes + edges) passed in request payload
- No special DSL needed — just dictionaries/JSON
- Same interpreter executes any DAG shape

## Verification Steps

To verify the implementation:

```bash
# 1. Start Restate infrastructure
docker compose up -d

# 2. Start the app server (in one terminal)
uv run hypercorn main:app --bind 0.0.0.0:9080

# 3. Register deployment (in another terminal)
curl localhost:9070/deployments --json '{"uri": "http://host.docker.internal:9080"}'

# 4. Run scenarios (in third terminal)
uv run python scenarios.py
```

Expected output:

- Scenarios 1-4: Original order processing demos
- Scenario 5: Ticket routed to `handle_bug` (output starts with "[URGENT]")
- Scenario 6: Three analyses run in parallel, aggregated
- Hyperconn logs show per-node execution with durable journaling

## Algorithm: `_run_graph()` Execution Model

```
1. Initialize: frontier = {start_node}, completed = {}, results = {}

2. While frontier not empty:
   a. Find all ready nodes in frontier
      - A node is ready if:
        - It has NO unconditional predecessors, OR
        - ALL unconditional predecessors are completed, AND
        - (no conditional predecessors OR at least ONE fires)

   b. Execute all ready nodes in parallel:
      - For each node: ctx.run_typed(_execute_node, node_id, config, input)
      - Wait for all with: restate.gather(*tasks)

   c. Store results and mark completed

   d. Advance frontier by following edges from newly-completed nodes

3. Return result from end_node
```

## Workflow Definition Format

```python
{
    "start": "node_id",        # First node to execute
    "end": "node_id",          # Terminal node for result extraction
    "nodes": {
        "node_id": {
            "type": "llm" | "transform",
            "config": {
                # For LLM:
                "prompt": "template with {input}",
                "mock": "response",

                # For transform:
                "mode": "passthrough" | "aggregate" | "template",
                "template": "string with {input}",
            }
        }
    },
    "edges": [
        {"from": "a", "to": "b"},                      # Unconditional
        {"from": "a", "to": "c", "when": "contains:X"} # Conditional
    ]
}
```

## Next Steps (For Production)

1. **Real LLM Integration**: Replace mock responses in `_run_llm()` with actual API calls
2. **Error Handling**: Add timeout, retry, and failure strategies per node
3. **Dynamic Scheduling**: Add human-in-the-loop waits via `ctx.promise()`
4. **Monitoring**: Export metrics for node execution duration, errors, retries
5. **Validation**: Validate DAG structure (no cycles, all referenced nodes exist)
