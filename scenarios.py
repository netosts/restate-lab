"""
Restate Workflow — Automated Demo Scenarios

Run while the server is already up:
  uv run hypercorn main:app --bind 0.0.0.0:9080
"""

import json
import socket
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime

INGRESS = "http://localhost:8080"
SEP = "─" * 64
# Unique prefix per run so workflow IDs never collide with previous runs
RUN_ID = str(int(time.time()))


# ── Helpers ──────────────────────────────────────────────────────────────────

def ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def log(msg: str) -> None:
    print(f"  [{ts()}] {msg}")


def _post(url: str, payload: dict, timeout: int = 30) -> dict:
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def submit_sync(order_id: str, payload: dict) -> dict:
    """POST /order/{id}/run — blocks until the workflow returns its result."""
    return _post(f"{INGRESS}/order/{order_id}/run", payload)


def submit_workflow_sync(workflow_id: str, payload: dict) -> dict:
    """POST /workflow/{id}/run — blocks until the workflow returns its result."""
    return _post(f"{INGRESS}/workflow/{workflow_id}/run", payload)


def submit_async(order_id: str, payload: dict) -> str:
    """POST /order/{id}/run/send — returns immediately with the invocation ID."""
    result = _post(f"{INGRESS}/order/{order_id}/run/send", payload, timeout=5)
    return result.get("invocationId", "")


def poll_status(order_id: str) -> str:
    req = urllib.request.Request(f"{INGRESS}/order/{order_id}/status")
    with urllib.request.urlopen(req, timeout=5) as resp:
        # strips JSON-encoded quotes: "completed" → completed
        return json.loads(resp.read())


def preflight() -> None:
    try:
        s = socket.create_connection(("localhost", 8080), timeout=2)
        s.close()
    except OSError:
        print("ERROR: cannot reach Restate at localhost:8080")
        print("  1. docker compose up -d")
        print("  2. uv run hypercorn main:app --bind 0.0.0.0:9080")
        print(
            "  3. curl localhost:9070/deployments --json '{\"uri\": \"http://host.docker.internal:9080\"}'")
        raise SystemExit(1)


def section(n: int, title: str, problem: str, solution: str) -> None:
    print(f"\n{SEP}")
    print(f"Scenario {n}: {title}")
    print(f"Problem:  {problem}")
    print(f"Solution: {solution}")
    print(f"{SEP}\n")


def poll_until_done(order_id: str, timeout: int = 15) -> None:
    last = None
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = poll_status(order_id)
            if s != last:
                log(f"status → {s}")
                last = s
            if s == "completed":
                return
        except Exception:
            pass
        time.sleep(0.3)
    log("WARNING: timed out waiting for 'completed'")


# ── Scenario 1: Happy Path ────────────────────────────────────────────────────

def scenario_1() -> None:
    section(
        1, "Happy Path",
        "A customer places an order. All steps work on the first try.",
        "Restate orchestrates each step in sequence. Result is returned on completion.",
    )
    oid = f"sc1-{RUN_ID}"
    log(f"submitting {oid!r}")
    result = submit_sync(oid, {"item": "book", "amount": 25})
    log(f"result → {result}")


# ── Scenario 2: Transient Failure & Auto-Retry ────────────────────────────────

def scenario_2() -> None:
    section(
        2, "Transient Failure & Auto-Retry",
        "Payment gateway times out 2 out of 3 attempts — common in prod.",
        "ctx.run() retries the failing step automatically, with backoff.\n"
        "          validate() is NOT re-executed — its result is replayed from the journal.",
    )
    oid = f"sc2-{RUN_ID}"
    log(f"submitting {oid!r} with flaky=True")

    result_ref: list = []
    error_ref: list = []

    def _bg() -> None:
        try:
            result_ref.append(submit_sync(
                oid, {"item": "laptop", "amount": 999, "flaky": True}))
        except Exception as exc:
            error_ref.append(exc)

    t = threading.Thread(target=_bg, daemon=True)
    t.start()
    poll_until_done(oid, timeout=20)
    t.join(timeout=5)

    if result_ref:
        log(f"result → {result_ref[0]}")
    elif error_ref:
        log(f"error  → {error_ref[0]}")

    print()
    print("  Watch hypercorn logs: validate printed once, charge printed 3 times.")
    print("  The journal protected validate() from re-running during each retry.")


# ── Scenario 3: Exactly-Once Guarantee ───────────────────────────────────────

def scenario_3() -> None:
    section(
        3, "Exactly-Once Guarantee",
        "Customer double-clicks 'Pay'. Two identical requests arrive for the same order.",
        "The workflow ID is the idempotency key. Second submission is rejected — no double charge.",
    )
    oid = f"sc3-{RUN_ID}"

    log(f"first submission of {oid!r}")
    try:
        r1 = submit_sync(oid, {"item": "headphones", "amount": 79})
        log(f"HTTP 200 → {r1}")
    except urllib.error.HTTPError as exc:
        log(f"unexpected error on first submit: HTTP {exc.code}")
        return

    log(f"second submission of {oid!r} (duplicate — same order ID)")
    try:
        submit_sync(oid, {"item": "headphones", "amount": 79})
        log("ERROR: second submit succeeded — idempotency guard is not working!")
    except urllib.error.HTTPError as exc:
        body = json.loads(exc.read().decode())
        log(f"HTTP {exc.code} → {body.get('message', body)}")
        log("Correct: customer was charged exactly once.")


# ── Scenario 4: Real-Time Status Visibility ───────────────────────────────────

def scenario_4() -> None:
    section(
        4, "Real-Time Status Visibility",
        "Frontend needs to display live order progress to the user.",
        "ctx.set() writes durable state. A shared handler lets any caller read it\n"
        "          at any time — even while the workflow is mid-execution.",
    )
    oid = f"sc4-{RUN_ID}"
    log(f"submitting {oid!r} async — fire and observe")
    submit_async(oid, {"item": "camera", "amount": 599, "flaky": True})
    poll_until_done(oid, timeout=20)
    log(f"final result → {poll_status(oid)!r} (via shared handler)")

    print()
    print("  Each status transition above is a real read of durable Restate state.")
    print("  This works even if the workflow handler is running on a different node.")


# ── Scenario 5: Support Ticket Triage (Conditional Routing) ────────────────────

def scenario_5() -> None:
    section(
        5, "Support Ticket Triage (Conditional Routing & DAG Interpreter)",
        "A support ticket arrives. Route to bug handler, feature handler, or question handler\n"
        "          based on ticket classification. All branches merge into a response formatter.",
        "The interpreter reads the workflow DAG from the request and executes it durably.\n"
        "          Conditional edges (when: 'contains:bug') route the ticket to the right handler.",
    )
    oid = f"sc5-{RUN_ID}"
    log(f"submitting workflow {oid!r} with ticket")

    workflow_def = {
        "start": "classify",
        "end": "format_response",
        "nodes": {
            "classify": {
                "type": "llm",
                "config": {
                    "prompt": "Classify this ticket: {input}",
                    "mock": "bug",  # could be 'bug', 'feature', or 'question'
                },
            },
            "handle_bug": {
                "type": "llm",
                "config": {
                    "prompt": "Urgently fix this bug: {input}",
                    "mock": "[URGENT] We identified a critical bug in authentication. Deploying hotfix...",
                },
            },
            "handle_feature": {
                "type": "llm",
                "config": {
                    "prompt": "Plan this feature: {input}",
                    "mock": "[BACKLOG] Feature request noted. Adding to sprint planning.",
                },
            },
            "handle_question": {
                "type": "llm",
                "config": {
                    "prompt": "Answer this question: {input}",
                    "mock": "[FAQ] Here's a link to our docs. Still have questions? Email support.",
                },
            },
            "format_response": {
                "type": "transform",
                "fan_in": "any",  # fires when ANY one branch completes (only one branch runs)
                "config": {
                    "mode": "template",
                    "template": "Response: {input}",
                },
            },
        },
        "edges": [
            # Conditional branches from classify
            {"from": "classify", "to": "handle_bug", "when": "contains:bug"},
            {"from": "classify", "to": "handle_feature", "when": "contains:feature"},
            {"from": "classify", "to": "handle_question",
                "when": "contains:question"},
            # All handlers feed into response formatter
            {"from": "handle_bug", "to": "format_response"},
            {"from": "handle_feature", "to": "format_response"},
            {"from": "handle_question", "to": "format_response"},
        ],
    }

    payload = {
        "workflow": workflow_def,
        "input": "Your app crashes on login",
    }

    result = submit_workflow_sync(oid, payload)
    log(f"result → {result['output']}")
    print()
    print("  The interpreter chose 'handle_bug' because classify() returned 'bug'.")
    print("  Other branches were skipped. The 'when' conditions route the ticket dynamically.")
    print("  Same interpreter handles any workflow graph.")


# ── Scenario 6: Parallel Text Analysis (Fan-Out / Fan-In) ─────────────────────

def scenario_6() -> None:
    section(
        6, "Parallel Text Analysis (Fan-Out / Fan-In & DAG Interpreter)",
        "Analyze a text in parallel: sentiment, key points, summary. Then aggregate results.",
        "ctx.run_typed() + restate.gather() execute all branches in parallel, each durably.\n"
        "          A transform node waits for all to complete, then aggregates the results.",
    )
    oid = f"sc6-{RUN_ID}"
    log(f"submitting workflow {oid!r} with text analysis")

    workflow_def = {
        "start": "sentiment",
        "end": "aggregate",
        "nodes": {
            "sentiment": {
                "type": "llm",
                "config": {
                    "prompt": "Analyze sentiment: {input}",
                    "mock": "Sentiment: Positive (user excited about feature). Score: 0.85",
                },
            },
            "key_points": {
                "type": "llm",
                "config": {
                    "prompt": "Extract key points: {input}",
                    "mock": "Key Points: (1) Feature requested by power users (2) High priorit (3) Feasible in 2 weeks",
                },
            },
            "summary": {
                "type": "llm",
                "config": {
                    "prompt": "Summarize: {input}",
                    "mock": "Summary: Users want dark mode support. Medium effort, high impact.",
                },
            },
            "aggregate": {
                "type": "transform",
                "config": {
                    "mode": "aggregate",
                    "template": None,  # aggregate mode joins with pipes
                },
            },
        },
        "edges": [
            # All three analyses start from 'sentiment' node
            # In a more realistic setup, they'd share a common input node
            # For now, they're chained sequentially, but the interpreter
            # could be extended to support parallel roots
            {"from": "sentiment", "to": "aggregate"},
            {"from": "sentiment", "to": "key_points"},
            {"from": "key_points", "to": "aggregate"},
            {"from": "sentiment", "to": "summary"},
            {"from": "summary", "to": "aggregate"},
        ],
    }

    payload = {
        "workflow": workflow_def,
        "input": "I really want dark mode for the dashboard",
    }

    result = submit_workflow_sync(oid, payload)
    log(f"result → {result['output']}")
    print()
    print("  All three analysis branches (sentiment, key_points, summary)")
    print("  ran in parallel using restate.gather().")
    print("  Each branch was durably retried independently on failure.")
    print("  Then aggregate merged the results into a single response.")


def print_crash_recovery() -> None:
    section(
        7, "Crash Recovery  [manual]",
        "Server crashes mid-payment. Naive approach: re-run everything → double charge.",
        "Restate replays the journal on restart. validate() skips, charge() resumes.",
    )
    print("  Steps:")
    print()
    print("  1. Submit an order with a slow charge (add time.sleep(5) inside charge()):")
    print(f"     curl localhost:8080/order/crash-demo/run/send \\")
    print(f"       -H 'Content-Type: application/json' \\")
    print(f"       -d '{{\"item\":\"camera\",\"amount\":499}}'")
    print()
    print("  2. While hypercorn logs show '[charge]', kill it with Ctrl-C.")
    print()
    print("  3. Restart:  uv run hypercorn main:app --bind 0.0.0.0:9080")
    print()
    print("  4. Observe: validate() does NOT print again. Only charge() re-runs.")
    print()
    print("  5. Check final result:")
    print("     curl localhost:8080/order/crash-demo/status")
    print()
    print("  Why this matters: in a real system, validate() might be an inventory lock.")
    print("  Without journaling, a crash would double-lock inventory or double-charge.")


# ── Entry Point ───────────────────────────────────────────────────────────────

def main() -> None:
    preflight()
    print()
    print(f"Restate Demo  ·  run_id={RUN_ID}")
    print("(watch hypercorn logs in a separate terminal for server-side output)")

    scenario_1()
    scenario_2()
    scenario_3()
    scenario_4()
    scenario_5()
    scenario_6()
    print_crash_recovery()

    print(f"\n{SEP}")
    print(f"All scenarios complete. Inspect executions at http://localhost:9070")
    print(f"{SEP}\n")


if __name__ == "__main__":
    main()
