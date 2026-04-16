import time
import restate
from restate import Workflow, WorkflowContext, WorkflowSharedContext

_charge_attempts: dict[str, int] = {}

order = Workflow("order")


@order.main()
async def run(ctx: WorkflowContext, payload: dict) -> dict:
    order_id = ctx.key()
    item = payload["item"]
    amount = payload["amount"]
    flaky = payload.get("flaky", False)

    ctx.set("status", "validating")
    await ctx.run("validate", lambda: validate(item))

    ctx.set("status", "charging")
    tx_id = await ctx.run("charge", lambda: charge(order_id, amount, flaky=flaky))

    ctx.set("status", "completed")
    return {"order_id": order_id, "tx_id": tx_id, "status": "completed"}


@order.handler()
async def status(ctx: WorkflowSharedContext) -> str:
    return await ctx.get("status", type_hint=str) or "unknown"


def validate(item: str) -> bool:
    print(f"  [validate] checking stock for '{item}'...")
    time.sleep(1)  # simulates inventory API latency
    print(f"  [validate] '{item}' is in stock ✓")
    return True


def charge(order_id: str, amount: int, *, flaky: bool = False) -> str:
    attempt = _charge_attempts.get(order_id, 0) + 1
    _charge_attempts[order_id] = attempt
    print(f"  [charge] order={order_id!r} amount=${amount} attempt={attempt}")
    if flaky and attempt <= 2:
        backoff_ms = 500 * (2 ** (attempt - 1))
        print(f"  [charge] gateway timeout — Restate retries in {backoff_ms}ms")
        raise Exception("Payment gateway timeout")
    tx_id = f"tx-{order_id}"
    print(f"  [charge] charged ${amount} ✓  tx_id={tx_id}")
    return tx_id


app = restate.app(services=[order])
