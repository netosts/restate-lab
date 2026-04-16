import time
import restate
from restate import Workflow, WorkflowContext, WorkflowSharedContext

order = Workflow("order")


@order.main()
async def run(ctx: WorkflowContext, payload: dict) -> dict:
    ctx.set("status", "validating")
    await ctx.run("validate", lambda: validate(payload["item"]))

    ctx.set("status", "charging")
    await ctx.run("charge", lambda: charge(payload["amount"]))

    ctx.set("status", "completed")
    return {"order_id": ctx.key(), "status": "completed"}


@order.handler()
async def status(ctx: WorkflowSharedContext) -> str:
    return await ctx.get("status", type_hint=str) or "unknown"


def validate(item: str) -> bool:
    print(f"[validate] item={item}")
    return True


def charge(amount: int) -> bool:
    print(f"[charge] amount={amount}")
    return True


app = restate.app(services=[order])
