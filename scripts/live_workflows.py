"""Run production live workflows with safe closed-choice/context diagnostics."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sys

from _live_support import load_live_key, request_controls, source_manifest

from signal_reducers import cli
from signal_reducers.demo import write_private
from signal_reducers.models import canonical
from signal_reducers.providers import DrexGate, OpenRouterClient
from signal_reducers.runtime import Runtime


class DiagnosticClient(OpenRouterClient):
    async def post(self, endpoint, body, **kwargs):
        response = await super().post(endpoint, body, **kwargs)
        event = self.calls[-1]
        event["request_sha256"] = hashlib.sha256(canonical(body)).hexdigest()
        if endpoint == "systemone":
            state = body["state"]
            answers = response.get("answers", {})
            if not isinstance(answers, dict):
                answers = {}
            event["diagnostic"] = {
                "batch_id": state["id"],
                "records": [
                    {
                        "id": r["observation"]["id"],
                        "code": r["observation"]["code"],
                        "finality": r["observation"]["finality"],
                        "resolution": r["resolution"],
                    }
                    for r in state["records"]
                ],
                "selected": {
                    name: answer.get("choice")
                    if isinstance(answer, dict)
                    and isinstance(answer.get("choice"), str)
                    and answer["choice"] in definition["criteria"]
                    else "<invalid>"
                    for name, definition in body["questions"].items()
                    for answer in [answers.get(name)]
                },
            }
        elif kwargs.get("component") == "system2":
            payload = json.loads(body["messages"][-1]["content"])
            captured = payload["context"]
            event["diagnostic"] = {
                "captured_batch_id": captured["id"],
                "context_version": captured["state_version"],
                "target_ids": payload["target_ids"],
                "captured_observations": [
                    {
                        "id": r["observation"]["id"],
                        "code": r["observation"]["code"],
                        "reducer_id": r["observation"]["reducer_id"],
                        "facts": r["observation"]["facts"],
                    }
                    for r in captured.get("records", [])
                ],
                **request_controls(body),
            }
        return response


def annotate_error(client, batch, error, phase):
    for event in reversed(client.calls):
        if event.get("component") == "system1" and event.get("diagnostic", {}).get(
            "batch_id"
        ) == str(batch.id):
            event[phase + "_error"] = getattr(error, "code", type(error).__name__)
            break


class DiagnosticRuntime(Runtime):
    def __init__(self, *args, **kwargs):
        config = args[1] if len(args) > 1 else kwargs["config"]
        client = DiagnosticClient(config)
        super().__init__(
            *args,
            **kwargs,
            client=client,
            gate_model=DrexGate(client),
            gate_error_handler=lambda batch, error, phase: annotate_error(
                client, batch, error, phase
            ),
        )

    async def start(self):
        await super().start()

        async def progress():
            while True:
                await asyncio.sleep(30)
                view = await self.inspect()
                print(
                    json.dumps(
                        {
                            "progress": {
                                "run_id": str(self.run_id),
                                "popped": view["transport"]["popped"],
                                "expired": view["transport"]["expired"],
                                "observations": len(view["records"]),
                                "models": view["models"],
                                "results": [
                                    {
                                        "status": result["status"],
                                        "error_code": result.get("error_code"),
                                    }
                                    for result in view["results"]
                                ],
                                "rejected_gate_evaluations": self.scheduler.failures,
                            }
                        }
                    ),
                    flush=True,
                )

        self._task(progress())


if __name__ == "__main__":
    try:
        source = load_live_key()
        arguments = cli.parser().parse_args(sys.argv[1:])
        if arguments.command != "demo" or arguments.mode != "live":
            raise ValueError("live_demo_required")
        write_private(
            arguments.output / "execution-manifest.json",
            {**source_manifest(), "credential_source": source},
        )
        code = asyncio.run(cli.execute(arguments, runtime_factory=DiagnosticRuntime))
    except KeyboardInterrupt:
        code = 130
    except Exception as error:
        print({"error": getattr(error, "code", type(error).__name__)}, file=sys.stderr)
        code = 1
    raise SystemExit(code)
