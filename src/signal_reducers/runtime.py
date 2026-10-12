"""One asyncio application. Models never receive mutable store access."""

from __future__ import annotations

import asyncio
import hashlib
from collections import Counter
from datetime import timedelta
from uuid import UUID

from .config import Config
from .fixtures import FixtureGate, FixtureReducer, FixtureSystem2
from .gate import GateScheduler
from .models import (
    REDUCER_IDS,
    ContractError,
    EscalationRequest,
    OperationResult,
    canonical,
    public,
    utcnow,
)
from .providers import DrexGate, OpenRouterClient, OpenRouterReducer, OpenRouterSystem2
from .reducers import ReducerWorker
from .router import FixtureClassifier, LayaClassifier, SignalRouter
from .state import ObservationStore
from .transport import SignalQueue


class Runtime:
    def __init__(
        self,
        run_id: UUID,
        config: Config,
        *,
        classifier=None,
        reducer_model=None,
        gate_model=None,
        system2_model=None,
        client=None,
        gate_error_handler=None,
    ):
        self.run_id, self.config = run_id, config
        self.client = None
        if config.mode == "fixture":
            if client is not None:
                raise ContractError("fixture_hosted_client")
            classifier = classifier or FixtureClassifier()
            reducer_model = reducer_model or FixtureReducer()
            gate_model = gate_model or FixtureGate()
            system2_model = system2_model or FixtureSystem2()
        else:
            self.client = client if client is not None else OpenRouterClient(config)
            classifier = classifier or LayaClassifier(config)
            reducer_model = reducer_model or OpenRouterReducer(self.client)
            gate_model = gate_model or DrexGate(self.client)
            system2_model = system2_model or OpenRouterSystem2(self.client)
        self.classifier, self.reducer_model = classifier, reducer_model
        self.gate_model, self.system2_model = gate_model, system2_model
        self.router = SignalRouter(classifier, config)
        self.queue = SignalQueue(run_id, config)
        self.store = ObservationStore(run_id, config)
        self.workers = {
            rid: ReducerWorker(rid, self.store.writer(rid), reducer_model, config)
            for rid in REDUCER_IDS
        }
        self.scheduler = GateScheduler(
            self.store, gate_model, config, self._launch, on_error=gate_error_handler
        )
        self.tasks = set()
        self.operation_tasks = set()
        self.stopped = asyncio.Event()
        self.started = False
        self.counters = Counter()
        self.fatal_error = None
        self.configuration_sha256 = hashlib.sha256(
            canonical(public(config))
        ).hexdigest()

    async def start(self):
        try:
            await self.queue.check()
            if self.client:
                await self.client.check_authentication()
                await self.client.check_models()
                prepare = getattr(self.classifier, "prepare", None)
                if prepare:
                    await prepare()
            await self.queue.claim()
            for worker in self.workers.values():
                worker.start()
            self.scheduler.start()
            self._task(self._consume())
            self._task(self._lease())
            self.started = True
        except BaseException:
            await self.close()
            raise

    def _task(self, coro, operation=False):
        task = asyncio.create_task(coro)
        group = self.operation_tasks if operation else self.tasks
        group.add(task)
        task.add_done_callback(group.discard)
        return task

    async def _consume(self):
        try:
            while not self.stopped.is_set():
                delivery = await self.queue.pull()
                if delivery is None:
                    continue
                try:
                    destinations = await self.router.route(delivery)
                except Exception as exc:
                    self.counters["routing_failed"] += 1
                    if not self.router.healthy:
                        raise ContractError("router_unhealthy") from exc
                    continue
                self.counters["routed"] += 1
                for reducer_id in destinations:
                    if await self.workers[reducer_id].offer(delivery):
                        self.counters[f"delivered_{reducer_id}"] += 1
                    else:
                        self.counters[f"dropped_{reducer_id}"] += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.fatal_error = getattr(exc, "code", type(exc).__name__)
            self.stopped.set()

    async def _lease(self):
        try:
            while not self.stopped.is_set():
                await asyncio.sleep(60)
                await self.queue.renew()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.fatal_error = getattr(exc, "code", "run_lease_lost")
            self.stopped.set()

    async def _launch(self, operation, batch, decision):
        self._task(self._execute(operation, batch, decision), operation=True)

    async def _execute(self, operation, batch, decision):
        started = utcnow()
        kind = "tool" if operation.action == "act" else "system2"
        try:
            if kind == "tool":
                inspection = await self.store.inspect()
                requested = decision.operation.arguments["reducer_id"]
                activities = inspection.get(
                    "activity", inspection.get("activities", [])
                )
                if isinstance(activities, dict):
                    activities = list(activities.values())
                data = [
                    {
                        "id": a["reducer_id"],
                        "status": a["status"],
                        "queued": a["buffered_signals"],
                        "active": a["active_processing"],
                    }
                    for a in activities
                    if requested is None or a["reducer_id"] == requested
                ]
                result = OperationResult(
                    operation_id=operation.id,
                    type=kind,
                    batch_id=batch.id,
                    target_ids=operation.target_ids,
                    status="success",
                    content="Read current reducer processing status.",
                    data_json=canonical(
                        {"activity": data, "read_version": inspection["state_version"]},
                        maximum=512,
                    ),
                    context_version=batch.state_version,
                    started_at=started,
                )
            else:
                request = EscalationRequest(
                    id=operation.id,
                    batch_id=batch.id,
                    run_id=self.run_id,
                    context_version=batch.state_version,
                    target_ids=operation.target_ids,
                    reason=decision.operation.escalation_reason,
                    thinking_level=decision.operation.thinking_level,
                    context_json=canonical(public(batch), maximum=24_000),
                    deadline_at=started
                    + timedelta(seconds=self.config.system2_timeout),
                )
                async with asyncio.timeout(self.config.system2_timeout):
                    result = await self.system2_model.execute(request)
                if utcnow() >= request.deadline_at and result.status != "timeout":
                    raise TimeoutError
            await self.store.add_result(result)
        except asyncio.CancelledError:
            await self.store.add_result(
                OperationResult(
                    operation_id=operation.id,
                    type=kind,
                    batch_id=batch.id,
                    target_ids=operation.target_ids,
                    status="cancelled",
                    error_code="operation_cancelled",
                    context_version=batch.state_version,
                    started_at=started,
                )
            )
            raise
        except Exception as exc:
            status = "timeout" if isinstance(exc, TimeoutError) else "failed"
            await self.store.add_result(
                OperationResult(
                    operation_id=operation.id,
                    type=kind,
                    batch_id=batch.id,
                    target_ids=operation.target_ids,
                    status=status,
                    error_code=getattr(exc, "code", "operation_failed"),
                    context_version=batch.state_version,
                    started_at=started,
                )
            )

    async def recheck(self):
        await self.store.recheck()

    async def inspect(self):
        view = await self.store.inspect()
        view.update(
            {
                "run_id": str(self.run_id),
                "mode": self.config.mode,
                "fatal_error": self.fatal_error,
                "configuration_sha256": self.configuration_sha256,
                "transport": dict(self.queue.counters),
                "runtime": dict(self.counters),
                "reducers": {k: dict(v.counters) for k, v in self.workers.items()},
            }
        )
        if self.client:
            view["models"] = public(
                getattr(self.client, "stats", getattr(self.client, "counters", {}))
            )
            view["model_calls"] = public(self.client.calls)
        else:
            view["models"] = {
                "reducer_calls": dict(self.reducer_model.calls),
                "system1_calls": self.gate_model.calls,
                "system2_calls": self.system2_model.calls,
            }
        view["router_manifest"] = public(self.router.manifest)
        view["router"] = {
            "counters": dict(self.router.counters),
            "latencies": list(self.router.latencies),
        }
        return view

    async def close(self):
        self.stopped.set()
        for task in tuple(self.tasks):
            task.cancel()
        if self.tasks:
            await asyncio.gather(*tuple(self.tasks), return_exceptions=True)
        await self.scheduler.close()
        for worker in self.workers.values():
            await worker.close()
        if self.operation_tasks:
            done, pending = await asyncio.wait(tuple(self.operation_tasks), timeout=5)
            for task in pending:
                task.cancel()
            await asyncio.gather(*tuple(pending), return_exceptions=True)
        try:
            await self.queue.close()
        finally:
            if self.client:
                await self.client.close()
            await self.router.close()
