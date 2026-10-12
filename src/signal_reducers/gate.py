"""One gate evaluation at a time, with bounded waits/retries and no polling."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from .config import Config
from .models import ActiveOperation, Batch, ContractError, GateDecision, OperationResult
from .state import ObservationStore


class GateScheduler:
    def __init__(
        self,
        store: ObservationStore,
        model,
        config: Config,
        on_operation: Callable[[ActiveOperation, Batch, GateDecision], Awaitable[None]],
        *,
        on_error: Callable[[Batch, Exception, str], None] | None = None,
    ):
        self.store, self.model, self.config, self.on_operation = (
            store,
            model,
            config,
            on_operation,
        )
        self.on_error = on_error
        self._task: asyncio.Task | None = None
        self._timers: set[asyncio.Task] = set()
        # The empty registered areas are the initial evaluated status baseline.
        self._last_fingerprint: tuple | None = (0, ("idle", "idle", "idle"), (), 0)
        self._forced_fingerprint: tuple | None = None
        self._wait_followed: tuple | None = None
        self._failed_fingerprint: tuple | None = None
        self._failure_count = 0
        self.calls = self.failures = 0
        self.needs_attention = False

    def start(self) -> asyncio.Task:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="system1-gate")
        return self._task

    async def close(self) -> None:
        tasks = set(self._timers)
        if self._task:
            tasks.add(self._task)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._task = None

    def _later(self, fingerprint: tuple, delay: float) -> None:
        async def wake() -> None:
            await asyncio.sleep(delay)
            if await self.store.fingerprint() == fingerprint:
                self._forced_fingerprint = fingerprint
                self.store.wake.set()

        task = asyncio.create_task(wake(), name="gate-followup")
        self._timers.add(task)
        task.add_done_callback(self._timers.discard)

    async def _coalesce(self) -> None:
        loop = asyncio.get_running_loop()
        started = loop.time()
        previous = await self.store.fingerprint()
        while True:
            delay = min(
                self.config.debounce,
                max(0, self.config.max_debounce - (loop.time() - started)),
            )
            if delay <= 0:
                return
            await asyncio.sleep(delay)
            current = await self.store.fingerprint()
            if current == previous:
                return
            previous = current

    async def _run(self) -> None:
        while True:
            await self.store.wake.wait()
            await self._coalesce()
            fingerprint, page = await self.store.consume_wake()
            forced = self._forced_fingerprint == fingerprint
            self._forced_fingerprint = None
            if fingerprint == self._last_fingerprint and not (forced or page):
                continue
            if fingerprint != self._failed_fingerprint:
                self._failure_count = 0
            self._last_fingerprint = fingerprint
            self.needs_attention = False
            self.store.diagnostics["needs_attention"] = False
            batch = None
            phase = "capture"
            try:
                batch = await self.store.create_batch(
                    getattr(self.model, "request_size", None)
                )
                self.calls += 1
                phase = "compile"
                decision = await asyncio.wait_for(
                    self.model.evaluate(batch), self.config.gate_timeout
                )
                phase = "state_validation"
                operation = await self.store.apply(batch, decision)
                self._failure_count = 0
                if operation is not None:
                    try:
                        await self.on_operation(operation, batch, decision)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        await self.store.add_result(
                            OperationResult(
                                operation_id=operation.id,
                                type="system2"
                                if operation.action == "escalate"
                                else "tool",
                                batch_id=batch.id,
                                target_ids=operation.target_ids,
                                status="failed",
                                error_code="operation_launch_failed",
                                context_version=operation.context_version,
                            )
                        )
                if decision.action == "wait":
                    if self._wait_followed != fingerprint:
                        self._wait_followed = fingerprint
                        self._later(fingerprint, self.config.wait_followup)
                    else:
                        self.needs_attention = True
                        self.store.diagnostics["needs_attention"] = True
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if self.on_error is not None and batch is not None:
                    try:
                        self.on_error(batch, error, phase)
                    except Exception:
                        # Optional diagnostics cannot change failure or retry behavior.
                        pass
                self.failures += 1
                self._failed_fingerprint = fingerprint
                self._failure_count += 1
                self.store.diagnostics["gate_error"] = (
                    error.code
                    if isinstance(error, ContractError)
                    else type(error).__name__
                )
                if self._failure_count == 1:
                    self._later(fingerprint, self.config.retry_delay)
                else:
                    self.needs_attention = True
                    self.store.diagnostics["needs_attention"] = True
            finally:
                if batch is not None:
                    await self.store.finish_batch(batch)
