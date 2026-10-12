"""Transactional observations and gate authority; never holds a lock across I/O."""

from __future__ import annotations

import asyncio
from collections import OrderedDict, deque
from datetime import datetime
from typing import Callable
from uuid import UUID, uuid4

from .config import Config
from .models import (
    REDUCER_IDS,
    ActiveOperation,
    Activity,
    Batch,
    ContractError,
    GateDecision,
    Observation,
    OperationResult,
    Record,
    Snapshot,
    canonical,
    public,
    utcnow,
)


class ReducerWriter:
    """A reducer-specific capability: semantic output, status and freshness only."""

    def __init__(self, store: ObservationStore, reducer_id: str):
        self._store, self.reducer_id = store, reducer_id

    async def emit(self, observations: tuple[Observation, ...]) -> tuple[UUID, ...]:
        if any(o.reducer_id != self.reducer_id for o in observations):
            raise ContractError("wrong_reducer_writer")
        return await self._store._emit(observations)

    async def activity(self, activity: Activity) -> None:
        if activity.reducer_id != self.reducer_id:
            raise ContractError("wrong_reducer_writer")
        await self._store._activity(activity)

    async def refresh(
        self, subject_id: str, source_seq: int | None, received_at: datetime
    ) -> None:
        await self._store._refresh(self.reducer_id, subject_id, source_seq, received_at)


class ObservationStore:
    def __init__(self, run_id: UUID, config: Config):
        self.run_id, self.config = run_id, config
        self.lock = asyncio.Lock()
        self.wake = asyncio.Event()
        self.state_version = self.input_generation = self.semantic_generation = 0
        self._records: OrderedDict[UUID, Record] = OrderedDict()
        self._roots: dict[UUID, UUID] = {}
        self._leaves: dict[UUID, UUID] = {}
        self._depth: dict[UUID, int] = {}
        self._subject_seq: dict[tuple[str, str], int] = {}
        self._snapshots: dict[tuple[str, str], Snapshot] = {}
        self._snapshot_versions: dict[tuple[str, str], int] = {}
        self._activity_data = {r: Activity(reducer_id=r) for r in REDUCER_IDS}
        self._operations: OrderedDict[UUID, ActiveOperation] = OrderedDict()
        self._contexts: dict[UUID, frozenset[UUID]] = {}
        self._results: OrderedDict[UUID, OperationResult] = OrderedDict()
        self._reviewed: set[UUID] = set()
        self._initiators: set[UUID] = set()
        self._applied: OrderedDict[UUID, bytes] = OrderedDict()
        self._active_batch: Batch | None = None
        self._recent_batches: deque[UUID] = deque(maxlen=32)
        self._unresolved_cursor = self._recheck = 0
        self._page_pending = False
        self.diagnostics: dict[str, int | str | bool] = {}

    def writer(self, reducer_id: str) -> ReducerWriter:
        if reducer_id not in REDUCER_IDS:
            raise ContractError("unknown_reducer")
        return ReducerWriter(self, reducer_id)

    def _changed(self, *, semantic: bool = False, eligible: bool = False) -> None:
        self.state_version += 1
        if semantic:
            self.semantic_generation += 1
        if semantic or eligible:
            self.input_generation += 1
            self.wake.set()

    def _ancestry(
        self, observation_id: UUID, records: dict[UUID, Record] | None = None
    ) -> tuple[UUID, ...]:
        records = self._records if records is None else records
        ids, seen = [], set()
        while observation_id in records:
            if observation_id in seen:
                raise ContractError("revision_cycle")
            seen.add(observation_id)
            ids.append(observation_id)
            previous = records[observation_id].observation.revises
            if previous is None:
                break
            observation_id = previous
        return tuple(reversed(ids))

    def _pins(self) -> set[UUID]:
        pins = {s.observation_id for s in self._snapshots.values()}
        pins.update(
            r.id
            for r in self._records.values()
            if r.consumption != "acknowledged"
            or r.resolution in ("unassessed", "unresolved")
        )
        if self._active_batch:
            pins.update(r.id for r in self._active_batch.records)
        for operation in self._operations.values():
            if not operation.terminal or any(
                r.operation_id == operation.id and r.id not in self._reviewed
                for r in self._results.values()
            ):
                pins.update(self._contexts.get(operation.id, ()))
        for result in self._results.values():
            if result.id not in self._reviewed:
                pins.update(result.target_ids)
                pins.update(result.evidence_ids)
        for observation_id in tuple(pins):
            pins.update(self._ancestry(observation_id))
        return pins

    def _fit_records(
        self, candidate: OrderedDict[UUID, Record], pins: set[UUID]
    ) -> OrderedDict[UUID, Record]:
        sizes = {i: len(canonical(public(r))) for i, r in candidate.items()}
        total = sum(sizes.values())
        for i, record in tuple(candidate.items()):
            if (
                len(candidate) <= self.config.state_records
                and total <= self.config.state_bytes
            ):
                return candidate
            if (
                i not in pins
                and record.consumption == "acknowledged"
                and record.resolution in ("no_action_required", "resolved")
            ):
                del candidate[i]
                total -= sizes[i]
        if (
            len(candidate) > self.config.state_records
            or total > self.config.state_bytes
        ):
            raise ContractError("state_capacity_exceeded")
        return candidate

    def _cleanup_indexes(self) -> None:
        retained = set(self._records)
        self._roots = {i: root for i, root in self._roots.items() if i in retained}
        self._depth = {i: depth for i, depth in self._depth.items() if i in retained}
        self._leaves = {
            root: leaf for root, leaf in self._leaves.items() if leaf in retained
        }
        self._initiators.intersection_update(retained)

    async def _emit(self, observations: tuple[Observation, ...]) -> tuple[UUID, ...]:
        if not observations:
            return ()
        observations = tuple(
            Observation.model_validate(o.model_dump()) for o in observations
        )
        async with self.lock:
            records = self._records.copy()
            roots, leaves, depth = (
                self._roots.copy(),
                self._leaves.copy(),
                self._depth.copy(),
            )
            sequences, snapshots = self._subject_seq.copy(), self._snapshots.copy()
            snapshot_versions = self._snapshot_versions.copy()
            version, new_ids = self.state_version + 1, []
            for obs in observations:
                if obs.id in records:
                    if records[obs.id].observation != obs:
                        raise ContractError("conflicting_observation_id")
                    continue
                key = (obs.reducer_id, obs.subject_id)
                if (
                    key not in sequences
                    and sum(k[0] == obs.reducer_id for k in sequences)
                    >= self.config.max_subjects
                ):
                    raise ContractError("subject_capacity_exceeded")
                if obs.subject_seq <= sequences.get(key, 0):
                    raise ContractError("stale_subject_sequence")
                root = obs.id
                inherited_kind = obs.kind
                if obs.revises is not None:
                    target = records.get(obs.revises)
                    if target is None:
                        raise ContractError("missing_revision_target")
                    previous = target.observation
                    if (previous.reducer_id, previous.subject_id) != key:
                        raise ContractError("cross_area_revision")
                    root = roots[obs.revises]
                    if (
                        leaves.get(root) != obs.revises
                        or previous.finality == "retracted"
                    ):
                        raise ContractError("stale_revision_target")
                    if depth[obs.revises] >= 8:
                        raise ContractError("revision_depth_exceeded")
                    inherited_kind = records[root].observation.kind
                    depth[obs.id] = depth[obs.revises] + 1
                else:
                    depth[obs.id] = 0
                roots[obs.id], leaves[root], sequences[key] = (
                    root,
                    obs.id,
                    obs.subject_seq,
                )
                records[obs.id] = Record(observation=obs, accepted_version=version)
                new_ids.append(obs.id)
                if inherited_kind == "snapshot":
                    old = snapshots.get(key)
                    same_lineage = (
                        old is not None and roots.get(old.observation_id) == root
                    )
                    if obs.kind == "snapshot" or same_lineage:
                        snapshot_versions[key] = snapshot_versions.get(key, 0) + 1
                        if obs.finality == "retracted":
                            snapshots.pop(key, None)
                        else:
                            snapshots[key] = Snapshot(
                                observation_id=obs.id,
                                version=snapshot_versions[key],
                                last_seen_at=obs.produced_at,
                            )
            if not new_ids:
                return tuple(o.id for o in observations)
            pins = (
                self._pins()
                | set(new_ids)
                | {s.observation_id for s in snapshots.values()}
            )
            for i in tuple(pins):
                pins.update(self._ancestry(i, records))
            try:
                records = self._fit_records(records, pins)
            except ContractError:
                self.diagnostics["last_error_code"] = "state_capacity_exceeded"
                raise
            self._records, self._roots, self._leaves, self._depth = (
                records,
                roots,
                leaves,
                depth,
            )
            self._subject_seq, self._snapshots = sequences, snapshots
            self._snapshot_versions = snapshot_versions
            self._cleanup_indexes()
            self._changed(semantic=True)
            return tuple(o.id for o in observations)

    async def _activity(self, value: Activity) -> None:
        async with self.lock:
            old = self._activity_data[value.reducer_id]
            if value == old:
                return
            self._activity_data[value.reducer_id] = value
            self._changed(eligible=old.status != value.status)

    async def _refresh(
        self,
        reducer_id: str,
        subject_id: str,
        source_seq: int | None,
        received_at: datetime,
    ) -> None:
        async with self.lock:
            key = reducer_id, subject_id
            snapshot = self._snapshots.get(key)
            if snapshot is None:
                return
            if (
                source_seq is not None
                and snapshot.last_seen_seq is not None
                and source_seq <= snapshot.last_seen_seq
            ):
                return
            if received_at < snapshot.last_seen_at:
                return
            value = snapshot.model_copy(
                update={"last_seen_seq": source_seq, "last_seen_at": received_at}
            )
            if value != snapshot:
                self._snapshots[key] = value
                self._changed()

    def _fingerprint(self) -> tuple:
        return (
            self.semantic_generation,
            tuple(self._activity_data[r].status for r in REDUCER_IDS),
            tuple(i for i in self._results if i not in self._reviewed),
            self._recheck,
        )

    async def fingerprint(self) -> tuple:
        async with self.lock:
            return self._fingerprint()

    async def consume_wake(self) -> tuple[tuple, bool]:
        async with self.lock:
            self.wake.clear()
            page, self._page_pending = self._page_pending, False
            return self._fingerprint(), page

    async def recheck(self) -> None:
        async with self.lock:
            self._recheck += 1
            self.wake.set()

    async def create_batch(
        self, request_size: Callable[[Batch], int] | None = None
    ) -> Batch:
        async with self.lock:
            if self._active_batch is not None:
                raise ContractError("batch_already_active")
            batch_id, now = uuid4(), utcnow()
            selected: OrderedDict[UUID, Record] = OrderedDict()
            new_ids, unresolved_ids, result_ids = [], [], []
            snapshot_ids = []
            unreviewed = [
                r for i, r in self._results.items() if i not in self._reviewed
            ]
            unacknowledged = [
                r for r in self._records.values() if r.consumption != "acknowledged"
            ]
            unresolved = [
                r for r in self._records.values() if r.resolution == "unresolved"
            ]

            def presented(record: Record) -> Record:
                if record.consumption == "acknowledged":
                    return record
                return record.model_copy(
                    update={
                        "consumption": "presented",
                        "first_batch_id": record.first_batch_id or batch_id,
                        "last_batch_id": batch_id,
                        "presentation_count": record.presentation_count + 1,
                    }
                )

            def candidate() -> Batch:
                return Batch(
                    id=batch_id,
                    run_id=self.run_id,
                    state_version=self.state_version + 1,
                    input_generation=self.input_generation,
                    created_at=now,
                    records=tuple(presented(r) for r in selected.values()),
                    new_ids=tuple(new_ids),
                    unresolved_ids=tuple(unresolved_ids),
                    snapshots=tuple(
                        s
                        for s in self._snapshots.values()
                        if s.observation_id in selected
                    ),
                    activity=tuple(self._activity_data[r] for r in REDUCER_IDS),
                    operations=tuple(
                        o for o in self._operations.values() if not o.terminal
                    ),
                    results=tuple(self._results[i] for i in result_ids),
                    attempted_request_ids=tuple(
                        i for i in selected if i in self._initiators
                    ),
                    deferred_json=canonical(
                        {
                            "new": len(unacknowledged) - len(new_ids),
                            "unresolved": len(unresolved) - len(unresolved_ids),
                            "results": len(unreviewed) - len(result_ids),
                        }
                    ),
                )

            def fits() -> bool:
                batch = candidate()
                budget = (
                    request_size(batch)
                    if request_size
                    else len(canonical(public(batch))) + 5000
                )
                return len(selected) <= 32 and budget <= self.config.batch_bytes

            def add_group(
                ids: tuple[UUID, ...], category: str, result_id: UUID | None = None
            ) -> bool:
                original = set(selected)
                for i in ids:
                    for ancestor in self._ancestry(i):
                        selected.setdefault(ancestor, self._records[ancestor])
                category_ids = (
                    new_ids
                    if category == "new"
                    else unresolved_ids
                    if category == "unresolved"
                    else snapshot_ids
                )
                extra_ids = [i for i in ids if i not in category_ids]
                category_ids.extend(extra_ids)
                if result_id is not None:
                    result_ids.append(result_id)
                if fits():
                    return True
                for i in tuple(selected):
                    if i not in original:
                        del selected[i]
                for i in extra_ids:
                    category_ids.remove(i)
                if result_id is not None:
                    result_ids.remove(result_id)
                return False

            if not fits():
                raise ContractError("context_capacity_exceeded")
            for result in unreviewed[:4]:
                if not add_group(
                    result.target_ids + result.evidence_ids, "context", result.id
                ):
                    if not selected:
                        raise ContractError("context_capacity_exceeded")
                    break
            for record in unacknowledged:
                if len(new_ids) >= 16:
                    break
                if not add_group((record.id,), "new"):
                    if not selected:
                        raise ContractError("context_capacity_exceeded")
                    break
            if unresolved:
                start = self._unresolved_cursor % len(unresolved)
                rotated = unresolved[start:] + unresolved[:start]
                for record in rotated[:8]:
                    if not add_group((record.id,), "unresolved"):
                        break
                self._unresolved_cursor = (start + max(1, len(unresolved_ids))) % len(
                    unresolved
                )
            subjects = {
                (r.observation.reducer_id, r.observation.subject_id)
                for r in selected.values()
            }
            services = {
                r.observation.facts.get("service") for r in selected.values()
            } - {None}
            for key, snapshot in self._snapshots.items():
                if len(snapshot_ids) >= 8:
                    break
                record = self._records[snapshot.observation_id]
                if (
                    key in subjects
                    or record.observation.facts.get("service") in services
                ):
                    if snapshot.observation_id not in selected:
                        add_group((snapshot.observation_id,), "snapshot")
            batch = candidate()
            records = self._records.copy()
            for record in batch.records:
                records[record.id] = record
            # Presentation metadata is also retained state; never exceed the cap.
            records = self._fit_records(records, self._pins() | set(selected))
            self._records = records
            self._cleanup_indexes()
            self._active_batch = batch
            self._changed()
            return batch

    def _applicable(self, observation_id: UUID) -> bool:
        record = self._records.get(observation_id)
        if record is None:
            return False
        obs = record.observation
        root = self._roots[observation_id]
        if self._leaves.get(root) != observation_id or obs.finality == "retracted":
            return False
        root_obs = self._records[root].observation
        if root_obs.kind == "snapshot":
            return (
                self._snapshots.get((obs.reducer_id, obs.subject_id), None) is not None
                and self._snapshots[obs.reducer_id, obs.subject_id].observation_id
                == observation_id
            )
        if root_obs.code == "conversation.message":
            snapshot = self._snapshots.get((obs.reducer_id, obs.subject_id))
            if snapshot is not None:
                current = self._records[snapshot.observation_id].observation
                if current.facts.get("message_id") != obs.facts.get("message_id"):
                    return False
        return True

    def _withdrawn_by(self, observation_id: UUID, basis_ids: tuple[UUID, ...]) -> bool:
        root = self._roots[observation_id]
        for i in basis_ids:
            obs = self._records[i].observation
            if (
                self._roots[i] == root
                and self._leaves.get(root) == i
                and obs.kind == "revision"
                and (obs.finality == "retracted" or obs.facts.get("withdrawn") is True)
            ):
                return True
        return False

    def _validate_resolution(
        self, record: Record, disposition, batch_results: dict
    ) -> None:
        if disposition.resolution is None:
            return
        if (
            record.resolution != "unassessed"
            and record.resolution != disposition.resolution
        ):
            if not disposition.reason or not (
                disposition.basis_observation_ids or disposition.basis_result_ids
            ):
                raise ContractError("resolution_change_requires_basis")
        if disposition.resolution != "resolved":
            return
        obs = record.observation
        if self._withdrawn_by(record.id, disposition.basis_observation_ids):
            return
        root_obs = self._records[self._roots[record.id]].observation
        if root_obs.code == "conversation.message":
            if not any(
                batch_results[i].status == "success"
                and batch_results[i].applicable
                and record.id in batch_results[i].target_ids
                for i in disposition.basis_result_ids
            ):
                raise ContractError("request_requires_successful_result")
        if obs.reducer_id == "telemetry" and obs.facts.get("status") in (
            "degraded",
            "down",
        ):
            healthy = False
            for i in disposition.basis_observation_ids:
                basis = self._records[i].observation
                root = self._records[self._roots[i]].observation
                healthy |= (
                    basis.reducer_id == obs.reducer_id
                    and basis.subject_id == obs.subject_id
                    and basis.subject_seq > obs.subject_seq
                    and basis.facts.get("status") == "ok"
                    and root.kind == "snapshot"
                    and root.finality == "committed"
                    and self._applicable(i)
                )
            if not healthy:
                raise ContractError("health_requires_recovery_evidence")

    def _prune_completed(self) -> None:
        for i, result in tuple(self._results.items()):
            if (
                len(self._results) < self.config.result_capacity
                and len(self._operations) < self.config.operation_capacity
            ):
                break
            if i in self._reviewed:
                del self._results[i]
                self._reviewed.remove(i)
                op = self._operations.get(result.operation_id)
                if (
                    op is not None
                    and op.terminal
                    and not any(r.operation_id == op.id for r in self._results.values())
                ):
                    self._operations.pop(op.id, None)
                    self._contexts.pop(op.id, None)

    async def apply(
        self, batch: Batch, decision: GateDecision
    ) -> ActiveOperation | None:
        decision = GateDecision.model_validate(decision.model_dump())
        async with self.lock:
            digest = canonical(public(decision))
            if batch.id in self._applied:
                if self._applied[batch.id] == digest:
                    return None
                raise ContractError("batch_already_applied")
            if self._active_batch != batch or decision.batch_id != batch.id:
                raise ContractError("invalid_active_batch")
            captured = {r.id: r for r in batch.records}
            captured_results = {r.id: r for r in batch.results}
            if len(set(decision.reviewed_result_ids)) != len(
                decision.reviewed_result_ids
            ) or any(i not in captured_results for i in decision.reviewed_result_ids):
                raise ContractError("out_of_batch_result")
            updated = self._records.copy()
            progress = False
            for disposition in decision.dispositions:
                i = disposition.observation_id
                if (
                    i not in captured
                    or any(j not in captured for j in disposition.basis_observation_ids)
                    or any(
                        j not in captured_results for j in disposition.basis_result_ids
                    )
                ):
                    raise ContractError("out_of_batch_reference")
                if i not in updated:
                    raise ContractError("missing_disposition_target")
                record = updated[i]
                self._validate_resolution(record, disposition, captured_results)
                progress |= (
                    disposition.acknowledge and record.consumption != "acknowledged"
                )
                # A bounded follow-on page can carry outstanding records omitted
                # from a crowded evidence batch. It must release real work, not
                # merely restate an unresolved assessment.
                progress |= (
                    record.resolution == "unresolved"
                    and disposition.resolution in ("no_action_required", "resolved")
                )
                updated[i] = record.model_copy(
                    update={
                        "consumption": "acknowledged"
                        if disposition.acknowledge
                        else record.consumption,
                        "resolution": disposition.resolution or record.resolution,
                        "reason": disposition.reason or record.reason,
                    }
                )
            operation = None
            if decision.operation is not None:
                op = decision.operation
                if any(i not in captured for i in op.observation_ids):
                    raise ContractError("out_of_batch_operation")
                if any(not self._applicable(i) for i in op.observation_ids):
                    raise ContractError("stale_operation_target")
                initiators = [
                    i
                    for i in op.observation_ids
                    if self._records[i].observation.code == "conversation.message"
                    and self._records[i].observation.kind == "event"
                    and self._records[i].observation.finality == "committed"
                ]
                if len(initiators) != 1:
                    raise ContractError("invalid_operation_initiator")
                initiator = initiators[0]
                if initiator in self._initiators:
                    raise ContractError("initiator_already_attempted")
                if any(
                    updated[i].resolution != "unresolved" for i in op.observation_ids
                ):
                    raise ContractError("operation_targets_must_be_unresolved")
                if any(
                    set(op.observation_ids) & set(o.target_ids)
                    for o in self._operations.values()
                    if not o.terminal
                ):
                    raise ContractError("overlapping_operation")
                if decision.action == "escalate" and any(
                    o.action == "escalate" and not o.terminal
                    for o in self._operations.values()
                ):
                    raise ContractError("system2_busy")
                # Pruning happens only after the entire decision has validated.
                reviewed = self._reviewed | set(decision.reviewed_result_ids)
                retained_results = [
                    r for r in self._results.values() if r.id not in reviewed
                ]
                retained_operations = [
                    o
                    for o in self._operations.values()
                    if not o.terminal
                    or any(r.operation_id == o.id for r in retained_results)
                ]
                if len(retained_operations) >= self.config.operation_capacity:
                    raise ContractError("operation_capacity_exceeded")
                active_count = sum(not o.terminal for o in retained_operations)
                if len(retained_results) + active_count >= self.config.result_capacity:
                    raise ContractError("result_capacity_exceeded")
                operation = ActiveOperation(
                    batch_id=batch.id,
                    action=decision.action,
                    target_ids=op.observation_ids,
                    initiator_id=initiator,
                    thinking_level=op.thinking_level,
                    context_version=batch.state_version,
                )
            progress |= any(
                i not in self._reviewed for i in decision.reviewed_result_ids
            )
            updated = self._fit_records(updated, self._pins())
            self._records = updated
            self._cleanup_indexes()
            self._reviewed.update(decision.reviewed_result_ids)
            self._prune_completed()
            if operation:
                self._operations[operation.id] = operation
                self._contexts[operation.id] = frozenset(captured)
                self._initiators.add(operation.initiator_id)
            self._applied[batch.id] = digest
            while len(self._applied) > 32:
                self._applied.popitem(last=False)
            self._changed()
            if progress and any(
                batch.deferred.get(category, 0)
                for category in ("new", "results", "unresolved")
            ):
                self._page_pending = True
                self.wake.set()
            return operation

    async def finish_batch(self, batch: Batch) -> None:
        async with self.lock:
            if self._active_batch and self._active_batch.id == batch.id:
                self._recent_batches.append(batch.id)
                self._active_batch = None

    async def add_result(self, result: OperationResult) -> bool:
        result = OperationResult.model_validate(result.model_dump())
        async with self.lock:
            operation = self._operations.get(result.operation_id)
            if operation is None:
                raise ContractError("unknown_result_operation")
            if operation.terminal:
                if result.id in self._results:
                    # Applicability is supplied by the runtime, not the caller.
                    if (
                        self._results[result.id].model_copy(
                            update={"applicable": result.applicable}
                        )
                        != result
                    ):
                        raise ContractError("conflicting_result_id")
                    return False
                self.diagnostics["late_results"] = (
                    int(self.diagnostics.get("late_results", 0)) + 1
                )
                return False
            if result.id in self._results:
                raise ContractError("conflicting_result_id")
            if (
                result.batch_id != operation.batch_id
                or result.target_ids != operation.target_ids
                or result.context_version != operation.context_version
                or result.type
                != ("system2" if operation.action == "escalate" else "tool")
                or any(
                    i not in self._contexts[operation.id] for i in result.evidence_ids
                )
            ):
                raise ContractError("invalid_result_correlation")
            self._prune_completed()
            if len(self._results) >= self.config.result_capacity:
                raise ContractError("result_capacity_exceeded")
            value = result.model_copy(
                update={
                    "applicable": all(self._applicable(i) for i in result.target_ids)
                }
            )
            self._results[value.id] = value
            self._operations[operation.id] = operation.model_copy(
                update={"terminal": True}
            )
            self._changed(eligible=True)
            return True

    async def inspect(self) -> dict:
        async with self.lock:
            return public(
                {
                    "run_id": self.run_id,
                    "state_version": self.state_version,
                    "input_generation": self.input_generation,
                    "semantic_generation": self.semantic_generation,
                    "records": tuple(self._records.values()),
                    "snapshots": tuple(self._snapshots.values()),
                    "activity": tuple(self._activity_data[r] for r in REDUCER_IDS),
                    "operations": tuple(self._operations.values()),
                    "results": tuple(self._results.values()),
                    "reviewed_result_ids": tuple(self._reviewed),
                    "diagnostics": self.diagnostics.copy(),
                    "evidence_storage": "provenance_only",
                }
            )
