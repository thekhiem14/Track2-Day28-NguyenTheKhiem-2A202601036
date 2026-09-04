"""Four student-owned boundaries used by the live platform.

Run ``uv run pytest starter-tests -q`` while completing these functions.  Do
not change their signatures: Kafka, Delta, Feast and ``/ready`` call them.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from lab28_platform.contracts import FEATURE_REFS, IngestionEvent

#: Header names carried on every ``data.raw`` message. Kafka headers are bytes
#: on the wire, so both values are encoded before they leave this module.
TRACEPARENT_HEADER = "traceparent"
IDEMPOTENCY_HEADER = "idempotency-key"

#: Readiness verdicts, ordered from healthiest to worst. ``/ready`` and the
#: gateway branch on these exact strings.
STATUS_READY = "ready"
STATUS_DEGRADED = "degraded"
STATUS_NOT_READY = "not_ready"


def event_headers(
    traceparent: str | None, idempotency_key: str
) -> list[tuple[str, bytes]]:
    """Return byte-valued Kafka headers for trace and replay correlation.

    ``idempotency-key`` is always required.  Omit ``traceparent`` when no trace
    is active rather than sending an empty, invalid W3C header.
    """
    headers: list[tuple[str, bytes]] = []
    # An absent trace is a valid state (CLI producers, replays); an empty
    # traceparent is not — a consumer parsing "" gets a broken context instead
    # of a clean "start a new trace here" signal.
    if traceparent:
        headers.append((TRACEPARENT_HEADER, traceparent.encode("utf-8")))
    headers.append((IDEMPOTENCY_HEADER, idempotency_key.encode("utf-8")))
    return headers


def dedupe_latest(events: Iterable[IngestionEvent]) -> list[IngestionEvent]:
    """Return one newest event per idempotency key, in deterministic key order.

    Compare ``(occurred_at, event_id)`` so ties do not depend on Kafka delivery
    order.  The Spark Delta MERGE calls this through ``delta_store``.
    """
    latest: dict[str, IngestionEvent] = {}
    # One pass: the argument may be a generator draining a Kafka batch, so it
    # can only be consumed once.
    for event in events:
        winner = latest.get(event.idempotency_key)
        if winner is None or _merge_rank(event) > _merge_rank(winner):
            latest[event.idempotency_key] = event
    # Sorting by key makes the MERGE source byte-identical across replays of
    # the same batch, whatever order the partitions were polled in.
    return [latest[key] for key in sorted(latest)]


def _merge_rank(event: IngestionEvent) -> tuple[Any, str]:
    """Total order used to pick the surviving event for one merge key."""
    return (event.occurred_at, event.event_id)


def feast_online_request(asker_id: str) -> dict[str, Any]:
    """Build the Feast ``/get-online-features`` request for ``asker_activity_v1``."""
    return {
        # The feature refs live in contracts.py so the registry, the response
        # parser and this request cannot drift apart.
        "features": list(FEATURE_REFS),
        "entities": {"asker_id": [asker_id]},
        # Short names keep the response keys equal to the AskerFeatures fields.
        "full_feature_names": False,
    }


def readiness_status(probes: Iterable[dict[str, Any]]) -> str:
    """Return ``ready``, ``degraded`` or ``not_ready`` from probe severity."""
    # The caller passes a generator; materialise it before two passes.
    results = list(probes)
    # Unknown severity is treated as mandatory and unknown health as failed:
    # a readiness endpoint must fail closed, never optimistically.
    failed = [probe for probe in results if not probe.get("ready", False)]
    if any(probe.get("mandatory", True) for probe in failed):
        return STATUS_NOT_READY
    if failed:
        return STATUS_DEGRADED
    return STATUS_READY
