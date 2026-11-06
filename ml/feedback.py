"""Pure reconstruction of server feedback facts; no serving weights or DB I/O.

Unknown state stays None. Outcomes are independent objectives, never a summed
"approval" label. Only a qualified impression can anchor a training observation.
The dataset adapter must additionally validate current content availability and
access: history is neither an authorization source nor proof a deleted target
still exists. Account erasure/retention may remove its lifecycle history too.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping


KINDS = frozenset({"like", "less_similar", "follow", "hide_author", "comment",
                   "comment_like", "repost", "quote", "report", "reset", "content",
                   "impression", "dwell", "open_post"})
POLICY = "feedback-outcomes-v1"


def instant(value: str | datetime) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if not isinstance(result, datetime) or result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("server timestamps must include a timezone")
    return result


@dataclass(frozen=True)
class Fact:
    sequence: int
    event_id: str
    recorded_at: datetime
    user_id: str
    epoch: int
    kind: str
    target_type: str
    target_id: str
    post_id: str | None = None
    post_version: int | None = None
    author_id: str | None = None
    target_version: int | None = None
    state_version: int | None = None
    active: bool | None = None
    own_content: bool = False
    delivery_id: str | None = None
    active_ms: int | None = None
    opened: bool | None = None
    surface: str = "unattributed"

    @classmethod
    def read(cls, row: Mapping[str, Any]) -> "Fact":
        if (type(row.get("contract_version")) is not int or row["contract_version"] != 1
                or not isinstance(row.get("kind"), str) or row["kind"] not in KINDS):
            raise ValueError("unsupported feedback contract")
        values = {name: row[name] for name in cls.__dataclass_fields__ if name in row}
        values["recorded_at"] = instant(row["recorded_at"])
        fact = cls(**values)
        if type(fact.sequence) is not int or fact.sequence < 1 or type(fact.epoch) is not int or fact.epoch < 1:
            raise ValueError("invalid server sequence/epoch")
        if (any(not isinstance(value, str) or not value for value in (fact.event_id, fact.user_id, fact.target_id))
                or not isinstance(fact.target_type, str)
                or fact.target_type not in {"post", "comment", "repost", "author", "user"}):
            raise ValueError("invalid feedback identity")
        if any(value is not None and (not isinstance(value, str) or not value)
               for value in (fact.post_id, fact.author_id, fact.delivery_id)):
            raise ValueError("invalid feedback context identity")
        if (fact.post_id is None) != (fact.post_version is None):
            raise ValueError("incomplete content version")
        for value in (fact.target_version, fact.post_version, fact.state_version):
            if value is not None and (type(value) is not int or value < 1):
                raise ValueError("invalid feedback version")
        for value in (fact.active, fact.opened, fact.own_content):
            if value is not None and type(value) is not bool:
                raise ValueError("invalid boolean state")
        if (type(fact.own_content) is not bool or not isinstance(fact.surface, str)
                or fact.surface not in {"unattributed", "for_you", "friends", "nearby", "direct"}):
            raise ValueError("invalid feedback context")
        if fact.active_ms is not None and (type(fact.active_ms) is not int or not 0 <= fact.active_ms <= 120000):
            raise ValueError("invalid cumulative attention")
        return fact


def ordered_facts(rows: Iterable[Mapping[str, Any]], as_of: datetime) -> list[Fact]:
    """Deduplicate an export/retry, rejecting conflicting copies instead of guessing.

    Sequence orders serialized server transitions; stale commands are rejected
    by RPC version checks before they reach this journal. A state's version may
    restart after physical removal, so taking
    a global max(version) would incorrectly discard a later accepted transition.
    A caller's client timestamp is never consulted.
    """
    limit = instant(as_of)
    by_sequence: dict[int, Fact] = {}
    by_id: dict[tuple[str, str], int] = {}
    for row in rows:
        fact = Fact.read(row)
        if fact.recorded_at > limit:
            continue
        key = (fact.user_id, fact.event_id)
        if fact.sequence in by_sequence and by_sequence[fact.sequence] != fact:
            raise ValueError("conflicting feedback sequence")
        if key in by_id and by_id[key] != fact.sequence:
            raise ValueError("conflicting feedback event")
        by_sequence[fact.sequence] = fact
        by_id[key] = fact.sequence
    return sorted(by_sequence.values(), key=lambda fact: fact.sequence)


def _last(facts: Iterable[Fact]) -> Fact | None:
    current = None
    for fact in facts:
        current = fact
    return current


def _by_target(facts: Iterable[Fact]) -> dict[str, Fact]:
    states: dict[str, Fact] = {}
    for fact in facts:
        states[fact.target_id] = fact
    return states


def _state(facts: Iterable[Fact]) -> bool | None:
    fact = _last(facts)
    return fact.active if fact is not None else None


def derive_outcomes(rows: Iterable[Mapping[str, Any]], *, exposure_event: str,
                    window_end: datetime, as_of: datetime, history_start: datetime) -> dict[str, Any]:
    """Reconstruct one matured, confirmed exposure from an explicit history prefix.

    Missing state is unknown, not dislike. Pre-existing likes/follows are state
    outcomes, not proof the exposure caused an action. Groups/splits/utility are
    intentionally left to the dataset layer (stage 6.2).
    """
    end, available, start = instant(window_end), instant(as_of), instant(history_start)
    if end > available:
        raise ValueError("label window has not matured")
    facts = [f for f in ordered_facts(rows, end) if f.recorded_at >= start]
    matches = [f for f in facts if f.event_id == exposure_event and f.kind == "impression"]
    if len(matches) != 1:
        raise ValueError("one confirmed exposure is required")
    exposure = matches[0]
    if exposure.delivery_id is None or exposure.active is not True or exposure.post_id is None or exposure.surface == "unattributed":
        raise ValueError("unqualified exposure")
    if not start <= exposure.recorded_at <= end:
        raise ValueError("exposure outside available history/window")
    actor = [f for f in facts if f.user_id == exposure.user_id]
    reasons = []
    if exposure.own_content:
        reasons.append("own_content")
    # Only the first impression for this user/version/epoch/UTC day anchors an
    # observation; repeated deliveries/telemetry cannot multiply training rows.
    day = exposure.recorded_at.astimezone(timezone.utc).date()
    prior_views = [f for f in actor if f.kind == "impression" and f.active is True
                   and f.post_id == exposure.post_id and f.post_version == exposure.post_version
                   and f.epoch == exposure.epoch and f.recorded_at.astimezone(timezone.utc).date() == day]
    if prior_views and prior_views[0].sequence != exposure.sequence:
        reasons.append("repeat_exposure")
    if any(f.kind == "reset" and f.epoch > exposure.epoch for f in actor):
        reasons.append("interests_reset")
    content = _last(f for f in facts if f.kind == "content" and f.post_id == exposure.post_id)
    if content is not None and (not content.active or content.post_version != exposure.post_version):
        reasons.append("content_changed")
    relevant = [f for f in actor if f.epoch == exposure.epoch and not f.own_content
                and f.post_id == exposure.post_id and f.post_version == exposure.post_version]
    qualified_receipts = {f.delivery_id for f in relevant if f.kind == "impression" and f.active is True
                          and f.sequence >= exposure.sequence
                          and f.recorded_at.astimezone(timezone.utc).date() == day}
    attention = [f for f in relevant if f.delivery_id in qualified_receipts
                 and f.sequence >= exposure.sequence and f.kind in {"impression", "dwell", "open_post"}]
    outcomes: dict[str, bool | int | None] = {
        "liked": _state(f for f in relevant if f.kind == "like"),
        "less_similar": _state(f for f in relevant if f.kind == "less_similar"),
        "active_ms": max((f.active_ms or 0 for f in attention), default=0),
        "opened": any(f.opened is True for f in attention),
    }
    comments = _by_target(f for f in facts if f.kind == "comment")
    for kind, name in [("comment", "commented"), ("comment_like", "comment_liked"),
                       ("repost", "reposted"), ("quote", "quoted")]:
        entities = _by_target(f for f in relevant if f.kind == kind)
        states: list[bool | None] = []
        for entity, event in entities.items():
            if kind == "comment_like":
                current = comments.get(entity)
                if current is not None and (not current.active or current.target_version != event.target_version):
                    states.append(None)
                    continue
            states.append(event.active)
        outcomes[name] = True if True in states else (None if not states or None in states else False)
    outcomes["post_reported"] = _state(f for f in relevant if f.kind == "report" and f.target_type == "post")
    outcomes["comment_reported"] = _state(f for f in relevant if f.kind == "report" and f.target_type == "comment")
    for kind, name in [("follow", "follows_author"), ("hide_author", "author_hidden")]:
        # Explicit relationships survive a reset; they are not post approval.
        outcomes[name] = _state(f for f in actor if f.kind == kind and f.target_id == exposure.author_id)
    if reasons:
        outcomes = dict.fromkeys(outcomes)
    return {"contract_version": 1, "label_policy": POLICY, "eligible": not reasons,
            "excluded": reasons, "exposure_time": exposure.recorded_at.isoformat(),
            "label_window_end": end.isoformat(), "outcomes": outcomes,
            "known": {key: value is not None for key, value in outcomes.items()}}
