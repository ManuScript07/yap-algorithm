"""Build a time-safe experimental dataset from immutable Supabase captures."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import tempfile
from collections import Counter, defaultdict
from datetime import timedelta
from pathlib import Path

from feedback import Fact, POLICY, derive_outcomes, instant

SPLITS = ("train", "validation", "test")
HEX = re.compile(r"[0-9a-f]{64}")
PHOTO_HASH = re.compile(r"[0-9a-f]{16}")
FEATURE_WIDTHS = {"feed-features-v2": 8}
MAX_CAPTURE_BYTES = 256 * 1024 * 1024


def canonical(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                       allow_nan=False) + "\n").encode("utf-8")


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def read_capture(path):
    raw_manifest = (path / "manifest.json").read_bytes()
    meta = json.loads(raw_manifest)
    db = meta["database"]
    if (meta["schema"] != "yap-feedback-capture-v1" or db["schema_version"] != 1
            or db["feedback_contract"] != 1 or db["origin"] != "synthetic"
            or not all(HEX.fullmatch(value) for value in
                       (db["source_hash"], db["member_sha256"], db["sql_sha256"], meta["binary_sha256"]))):
        raise ValueError("unsupported capture provenance")
    completed = instant(meta["completed_at"])
    started = instant(db["snapshot_started_at"])
    floor = instant(db["history_start"])
    if db["unmatched_writer_count"] or db["settled_through"] is None:
        raise ValueError("capture does not prove a settled label window; retry capture")
    settled = instant(db["settled_through"])
    if not settled <= started <= completed or floor > started:
        raise ValueError("invalid capture time bounds")
    rows = []
    seen = set()
    h = hashlib.sha256()
    size = 0
    seq = 0
    with (path / "facts.jsonl").open("rb") as stream:
        while line := stream.readline(256 * 1024 + 2):
            size += len(line)
            if size > MAX_CAPTURE_BYTES or len(rows) >= 250000 or len(line) > 256 * 1024 + 1:
                raise ValueError("capture exceeds replay limits")
            h.update(line)
            row = json.loads(line)
            fact = Fact.read(row)
            if (fact.sequence <= seq or (fact.user_id, fact.event_id) in seen
                    or not floor <= fact.recorded_at <= completed):
                raise ValueError("invalid capture sequence, identity or time")
            seen.add((fact.user_id, fact.event_id))
            seq = fact.sequence
            rows.append(row)
    if len(rows) != meta["rows"] or seq > meta["last_sequence"] or h.hexdigest() != meta["facts_sha256"]:
        raise ValueError("capture integrity check failed")
    return meta, digest(raw_manifest), rows, floor, settled, completed


def versions(row):
    ctx = row["context"]
    fields = ("model", "feature_version", "policy_version", "embedding_space", "embedding_model")
    signature = tuple(ctx.get(key) for key in fields)
    if (not all(isinstance(value, str) and value and value != "none" for value in signature)
            or signature[3] != signature[4]
            or not isinstance(ctx.get("snapshot_id"), str) or not ctx["snapshot_id"]
            or type(ctx.get("position")) is not int or ctx["position"] < 0):
        return None
    values = ctx.get("features")
    if (not isinstance(values, list) or len(values) != FEATURE_WIDTHS.get(signature[1])
            or any(type(value) not in (int, float) or not math.isfinite(value) for value in values)):
        return None
    if "features_at" not in ctx or instant(ctx["features_at"]) > instant(row["recorded_at"]):
        return None
    return signature


def index_facts(rows):
    by_actor_post, by_post, by_author, resets, groups = [defaultdict(list) for _ in range(5)]
    for row in rows:
        actor, post, kind = row["user_id"], row["post_id"], row["kind"]
        if post is not None:
            by_actor_post[actor, post].append(row)
            if kind in ("content", "comment"):
                by_post[post].append(row)
        if kind in ("follow", "hide_author"):
            by_author[actor, row["target_id"]].append(row)
        if kind == "reset":
            resets[actor].append(row)
        if kind == "impression" and row["surface"] == "for_you":
            group = actor, row["transaction_id"], row["epoch"], row["context"].get("snapshot_id")
            groups[group].append(row)
    return by_actor_post, by_post, by_author, resets, groups


def extract(rows, floor, settled, completed, start, end, horizon, signature, photos, exclusions):
    by_actor_post, by_post, by_author, resets, groups = index_facts(rows)
    for key, batch in sorted(groups.items(), key=lambda pair: pair[1][0]["sequence"]):
        times = [instant(row["recorded_at"]) for row in batch]
        if not any(start <= moment < end for moment in times):
            continue
        window_end = max(times) + horizon
        if min(times) < start or window_end > min(end, settled):
            exclusions["unsettled_or_cross_boundary_group"] += len(batch)
            continue
        positions = [row["context"].get("position") for row in batch]
        if any(type(position) is not int for position in positions) or len(set(positions)) != len(positions):
            exclusions["unknown_or_repeated_position"] += len(batch)
            continue
        if any(versions(row) != signature for row in batch):
            exclusions["incompatible_producing_snapshot"] += len(batch)
            continue
        if any(checksum not in photos for row in batch if row["content"] is not None
               for fp in row["content"]["fingerprints"] for checksum in fp["photos"]):
            exclusions["missing_photo_fingerprint_group"] += len(batch)
            continue
        group_id = digest(canonical([*key, batch[0]["surface"]]))
        output = []
        for row in batch:
            content = row["content"]
            if content is None:
                exclusions["unavailable_content_or_epoch"] += 1
                continue
            actor, post, author = row["user_id"], row["post_id"], content["author_id"]
            related = by_actor_post[actor, post] + by_post[post] + by_author[actor, author] + resets[actor]
            related = sorted({event["sequence"]: event for event in related}.values(), key=lambda event: event["sequence"])
            options = dict(exposure_event=row["event_id"], as_of=completed, history_start=floor)
            label = derive_outcomes(related, window_end=window_end, **options)
            if not label["eligible"]:
                exclusions.update(label["excluded"])
                continue
            # Keep pre-existing state separate from end-of-window state. A
            # carried like/follow isn't evidence of a new reaction to this view.
            baseline = derive_outcomes([event for event in related if event["sequence"] <= row["sequence"]],
                                       window_end=row["recorded_at"], **options)["outcomes"]
            for name in ("active_ms", "opened"):
                baseline.pop(name)
            output.append(dict(label, group_id=group_id, exposure_id=row["event_id"], user_id=actor,
                               post_id=post, post_version=row["post_version"], author_id=author,
                               language=content["language"], epoch=row["epoch"], surface="for_you",
                               label_available_at=completed.isoformat(), baseline_state=baseline,
                               producing_snapshot=row["context"], fingerprints=content["fingerprints"]))
        if output:
            yield output


def duplicate_components(groups, photos=None):
    """Union exact and near matches, including bridges across later splits."""
    parent = {}
    exact = {}
    inverted = defaultdict(set)
    shingles = {}
    photo_buckets = defaultdict(set)
    photos = photos or {}

    def root(key):
        parent.setdefault(key, key)
        while key != parent[key]:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    def union(a, b):
        a, b = sorted((root(a), root(b)))
        parent[b] = a

    for batch in groups:
        for row in batch:
            for fp in row["fingerprints"]:
                key = digest(canonical(fp))
                if key in shingles:
                    continue
                root(key)
                grams = frozenset(fp["text_bigrams"])
                for identity in [("post", fp["id"]), ("text", fp["text_sha256"]),
                                 *(("photo", photo) for photo in fp["photos"])]:
                    if identity[1]:
                        union(key, exact.setdefault(identity, key))
                for checksum in fp["photos"]:
                    bits = int(photos[checksum], 16)
                    # With <=5 changed bits at least one of six disjoint
                    # blocks is identical. The final distance is always exact.
                    buckets = [(i, (bits >> (i * 11)) & 2047) for i in range(6)]
                    for old, other in set().union(*(photo_buckets[bucket] for bucket in buckets)):
                        if (bits ^ old).bit_count() <= 5:
                            union(key, other)
                    for bucket in buckets:
                        photo_buckets[bucket].add((bits, key))
                # Synthetic text only. Very short snippets get exact matching.
                if len(grams) >= 4:
                    candidates = set().union(*(inverted[gram] for gram in grams))
                    for other in candidates:
                        common = len(shingles[other] & grams)
                        if common * 4 >= len(shingles[other] | grams) * 3:
                            union(key, other)
                    for gram in grams:
                        inverted[gram].add(key)
                shingles[key] = grams
    return {key: root(key) for key in sorted(parent)}


def build(config, root):
    horizon = timedelta(seconds=config["label_window_seconds"])
    embargo = timedelta(seconds=config["embargo_seconds"])
    signature = tuple(config["snapshot_signature"])
    photos = config.get("photo_fingerprints", {})
    if (config["origin"] != "synthetic" or type(config["seed"]) is not int or config["seed"] < 0
            or not timedelta(seconds=1) <= horizon <= timedelta(hours=24) or embargo < horizon
            or len(signature) != 5 or not all(isinstance(x, str) and x and x != "none" for x in signature)
            or signature[1] not in FEATURE_WIDTHS or signature[3] != signature[4]
            or len(photos) > 10000 or any(not HEX.fullmatch(key) or not PHOTO_HASH.fullmatch(value)
                                         for key, value in photos.items())):
        raise ValueError("invalid synthetic dataset configuration")
    refs, batches, split_exclusions = {}, {}, {}
    prior_end = None
    provenance = None
    for split in SPLITS:
        rule = config["splits"][split]
        start, end = instant(rule["from"]), instant(rule["available_by"])
        meta, capture_hash, rows, floor, settled, completed = read_capture(root / rule["capture"])
        db = meta["database"]
        scope = tuple(db[key] for key in ("cohort_id", "origin", "seed", "source_hash", "member_sha256", "sql_sha256"))
        if (db["seed"] != config["seed"] or (provenance is not None and provenance != scope)
                or not floor <= start < end or not start <= completed <= end
                or (prior_end is not None and start < prior_end + embargo)
                or capture_hash in refs.values()):
            raise ValueError("capture, scope or split boundaries are not prospective and disjoint")
        provenance = scope
        prior_end = end
        refs[split] = capture_hash
        exclusions = split_exclusions[split] = Counter(filtered_at_source=meta["excluded_at_source"])
        batches[split] = list(extract(rows, floor, settled, completed, start, end, horizon, signature, photos, exclusions))

    components = duplicate_components((batch for split in SPLITS for batch in batches[split]), photos)
    component_split = {}
    used_groups = set()
    used_exposures = set()
    train_users, train_authors = set(), set()
    result = {split: [] for split in SPLITS}
    for split in SPLITS:
        for batch in batches[split]:
            group = batch[0]["group_id"]
            exposures = {row["exposure_id"] for row in batch}
            keys = {components[digest(canonical(fp))] for row in batch for fp in row["fingerprints"]}
            if group in used_groups or exposures & used_exposures:
                raise ValueError("observation repeated across captures")
            used_groups.add(group)
            used_exposures.update(exposures)
            if any(component_split.get(key, split) != split for key in keys):
                split_exclusions[split]["duplicate_cross_split_group"] += len(batch)
                continue
            component_split.update(dict.fromkeys(keys, split))
            for row in sorted(batch, key=lambda r: r["producing_snapshot"]["position"]):
                row["duplicate_clusters"] = sorted({components[digest(canonical(fp))] for fp in row.pop("fingerprints")})
                row["cold_user"] = split != "train" and row["user_id"] not in train_users
                row["cold_author"] = split != "train" and row["author_id"] not in train_authors
                if split == "train":
                    train_users.add(row["user_id"])
                    train_authors.add(row["author_id"])
                result[split].append(row)
    payloads = {split: b"".join(map(canonical, result[split])) for split in SPLITS}
    manifest = dict(schema="yap-offline-dataset-v1", origin=config["origin"], seed=config["seed"],
                    label_policy=POLICY, config=config, capture_sha256=refs,
                    builder_sha256={name: digest(Path(__file__).with_name(name).read_bytes())
                                    for name in ("dataset.py", "feedback.py")},
                    exclusions=split_exclusions, splits={split: dict(rows=len(result[split]),
                        groups=len({row["group_id"] for row in result[split]}), sha256=digest(payloads[split]))
                        for split in SPLITS})
    manifest["dataset_sha256"] = digest(canonical(manifest))
    return manifest, payloads


def publish(output, manifest, payloads):
    publish_files(output, {**{f"{k}.jsonl": v for k, v in payloads.items()},
                           "manifest.json": canonical(manifest)})


def publish_files(output, files):
    output = Path(output)
    if output.exists() or output.is_symlink():
        raise FileExistsError(output)
    temporary = Path(tempfile.mkdtemp(prefix=".dataset-", dir=output.parent))
    try:
        for name, raw in files.items():
            with os.fdopen(os.open(temporary / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
        temporary.rename(output)
    finally:
        shutil.rmtree(temporary, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    manifest, payloads = build(json.loads(args.config.read_bytes()), args.config.parent)
    publish(args.output, manifest, payloads)
    print(manifest["dataset_sha256"])


if __name__ == "__main__":
    main()
