"""Offline retrieval, fixed-pool reranking and end-to-end comparison for YapChat."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
import math
from pathlib import Path
import random
from statistics import mean
import subprocess
import tempfile
import time

from benchmark_text import peak_rss_bytes
from dataset import SPLITS, canonical, digest, publish_files
from feedback import instant
from model_artifacts import ranker_manifest, runtime_versions
from ranking_fixture import build, encode, fixture_hash, utility
from service import EMBEDDING_ID, FEATURES

PROTOCOL = Path(__file__).with_name("ranking_protocol.json")


def replay(path, weights):
    """The immutable capture has labels only for observed items."""
    manifest = json.loads((path / "manifest.json").read_bytes())
    identity = manifest.pop("dataset_sha256")
    if (manifest["schema"] != "yap-offline-dataset-v1" or manifest["origin"] != "synthetic"
            or digest(canonical(manifest)) != identity):
        raise ValueError("invalid replay manifest")
    result = {}
    previous_end = None
    earlier_duplicates = set()
    for split in SPLITS:
        raw = (path / f"{split}.jsonl").read_bytes()
        rows = [json.loads(line) for line in raw.splitlines()]
        meta = manifest["splits"][split]
        groups = {row["group_id"] for row in rows}
        if digest(raw) != meta["sha256"] or len(rows) != meta["rows"] or len(groups) != meta["groups"]:
            raise ValueError("replay integrity mismatch")
        latest = []
        duplicates = set()
        tally = Counter()
        for row in rows:
            exposed = instant(row["exposure_time"])
            matured = instant(row["label_available_at"])
            if not (instant(row["producing_snapshot"]["features_at"]) <= exposed
                    < instant(row["label_window_end"]) <= matured):
                raise ValueError("invalid replay timeline")
            if previous_end is not None and exposed <= previous_end:
                raise ValueError("observed labels cross temporal split")
            latest.append(matured)
            duplicates.update(row["duplicate_clusters"])
            value, transitions = utility(row["baseline_state"], row["outcomes"], weights)
            tally["positive"] += value > 0
            tally["explicit_negative"] += any(weight < 0 for weight in transitions.values())
            tally["without_transition"] += not transitions
        if duplicates & earlier_duplicates:
            raise ValueError("duplicate component crosses replay split")
        earlier_duplicates.update(duplicates)
        if latest:
            previous_end = max(latest)
        result[split] = dict(tally, users=len({row["user_id"] for row in rows}), groups=len(groups), rows=len(rows))
    return {"dataset_sha256": identity, "origin": "synthetic_supabase", "splits": result,
            "excluded": manifest.get("exclusions", {}), "retrieval_observable": False}


def run_go(binary, cases):
    with tempfile.TemporaryFile() as requests, tempfile.TemporaryFile() as output:
        for case in cases:
            requests.write(encode(case["request"]))
        requests.seek(0)
        subprocess.run([str(binary.resolve())], stdin=requests, stdout=output, check=True, timeout=180)
        output.seek(0)
        for case, line in zip(cases, output, strict=True):
            case["go"] = json.loads(line)


def scoped_order(ids, by_id, key):
    unseen = [i for i in ids if not by_id[i]["go"]["Seen"]]
    ordered = sorted(unseen, key=key)
    if len(unseen) < 20:
        ordered.extend(sorted((i for i in ids if by_id[i]["go"]["Seen"]), key=key))
    return ordered


def metric(order, eligible, by_id, k):
    top = order[:k]
    if not set(order) <= set(eligible) or len(set(order)) != len(order):
        raise ValueError("policy ranked ineligible or duplicate candidates")
    relevant = sum(by_id[i]["relevant"] for i in eligible)
    hits = sum(by_id[i]["relevant"] for i in top)
    gains = sorted((by_id[i]["gain"] for i in eligible), reverse=True)

    ideal = dcg(gains[:k])
    items = [by_id[i] for i in top]
    return {
        "recall": hits / relevant if relevant else None,
        "ndcg": dcg([p["gain"] for p in items]) / ideal if ideal else None,
        "unique_authors": len({p["author"] for p in items}),
        "unique_topics": len({p["topic"] for p in items}),
        "new_author_share": sum(p["new_author"] for p in items) / max(1, len(items)),
        "repeat_share": sum(p["go"]["Seen"] for p in items) / max(1, len(items)),
        "negative_share": sum(p["negative"] for p in items) / max(1, len(items)),
        "conflicting_caption_share": sum(p["conflict"] for p in items) / max(1, len(items)),
        "fill": len(top) / k,
        "nonempty": int(bool(top)),
    }


def dcg(values):
    return sum((2**v - 1) / math.log2(rank + 2) for rank, v in enumerate(values))


def matches_cohort(post, cohort):
    if cohort in ("photo", "text", "mixed"):
        return post["modality"] == cohort
    if cohort == "conflict":
        return post["conflict"]
    return post["new_author"] == (cohort == "new_author")


def compare_users(left, right, seed, samples):
    """Resample viewers with replacement, keeping all their sessions together."""
    grouped = defaultdict(list)
    for (user, a), (other, b) in zip(left, right, strict=True):
        if user != other:
            raise ValueError("unpaired model comparison")
        if a is not None and b is not None:
            grouped[user].append(a - b)
    values = [mean(grouped[user]) for user in sorted(grouped)]
    if not values:
        return None
    rng = random.Random(seed)
    boot = sorted(mean(rng.choices(values, k=len(values))) for _ in range(samples))
    return {"viewers": len(values), "delta": mean(values),
            "ci95": [boot[int(samples * .025)], boot[int(samples * .975) - 1]]}


def experiment_orders(case, protocol):
    ids = [p["id"] for p in case["candidates"]]
    by_id = {p["id"]: p for p in case["candidates"]}
    pools = {name: [ids[i] for i in selection["IDs"]] for name, selection in case["request"]["Pools"].items()}
    orders = {name: result["order"] for name, result in case["go"].items()}
    common = pools[protocol["common_pool"]]
    keys = {"chronological": lambda i: (-by_id[i]["created"].timestamp(), i),
            "popularity": lambda i: (-by_id[i]["popularity"], i),
            "semantic": lambda i: (-by_id[i]["semantic"], i)}
    fixed = {name: scoped_order(common, by_id, key) for name, key in keys.items()}
    fixed.update(hybrid=orders["hybrid"], hybrid_vision=orders["fixed_vision"])
    if "scores" in case:
        fixed["catboost"] = scoped_order(common, by_id, lambda i: (-case["scores"][i], i))
    end_to_end = {name: scoped_order(pools[name], by_id, keys[name]) if name in keys else orders[name]
                  for name in protocol["retrieval"]}
    if "catboost" in fixed:
        end_to_end["catboost"] = fixed["catboost"]
    return by_id, {key: pools[key] for key in protocol["retrieval"]}, fixed, end_to_end


def summarize(cases, protocol):
    k = protocol["k"]
    values, paired = defaultdict(list), defaultdict(list)
    counts, delays, coverage = Counter(), defaultdict(list), defaultdict(lambda: (set(), set()))
    for case in cases:
        by_id, pools, fixed, final = experiment_orders(case, protocol)
        scope = list(by_id)
        fixed_scope = [scope[i] for i in case["request"]["Pools"][protocol["common_pool"]]["IDs"]]
        cohorts = ("all", case["language"], case["status"],
                   "new_user" if case["status"] != "active" else "active_user")
        counts.update(sessions=1, excluded_candidates=case["excluded"], candidates=len(scope),
                      positive=sum(p["gain"] > 0 for p in by_id.values()),
                      negative=sum(p["negative"] for p in by_id.values()))
        for surface, orders, eligible in (("retrieval", pools, scope), ("reranking", fixed, fixed_scope),
                                          ("end_to_end", final, scope)):
            for name, order in orders.items():
                row = metric(order, eligible, by_id, k)
                if surface == "retrieval":
                    total = sum(by_id[i]["relevant"] for i in eligible)
                    row["candidate_recall"] = sum(by_id[i]["relevant"] for i in order) / total if total else None
                    row["pool_size"] = len(order)
                paired[surface, name].append(
                    (case["user"], row["candidate_recall"] if surface == "retrieval" else row["ndcg"]))
                for cohort in cohorts:
                    values[surface, name, cohort].append(row)
                new_eligible, new_shown = coverage[surface, name]
                new_eligible.update(by_id[i]["author"] for i in eligible if by_id[i]["new_author"])
                new_shown.update(by_id[i]["author"] for i in order[:k] if by_id[i]["new_author"])
                for cohort in ("photo", "text", "mixed", "new_author", "known_author", "conflict"):
                    scoped = {i for i in eligible if matches_cohort(by_id[i], cohort)}
                    if not scoped:
                        continue
                    # Preserve rank positions across modalities; never reindex the top K.
                    total = sum(by_id[i]["relevant"] for i in scoped)
                    hits = sum(by_id[i]["relevant"] and i in scoped for i in order[:k])
                    grades = [by_id[i]["gain"] if i in scoped else 0 for i in order[:k]]
                    ideal = sorted((by_id[i]["gain"] for i in scoped), reverse=True)[:k]
                    best = dcg(ideal)
                    values[surface, name, cohort].append({"recall": hits / total if total else None,
                                                         "ndcg": dcg(grades) / best if best else None})
                if surface == "end_to_end" and case["status"] == "active" and name in ("hybrid", "hybrid_vision"):
                    delays[name, case["offset"]].append(row["recall"])
    report = {}
    for (surface, name, cohort), rows in sorted(values.items()):
        result = {"sessions": len(rows)}
        for key in rows[0]:
            observed = [row[key] for row in rows if row[key] is not None]
            result[key] = mean(observed) if observed else None
            if key in ("recall", "ndcg"):
                result[key + "_sessions"] = len(observed)
        if cohort == "all":
            eligible, exposed = coverage[surface, name]
            result["new_author_coverage"] = len(exposed) / len(eligible) if eligible else None
        report.setdefault(surface, {}).setdefault(name, {})[cohort] = result
    uncertainty = defaultdict(dict)
    for (surface, name), rows in paired.items():
        if name != "hybrid":
            uncertainty[surface][name] = compare_users(
                rows, paired[surface, "hybrid"], protocol["bootstrap_seed"], protocol["bootstrap_samples"])
    return {"counts": dict(counts, users=len({case["user"] for case in cases})),
            "metrics": report, "paired_delta_vs_hybrid": uncertainty,
            "simulated_shift_recall": {f"{name}_{offset}s": mean(x for x in rows if x is not None)
                                       for (name, offset), rows in sorted(delays.items()) if any(x is not None for x in rows)}}


def train(cases, protocol, identity):
    from catboost import CatBoostRanker, Pool
    import numpy as np

    def grouped_pool(split):
        rows, targets, groups = [], [], []
        for group, case in enumerate(c for c in cases if c["split"] == split):
            features = case["go"][protocol["common_pool"]]["features"]
            for p in case["candidates"]:
                if p["id"] in features:
                    rows.append(features[p["id"]])
                    targets.append(p["gain"])
                    groups.append(group)
        return Pool(rows, targets, group_id=groups, feature_names=FEATURES)

    training, validation = grouped_pool("train"), grouped_pool("validation")
    learned, files, manifests = [], {}, []
    params = dict(protocol["catboost"])
    early_stop = params.pop("early_stopping_rounds")
    runtime = runtime_versions(("catboost", "numpy"))
    started = time.process_time()
    for seed in protocol["seeds"]:
        model = CatBoostRanker(**params, random_seed=seed, task_type="CPU", verbose=False, allow_writing_files=False)
        model.fit(training, eval_set=validation, early_stopping_rounds=early_stop)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ranker.cbm"
            model.save_model(str(path))
            weights = path.read_bytes()
        sha = digest(weights)
        provenance = ranker_manifest({**identity, "weights_sha256": sha, "seed": seed, "runtime": runtime,
                                      "params": protocol["catboost"], "trees": model.tree_count_})
        files[f"ranker-{seed}.cbm"] = weights
        files[f"ranker-{seed}.json"] = canonical(provenance)
        manifests.append(provenance)
        learned.append(model)
    training_cpu = time.process_time() - started
    inference, prediction_fingerprints = [], []
    per_seed = defaultdict(list)
    for case in (c for c in cases if c["split"] != "train"):
        features = case["go"][protocol["common_pool"]]["features"]
        keys = sorted(features)
        matrix = [features[k] for k in keys]
        started = time.perf_counter()
        predictions = [m.predict(matrix, thread_count=params["thread_count"]) for m in learned]
        scores = np.mean(predictions, axis=0)
        inference.append((time.perf_counter() - started) * 1000)
        case["scores"] = {key: float(score) for key, score in zip(keys, scores, strict=True)}
        prediction_fingerprints.append(digest(canonical(case["scores"])))
        by_id = {p["id"]: p for p in case["candidates"]}
        for seed, prediction in zip(protocol["seeds"], predictions, strict=True):
            table = dict(zip(keys, prediction, strict=True))
            order = scoped_order(keys, by_id, lambda i: (-table[i], i))
            value = metric(order, keys, by_id, protocol["k"])["ndcg"]
            if value is not None:
                per_seed[case["split"], seed].append(value)
    return files, {"models": manifests, "train_cpu_seconds": training_cpu, "predict_ms": times(inference),
                   "prediction_sha256": digest(canonical(prediction_fingerprints)),
                   "ndcg_per_seed": {split: {seed: mean(per_seed[split, seed]) for seed in protocol["seeds"]}
                                     for split in ("validation", "test")}}


def times(values):
    ordered = sorted(values)
    return {f"p{p}": ordered[math.ceil(len(ordered) * p / 100) - 1] for p in (50, 95, 99)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("corpus", "photos", "replay", "go-ranker", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    raw = PROTOCOL.read_bytes()
    protocol = json.loads(raw)
    started = time.perf_counter()
    captured = replay(args.replay, protocol["utility"])
    cases, identity = build(args.corpus, args.photos, protocol)
    identity.update(fixture_sha256=fixture_hash(cases), protocol_sha256=digest(raw), embedding_space=EMBEDDING_ID,
                    split_sha256={split: fixture_hash([case for case in cases if case["split"] == split])
                                  for split in SPLITS},
                    ranker_binary_sha256=digest(args.go_ranker.read_bytes()),
                    code_sha256={p.name: digest(p.read_bytes()) for p in
                                 (Path(__file__), Path(__file__).with_name("ranking_fixture.py"),
                                  Path(__file__).with_name("model_artifacts.py"))})
    run_go(args.go_ranker, cases)
    models, training = train(cases, protocol, identity)
    evaluation = {split: summarize([c for c in cases if c["split"] == split], protocol) for split in SPLITS}
    go_timing = {key: times([c["go"][key]["ms"] for c in cases]) for key in cases[0]["go"]}
    report = {**identity, "origin": "synthetic", "protocol": protocol, "historical_replay": captured,
              "evaluation": evaluation, "training": training, "go_rank_ms": go_timing,
              "peak_rss_bytes": peak_rss_bytes(), "wall_seconds": time.perf_counter() - started}
    publish_files(args.output, {"report.json": canonical(report), **models})
    print(f"fixture={identity['fixture_sha256']} test={evaluation['test']['counts']}")


if __name__ == "__main__":
    main()
