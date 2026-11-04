"""Time-split CatBoost ranking experiment; synthetic artifacts stay research-only."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import random
from collections import defaultdict
from service import FEATURES, EMBEDDING_ID


def ndcg(labels, scores, k=10):
    def dcg(indices):
        return sum((2 ** labels[i] - 1) / math.log2(rank + 2) for rank, i in enumerate(indices[:k]))
    ideal = dcg(sorted(range(len(labels)), key=lambda i: labels[i], reverse=True))
    return dcg(sorted(range(len(labels)), key=lambda i: scores[i], reverse=True)) / ideal if ideal else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("data", type=Path)
    parser.add_argument("--out", type=Path, default=Path("output/models/ranker.cbm"))
    parser.add_argument("--iterations", type=int, default=250)
    args = parser.parse_args()
    from catboost import CatBoostRanker, Pool
    raw = args.data.read_bytes()
    rows = [json.loads(line) for line in raw.decode("utf-8-sig").splitlines() if line.strip()]
    if any(len(r["features"]) != len(FEATURES) or r["embedding_model"] != EMBEDDING_ID for r in rows):
        raise ValueError("Feature or embedding version mismatch")
    groups = defaultdict(list)
    for row in rows:
        if any(not math.isfinite(x) for x in row["features"]):
            raise ValueError("Nonfinite feature")
        groups[row["group"]].append(row)
    ordered = sorted((group for group in groups.values() if len(group) >= 2), key=lambda g: (g[0]["at"], g[0]["group"]))
    if len(ordered) < 12:
        raise ValueError("At least 12 chronological groups required; collect more sessions")
    n = len(ordered)
    train, valid, test = ordered[: int(n * 0.7)], ordered[int(n * 0.7):int(n * 0.85)], ordered[int(n * 0.85):]
    if not any(len({r["label"] for r in g}) > 1 for g in train):
        raise ValueError("No ranking contrast in the training observations")

    def pool(split):
        flat = [r for g in split for r in g]
        ids = [i for i, g in enumerate(split) for _ in g]
        return Pool([r["features"] for r in flat], [r["label"] for r in flat], group_id=ids, feature_names=FEATURES)

    model = CatBoostRanker(iterations=args.iterations, depth=5, loss_function="YetiRank", random_seed=42, thread_count=2, verbose=False, allow_writing_files=False)
    model.fit(pool(train), eval_set=pool(valid), early_stopping_rounds=35)

    def evaluate(split):
        deltas, learned, baseline = [], [], []
        for group in split:
            labels = [r["label"] for r in group]
            scores = model.predict([r["features"] for r in group], thread_count=2)
            a = ndcg(labels, scores)
            b = ndcg(labels, [r["baseline"] for r in group])
            if a is not None:
                learned.append(a)
                baseline.append(b)
                deltas.append(a - b)
        if not learned:
            raise ValueError("Evaluation split has no observed positive signal")
        rng = random.Random(42)
        boot = sorted(sum(rng.choices(deltas, k=len(deltas))) / len(deltas) for _ in range(500))
        return {"groups": len(learned), "ndcg10": sum(learned) / len(learned), "baseline_ndcg10": sum(baseline) / len(baseline), "paired_delta_95ci": [boot[12], boot[487]]}

    validation, testing = evaluate(valid), evaluate(test)
    origin = "synthetic" if any(r.get("origin") != "pilot" for r in rows) else "pilot"
    version = "catboost-v1-" + hashlib.sha256(raw).hexdigest()[:12]
    manifest = {"version": version, "feature_version": "hybrid-v1", "features": FEATURES, "embedding_model": EMBEDDING_ID, "origin": origin, "train_groups": len(train), "validation": validation, "test": testing, "data_sha256": hashlib.sha256(raw).hexdigest(), "eligible_for_production": origin == "pilot" and testing["groups"] >= 100 and testing["paired_delta_95ci"][0] > 0, "label_note": "Observed positive-action proxy; absence is not an explicit dislike. Historical exposure is biased."}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    model.save_model(str(args.out))
    args.out.with_suffix(".json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
