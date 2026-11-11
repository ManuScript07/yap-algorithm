"""Compare photo retrieval and multimodal fusion on the fixed YapChat corpus."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import platform
import random
import time

from benchmark_text import MODELS as TEXT_MODELS, encode_rows, measures, model_files, peak_rss_bytes
from benchmark_vector import benchmark_pgvector, percentile_ms
from dataset import canonical, digest
from embedding import encode_batch
from quality_corpus import photo_index
from vision import MODELS, VisionEncoder, artifacts


def corpus_files(corpus, photo_folder):
    manifest = json.loads((corpus / "manifest.json").read_bytes())
    identity = manifest.pop("dataset_sha256")
    if manifest["schema"] != "yap-quality-corpus-v1" or digest(canonical(manifest)) != identity:
        raise ValueError("invalid quality corpus manifest")
    rows = {}
    for key in ("documents", "queries", "photos"):
        raw = (corpus / f"{key}.jsonl").read_bytes()
        values = [json.loads(line) for line in raw.splitlines()]
        if {"count": len(values), "sha256": digest(raw)} != manifest["files"][key]:
            raise ValueError(f"invalid quality corpus file: {key}")
        rows[key] = values
    photos, photo_hash = photo_index(photo_folder)
    if photo_hash != manifest["photo_index_sha256"]:
        raise ValueError("photo index does not match the corpus")
    by_id = {photo["id"]: photo for photo in photos}
    if not rows["photos"] or any(by_id.get(row["id"]) != row for row in rows["photos"]):
        raise ValueError("photo corpus does not match verified bytes and attribution")
    return identity, rows


def evaluate(scores, queries, targets, *, exclude_self=False, k=10):
    totals = []
    for query, similarities in zip(queries, scores, strict=True):
        ranked = sorted(zip(targets, similarities, strict=True), key=lambda pair: (-pair[1], pair[0]["id"]))
        if exclude_self:
            ranked = [item for item in ranked if item[0]["id"] != query["id"]]
        order = [target["id"] for target, _ in ranked]
        grades = {target["id"]: 1 for target, _ in ranked if target["topic"] == query["topic"]}
        totals.append(measures(order, grades, k))
    if not totals:
        raise ValueError("empty quality slice")
    return {"queries": len(totals), **{
        key: sum(row[key] for row in totals) / len(totals) for key in ("ndcg", "recall")
    }}


def photo_scores(queries, photos, ids, policy):
    import numpy as np

    if not 1 <= len(ids) <= 3:
        raise ValueError("photo post must have 1–3 images")
    vectors = np.stack([photos[key] for key in dict.fromkeys(ids)])
    if policy == "max":
        return (queries @ vectors.T).max(axis=1)
    if policy != "mean":
        raise ValueError("unknown photo pooling policy")
    centroid = vectors.mean(axis=0)
    norm = np.linalg.norm(centroid)
    return queries @ (centroid / norm) if norm > 1e-12 else np.zeros(len(queries))


def similarity_calibration(vectors, photos):
    import numpy as np

    left, right = np.triu_indices(len(photos), k=1)
    scores = (vectors @ vectors.T)[left, right]
    topics = np.asarray([row["topic"] for row in photos])
    same = topics[left] == topics[right]
    return {
        name: {
            "pairs": len(values), "p50": float(np.quantile(values, 0.50)),
            "p75": float(np.quantile(values, 0.75)), "p95": float(np.quantile(values, 0.95)),
        }
        for name, values in (("same_topic", scores[same]), ("different_topic", scores[~same]))
    }


def percentiles(scores):
    """Per-query empirical CDF across a common candidate set, ties at midrank."""
    import numpy as np

    result = np.full_like(scores, np.nan, dtype=np.float64)
    for row, values in enumerate(scores):
        valid = np.isfinite(values)
        ordered = np.sort(values[valid])
        if len(ordered):
            first = np.searchsorted(ordered, values[valid], side="left")
            last = np.searchsorted(ordered, values[valid], side="right")
            result[row, valid] = (first + last) / (2 * len(ordered))
    return result


def fuse(text, vision):
    import numpy as np

    scores = np.stack([percentiles(text), percentiles(vision)])
    available = np.isfinite(scores).sum(axis=0)
    return np.divide(np.nansum(scores, axis=0), available, out=np.zeros_like(text), where=available > 0)


@dataclass(frozen=True)
class Scenario:
    caption: str
    photos: tuple[str, ...]


def scenarios(post, contrast):
    one, two, three = post["photos"]
    return {
        "text_only": Scenario(post["text"], ()),
        "photo_only": Scenario("", (one, two, three)),
        "aligned": Scenario(post["text"], (one, two, three)),
        "one_image": Scenario(post["text"], (one,)),
        "repeat_image": Scenario(post["text"], (one, one, one)),
        "mixed_topics": Scenario(post["text"], (one, two, contrast["photos"][0])),
        "caption_conflict": Scenario(contrast["text"], (one, two, three)),
    }


def e5_texts(rows, threads):
    import onnxruntime as ort
    from transformers import AutoTokenizer

    model = TEXT_MODELS["e5-int8"]
    files = model_files(model, False)
    tokenizer = AutoTokenizer.from_pretrained(
        files["tokenizer.json"].parent, local_files_only=True, trust_remote_code=False,
    )
    options = ort.SessionOptions()
    options.intra_op_num_threads = threads
    options.inter_op_num_threads = 1
    session = ort.InferenceSession(str(files[model.weights]), sess_options=options, providers=["CPUExecutionProvider"])

    def encode(texts, prefix):
        return encode_batch(session, tokenizer, texts, prefix=prefix, pooling=model.pooling, max_tokens=model.max_tokens)

    unique = list(dict.fromkeys(rows))
    return dict(zip(unique, encode_rows(encode, [{"text": text} for text in unique], model.query_prefix), strict=True))


def evaluate_posts(documents, queries, visual_queries, photos, threads):
    import numpy as np

    posts = [row for row in documents if row["kind"] == "mixed"]
    by_topic = {post["topic"]: post for post in posts}
    contrasts = {row["topic"]: row["contrast"] for row in queries}
    setups = [scenarios(post, by_topic[contrasts[post["topic"]]]) for post in posts]
    representations = e5_texts(
        [row["text"] for row in queries] + [post["text"] for post in posts], threads,
    )
    text_queries = np.stack([representations[row["text"]] for row in queries])
    missing = np.full(len(queries), np.nan)
    quality = {}
    for name in setups[0]:
        values = [item[name] for item in setups]
        text_scores = np.stack([
            text_queries @ representations[item.caption] if item.caption else missing for item in values
        ], axis=1)
        methods = {"text": text_scores}
        for policy in ("max", "mean"):
            image_scores = np.stack([
                photo_scores(visual_queries, photos, item.photos, policy) if item.photos else missing for item in values
            ], axis=1)
            methods[f"vision_{policy}"] = image_scores
            methods[f"fusion_{policy}"] = fuse(text_scores, image_scores)
        quality[name] = {
            method: evaluate(scores, queries, posts)
            for method, scores in methods.items() if np.isfinite(scores).any()
        }
    if quality["one_image"] != quality["repeat_image"]:
        raise RuntimeError("duplicate photo amplified post relevance")
    return quality


def time_images(encode, paths, seed):
    report = {}
    paths = list(dict.fromkeys(paths))
    random.Random(seed).shuffle(paths)
    for batch in (1, 4, 8):
        calls = min(56, len(paths) // batch)
        if calls < 16:
            raise ValueError("not enough distinct photos for performance measurement")
        timings, cpu = [], 0.0
        for start in range(0, calls * batch, batch):
            cpu_start, start_at = time.process_time(), time.perf_counter()
            encode(paths[start:start + batch])
            timings.append(time.perf_counter() - start_at)
            cpu += time.process_time() - cpu_start
        report[str(batch)] = {
            "calls": calls, "images_per_second": calls * batch / sum(timings),
            "cpu_ms_per_image": cpu * 1000 / (calls * batch), **percentile_ms(timings),
        }
    return report


def batches(fn, values, size):
    import numpy as np

    return np.concatenate([fn(values[start:start + size]) for start in range(0, len(values), size)])


def run(args):
    import numpy as np
    import onnxruntime as ort
    import PIL
    import transformers

    model = MODELS[args.model]
    files, space = artifacts(model, args.download)
    corpus, rows = corpus_files(args.corpus, args.photos)
    photos, queries = rows["photos"], rows["queries"]
    paths = [args.photos / row["filename"] for row in photos]
    start = time.perf_counter()
    encoder = VisionEncoder(model, files, args.threads)
    ready = time.perf_counter()
    first = encoder.images(paths[:1])
    first_ms = (time.perf_counter() - ready) * 1000
    paired = encoder.images(paths[:2])
    agreement = float(first[0] @ paired[0])
    if agreement < 0.99 or not np.allclose(np.linalg.norm(paired, axis=1), 1, atol=1e-6):
        raise ValueError("vision smoke check failed")
    report = {
        "corpus": corpus, "model": args.model, "space": space, "threads": args.threads, "seed": args.seed,
        "platform": platform.platform(), "cpu": args.cpu,
        "versions": {"python": platform.python_version(), "numpy": np.__version__, "pillow": PIL.__version__,
                     "onnxruntime": ort.__version__, "transformers": transformers.__version__},
        "startup_ms": (ready - start) * 1000, "first_photo_ms": first_ms, "single_batch_cosine": agreement,
    }
    if args.smoke:
        encoder.texts([queries[0]["text"], queries[-1]["text"]])
    else:
        report["inference"] = time_images(encoder.images, paths[2:], args.seed)
        image_vectors = batches(encoder.images, paths, 8)
        report["image_peak_rss_bytes"] = peak_rss_bytes()
        query_vectors = batches(encoder.texts, [row["text"] for row in queries], 8)
        cosine = query_vectors @ image_vectors.T
        report["quality"] = {"image_to_image": evaluate(image_vectors @ image_vectors.T, photos, photos, exclude_self=True)}
        report["similarity_calibration"] = similarity_calibration(image_vectors, photos)
        for language in ("ru", "en"):
            indices = [i for i, row in enumerate(queries) if row["language"] == language]
            selected = [queries[i] for i in indices]
            report["quality"][f"text_{language}_to_image"] = evaluate(cosine[indices], selected, photos)
            report["quality"][f"image_to_text_{language}"] = evaluate(cosine[indices].T, photos, selected)
        report["both_towers_peak_rss_bytes"] = peak_rss_bytes()
        # Vision measurements are complete before the independent E5 reference is loaded.
        del encoder
        report["posts"] = evaluate_posts(
            rows["documents"], queries, query_vectors,
            dict(zip([row["id"] for row in photos], image_vectors, strict=True)), args.threads,
        )
        if args.pg:
            report["pgvector"] = benchmark_pgvector(image_vectors, query_vectors, 10, args.pg_port, args.seed)
    args.output.write_bytes(canonical(report))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", choices=MODELS, required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--photos", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--seed", type=int, default=640)
    parser.add_argument("--cpu", default=platform.processor())
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--pg", action="store_true", help="use temporary tables in local Supabase; reads PGPASSWORD")
    parser.add_argument("--pg-port", type=int, default=54322)
    args = parser.parse_args()
    if not 1 <= args.threads <= 32 or args.smoke and args.pg:
        parser.error("threads must be 1–32 and smoke mode cannot use pgvector")
    os.environ.update(USE_TORCH="0", HF_HUB_DISABLE_IMPLICIT_TOKEN="1", TRANSFORMERS_VERBOSITY="error",
                      TOKENIZERS_PARALLELISM="false", OPENBLAS_NUM_THREADS=str(args.threads))
    run(args)


if __name__ == "__main__":
    main()
