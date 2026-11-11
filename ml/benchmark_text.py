"""Compare pinned text embedding spaces on the local quality corpus."""
from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
import platform
import random
import time

from benchmark_vector import benchmark_pgvector, percentile_ms
from dataset import canonical, digest
from embedding import encode_batch
from service import MODEL, REVISION


@dataclass(frozen=True)
class Model:
    repository: str
    revision: str
    weights: str
    pooling: str
    precision: str
    dimensions: int
    query_prefix: str = ""
    document_prefix: str = ""
    external_files: tuple[str, ...] = ()
    max_tokens: int = 512


MODELS = {
    "e5-int8": Model(MODEL, REVISION, "onnx/model_qint8_avx512_vnni.onnx", "mean", "int8", 384, "query: ", "query: "),
    "e5-fp32": Model(MODEL, REVISION, "onnx/model.onnx", "mean", "fp32", 384, "query: ", "query: "),
    "bge-m3": Model(
        "BAAI/bge-m3", "5617a9f61b028005a4858fdac845db406aefb181",
        "onnx/model.onnx", "cls", "fp32", 1024,
        external_files=("onnx/model.onnx_data", "onnx/Constant_7_attr__value"),
    ),
}
TOKENIZER_FILES = (
    "config.json", "tokenizer.json", "tokenizer_config.json", "special_tokens_map.json",
    "sentencepiece.bpe.model",
)


def load_corpus(folder):
    manifest = json.loads((folder / "manifest.json").read_bytes())
    identity = manifest.pop("dataset_sha256")
    if manifest["schema"] != "yap-quality-corpus-v1" or digest(canonical(manifest)) != identity:
        raise ValueError("invalid corpus manifest")

    def read(name):
        data = (folder / f"{name}.jsonl").read_bytes()
        rows = [json.loads(line) for line in data.splitlines()]
        if manifest["files"][name] != {"count": len(rows), "sha256": digest(data)}:
            raise ValueError(f"invalid corpus file: {name}")
        return rows

    docs = [row for row in read("documents") if row["kind"] == "text"]
    queries = read("queries")
    ids = {row["id"] for row in docs}
    query_ids = {row["id"] for row in queries}
    if len(ids) != len(docs) or len(query_ids) != len(queries):
        raise ValueError("duplicate corpus ID")
    grades = defaultdict(dict)
    for row in read("judgments"):
        if row["document_id"] not in ids:
            continue
        grade = row["relevance"]
        qid, did = row["query_id"], row["document_id"]
        if qid not in query_ids or grade not in (0, 1, 2) or did in grades[qid]:
            raise ValueError("invalid text judgment")
        grades[qid][did] = grade
    if set(grades) != query_ids:
        raise ValueError("missing text judgments")
    return identity, docs, queries, grades


def measures(order, grades, k):
    import math

    def dcg(values):
        return sum((2**grade - 1) / math.log2(rank + 2) for rank, grade in enumerate(values[:k]))

    ideal = sorted(grades.values(), reverse=True)
    if not ideal or ideal[0] <= 0:
        raise ValueError("slice has no relevant documents")
    top = [grades.get(did, 0) for did in order[:k]]
    return {
        "recall": sum(grade > 0 for grade in top) / sum(grade > 0 for grade in ideal),
        "ndcg": dcg(top) / dcg(ideal),
    }


def evaluate(documents, queries, grades, scores, k):
    cohorts = defaultdict(list)
    for query, similarities in zip(queries, scores, strict=True):
        qid, language = query["id"], query["language"]
        # Stable order at equal similarity, independent of input row order.
        ranked = sorted(zip(documents, similarities, strict=True), key=lambda pair: (-pair[1], pair[0]["id"]))
        slices = {
            "all": ranked,
            f"{language}_to_{language}": [pair for pair in ranked if pair[0]["language"] == language],
            f"{language}_cross": [pair for pair in ranked if pair[0]["language"] != language],
            "authored_contrasts": [pair for pair in ranked if pair[0]["id"] in grades[qid]],
        }
        if len(query["text"]) <= 24:
            slices["short_queries"] = ranked
        for name, pairs in slices.items():
            order = [row["id"] for row, _ in pairs]
            eligible = {did: grades[qid][did] for did in order if did in grades[qid]}
            cohorts[name].append({"id": qid, "topic": query["topic"], **measures(order, eligible, k)})
    return {
        name: {
            "queries": len(rows),
            **{metric: sum(row[metric] for row in rows) / len(rows) for metric in ("recall", "ndcg")},
            "per_query": rows,
        }
        for name, rows in sorted(cohorts.items())
    }


def compare_quality(report, baseline, seed):
    import numpy as np

    if report["corpus"] != baseline["corpus"] or report["k"] != baseline["k"]:
        raise ValueError("baseline must use the same corpus and k")
    rng = np.random.default_rng(seed)
    comparison = {}
    for name, values in report["quality"].items():
        current = {row["id"]: row for row in values["per_query"]}
        previous = {row["id"]: row for row in baseline["quality"][name]["per_query"]}
        if current.keys() != previous.keys() or any(row["topic"] != previous[key]["topic"] for key, row in current.items()):
            raise ValueError(f"baseline has different queries in {name}")
        groups = defaultdict(list)
        for key, row in current.items():
            groups[row["topic"]].append([row[m] - previous[key][m] for m in ("ndcg", "recall")])
        # Topic blocks retain dependencies between a topic's prompts and translations.
        totals = np.asarray([np.sum(rows, axis=0) for _, rows in sorted(groups.items())])
        counts = np.asarray([len(rows) for _, rows in sorted(groups.items())])
        choices = rng.integers(0, len(counts), (2000, len(counts)))
        sampled = totals[choices].sum(axis=1) / counts[choices].sum(axis=1, keepdims=True)
        bounds = np.percentile(sampled, [2.5, 97.5], axis=0)
        comparison[name] = {
            "topics": len(counts),
            **{metric: {"delta": float(totals[:, index].sum() / counts.sum()),
                        "topic_bootstrap_95": bounds[:, index].tolist()}
               for index, metric in enumerate(("ndcg", "recall"))},
        }
    return {"baseline": baseline["model"], "space": baseline["space"]["id"], "slices": comparison}


def peak_rss_bytes():
    if platform.system() != "Windows":
        import resource

        unit = 1 if platform.system() == "Darwin" else 1024
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * unit

    import ctypes
    from ctypes import wintypes

    class Counters(ctypes.Structure):
        _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [
            (name, ctypes.c_size_t)
            for name in ("PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
                         "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage", "QuotaNonPagedPoolUsage",
                         "PagefileUsage", "PeakPagefileUsage")
        ]

    counters = Counters()
    counters.cb = ctypes.sizeof(counters)
    current_process = ctypes.windll.kernel32.GetCurrentProcess
    current_process.restype = wintypes.HANDLE
    get_info = ctypes.windll.psapi.GetProcessMemoryInfo
    get_info.argtypes = (wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD)
    if not get_info(current_process(), ctypes.byref(counters), counters.cb):
        raise OSError("GetProcessMemoryInfo failed")
    return counters.PeakWorkingSetSize


def model_files(model, download):
    from huggingface_hub import hf_hub_download

    # BGE's ONNX graph and its external weights are siblings. E5's tokenizer
    # lives at the root of its upstream snapshot, as it does in serving.
    tokenizer_dir = "onnx/" if model.pooling == "cls" else ""
    files = [model.weights, *model.external_files, *(tokenizer_dir + f for f in TOKENIZER_FILES)]
    return {
        name: Path(hf_hub_download(
            model.repository, name, revision=model.revision, token=False, local_files_only=not download,
        ))
        for name in files
    }


def encode_rows(encode, rows, prefix):
    import numpy as np

    return np.concatenate([
        encode([row["text"] for row in rows[start:start + 8]], prefix)
        for start in range(0, len(rows), 8)
    ])


def benchmark_inference(encode, queries, docs, first_text, prefix, seed):
    samples = list(dict.fromkeys(row["text"] for row in queries + docs if row["text"] != first_text))
    random.Random(seed).shuffle(samples)
    report = {}
    for batch_size in (1, 4, 8):
        timings = []
        calls = min(56, len(samples) // batch_size)
        if calls < 20:
            raise ValueError("not enough unique texts for inference latency")
        for start in range(0, calls * batch_size, batch_size):
            before = time.perf_counter()
            encode(samples[start:start + batch_size], prefix)
            timings.append(time.perf_counter() - before)
        report[str(batch_size)] = {
            "calls": calls, "texts_per_second": calls * batch_size / sum(timings), **percentile_ms(timings),
        }
    return report


def run(args):
    start = time.perf_counter()
    import numpy as np
    import onnxruntime as ort
    import tokenizers
    import transformers

    model = MODELS[args.model]
    files = model_files(model, args.download)
    identity, documents, queries, grades = load_corpus(args.corpus)
    init = time.perf_counter()
    tokenizer_file = next(path for name, path in files.items() if name.endswith("/tokenizer.json") or name == "tokenizer.json")
    tokenizer = transformers.AutoTokenizer.from_pretrained(
        tokenizer_file.parent, local_files_only=True, trust_remote_code=False,
    )
    options = ort.SessionOptions()
    options.intra_op_num_threads = args.threads
    options.inter_op_num_threads = 1
    session = ort.InferenceSession(str(files[model.weights]), sess_options=options, providers=["CPUExecutionProvider"])
    ready = time.perf_counter()

    def encode(texts, prefix):
        vectors = encode_batch(session, tokenizer, texts, prefix=prefix, pooling=model.pooling, max_tokens=model.max_tokens)
        if vectors.shape != (len(texts), model.dimensions) or not np.isfinite(vectors).all():
            raise ValueError("invalid encoder output")
        return vectors

    first_text = documents[0]["text"]
    first = encode([first_text], model.document_prefix)
    first_ms = (time.perf_counter() - ready) * 1000
    probe = encode([first_text, queries[0]["text"]], model.document_prefix)
    # Dynamic int8 activation quantization can vary slightly with batch composition.
    agreement = float(first[0] @ probe[0])
    if not np.allclose(np.linalg.norm(probe, axis=1), 1, atol=1e-5) or agreement < 0.99:
        raise ValueError("encoder smoke check failed")
    report = {
        "corpus": identity, "model": args.model, "threads": args.threads,
        "platform": platform.platform(), "cpu": args.cpu, "seed": args.seed,
        "versions": {"python": platform.python_version(), "numpy": np.__version__,
                     "onnxruntime": ort.__version__, "transformers": transformers.__version__,
                     "tokenizers": tokenizers.__version__},
        "startup_ms": (ready - init) * 1000, "first_inference_ms": first_ms,
        "module_to_first_ms": (ready - start) * 1000 + first_ms,
        "single_batch_cosine": agreement,
    }
    if not args.smoke:
        report["inference"] = benchmark_inference(encode, queries, documents, first_text, model.document_prefix, args.seed)
        post_vectors = encode_rows(encode, documents, model.document_prefix)
        query_vectors = encode_rows(encode, queries, model.query_prefix)
        report["quality"] = evaluate(documents, queries, grades, query_vectors @ post_vectors.T, args.k)
        report["k"] = args.k
    report["peak_rss_bytes"] = peak_rss_bytes()
    artifacts = {}
    for name, file in files.items():
        with file.open("rb") as stream:
            artifacts[name] = {"sha256": hashlib.file_digest(stream, "sha256").hexdigest(), "bytes": file.stat().st_size}
    space = {**asdict(model), "normalization": "l2", "artifacts": artifacts}
    report["space"] = {**space, "id": digest(canonical(space))}
    if args.baseline:
        report["comparison"] = compare_quality(report, json.loads(args.baseline.read_bytes()), args.seed)
    if args.pg:
        report["pgvector"] = benchmark_pgvector(post_vectors, query_vectors, args.k, args.pg_port, args.seed)
    if args.pg_growth:
        rng = np.random.default_rng(args.seed)
        indices = rng.integers(0, len(post_vectors), args.pg_growth - len(post_vectors))
        synthetic = post_vectors[indices] + rng.normal(0, 0.12 / model.dimensions**0.5,
                                                     (len(indices), model.dimensions)).astype(np.float32)
        synthetic /= np.linalg.norm(synthetic, axis=1, keepdims=True)
        report["pgvector_growth"] = benchmark_pgvector(
            np.concatenate((post_vectors, synthetic)), query_vectors, args.k, args.pg_port, args.seed,
        )
        report["pgvector_growth"]["synthetic_vectors"] = len(synthetic)
    args.output.write_bytes(canonical(report))


def main():
    import os

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", choices=MODELS, required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--cpu", default=platform.processor())
    parser.add_argument("--seed", type=int, default=630)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--baseline", type=Path, help="compare quality with a completed report on the same corpus")
    parser.add_argument("--pg", action="store_true", help="measure pgvector in the local Supabase; uses PGPASSWORD")
    parser.add_argument("--pg-port", type=int, default=54322)
    parser.add_argument("--pg-growth", type=int, choices=(10000,), help="also test 10,000 vectors with synthetic growth")
    args = parser.parse_args()
    if not 1 <= args.threads <= 32 or not 1 <= args.k <= 10:
        parser.error("threads must be 1–32 and k must be 1–10")
    if args.smoke and (args.pg or args.baseline):
        parser.error("smoke mode cannot use pgvector or a baseline report")
    if args.pg_growth and not args.pg:
        parser.error("--pg-growth requires --pg")
    os.environ["USE_TORCH"] = "0"
    os.environ["HF_HUB_DISABLE_IMPLICIT_TOKEN"] = "1"
    os.environ["TRANSFORMERS_VERBOSITY"] = "error"
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    os.environ["OPENBLAS_NUM_THREADS"] = str(args.threads)
    run(args)


if __name__ == "__main__":
    main()
