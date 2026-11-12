"""Verified model inputs for serving and local research."""
from __future__ import annotations

import argparse
import hashlib
from importlib.metadata import version
import json
from pathlib import Path
import re
import shutil
import tempfile

MODEL = "intfloat/multilingual-e5-small"
REVISION = "614241f622f53c4eeff9890bdc4f31cfecc418b3"
EMBEDDING_ID = f"{MODEL}@{REVISION}:onnx-int8:query"
FEATURES = ["semantic", "social", "quality", "freshness", "negative_similarity", "low_exposure", "nearby", "seen"]
DIMENSIONS = 384
VISION_SPACE = "8f6ef19478e47860edda9fd5d952f71c8e03d069f1f93099097620f538fac1cf"
TEXT_WEIGHTS = "onnx/model_qint8_avx512_vnni.onnx"
TEXT_PREPROCESSING = {"prefix": "query: ", "pooling": "mean", "max_tokens": 512, "normalization": "l2"}
TEXT_SHA256 = {
    "config.json": "69137736cab8b8903a07fe8afaafdda25aac55415a12a55d1bffa9f581abf959",
    "tokenizer.json": "0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39",
    "tokenizer_config.json": "a1d6bc8734a6f635dc158508bef000f8e2e5a759c7d92f984b2c86e5ff53425b",
    "special_tokens_map.json": "d05497f1da52c5e09554c0cd874037a083e1dc1b9cfd48034d1c717f1afc07a7",
    "sentencepiece.bpe.model": "cfc8146abe2a0488e9e2a0c56de7952f7c11ab059eca145a0a727afce0db2865",
    TEXT_WEIGHTS: "dd476dd0c2514e9b9be83aeb3853fac0763e0bdf4a71645407587d77c48a2d88",
}
HEX = re.compile(r"[0-9a-f]{64}")
FEATURE_VERSION = "feed-features-v2"
POLICY_VERSION = "hybrid-v2"
LABEL_VERSION = "offline-ranking-v1"
RANKER_SCHEMA = "yap-ranker-artifact-v1"


def canonical(data):
    return (json.dumps(data, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n").encode()


def file_sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def verify_files(paths, expected):
    if paths.keys() != expected.keys():
        raise ValueError("model artifact file set mismatch")
    for name, path in paths.items():
        if file_sha256(path) != expected[name]:
            raise ValueError(f"model artifact checksum mismatch: {name}")


def runtime_versions(packages):
    import platform

    return {"python": platform.python_version(), **{name: version(name) for name in packages}}


def resolve_text(download=False):
    from huggingface_hub import hf_hub_download

    files = {name: Path(hf_hub_download(MODEL, name, revision=REVISION, token=False,
                                        local_files_only=not download)) for name in TEXT_SHA256}
    verify_files(files, TEXT_SHA256)
    return files


def text_manifest(threads):
    return {"schema": "yap-encoder-artifact-v1", "repository": MODEL, "revision": REVISION,
            "embedding_space": EMBEDDING_ID, "dimensions": DIMENSIONS,
            "files": TEXT_SHA256, "preprocessing": TEXT_PREPROCESSING,
            "runtime": runtime_versions(("onnxruntime", "numpy", "transformers", "tokenizers")),
            "config": {"provider": "CPUExecutionProvider", "threads": threads},
            "code_sha256": {name: file_sha256(Path(__file__).with_name(name))
                            for name in ("model_artifacts.py", "embedding.py", "service.py")}}


def manifest_id(manifest):
    return hashlib.sha256(canonical(manifest)).hexdigest()


def load_text_tokenizer(files):
    from transformers import AutoTokenizer

    # Load only the verified tokenizer files, even if the hub cache contains extras.
    with tempfile.TemporaryDirectory() as folder:
        for name, path in files.items():
            if name != TEXT_WEIGHTS:
                shutil.copyfile(path, Path(folder) / name)
        return AutoTokenizer.from_pretrained(folder, local_files_only=True, trust_remote_code=False, use_fast=True)


def ranker_manifest(provenance):
    meta = {**provenance, "schema": RANKER_SCHEMA, "origin": "synthetic", "eligible_for_production": False,
            "feature_version": FEATURE_VERSION, "policy_version": POLICY_VERSION, "label_version": LABEL_VERSION,
            "features": FEATURES, "embedding_space": EMBEDDING_ID}
    return {**meta, "model_id": "catboost-" + manifest_id(meta)}


def load_research_ranker(path):
    meta = json.loads(path.with_suffix(".json").read_bytes())
    model_id = meta.pop("model_id", None)
    identity = ("catboost-" + manifest_id(meta))
    if (model_id != identity or meta.get("schema") != RANKER_SCHEMA or meta.get("origin") != "synthetic"
            or meta.get("eligible_for_production") is not False or meta.get("features") != FEATURES
            or meta.get("feature_version") != FEATURE_VERSION or meta.get("policy_version") != POLICY_VERSION
            or meta.get("label_version") != LABEL_VERSION or meta.get("embedding_space") != EMBEDDING_ID
            or meta.get("runtime") != runtime_versions(("catboost", "numpy"))
            or meta.get("weights_sha256") != file_sha256(path)):
        raise ValueError("incompatible or damaged research ranker artifact")
    required = ("fixture_sha256", "protocol_sha256", "ranker_binary_sha256", "corpus_sha256")
    if (any(not HEX.fullmatch(str(meta.get(key, ""))) for key in required)
            or set(meta.get("split_sha256", {})) != {"train", "validation", "test"}
            or not meta.get("code_sha256")
            or any(not HEX.fullmatch(str(value)) for field in ("split_sha256", "code_sha256")
                   for value in meta[field].values())
            or type(meta.get("seed")) is not int or not isinstance(meta.get("params"), dict)):
        raise ValueError("incomplete research ranker provenance")
    from catboost import CatBoostRanker

    ranker = CatBoostRanker()
    ranker.load_model(str(path))
    if ranker.feature_names_ != FEATURES:
        raise ValueError("ranker weights use a different feature order")
    return ranker, model_id


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prefetch-e5", "verify-e5"))
    args = parser.parse_args()
    resolve_text(download=args.action == "prefetch-e5")
    print(canonical(text_manifest(2)).decode().strip())


if __name__ == "__main__":
    main()
