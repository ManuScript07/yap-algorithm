"""Local CPU embedding / optional trained ranking service. No client credentials."""
from __future__ import annotations
import argparse
from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
import threading

MODEL = "intfloat/multilingual-e5-small"
REVISION = "614241f622f53c4eeff9890bdc4f31cfecc418b3"
EMBEDDING_ID = f"{MODEL}@{REVISION}:onnx-int8:query"
FEATURES = ["semantic", "social", "quality", "freshness", "negative_similarity", "low_exposure", "nearby", "seen"]
DIMENSIONS = 384


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8091)
    parser.add_argument("--ranker", type=Path)
    parser.add_argument("--allow-research-ranker", action="store_true")
    parser.add_argument("--threads", type=int, default=2)
    args = parser.parse_args()
    os.environ.setdefault("USE_TORCH", "0")
    os.environ.setdefault("TRANSFORMERS_VERBOSITY", "error")
    import numpy as np
    from transformers import AutoTokenizer
    from huggingface_hub import hf_hub_download
    import onnxruntime as ort
    tokenizer = AutoTokenizer.from_pretrained(MODEL, revision=REVISION)
    model_path = hf_hub_download(MODEL, "onnx/model_qint8_avx512_vnni.onnx", revision=REVISION)
    options = ort.SessionOptions()
    options.intra_op_num_threads = args.threads
    options.inter_op_num_threads = 1
    encoder = ort.InferenceSession(model_path, sess_options=options, providers=["CPUExecutionProvider"])
    input_names = {x.name for x in encoder.get_inputs()}
    gate = threading.Lock()
    ranker = None
    rank_version = None
    if args.ranker:
        from catboost import CatBoostRanker
        meta = json.loads(args.ranker.with_suffix(".json").read_text(encoding="utf-8"))
        if meta["feature_version"] != "hybrid-v1" or meta["features"] != FEATURES or meta["embedding_model"] != EMBEDDING_ID:
            raise ValueError("Incompatible ranking artifact")
        if not meta.get("eligible_for_production") and not args.allow_research_ranker:
            raise ValueError("Research-only ranker; use --allow-research-ranker for local experiments")
        ranker = CatBoostRanker()
        ranker.load_model(str(args.ranker))
        rank_version = meta["version"]

    embedding_cache = OrderedDict()

    def embed_batch(texts):
        # One bounded ONNX invocation. Deduplicate repeated texts and share the
        # same bounded cache between single and batch requests.
        with gate:
            for text in texts:
                if text in embedding_cache:
                    embedding_cache.move_to_end(text)
            missing = list(dict.fromkeys(text for text in texts if text not in embedding_cache))
            if missing:
                tokens = tokenizer(["query: " + text for text in missing], max_length=512, truncation=True, padding=True, return_tensors="np")
                feeds = {name: np.asarray(value, dtype=np.int64) for name, value in tokens.items() if name in input_names}
                if "token_type_ids" in input_names and "token_type_ids" not in feeds:
                    feeds["token_type_ids"] = np.zeros_like(feeds["input_ids"], dtype=np.int64)
                hidden = encoder.run(None, feeds)[0]
                mask = np.asarray(tokens["attention_mask"], dtype=np.float32)[..., None]
                pooled = (hidden * mask).sum(axis=1) / mask.sum(axis=1).clip(min=1)
                pooled /= np.linalg.norm(pooled, axis=1, keepdims=True).clip(min=1e-12)
                for text, vector in zip(missing, pooled.tolist()):
                    embedding_cache[text] = vector
                while len(embedding_cache) > 2048:
                    embedding_cache.popitem(last=False)
            return [embedding_cache[text] for text in texts]

    def embed(text):
        return embed_batch([text])[0]

    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(20)

        def reply(self, status, data):
            body = json.dumps(data, allow_nan=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/healthz":
                self.reply(200, {"embedding_model": EMBEDDING_ID, "ranker": rank_version})
            else:
                self.reply(404, {"error": "not_found"})

        def do_POST(self):
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 1 or length > 1_000_000:
                    raise ValueError("body_size")
                data = json.loads(self.rfile.read(length))
                if self.path == "/embed":
                    text = data["text"]
                    if not isinstance(text, str) or not text.strip() or len(text) > 5000:
                        raise ValueError("text")
                    self.reply(200, {"embedding": embed(text), "revision": EMBEDDING_ID})
                elif self.path == "/embed-batch":
                    texts = data["texts"]
                    if not isinstance(texts, list) or not 1 <= len(texts) <= 8:
                        raise ValueError("batch")
                    if any(not isinstance(text, str) or not text.strip() or len(text) > 5000 for text in texts):
                        raise ValueError("texts")
                    self.reply(200, {"embeddings": embed_batch(texts), "revision": EMBEDDING_ID})
                elif self.path == "/score":
                    if ranker is None:
                        self.reply(503, {"error": "ranker_not_loaded"})
                        return
                    features = data["features"]
                    if data["feature_version"] != "hybrid-v1" or not 0 < len(features) <= 1200:
                        raise ValueError("features")
                    if any(len(row) != len(FEATURES) or any(not isinstance(x, (int, float)) or not math.isfinite(x) for x in row) for row in features):
                        raise ValueError("features")
                    with gate:
                        raw = ranker.predict(features, thread_count=args.threads)
                    # Bounded utility for MMR, NOT a calibrated action probability.
                    scores = [1 / (1 + math.exp(-max(-30, min(30, float(x))))) for x in raw]
                    self.reply(200, {"scores": scores, "version": rank_version})
                else:
                    self.reply(404, {"error": "not_found"})
            except (ValueError, TypeError, KeyError, json.JSONDecodeError):
                self.reply(400, {"error": "invalid_request"})

        def log_message(self, *args):
            pass  # Never log input texts or authorization headers.

    print(json.dumps({"ready": True, "embedding_model": EMBEDDING_ID, "ranker": rank_version}), flush=True)
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
