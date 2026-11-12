"""Local CPU embedding / optional trained ranking service. No client credentials."""
from __future__ import annotations
import argparse
from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import ipaddress
import math
import os
from pathlib import Path
import threading

if __package__:
    from .embedding import encode_batch
    from .model_artifacts import (
        EMBEDDING_ID, FEATURES, FEATURE_VERSION, MODEL, REVISION, DIMENSIONS,
        TEXT_PREPROCESSING, TEXT_WEIGHTS, load_research_ranker, load_text_tokenizer,
        manifest_id, resolve_text, text_manifest,
    )
else:
    from embedding import encode_batch
    from model_artifacts import (
        EMBEDDING_ID, FEATURES, FEATURE_VERSION, MODEL, REVISION, DIMENSIONS,
        TEXT_PREPROCESSING, TEXT_WEIGHTS, load_research_ranker, load_text_tokenizer,
        manifest_id, resolve_text, text_manifest,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8091)
    parser.add_argument("--ranker", type=Path)
    parser.add_argument("--allow-research-ranker", action="store_true")
    parser.add_argument("--threads", type=int, choices=range(1, 9), default=2)
    args = parser.parse_args()
    if args.ranker and (not args.allow_research_ranker or not ipaddress.ip_address(args.host).is_loopback):
        parser.error("research ranker requires explicit opt-in and a loopback address")
    os.environ.setdefault("USE_TORCH", "0")
    os.environ.setdefault("TRANSFORMERS_VERBOSITY", "error")
    import onnxruntime as ort

    files = resolve_text()
    artifact_id = manifest_id(text_manifest(args.threads))
    tokenizer = load_text_tokenizer(files)
    options = ort.SessionOptions()
    options.intra_op_num_threads = args.threads
    options.inter_op_num_threads = 1
    encoder = ort.InferenceSession(str(files[TEXT_WEIGHTS]), sess_options=options, providers=["CPUExecutionProvider"])
    gate = threading.Lock()
    ranker = None
    rank_version = None
    if args.ranker:
        ranker, rank_version = load_research_ranker(args.ranker)

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
                pooled = encode_batch(
                    encoder, tokenizer, missing, prefix=TEXT_PREPROCESSING["prefix"],
                    pooling=TEXT_PREPROCESSING["pooling"], max_tokens=TEXT_PREPROCESSING["max_tokens"],
                )
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
                self.reply(200, {"embedding_model": EMBEDDING_ID, "artifact_id": artifact_id, "ranker": rank_version})
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
                    if data["feature_version"] != FEATURE_VERSION or not 0 < len(features) <= 1200:
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

    print(json.dumps({"ready": True, "embedding_model": EMBEDDING_ID,
                      "artifact_id": artifact_id, "ranker": rank_version}), flush=True)
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
