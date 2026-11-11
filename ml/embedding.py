"""Shared ONNX encoding used by the model service and offline benchmark."""
from __future__ import annotations


def encode_batch(session, tokenizer, texts, *, prefix, pooling, max_tokens):
    import numpy as np

    tokens = tokenizer(
        [prefix + text for text in texts], max_length=max_tokens,
        truncation=True, padding=True, return_tensors="np",
    )
    names = {item.name for item in session.get_inputs()}
    feeds = {name: np.asarray(value, dtype=np.int64) for name, value in tokens.items() if name in names}
    if "token_type_ids" in names and "token_type_ids" not in feeds:
        feeds["token_type_ids"] = np.zeros_like(feeds["input_ids"], dtype=np.int64)
    hidden = session.run(None, feeds)[0]
    if hidden.ndim != 3 or hidden.shape[:2] != feeds["input_ids"].shape:
        raise ValueError("expected per-token ONNX embeddings")
    if pooling == "mean":
        mask = np.asarray(tokens["attention_mask"], dtype=np.float32)[..., None]
        pooled = (hidden * mask).sum(axis=1) / mask.sum(axis=1).clip(min=1)
    elif pooling == "cls":
        pooled = hidden[:, 0, :].copy()
    else:
        raise ValueError(f"unsupported pooling: {pooling}")
    pooled /= np.linalg.norm(pooled, axis=1, keepdims=True).clip(min=1e-12)
    return pooled
