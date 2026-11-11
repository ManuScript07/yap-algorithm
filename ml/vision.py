"""Pinned CPU encoders for the offline image semantics comparison."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path

from dataset import canonical, digest


@dataclass(frozen=True)
class VisionModel:
    source: str
    repository: str
    revision: str
    license: str
    dimension: int
    tokens: int
    fixed_text_padding: bool
    vision_sha: str
    text_sha: str
    image_output: str
    text_output: str
    tokenizer_files: tuple[str, ...]


MODELS = {
    "clip": VisionModel(
        "openai/clip-vit-base-patch32", "Xenova/clip-vit-base-patch32",
        "6b7c523703ae26e39be4b03774966f8571c7f523", "MIT", 512, 77, False,
        "fd6e1402a588279d1723c7534d4bcba5bc0b14b47dfab0e46f8c47b8270d7d40",
        "18845f2ccc35223bb7fec403383a131154b11ac0918df25cf51986df5efd3a21",
        "image_embeds", "text_embeds",
        ("merges.txt", "vocab.json"),
    ),
    "siglip2": VisionModel(
        "google/siglip2-base-patch16-224", "onnx-community/siglip2-base-patch16-224-ONNX",
        "ba1f3b0843f24bc5417d38e19c37b287d719b2f4", "Apache-2.0", 768, 64, True,
        "c0573e3f4140c3a7c4e9cc5912bd6b26a033b46a6a8e8af26cbea262b163bcad",
        "3a0603d3a00c05a80a6ded4743c16aaac7b1e62cdcc7e362e7ce418659b96400",
        "pooler_output", "pooler_output",
        ("tokenizer.model",),
    ),
}
VISION_WEIGHTS = "onnx/vision_model.onnx"
TEXT_WEIGHTS = "onnx/text_model_int8.onnx"
MAX_IMAGE_BYTES = 5 * 1024 * 1024
MAX_IMAGE_PIXELS = 20_000_000


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def artifacts(model, download):
    from huggingface_hub import hf_hub_download

    names = [
        "config.json", "preprocessor_config.json", "tokenizer.json", "tokenizer_config.json",
        "special_tokens_map.json", *model.tokenizer_files, VISION_WEIGHTS, TEXT_WEIGHTS,
    ]
    files = {}
    for name in names:
        path = hf_hub_download(
            model.repository, name, revision=model.revision, token=False, local_files_only=not download,
        )
        files[name] = Path(path)
    hashes = {name: {"bytes": file.stat().st_size, "sha256": sha256(file)} for name, file in files.items()}
    if hashes[VISION_WEIGHTS]["sha256"] != model.vision_sha or hashes[TEXT_WEIGHTS]["sha256"] != model.text_sha:
        raise ValueError("vision checkpoint does not match the pinned SHA-256")
    space = {
        "source": model.source, "export": model.repository, "revision": model.revision,
        "license": model.license, "dimension": model.dimension, "precision": {"image": "fp32-onnx", "text": "int8-onnx"},
        "text_max_tokens": model.tokens, "fixed_text_padding": model.fixed_text_padding,
        "preprocessing": "RGB with EXIF transpose, model image processor (slow), v1",
        "text_prefix": "", "image_output": model.image_output, "text_output": model.text_output,
        "normalization": "l2",
        "artifacts": hashes,
    }
    return files, {**space, "id": digest(canonical(space))}


def open_photo(source):
    from io import BytesIO
    from PIL import Image, ImageOps

    size = len(source) if isinstance(source, bytes) else source.stat().st_size
    if not 0 < size <= MAX_IMAGE_BYTES:
        raise ValueError("invalid photo size")
    with Image.open(BytesIO(source) if isinstance(source, bytes) else source, formats=["JPEG", "PNG"]) as image:
        if image.width * image.height > MAX_IMAGE_PIXELS or getattr(image, "n_frames", 1) != 1:
            raise ValueError("unsupported photo dimensions or frame count")
        return ImageOps.exif_transpose(image).convert("RGB")


def project(session, inputs, output, dimension, run_options=None):
    import numpy as np

    feeds = {item.name: np.asarray(inputs[item.name]) for item in session.get_inputs()}
    raw = session.run([output], feeds, run_options) if run_options else session.run([output], feeds)
    result = np.asarray(raw[0], dtype=np.float32)
    batch = len(next(iter(feeds.values())))
    if result.shape != (batch, dimension) or not np.isfinite(result).all():
        raise ValueError("invalid projected embeddings")
    norms = np.linalg.norm(result, axis=1, keepdims=True)
    if not np.isfinite(norms).all() or np.any(norms < 1e-12):
        raise ValueError("invalid embedding norm")
    return result / norms


class VisionEncoder:
    def __init__(self, model, files, threads):
        import onnxruntime as ort
        from transformers import AutoImageProcessor, AutoTokenizer

        self.model = model
        self.files = files
        self.options = ort.SessionOptions()
        self.options.intra_op_num_threads = threads
        self.options.inter_op_num_threads = 1
        self.processor = AutoImageProcessor.from_pretrained(
            files["preprocessor_config.json"].parent, local_files_only=True, trust_remote_code=False, use_fast=False,
        )
        self.vision = self._session(VISION_WEIGHTS, model.image_output)
        self.text_model = None
        self.tokenizer = None
        self.tokenizer_type = AutoTokenizer

    def _session(self, name, output):
        import onnxruntime as ort

        session = ort.InferenceSession(str(self.files[name]), sess_options=self.options,
                                       providers=["CPUExecutionProvider"])
        if output not in {item.name for item in session.get_outputs()}:
            raise ValueError(f"model lacks expected projected output: {output}")
        return session

    def images(self, sources, run_options=None):
        from contextlib import ExitStack

        with ExitStack() as stack:
            images = [stack.enter_context(open_photo(source)) for source in sources]
            inputs = self.processor(images=images, return_tensors="np")
            return project(self.vision, inputs, self.model.image_output, self.model.dimension, run_options)

    def texts(self, texts):
        if self.text_model is None:
            self.tokenizer = self.tokenizer_type.from_pretrained(
                self.files["tokenizer.json"].parent, local_files_only=True, trust_remote_code=False,
            )
            self.text_model = self._session(TEXT_WEIGHTS, self.model.text_output)
        inputs = self.tokenizer(
            texts, max_length=self.model.tokens, truncation=True,
            padding="max_length" if self.model.fixed_text_padding else True, return_tensors="np",
        )
        return project(self.text_model, inputs, self.model.text_output, self.model.dimension)
