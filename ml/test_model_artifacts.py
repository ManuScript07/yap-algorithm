import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from model_artifacts import (
    FEATURES, canonical, load_research_ranker, manifest_id, ranker_manifest, verify_files,
)


class ModelArtifactsTests(unittest.TestCase):
    def test_checksum_covers_tokenizer_as_well_as_weights(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            files = {name: root / name for name in ("weights", "tokenizer")}
            for path in files.values():
                path.write_bytes(path.name.encode())
            expected = {name: hashlib.sha256(name.encode()).hexdigest() for name in files}
            verify_files(files, expected)
            files["tokenizer"].write_bytes(b"modified")
            with self.assertRaisesRegex(ValueError, "checksum mismatch: tokenizer"):
                verify_files(files, expected)

    def test_research_ranker_rejects_incompatible_or_damaged_inputs_before_model_load(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "ranker.cbm"
            path.write_bytes(b"not a valid model")
            runtime = {"catboost": "test"}
            actual = hashlib.sha256(path.read_bytes()).hexdigest()
            original = ranker_manifest({"runtime": runtime, "weights_sha256": actual})
            for changed in ({"features": list(reversed(FEATURES))}, {"embedding_space": "other-space"},
                            {"eligible_for_production": True}, {"weights_sha256": "0" * 64}):
                with self.subTest(changed=changed):
                    meta = original | changed
                    del meta["model_id"]
                    meta["model_id"] = "catboost-" + manifest_id(meta)
                    path.with_suffix(".json").write_bytes(canonical(meta))
                    with patch("model_artifacts.runtime_versions", return_value=runtime):
                        with self.assertRaisesRegex(ValueError, "incompatible or damaged"):
                            load_research_ranker(path)
