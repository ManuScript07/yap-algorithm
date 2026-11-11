import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import numpy as np

from benchmark_text import canonical, compare_quality, digest, evaluate, load_corpus, measures
from embedding import encode_batch


class BenchmarkTextTests(unittest.TestCase):
    def test_comparison_pairs_by_query_id_and_rejects_another_corpus(self):
        def report(values):
            return {
                "corpus": "fixture", "k": 10, "model": "baseline", "space": {"id": "test"},
                "quality": {"all": {"per_query": [
                    {"id": key, "topic": "cats", "recall": value, "ndcg": value}
                    for key, value in values
                ]}},
            }

        baseline = report([("a", 0.5), ("b", 0.0)])
        current = report([("b", 1.0), ("a", 0.0)])
        self.assertEqual(compare_quality(current, baseline, 630)["slices"]["all"]["ndcg"], {
            "delta": 0.25, "topic_bootstrap_95": [0.25, 0.25],
        })
        current["corpus"] = "another-corpus"
        with self.assertRaisesRegex(ValueError, "same corpus"):
            compare_quality(current, baseline, 630)

    def test_graded_retrieval_penalizes_unknowns_and_missed_relevant_documents(self):
        grades = {"strong": 2, "weak": 1, "negative": 0}
        result = measures(["unknown", "weak", "strong"], grades, 2)
        self.assertEqual(result["recall"], 0.5)
        self.assertGreater(result["ndcg"], 0.0)
        self.assertLess(result["ndcg"], 0.2)
        self.assertEqual(measures(["strong", "weak"], grades, 2), {"ndcg": 1.0, "recall": 1.0})

    def test_cohorts_filter_documents_before_selecting_top_k(self):
        documents = [
            {"id": "ru-relevant", "language": "ru"},
            {"id": "en-relevant", "language": "en"},
            {"id": "en-unknown", "language": "en"},
        ]
        queries = [{"id": "q", "language": "ru", "text": "кот", "topic": "cats"}]
        result = evaluate(documents, queries, {"q": {"ru-relevant": 2, "en-relevant": 2}}, [[0.1, 0.2, 0.9]], 1)
        self.assertEqual(result["ru_to_ru"]["recall"], 1.0)
        self.assertEqual(result["ru_cross"]["recall"], 0.0)
        self.assertEqual(result["short_queries"]["ndcg"], 0.0)
        self.assertEqual(result["authored_contrasts"]["ndcg"], 1.0)

    def test_corpus_rejects_modified_file_even_when_row_count_is_unchanged(self):
        rows = {
            "documents": [{"id": "a", "kind": "text", "text": "original"}],
            "queries": [{"id": "q"}],
            "judgments": [{"query_id": "q", "document_id": "a", "relevance": 2}],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            manifest = {"schema": "yap-quality-corpus-v1", "files": {}}
            for name, values in rows.items():
                raw = b"".join(map(canonical, values))
                (path / f"{name}.jsonl").write_bytes(raw)
                manifest["files"][name] = {"count": len(values), "sha256": digest(raw)}
            manifest["dataset_sha256"] = digest(canonical(manifest))
            (path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            self.assertEqual(load_corpus(path)[1][0]["text"], "original")
            rows["documents"][0]["text"] = "modified"
            (path / "documents.jsonl").write_bytes(canonical(rows["documents"][0]))
            with self.assertRaisesRegex(ValueError, "invalid corpus file: documents"):
                load_corpus(path)

    def test_serving_prefix_padding_and_pooling_are_preserved(self):
        def tokenizer(texts, **options):
            self.assertEqual(texts, ["query: кот", "query: tea"])
            self.assertEqual(options, dict(max_length=512, truncation=True, padding=True, return_tensors="np"))
            return {"input_ids": [[1, 2, 0], [1, 2, 3]], "attention_mask": [[1, 1, 0], [1, 1, 1]]}

        def inference(_, feeds):
            np.testing.assert_array_equal(feeds["token_type_ids"], np.zeros((2, 3), dtype=np.int64))
            return [np.array([[[2, 0], [0, 2], [999, 999]], [[0, 2], [0, 1], [0, 1]]], dtype=np.float32)]

        session = SimpleNamespace(
            run=inference, get_inputs=lambda: [SimpleNamespace(name=n) for n in ("input_ids", "attention_mask", "token_type_ids")],
        )
        options = dict(prefix="query: ", max_tokens=512)
        mean = encode_batch(session, tokenizer, ["кот", "tea"], pooling="mean", **options)
        cls = encode_batch(session, tokenizer, ["кот", "tea"], pooling="cls", **options)
        np.testing.assert_allclose(mean, [[2**-0.5, 2**-0.5], [0, 1]], atol=1e-6)
        np.testing.assert_array_equal(cls, [[1, 0], [0, 1]])
