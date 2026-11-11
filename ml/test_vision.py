from pathlib import Path
import base64
import json
import time
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

import numpy as np

from benchmark_vision import evaluate, fuse, photo_scores, scenarios
from vision import open_photo, project
from vision_worker import PreparedStorage


class VisionTests(unittest.TestCase):
    def test_storage_adapter_only_accepts_leased_derivative_path_shape(self):
        def token(role):
            data = json.dumps({"role": role, "exp": int(time.time()) + 3600}).encode()
            return "header." + base64.urlsafe_b64encode(data).decode().rstrip("=") + ".signature"

        key, worker = token("anon"), token("yap_feed_vision_worker")
        storage = PreparedStorage("http://127.0.0.1:54321", key, worker)
        for path in ("../image.jpg", "https://example.org/image.jpg", "a/thumbnail.jpg", "a//image.jpg"):
            with self.assertRaises(ValueError):
                storage.read(path, 1)
        with self.assertRaises(ValueError):
            PreparedStorage("https://example.org/feed-media", key, worker)
        with self.assertRaises(ValueError):
            PreparedStorage("http://127.0.0.1:54321", token("service_role"), worker)
        with self.assertRaises(ValueError):
            PreparedStorage("http://127.0.0.1:54321", key, token("authenticated"))

    def test_photo_similarity_deduplicates_and_caps_photos(self):
        query = np.asarray([[1.0, 0.0]])
        photos = {"a": np.asarray([1.0, 0.0]), "b": np.asarray([0.0, 1.0])}
        for policy in ("max", "mean"):
            with self.subTest(policy=policy):
                one = photo_scores(query, photos, ["a"], policy)
                np.testing.assert_array_equal(one, photo_scores(query, photos, ["a", "a", "a"], policy))
                mixed = photo_scores(query, photos, ["a", "b"], policy)
                self.assertLessEqual(mixed[0], one[0])
                with self.assertRaises(ValueError):
                    photo_scores(query, photos, ["a"] * 4, policy)
        self.assertLess(photo_scores(query, photos, ["a", "b"], "mean")[0], 1.0)

    def test_vision_or_text_can_supply_fallback_without_additive_boost(self):
        text = np.asarray([[5.0, 5.0, np.nan, np.nan], [4.0, 2.0, 1.0, 0.0]])
        vision = np.asarray([[6.0, np.nan, 6.0, np.nan], [40.0, 20.0, 10.0, 0.0]])
        actual = fuse(text, vision)
        np.testing.assert_array_equal(actual[0], [0.5, 0.5, 0.5, 0.0])
        np.testing.assert_array_equal(actual[1], [0.875, 0.625, 0.375, 0.125])

    def test_photo_retrieval_cannot_rank_its_own_example(self):
        query = [{"id": "self", "topic": "cats"}]
        targets = [*query, {"id": "other", "topic": "cats"}, {"id": "dog", "topic": "dogs"}]
        score = [[1.0, 0.1, 0.7]]
        with_self = evaluate(score, query, targets, k=1)
        excluded = evaluate(score, query, targets, exclude_self=True, k=1)
        self.assertEqual(with_self["ndcg"], 1.0)
        self.assertEqual(excluded["ndcg"], 0.0)

    def test_conflicting_caption_does_not_change_photos_or_reference_topic(self):
        case = scenarios({"text": "cats", "photos": ["a", "b", "c"]}, {"text": "dogs", "photos": ["d"]})
        self.assertEqual(case["aligned"].photos, case["caption_conflict"].photos)
        self.assertEqual(case["caption_conflict"].caption, "dogs")
        self.assertEqual(case["mixed_topics"].photos, ("a", "b", "d"))

    def test_projected_output_requires_finite_nonzero_correct_dimension(self):
        inputs = {"pixel_values": np.zeros((1, 3, 224, 224)), "unused": []}

        def session(value):
            def run(outputs, feeds):
                self.assertEqual(outputs, ["pooler_output"])
                self.assertEqual(set(feeds), {"pixel_values"})
                return [value]
            return SimpleNamespace(get_inputs=lambda: [SimpleNamespace(name="pixel_values")], run=run)

        np.testing.assert_allclose(project(session([[3, 4]]), inputs, "pooler_output", 2), [[0.6, 0.8]])
        for bad in ([[0, 0]], [[1]], [[np.inf, 1]]):
            with self.assertRaises(ValueError):
                project(session(bad), inputs, "pooler_output", 2)

    def test_decoder_rejects_animated_png(self):
        from PIL import Image

        with TemporaryDirectory() as folder:
            path = Path(folder) / "animation.png"
            with Image.new("RGB", (4, 4), "red") as red, Image.new("RGB", (4, 4), "blue") as blue:
                red.save(path, save_all=True, append_images=[blue], duration=100)
            with self.assertRaisesRegex(ValueError, "frame count"):
                open_photo(path)
