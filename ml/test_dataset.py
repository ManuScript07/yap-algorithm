import copy
import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from dataset import build, canonical, digest, duplicate_components, publish, read_capture
from test_feedback import BASE, fact

SIG = ["personalized-v2", "feed-features-v2", "hybrid-v2", "space-v1", "space-v1"]


def timestamp(sec):
    return BASE + timedelta(seconds=sec)


def moment(sec):
    return timestamp(sec).isoformat()


def fingerprint(identifier, shingles=(), photos=()):
    return dict(id=identifier, text_sha256=None, text_bigrams=list(shingles), photos=list(photos))


def row(seq, sec, kind="impression", **changes):
    values = fact(seq, kind)
    values.update(recorded_at=moment(sec), transaction_id=f"tx-{seq}", context={}, content=None)
    if kind == "impression":
        values.update(context=dict(zip(("model", "feature_version", "policy_version", "embedding_space",
                                       "embedding_model"), SIG), features_at=moment(sec-1),
                                   snapshot_id=f"snap-{seq}", position=0, features=[0.5] * 8),
                      content=dict(author_id="author", language="en",
                                   fingerprints=[fingerprint(f"post-{seq}")]))
    values.update(changes)
    return values


class DatasetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = dict(origin="synthetic", seed=42, snapshot_signature=SIG, label_window_seconds=5,
                           embargo_seconds=5, splits={name: dict(capture=name, **{"from": moment(start)},
                               available_by=moment(start+20))
                               for name, start in (("train", 0), ("validation", 30), ("test", 60))})
        self.rows = [row(1, 2), row(2, 3, "like"), row(3, 5, "like", active=False)]
        self.capture("train", 15, self.rows)
        self.capture("validation", 45, self.rows + [row(4, 33, user_id="new-user", post_id="new-post")])
        self.capture("test", 75, self.rows + [row(5, 63, post_id="next-post")])

    def capture(self, name, sec, rows, **overrides):
        folder = self.root / name
        folder.mkdir(exist_ok=True)
        raw = b"".join(map(canonical, rows))
        (folder / "facts.jsonl").write_bytes(raw)
        db = dict(cohort_id="cohort", origin="synthetic", seed=42, source_hash="a"*64, member_sha256="b"*64,
                  sql_sha256="c"*64, schema_version=1, feedback_contract=1, history_start=moment(0),
                  snapshot_started_at=moment(sec-1), settled_through=moment(sec-1), unmatched_writer_count=0)
        db.update(overrides)
        (folder / "manifest.json").write_bytes(canonical(dict(schema="yap-feedback-capture-v1", database=db,
            binary_sha256="d"*64, completed_at=moment(sec), rows=len(rows), last_sequence=rows[-1]["sequence"],
            excluded_at_source=0, facts_sha256=digest(raw))))

    def test_prospective_replay_and_state_cancellation(self):
        manifest, payload = build(self.config, self.root)
        self.assertEqual((manifest, payload), build(self.config, self.root))
        self.assertEqual([v["rows"] for v in manifest["splits"].values()], [1, 1, 1])
        train = json.loads(payload["train"])
        self.assertIsNone(train["baseline_state"]["liked"])
        self.assertIs(train["outcomes"]["liked"], False)
        self.assertFalse(train["known"]["less_similar"])
        self.assertTrue(json.loads(payload["validation"])["cold_user"])
        self.assertFalse(json.loads(payload["test"])["cold_user"])
        publish(self.root / "built", manifest, payload)
        with self.assertRaises(FileExistsError):
            publish(self.root / "built", manifest, payload)

    def test_future_capture_and_overlapping_embargo_are_rejected(self):
        config = copy.deepcopy(self.config)
        config["splits"]["train"]["available_by"] = moment(14)
        with self.assertRaisesRegex(ValueError, "prospective"):
            build(config, self.root)
        config = copy.deepcopy(self.config)
        config["splits"]["validation"]["from"] = moment(24)
        with self.assertRaisesRegex(ValueError, "prospective"):
            build(config, self.root)

    def test_unknown_fence_tampered_payload_and_unsettled_window(self):
        self.capture("train", 15, self.rows, settled_through=moment(6))
        m, payload = build(self.config, self.root)
        self.assertFalse(payload["train"])
        self.assertEqual(m["exclusions"]["train"]["unsettled_or_cross_boundary_group"], 1)
        self.capture("train", 15, self.rows, settled_through=None, unmatched_writer_count=1)
        with self.assertRaisesRegex(ValueError, "settled"):
            build(self.config, self.root)
        self.capture("train", 15, self.rows)
        with (self.root / "train/facts.jsonl").open("ab") as stream:
            stream.write(b" ")
        with self.assertRaises(ValueError):
            read_capture(self.root / "train")

    def test_duplicate_conflict_discards_the_whole_group(self):
        train = self.rows[0]
        one = row(4, 33, user_id="new-user", post_id="new-post")
        two = row(6, 34, user_id="new-user", post_id="another-post")
        one["content"] = copy.deepcopy(train["content"])
        two["transaction_id"] = one["transaction_id"]
        two["context"].update(snapshot_id=one["context"]["snapshot_id"], position=1)
        self.capture("validation", 45, self.rows + [one, two])
        m, payload = build(self.config, self.root)
        self.assertFalse(payload["validation"])
        self.assertEqual(m["exclusions"]["validation"]["duplicate_cross_split_group"], 2)

    def test_transitive_text_and_photo_duplicates_do_not_depend_on_input_order(self):
        a = fingerprint("a", "abcd")
        b = fingerprint("b", "abcde", ["1"*64])
        c = fingerprint("c", "bcdef", ["2"*64])
        d = fingerprint("d", photos=["3"*64])
        photos = {"1"*64: "0"*16, "2"*64: "0"*15+"7", "3"*64: "0"*14+"ff"}
        batch = [[{"fingerprints": [fp]}] for fp in (a, b, c, d)]
        forward = duplicate_components(batch, photos)
        self.assertEqual(forward, duplicate_components(reversed(batch), photos))
        self.assertEqual(len(set(forward.values())), 1)

    def test_missing_photo_fingerprint_and_unknown_features_are_excluded(self):
        a = row(1, 2)
        a["content"]["fingerprints"][0]["photos"] = ["1"*64]
        self.capture("train", 15, [a])
        m, _ = build(self.config, self.root)
        self.assertEqual(m["exclusions"]["train"]["missing_photo_fingerprint_group"], 1)
        a["context"]["features"] = [1]
        self.capture("train", 15, [a])
        m, _ = build(self.config, self.root)
        self.assertEqual(m["exclusions"]["train"]["incompatible_producing_snapshot"], 1)


if __name__ == "__main__":
    unittest.main()
