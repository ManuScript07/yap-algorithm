from datetime import datetime, timedelta, timezone
import json
import unittest

from benchmark_ranking import PROTOCOL, compare_users, metric, scoped_order
from ranking_fixture import eligible, partition, source_pools, utility


class OfflineRankingTests(unittest.TestCase):
    def test_common_pool_and_retrieval_denominator(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        items = []
        for i in range(60):
            items.append(dict(id=str(i), author=str(i), topic="t", gain=int(i == 59), relevant=i == 59,
                              new_author=False, negative=False, conflict=False, popularity=60-i,
                              semantic=0, go=dict(CreatedAt=now, Social=False, Seen=False,
                                                  AuthorViewers=40, Visual=1 if i == 59 else 0)))
        protocol = json.loads(PROTOCOL.read_bytes())
        pools = source_pools(items, protocol, True)
        self.assertEqual(pools["fixed_vision"]["IDs"], pools["hybrid"]["IDs"])
        self.assertNotIn(59, pools["hybrid"]["IDs"])
        self.assertIn(59, pools["hybrid_vision"]["IDs"])
        self.assertLessEqual(len(pools["hybrid_vision"]["IDs"]), protocol["pool_budget"])
        no_vision = source_pools(items, protocol, False)
        self.assertEqual(no_vision["hybrid_vision"], no_vision["hybrid"])
        by_id = {p["id"]: p for p in items}
        self.assertEqual(metric(["0"], list(by_id), by_id, 10)["ndcg"], 0)
        self.assertIsNone(metric(["0"], ["0"], by_id, 10)["ndcg"])
        with self.assertRaises(ValueError):
            metric(["59"], ["0"], by_id, 10)

    def test_shared_eligibility_and_repeat_fallback(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        post = dict(created=now, removed=False, blocked=False, audience="friends", friend=False, author="other")
        self.assertFalse(eligible(post, "viewer", now))
        self.assertTrue(eligible(dict(post, friend=True), "viewer", now))
        for change in (dict(created=now + timedelta(seconds=1)), dict(removed=True), dict(blocked=True)):
            self.assertFalse(eligible(dict(post, friend=True, **change), "viewer", now))
        ids = list(range(21))
        by_id = {i: {"go": {"Seen": i == 0}} for i in ids}
        self.assertNotIn(0, scoped_order(ids, by_id, lambda i: i))
        self.assertEqual(scoped_order(ids[:-1], by_id, lambda i: i)[-1], 0)

    def test_viewer_clustered_bootstrap_and_transitions(self):
        left = [("a", 1.0), ("a", 1.0), ("a", 1.0), ("b", 0.0)]
        right = [(user, 0.0) for user, _ in left]
        self.assertEqual(compare_users(left, right, 1, 1000),
                         {"viewers": 2, "delta": .5, "ci95": [0, 1]})
        value, evidence = utility({"follows_author": True}, {"follows_author": True, "liked": False,
                                  "less_similar": True, "comment_reported": True},
                                  {"follows_author": 2, "liked": 1, "less_similar": -2})
        self.assertEqual((value, evidence), (-2, {"less_similar": -2}))

    def test_related_texts_share_a_split(self):
        rows = [{"topic": "one", "group": i, "language": lang} for i in range(6) for lang in ("ru", "en")]
        outputs = partition(rows, lambda r: r["group"])
        seen = set()
        for split in outputs.values():
            for group in {row["group"] for row in split}:
                self.assertNotIn(group, seen)
                self.assertEqual(sum(row["group"] == group for row in split), 2)
                seen.add(group)
