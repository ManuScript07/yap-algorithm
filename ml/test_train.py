import unittest
from train import ndcg


class RankingMetricTests(unittest.TestCase):
    def test_perfect_and_reversed_order(self):
        self.assertAlmostEqual(ndcg([3, 2, 0], [3, 2, 0]), 1)
        self.assertLess(ndcg([3, 2, 0], [0, 2, 3]), 1)

    def test_no_observed_positive_is_not_perfect_relevance(self):
        self.assertIsNone(ndcg([0, 0], [1, 2]))


if __name__ == "__main__":
    unittest.main()
