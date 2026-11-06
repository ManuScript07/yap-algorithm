import unittest
from datetime import datetime, timedelta, timezone

from feedback import derive_outcomes, ordered_facts


BASE = datetime(2026, 11, 6, tzinfo=timezone.utc)


def fact(seq, kind, active=True, **changes):
    row = dict(contract_version=1, sequence=seq, event_id=f"event-{seq}", recorded_at=BASE + timedelta(seconds=seq),
               user_id="reader", epoch=1, kind=kind, target_type="post", target_id="post",
               post_id="post", post_version=1, author_id="author", target_version=1, active=active)
    if kind in {"impression", "dwell", "open_post"}:
        row.update(delivery_id="receipt", active_ms=0, opened=False, surface="for_you")
    row.update(changes)
    return row


class FeedbackOutcomeTests(unittest.TestCase):
    def outcome(self, *events, exposure="event-1", **bounds):
        return derive_outcomes([fact(1, "impression"), *events], exposure_event=exposure,
                               window_end=bounds.get("end", BASE+timedelta(minutes=30)),
                               as_of=bounds.get("as_of", BASE+timedelta(hours=1)), history_start=BASE)

    def test_no_observed_like_is_unknown_not_dislike(self):
        out = self.outcome()
        self.assertTrue(out["eligible"])
        self.assertIsNone(out["outcomes"]["liked"])
        self.assertFalse(out["known"]["liked"])
        self.assertEqual(out["outcomes"]["active_ms"], 0)

    def test_like_unlike_like_and_reordered_duplicate_exports(self):
        events = [fact(2, "like", state_version=1), fact(3, "like", False, state_version=2), fact(4, "like", state_version=3)]
        self.assertFalse(self.outcome(*events[:2])["outcomes"]["liked"])
        self.assertTrue(self.outcome(events[2], events[0], events[1], events[1])["outcomes"]["liked"])
        self.assertIsNone(self.outcome(*events[:2])["outcomes"]["less_similar"])

    def test_stale_version_does_not_overwrite_newer_state(self):
        self.assertFalse(self.outcome(fact(3, "like", False, state_version=7), fact(2, "like", state_version=6))["outcomes"]["liked"])

    def test_follow_counter_can_restart_after_system_removal(self):
        rows = [fact(2, "follow", False, target_type="author", target_id="author", post_id=None, post_version=None, state_version=7),
                fact(3, "follow", target_type="author", target_id="author", post_id=None, post_version=None, state_version=1)]
        self.assertTrue(self.outcome(*rows)["outcomes"]["follows_author"])

    def test_cumulative_dwell_is_max_not_sum_and_receipts_are_separate(self):
        out = self.outcome(fact(2, "dwell", active_ms=5000), fact(3, "dwell", active_ms=2000),
                           fact(4, "dwell", active_ms=5000), fact(5, "open_post", active_ms=5000, opened=True),
                           fact(6, "dwell", active_ms=100000, delivery_id="other"))
        self.assertEqual(out["outcomes"]["active_ms"], 5000)
        self.assertTrue(out["outcomes"]["opened"])

    def test_attention_on_confirmed_repeat_belongs_to_one_daily_observation(self):
        rows = [fact(2, "dwell", active_ms=5000), fact(3, "impression", delivery_id="second"),
                fact(4, "dwell", delivery_id="second", active_ms=8000)]
        self.assertEqual(self.outcome(*rows)["outcomes"]["active_ms"], 8000)
        self.assertIn("repeat_exposure", self.outcome(*rows, exposure="event-3")["excluded"])

    def test_repost_undo_and_quote_are_separate_entities(self):
        first = fact(2, "repost", target_id="repost")
        undo = fact(3, "repost", False, target_id="repost", target_version=2)
        quote = fact(4, "quote", target_id="quote")
        out = self.outcome(first, undo, quote)["outcomes"]
        self.assertFalse(out["reposted"])
        self.assertTrue(out["quoted"])
        self.assertIsNone(out["liked"])
        self.assertFalse(self.outcome(first, undo, quote, fact(5, "quote", False, target_id="quote"))["outcomes"]["quoted"])

    def test_deleting_one_comment_does_not_cancel_another_or_reply(self):
        out = self.outcome(fact(2, "comment", target_type="comment", target_id="root"),
                           fact(3, "comment", target_type="comment", target_id="reply"),
                           fact(4, "comment", False, target_type="comment", target_id="root"))["outcomes"]
        self.assertTrue(out["commented"])
        self.assertIsNone(out["liked"])

    def test_deleted_or_edited_comment_like_becomes_unknown(self):
        like = fact(2, "comment_like", target_type="comment", target_id="comment", target_version=1)
        changed = fact(3, "comment", target_type="comment", target_id="comment", target_version=2, user_id="other")
        self.assertIsNone(self.outcome(like, changed)["outcomes"]["comment_liked"])
        changed.update(active=False, target_version=1)
        self.assertIsNone(self.outcome(like, changed)["outcomes"]["comment_liked"])

    def test_comment_report_is_not_post_negative_or_dislike(self):
        out = self.outcome(fact(2, "report", target_type="comment", target_id="comment"))["outcomes"]
        self.assertTrue(out["comment_reported"])
        self.assertIsNone(out["post_reported"])
        self.assertIsNone(out["less_similar"])
        self.assertIsNone(out["liked"])

    def test_less_similar_cancel_removes_negative(self):
        self.assertFalse(self.outcome(fact(2, "less_similar"), fact(3, "less_similar", False))["outcomes"]["less_similar"])

    def test_own_comment_like_is_not_discussion_interest(self):
        self.assertIsNone(self.outcome(fact(2, "comment_like", own_content=True))["outcomes"]["comment_liked"])

    def test_post_report_does_not_set_semantic_dislike(self):
        out = self.outcome(fact(2, "report"))["outcomes"]
        self.assertTrue(out["post_reported"])
        self.assertIsNone(out["less_similar"])

    def test_user_report_is_not_a_post_outcome(self):
        out = self.outcome(fact(2, "report", target_type="user", target_id="author", post_id=None, post_version=None))["outcomes"]
        self.assertIsNone(out["post_reported"])
        self.assertIsNone(out["less_similar"])

    def test_no_state_is_invented_before_journal_coverage(self):
        self.assertIsNone(self.outcome(fact(2, "like", recorded_at=BASE-timedelta(days=1)))["outcomes"]["liked"])

    def test_duplicate_impressions_use_utc_day_not_local_timezone(self):
        repeat = fact(2, "impression", recorded_at=(BASE+timedelta(seconds=2)).astimezone(timezone(timedelta(hours=-3))))
        self.assertIn("repeat_exposure", self.outcome(repeat, exposure="event-2")["excluded"])

    def test_large_discussion_reconstructs_entities_in_one_pass(self):
        rows = [fact(i, "comment_like", target_type="comment", target_id=f"comment-{i}") for i in range(2, 10002)]
        # Enlarge only this observation's window: no timing assertion depending
        # on the test host, but enough entities to expose accidental rescanning.
        self.assertTrue(self.outcome(*rows, end=BASE+timedelta(hours=3), as_of=BASE+timedelta(hours=4))["outcomes"]["comment_liked"])

    def test_follow_is_author_state_and_unfollow_is_not_post_dislike(self):
        follow = fact(2, "follow", target_type="author", target_id="author", post_id=None, post_version=None)
        unfollow = dict(follow, sequence=3, event_id="unfollow", active=False)
        self.assertTrue(self.outcome(follow)["outcomes"]["follows_author"])
        out = self.outcome(follow, unfollow)["outcomes"]
        self.assertFalse(out["follows_author"])
        self.assertIsNone(out["liked"])
        self.assertIsNone(out["less_similar"])

    def test_reset_invalidates_old_observation_and_late_old_epoch_action(self):
        reset = fact(2, "reset", target_type="user", target_id="reader", epoch=2, post_id=None, post_version=None)
        out = self.outcome(reset, fact(3, "like", epoch=1))
        self.assertIn("interests_reset", out["excluded"])
        self.assertTrue(all(v is None for v in out["outcomes"].values()))

    def test_old_epoch_like_does_not_cross_new_exposure_but_follow_survives(self):
        rows = [fact(1, "follow", target_type="author", target_id="author", post_id=None, post_version=None),
                fact(2, "like"), fact(3, "impression", epoch=2)]
        out = derive_outcomes(rows, exposure_event="event-3", window_end=BASE+timedelta(minutes=1),
                              as_of=BASE+timedelta(hours=1), history_start=BASE)["outcomes"]
        self.assertIsNone(out["liked"])
        self.assertTrue(out["follows_author"])

    def test_future_and_late_events_do_not_leak_into_closed_window(self):
        late = fact(3, "like", recorded_at=BASE+timedelta(minutes=31), client_time=BASE)
        self.assertIsNone(self.outcome(late)["outcomes"]["liked"])
        self.assertTrue(self.outcome(late, end=BASE+timedelta(minutes=32))["outcomes"]["liked"])

    def test_edit_or_delete_post_excludes_version_mismatch(self):
        for event in [fact(2, "content", False, user_id="author"), fact(2, "content", user_id="author", post_version=2)]:
            self.assertIn("content_changed", self.outcome(event)["excluded"])

    def test_own_view_and_repeat_delivery_do_not_multiply_observations(self):
        self.assertIn("own_content", self.outcome(fact(2, "impression", own_content=True), exposure="event-2")["excluded"])
        self.assertIn("repeat_exposure", self.outcome(fact(2, "impression", delivery_id="second"), exposure="event-2")["excluded"])

    def test_delivery_without_impression_cannot_be_labelled(self):
        with self.assertRaises(ValueError):
            derive_outcomes([fact(1, "like")], exposure_event="event-1", window_end=BASE,
                            as_of=BASE, history_start=BASE)

    def test_immature_window_and_naive_timestamp_are_rejected(self):
        with self.assertRaises(ValueError):
            self.outcome(as_of=BASE)
        with self.assertRaises(ValueError):
            self.outcome(fact(2, "like", recorded_at=datetime(2026, 11, 6)))

    def test_conflicting_event_or_sequence_and_unsupported_schema_fail_closed(self):
        for rows in [[fact(1, "like"), fact(1, "like", False)],
                     [fact(1, "like"), fact(2, "like", event_id="event-1")],
                     [fact(1, "like", contract_version=2)], [fact(1, "dwell", active_ms=120001)]]:
            with self.assertRaises(ValueError):
                ordered_facts(rows, BASE+timedelta(hours=1))

    def test_malformed_identities_and_state_types_fail_closed(self):
        for values in [{"user_id": ["reader"]}, {"event_id": 123}, {"post_id": ""},
                       {"author_id": False}, {"delivery_id": []}, {"surface": []},
                       {"target_type": []}, {"kind": []}, {"active": 1},
                       {"own_content": None}, {"state_version": True}]:
            with self.subTest(values=values), self.assertRaises(ValueError):
                ordered_facts([dict(fact(1, "like"), **values)], BASE+timedelta(hours=1))


if __name__ == "__main__":
    unittest.main()
