"""Synthetic, time-ordered ranking cases built from the verified quality corpus."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from itertools import zip_longest
import json
import random

from benchmark_vision import batches, corpus_files, e5_texts
from dataset import SPLITS, canonical, digest
from vision import MODELS, VisionEncoder, artifacts


def partition(rows, group):
    groups = defaultdict(list)
    for row in rows:
        groups[row["topic"], group(row)].append(row)
    per_topic = defaultdict(list)
    for (topic, _), group_rows in sorted(groups.items()):
        per_topic[topic].append(group_rows)
    output = {name: [] for name in SPLITS}
    for topic_groups in per_topic.values():
        for index, group_rows in enumerate(topic_groups):
            output[SPLITS[index % len(SPLITS)]].extend(group_rows)
    return output


def inventory(text, photos):
    captions = {}
    posts, references = [], {}
    for row in text:
        topic = row["topic"]
        if topic not in captions:
            captions[topic] = row
            continue
        posts.append(dict(row, modality="text", photo=None, conflict=False))
    used_mixed = set()
    topics = sorted(captions)
    for row in photos:
        topic = row["topic"]
        if topic not in references:
            references[topic] = row["id"]
            continue
        if topic not in used_mixed:
            used_mixed.add(topic)
            i = topics.index(topic)
            conflict = (i // 2) % 2 == 1 and (i ^ 1) < len(topics)
            # Pairwise swaps keep each caption in one mixed post.
            caption = captions[topics[i ^ 1]] if conflict else captions[topic]
            posts.append(dict(caption, id="mixed-" + row["id"], topic=topic, modality="mixed",
                              photo=row["id"], conflict=conflict))
        else:
            posts.append(dict(row, modality="photo", photo=row["id"], conflict=False, language=None, text=None))
    return posts, references


def utility(before, after, weights):
    """Only new, still-active states in the settled window count as transitions."""
    contributions = {name: weight for name, weight in weights.items()
                     if after.get(name) is True and before.get(name) is not True}
    return sum(contributions.values()), contributions


def eligible(post, viewer, now):
    return (post["created"] <= now and not post["removed"] and not post["blocked"]
            and (post["audience"] == "public" or post["friend"]) and post["author"] != viewer)


def source_pools(posts, protocol, visual_allowed):
    ordering = {
        "fresh": (lambda p: -p["go"]["CreatedAt"].timestamp(), lambda p: True),
        "popular": (lambda p: -p["popularity"], lambda p: True),
        "semantic": (lambda p: -p["semantic"], lambda p: p["semantic"] > 0),
        "social": (lambda p: -p["go"]["CreatedAt"].timestamp(), lambda p: p["go"]["Social"]),
        "discovery": (lambda p: p["go"]["AuthorViewers"], lambda p: True),
        "vision": (lambda p: -p["go"]["Visual"], lambda p: visual_allowed and p["go"]["Visual"] > 0),
    }
    sources = {}
    for name, (key, include) in ordering.items():
        ordered = sorted((i for i, p in enumerate(posts) if include(p)),
                         key=lambda i: (key(posts[i]), posts[i]["id"]))
        sources[name] = ordered[:protocol["source_limits"][name]]
    pools = {}
    for name, lanes in protocol["retrieval"].items():
        union = dict.fromkeys(i for layer in zip_longest(*(sources[lane] for lane in lanes))
                              for i in layer if i is not None)
        ids = list(union)[:protocol["pool_budget"]]
        pools[name] = {"IDs": ids, "Vision": [i for i in sources["vision"] if i in ids] if "vision" in lanes else []}
    common = pools[protocol["common_pool"]]["IDs"]
    pools["fixed_vision"] = {"IDs": common, "Vision": [i for i in sources["vision"] if i in common]}
    return pools


def make_case(posts, text_vectors, photos, references, query, contrast, split, start, status, offset, protocol):
    topic, language = query["topic"], query["language"]
    viewer = f"{split}/{topic}/{language}/{status}"
    rng = random.Random(f'{protocol["fixture_seed"]}/{viewer}')
    now = start + timedelta(seconds=offset)
    old, current = text_vectors[contrast["text"]], text_vectors[query["text"]]
    active = status == "active"
    chosen = status == "chosen"
    signal = current if chosen or active and offset > 0 else old if active else None
    visual_profile = photos[references[topic if offset > 0 else query["contrast"]]] if active else None
    evidence = []
    if active:
        evidence.append({"PostID": "old", "Vector": old.tolist(), "Weight": 2, "At": start - timedelta(days=7)})
        if offset > 0:
            evidence.append({"PostID": "new", "Vector": current.tolist(), "Weight": 4, "At": start})
    candidates, excluded = [], 0
    for index, original in enumerate(posts):
        item = original.copy()
        author = f"{split}/author-{index % 43}"
        age = rng.randrange(-3, 144)
        created = start - timedelta(hours=age)
        friend = rng.random() < .10
        new_author = (index % 43) % 4 == 0
        item.update(author=author, created=created, friend=friend, new_author=new_author,
                    audience="friends" if rng.random() < .10 else "public",
                    removed=rng.random() < .04, blocked=rng.random() < .04)
        impressions = rng.randrange(10, 160)
        likes = rng.randrange(0, impressions // 8 + 1)
        reposts = rng.randrange(0, impressions // 24 + 1)
        followed = rng.random() < .10
        nearby = rng.random() < .12
        seen = rng.random() < .14 if active else False
        # Simulate distinct reactions independently of the features used to sort.
        # The report also retains topic relevance without this utility proxy.
        relevant = item["topic"] == topic
        negative = rng.random() < (.52 if item["topic"] == query["contrast"] else .03)
        after = {"liked": rng.random() < (.84 if relevant else .035),
                 "commented": rng.random() < (.30 if relevant else .025),
                 "reposted": rng.random() < (.23 if relevant else .015),
                 "follows_author": rng.random() < (.23 if relevant and new_author else .015),
                 "less_similar": negative}
        value, outcomes = utility({}, after, protocol["utility"])
        if not eligible(item, viewer, now):
            excluded += 1
            continue
        vector = text_vectors.get(item["text"])
        similarity = float(vector @ signal) if signal is not None and vector is not None else 0
        raw_visual = (float(photos[item["photo"]] @ visual_profile)
                      if item["modality"] == "photo" and visual_profile is not None else 0)
        item.update(relevant=relevant, outcomes=outcomes, gain=max(0, min(3, value)), negative=negative,
                    popularity=(likes + 2 * reposts + 1) / (impressions + 20),
                    semantic=max(0, similarity))
        item["go"] = dict(ID=item["id"], AuthorID=author, CreatedAt=created, PostVersion=1,
                          Embedding=vector.tolist() if vector is not None else [], Social=friend or followed,
                          Nearby=nearby, Impressions=impressions, Likes=likes, Reposts=reposts, Seen=seen,
                          AuthorViewers=0 if new_author else 20 + index % 40, HasAuthorExposure=True,
                          AuthorImpressions=0 if new_author else 50 + index,
                          Visual=max(0, min(1, (raw_visual - .62) / .17)))
        candidates.append(item)
    pools = source_pools(candidates, protocol, active)
    return {"user": viewer, "split": split, "language": language, "status": status, "offset": offset,
            "label_available": now + timedelta(seconds=protocol["label_window_seconds"]),
            "candidates": candidates, "excluded": excluded, "request": {
                "At": now, "Evidence": evidence, "Topics": [current.tolist()] if chosen else [],
                "Candidates": [p["go"] for p in candidates], "Pools": pools,
            }}


def build(corpus_dir, photo_dir, protocol):
    corpus, rows = corpus_files(corpus_dir, photo_dir)
    text = [row for row in rows["documents"] if row["kind"] == "text"]
    vectors = e5_texts([row["text"] for row in text + rows["queries"]], 2)
    model = MODELS["siglip2"]
    files, space = artifacts(model, False)
    encoder = VisionEncoder(model, files, 2)
    images = batches(encoder.images, [photo_dir / row["filename"] for row in rows["photos"]], 4)
    photos = dict(zip((row["id"] for row in rows["photos"]), images, strict=True))
    texts = partition(text, lambda row: row["translation_group"])
    photo_splits = partition(rows["photos"], lambda row: row["sha256"])
    queries = partition(rows["queries"], lambda row: row["id"].rsplit("-", 1)[0])
    cases = []
    for i, split in enumerate(SPLITS):
        posts, references = inventory(texts[split], photo_splits[split])
        selected = {}
        for query in queries[split]:
            selected.setdefault((query["topic"], query["language"]), query)
        start = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=7*i)
        for key, query in selected.items():
            contrast = selected[query["contrast"], key[1]]
            for status in ("skipped", "chosen", "active"):
                offsets = protocol["topic_shift_checkpoints_seconds"] if status == "active" else [30]
                cases.extend(make_case(posts, vectors, photos, references, query, contrast, split,
                                       start, status, offset, protocol) for offset in offsets)
    for left, right in zip(SPLITS, SPLITS[1:]):
        if max(c["label_available"] for c in cases if c["split"] == left) >= min(
                c["request"]["At"] for c in cases if c["split"] == right):
            raise ValueError("label maturity crosses a split boundary")
    return cases, {"corpus_sha256": corpus, "vision_space": space}


def encode(value):
    def serialize(item):
        if isinstance(item, datetime):
            return item.isoformat()
        raise TypeError(f"unsupported fixture value: {type(item).__name__}")

    return json.dumps(value, default=serialize,
                      allow_nan=False, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"


def fixture_hash(cases):
    return digest(canonical([digest(encode(case)) for case in cases]))
