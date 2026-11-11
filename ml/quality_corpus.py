"""Build a fixed bilingual retrieval benchmark independently of the serving ranker."""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path

from dataset import canonical, digest, publish_files


TOPICS = Path(__file__).with_name("benchmark_topics.txt")
SHORT = [
    ("cats", "Кот опять тыгыдыкал в три ночи.", "The cat had 3 a.m. zoomies again."),
    ("dogs", "Щенячьи зумики!", "Puppy zoomies!"),
    ("bicycles", "Велик починил. Погнали.", "Bike fixed. Let's ride."),
    ("motorcycles", "Мот завёлся с полтычка.", "The motorbike fired right up."),
    ("coffee", "Двойной шот, пожалуйста.", "Double shot, please."),
    ("tea", "Улун огонь.", "This oolong is unreal."),
    ("chess", "В блице зевнул мат.", "Missed mate in blitz."),
    ("guitars", "Баррэ опять дребезжит.", "My barre chord still buzzes."),
]


def source_topics(path=TOPICS):
    topics = {}
    for block in path.read_text(encoding="utf-8-sig").strip().split("\n\n"):
        header, *pairs = block.splitlines()
        if not header.startswith("[") or not header.endswith("]") or len(pairs) != 14:
            raise ValueError("invalid topic fixture")
        topic, contrast = header[1:-1].split("|")
        texts = [line.split("|") for line in pairs]
        if topic in topics or any(len(pair) != 2 or not all(pair) for pair in texts):
            raise ValueError("invalid bilingual pair")
        topics[topic] = (contrast, texts[:4], texts[4:])
    if any(contrast not in topics or contrast == topic for topic, (contrast, _, _) in topics.items()):
        raise ValueError("invalid hard-negative topic")
    return topics


def photo_index(folder):
    raw = (folder / "manifest.json").read_bytes()
    index = json.loads(raw)
    expected = index.pop("dataset_sha256")
    if index["schema"] != "yap-commons-photos-v1" or digest(canonical(index)) != expected:
        raise ValueError("untrusted photo index")
    identifiers = set()
    for photo in index["records"]:
        name = photo["filename"]
        if (not re.fullmatch(r"photo-[0-9a-f]{64}\.(jpg|png)", name)
                or not name.startswith("photo-" + photo["sha256"])
                or photo["id"] in identifiers or not photo["creator"] or not photo["license_url"]
                or digest((folder / name).read_bytes()) != photo["sha256"]):
            raise ValueError("photo identity, attribution or checksum mismatch")
        identifiers.add(photo["id"])
    return index["records"], digest(raw)


def build(photo_folder, review=None):
    topics = source_topics()
    photos, photo_hash = photo_index(photo_folder) if photo_folder is not None else ([], None)
    review_hash = None
    if review is not None:
        reviewed = review.read_bytes()
        keep = set(reviewed.decode("utf-8-sig").split())
        if not keep <= {entry["id"] for entry in photos}:
            raise ValueError("photo selection contains an unknown photo")
        photos = [entry for entry in photos if entry["id"] in keep]
        review_hash = digest(reviewed)
    if photo_folder is not None:
        counts = Counter(photo["topic"] for photo in photos)
        if set(counts) != topics.keys() or min(counts.values()) < 3 or not 150 <= len(photos) <= 300:
            raise ValueError("not enough licensed photos per topic")
    documents, queries, judgments, profiles = [], [], [], []
    by_topic = {}
    for topic, (contrast, prompts, texts) in topics.items():
        media = [entry["id"] for entry in photos if entry["topic"] == topic]
        records = []
        for i, pair in enumerate(texts):
            for language, body in zip(("ru", "en"), pair):
                # Keep parallel translations identifiable for cross-lingual slices.
                records.append(dict(id=f"{topic}-{i:02}-{language}", topic=topic,
                                    translation_group=f"{topic}-{i:02}", kind="text",
                                    language=language, text=body, photos=[]))
        for short_topic, ru, en in SHORT:
            if short_topic == topic:
                for language, body in (("ru", ru), ("en", en)):
                    records.append(dict(id=f"{topic}-short-{language}", topic=topic,
                                        translation_group=f"{topic}-short", kind="text",
                                        language=language, text=body, photos=[]))
        for identifier in media:
            records.append(dict(id=identifier, topic=topic, kind="photo", language=None, text=None, photos=[identifier]))
        if media:
            records.append(dict(id=f"{topic}-mixed", topic=topic, kind="mixed", language="ru",
                                text=texts[0][0], photos=media[:3]))
        by_topic[topic] = records
        documents.extend(records)
        for i, pair in enumerate(prompts):
            for language, query in zip(("ru", "en"), pair):
                queries.append(dict(id=f"query-{topic}-{i}-{language}", language=language,
                                    text=query, topic=topic, contrast=contrast))
        # Explicit topic shift: an older interest remains contextual; the most
        # recent explicit preference drives the present intent.
        profiles.append(dict(id=f"profile-{topic}", topic=topic, contrast=contrast,
                             history=[{"relative_time_days": -7, "kind": "like",
                                       "document_id": f"{contrast}-01-en"},
                                      {"relative_time_days": -1, "kind": "like",
                                       "document_id": f"{topic}-02-ru"}],
                             intent="recent-topic-shift"))
    for query in queries:
        for doc in by_topic[query["topic"]]:
            judgments.append(dict(query_id=query["id"], document_id=doc["id"],
                                  relevance=1 if doc["kind"] == "photo" else 2,
                                  provenance="weak_commons_topic" if doc["kind"] == "photo" else "authored_intent"))
        for doc in by_topic[query["contrast"]]:
            # Categories can overlap visually, so no negative grade is inferred
            # for photos. Remaining unjudged pairs also stay unjudged.
            if doc["kind"] == "text":
                judgments.append(dict(query_id=query["id"], document_id=doc["id"], relevance=0,
                                      provenance="authored_contrast"))
    rows = dict(documents=documents, queries=queries, judgments=judgments, photos=photos, profiles=profiles)
    files = {key + ".jsonl": b"".join(map(canonical, value)) for key, value in rows.items()}
    manifest = dict(schema="yap-quality-corpus-v1",
                    origin="authored-synthetic-text-and-commons-photos" if photos else "authored-synthetic-text",
                    judgment_policy="bilingual-intents-with-weak-photo-labels-v1" if photos else "bilingual-text-intents-v1",
                    topics_sha256=digest(TOPICS.read_bytes()), photo_index_sha256=photo_hash,
                    photo_selection_sha256=review_hash,
                    builder_sha256=digest(Path(__file__).read_bytes()),
                    files={key: {"count": len(value), "sha256": digest(files[key + ".jsonl"])}
                           for key, value in rows.items()})
    manifest["dataset_sha256"] = digest(canonical(manifest))
    return manifest, files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("photos", type=Path, nargs="?", help="Commons photo folder (omit with --text-only)")
    parser.add_argument("output", type=Path)
    parser.add_argument("--review", type=Path, help="private list of visually screened Commons photo IDs")
    parser.add_argument("--text-only", action="store_true", help="build the same text evaluation without photos")
    args = parser.parse_args()
    if args.text_only and (args.photos or args.review):
        parser.error("--text-only takes only the output folder")
    if not args.text_only and args.photos is None:
        parser.error("the photo folder is required unless --text-only is set")
    manifest, files = build(args.photos, args.review)
    publish_files(args.output, {**files, "manifest.json": canonical(manifest)})
    print(manifest["dataset_sha256"])


if __name__ == "__main__":
    main()
