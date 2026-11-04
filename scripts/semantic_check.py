"""A small bilingual regression probe and uncached CPU throughput measurement."""
import argparse
import json
import math
from pathlib import Path
import time
from urllib.request import Request, urlopen


def embed(text, url):
    request = Request(url + "/embed", json.dumps({"text": text}).encode(), headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=30) as response:
        return json.load(response)["embedding"]


def cosine(a, b):
    return sum(x * y for x, y in zip(a, b)) / math.sqrt(sum(x * x for x in a) * sum(y * y for y in b))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8091")
    parser.add_argument("--samples", type=int, default=100)
    args = parser.parse_args()
    probes = [
        ("Мне интересно программирование серверов на Go и работа с PostgreSQL.", "Building backend services with Go and PostgreSQL databases.", "Trying a new recipe for homemade pasta with tomato sauce."),
        ("Ищу компанию для похода в горы и ночёвки в палатке.", "Looking for hiking companions to camp in the mountains.", "A jazz concert with piano improvisation and live music."),
        ("Люблю джазовые концерты, музыку и игру на гитаре.", "Live jazz music, guitar playing and evening concerts.", "Football training, running and sports recovery."),
        ("Обсуждаем футбол, бег и спортивные тренировки.", "Talking about football, running and athletic training.", "Painting landscapes and visiting a modern art exhibition."),
    ]
    results = []
    for anchor, positive, negative in probes:
        a = embed(anchor, args.url)
        pos, neg = cosine(a, embed(positive, args.url)), cosine(a, embed(negative, args.url))
        results.append({"positive_similarity": pos, "negative_similarity": neg, "passed": pos > neg})
    started = time.perf_counter()
    for i in range(args.samples):
        embed(f"A developer shares database query optimization findings for PostgreSQL service number {i}, experiment {time.time_ns()}.", args.url)
    elapsed = time.perf_counter() - started
    report = {"probe_passed": sum(r["passed"] for r in results), "probe_total": len(results), "probes": results, "unique_embeddings": args.samples, "elapsed_seconds": elapsed, "embeddings_per_second": args.samples / elapsed, "note": "Small regression probe and local CPU measurement, not product recommendation-quality evidence."}
    Path("output").mkdir(exist_ok=True)
    Path("output/semantic-check.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    if not all(r["passed"] for r in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
