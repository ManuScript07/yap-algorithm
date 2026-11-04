"""Behavior simulator through the real HTTP API, never used as proof of quality."""
import argparse
import json
from pathlib import Path
import random
import time
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import uuid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8090")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--seed", type=int, default=7331)
    args = parser.parse_args()
    if urlparse(args.url).hostname not in ("127.0.0.1", "localhost"):
        raise ValueError("Simulator only targets the local fixture API")
    corpus = json.loads(Path(".local/corpus.json").read_text(encoding="utf-8"))
    if corpus["origin"] != "synthetic":
        raise ValueError("Synthetic corpus required")
    topic = {p["id"]: p["topic"] for p in corpus["posts"]}
    rng = random.Random(args.seed)
    counts = {"sessions": 0, "impressions": 0, "likes": 0, "not_interested": 0}

    def call(user, method, path, data=None):
        body = None if data is None else json.dumps(data).encode()
        request = Request(args.url + path, data=body, method=method, headers={"Authorization": "Bearer dev:" + user, "Content-Type": "application/json"})
        with urlopen(request, timeout=30) as response:
            raw = response.read()
            return json.loads(raw) if raw else None

    for _ in range(args.rounds):
        for user in corpus["users"]:
            tab = rng.choices(["for_you", "friends", "nearby"], [0.6, 0.25, 0.15])[0]
            page = call(user["id"], "GET", f"/v1/feed?tab={tab}&limit=10")
            counts["sessions"] += 1
            events = []
            for item in page["items"]:
                pid = item["post"]["id"]
                events.append({"id": str(uuid.uuid4()), "session_id": page["session_id"], "position": item["position"], "post_id": pid, "kind": "impression", "visible_ms": 1100, "visible_fraction": 0.9})
            # Qualified visibility requires actual elapsed time; no fabricated clocks.
            time.sleep(1.1)
            if events:
                call(user["id"], "POST", "/v1/events:batch", {"events": events})
                counts["impressions"] += len(events)
            for item in page["items"]:
                pid = item["post"]["id"]
                matched = topic.get(pid) in user["interests"]
                # Hidden preference model independent from the serving score.
                kind = "like" if rng.random() < (0.42 if matched else 0.025) else None
                if kind is None and not matched and rng.random() < 0.035:
                    kind = "not_interested"
                if kind:
                    call(user["id"], "POST", f"/v1/posts/{pid}/actions", {"kind": kind, "session_id": page["session_id"], "position": item["position"]})
                    counts["likes" if kind == "like" else "not_interested"] += 1
            print(json.dumps({"origin": "synthetic", **counts}), flush=True)
    Path("output").mkdir(exist_ok=True)
    Path("output/simulation.json").write_text(json.dumps({"origin": "synthetic", "seed": args.seed, **counts}, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
