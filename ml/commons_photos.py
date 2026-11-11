"""Fetch a bounded, attributed Wikimedia Commons photo benchmark."""
from __future__ import annotations

import argparse
import html
import io
from itertools import chain
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from PIL import Image

from dataset import canonical, digest, publish_files

CATEGORIES = {
    "cats": "Domestic cats", "dogs": "Dogs", "bicycles": "Bicycles",
    "motorcycles": ("Motorcycles", "Motorcycles by brand", "Motorcycles in the United States", "BMW motorcycles", "Honda motorcycles", "Yamaha motorcycles"), "trains": ("Trains", "Passenger trains"), "trams": ("Trams", "Trams in Germany", "Trams in Lisbon"),
    "pizza": ("Pizzas", "Pizza Margherita"), "bread": ("Bread", "Sourdough bread"), "coffee": ("Coffee", "Cups of coffee"), "tea": ("Tea", "Cups of tea"),
    "roses": ("Roses", "Red roses"), "tulips": ("Tulips", "Tulipa gesneriana"), "mountains": ("Mountains", "Mountains of Switzerland"),
    "waterfalls": ("Waterfalls", "Waterfalls in Iceland"), "chess": ("Chess", "Chess sets", "Chessboards"), "guitars": ("Guitars", "Acoustic guitars", "Electric guitars"),
}
ALLOWED = {"commons.wikimedia.org", "upload.wikimedia.org", "thumb.wikimedia.org"}
USER_AGENT = "YapChatResearch/1.0 (https://github.com/ManuScript07/yap-algorithm)"
LICENSE = re.compile(r"(?:CC (?:BY|BY-SA) [234]\.0|CC0|Public domain)")


def check_url(url):
    info = urllib.parse.urlsplit(url)
    if info.scheme != "https" or info.hostname not in ALLOWED or info.username or info.port:
        raise ValueError("unexpected Commons URL")


class WikimediaRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        check_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def fetch(url, limit):
    check_url(url)
    opener = urllib.request.build_opener(WikimediaRedirect())
    for attempt in range(3):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with opener.open(request, timeout=15) as response:
                raw = response.read(limit + 1)
            if len(raw) > limit:
                raise ValueError("Commons payload exceeds byte budget")
            return raw
        except urllib.error.HTTPError as exc:
            if attempt == 2 or exc.code not in (429, 500, 502, 503, 504):
                raise
            time.sleep(min(10, int(exc.headers.get("Retry-After") or 1) if
                           str(exc.headers.get("Retry-After", "")).isdigit() else 1 << attempt))


def photo_hash(data):
    with Image.open(io.BytesIO(data)) as image:
        if image.format not in ("JPEG", "PNG") or not 128 <= min(image.size) or image.width * image.height > 15_000_000:
            raise ValueError("unexpected benchmark photo")
        image.load()
        preview = image.convert("L").resize((9, 8), Image.Resampling.LANCZOS)
        value = 0
        for y in range(8):
            for x in range(8):
                value = value * 2 + (preview.getpixel((x, y)) > preview.getpixel((x + 1, y)))
        return f"{value:016x}", image.size


def candidates(category, search=False):
    params = dict(action="query", format="json", formatversion="2", generator="categorymembers",
                  gcmtitle="Category:" + category, gcmtype="file", gcmlimit="50", prop="imageinfo",
                  iiprop="url|mime|extmetadata", iiurlwidth="480",
                  iiextmetadatafilter="LicenseShortName|LicenseUrl|Artist|Credit|Restrictions")
    if search:
        for key in ('gcmtitle', 'gcmtype', 'gcmlimit'):
            del params[key]
        params.update(generator='search', gsrsearch=category + ' filetype:bitmap', gsrnamespace='6', gsrlimit='50')
    for _ in range(3):
        response = json.loads(fetch("https://commons.wikimedia.org/w/api.php?" +
                                    urllib.parse.urlencode(params), 2_000_000))
        if "error" in response:
            raise ValueError("Commons returned an API error")
        for page in sorted(response.get("query", {}).get("pages", []),
                           key=lambda item: (item.get("index", 0), item["title"])):
            if not page.get("imageinfo"):
                continue
            info = page["imageinfo"][0]
            meta = {key: field["value"] for key, field in info["extmetadata"].items()}
            if (info["mime"] != "image/jpeg" or not info.get("thumburl")
                    or re.search(r"\b(logo|diagram|flag|blender|poster|painting|DPLA)\b", page["title"], re.I)
                    or not LICENSE.fullmatch(meta.get("LicenseShortName", ""))
                    or not meta.get("Artist") or meta.get("Restrictions")):
                continue
            raw_url = urllib.parse.urlsplit(info["thumburl"])
            url = raw_url._replace(query="", fragment="").geturl()
            source_url = info["descriptionurl"]
            check_url(source_url)
            yield dict(source=source_url, download=url, title=page["title"],
                       license=meta["LicenseShortName"], license_url=meta.get("LicenseUrl") or source_url,
                       creator=html.unescape(re.sub(r"<[^>]*>", "", meta["Artist"])).strip())
        if "continue" not in response:
            break
        params.update(response["continue"])


def collect(topic, category, target):
    pool = []
    categories = (category,) if isinstance(category, str) else category
    search = {"cats": "domestic cat portrait", "dogs": "domestic dog portrait",
              "bicycles": "bicycle parked", "motorcycles": "motorcycle parked",
              "trains": "passenger train locomotive", "trams": "tram streetcar",
              "pizza": "pizza plate margherita", "bread": "bread loaf sliced",
              "coffee": "cup of coffee espresso", "tea": "cup of tea teapot",
              "roses": "rose flower closeup", "tulips": "tulip flowers",
              "chess": "chess pieces board", "guitars": "acoustic guitar",
              "mountains": "mountain peak landscape", "waterfalls": "natural waterfall"}
    for entry in chain(candidates(search[topic], search=True),
                       *(candidates(group) for group in categories)):
        try:
            raw = fetch(entry["download"], 3 * 1024 * 1024)
            fingerprint, (width, height) = photo_hash(raw)
        except (ValueError, OSError):
            continue
        if any((int(fingerprint, 16) ^ int(record["dhash"], 16)).bit_count() <= 5 for record, _ in pool):
            continue
        sha = digest(raw)
        entry.update(id="commons-" + sha[:24], topic=topic, sha256=sha, dhash=fingerprint, width=width, height=height,
                     filename="photo-" + sha + (".png" if raw.startswith(b"\x89PNG") else ".jpg"))
        pool.append((entry, raw))
        if len(pool) == target + 2:
            break
    if len(pool) < target:
        raise ValueError(f"only {len(pool)} usable photos for {topic}")
    return pool


def download(target):
    entries, payloads = [], {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        tasks = {topic: executor.submit(collect, topic, category, target)
                 for topic, category in CATEGORIES.items()}
        for topic, task in tasks.items():
            accepted = 0
            for record, raw in task.result():
                if any((int(record["dhash"], 16) ^ int(e["dhash"], 16)).bit_count() <= 5 for e in entries):
                    continue
                entries.append(record)
                payloads[record["filename"]] = raw
                accepted += 1
                if accepted == target:
                    break
            if accepted != target:
                raise ValueError(f"not enough distinct Commons photos for {topic}")
            print(f"{topic}: {accepted} attributed photos", flush=True)
    return entries, payloads


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--per-topic", type=int, default=10, choices=range(1, 16))
    args = parser.parse_args()
    records, photos = download(args.per_topic)
    manifest = dict(schema="yap-commons-photos-v1", provenance="wikimedia-commons-category",
                    collector_sha256=digest(Path(__file__).read_bytes()), records=records)
    manifest["dataset_sha256"] = digest(canonical(manifest))
    # Photo paths are stored exactly as specified in the checksummed index.
    publish_files(args.output, {**photos, "manifest.json": canonical(manifest)})


if __name__ == "__main__":
    main()
