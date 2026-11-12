"""Single-slot internal vision worker. Supabase owns all content and visibility."""
from __future__ import annotations

import argparse
import base64
from collections import Counter, OrderedDict
import hashlib
import ipaddress
import json
import os
import re
import threading
import time
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
import uuid

from vision import MAX_IMAGE_BYTES, MODELS, VisionEncoder, artifacts


ROLE = "yap_feed_vision_worker"
PATH = re.compile(r"[a-z0-9-]+(?:/[a-z0-9-]+)*/image\.jpg")


class InvalidDerivative(ValueError):
    pass


def loopback(host):
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def claims(token):
    try:
        head, body, signature = token.split(".")
        if not head or not signature:
            raise ValueError
        payload = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
        if not isinstance(payload, dict):
            raise ValueError
        return payload
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError("invalid Storage token") from exc


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class PreparedStorage:
    def __init__(self, endpoint, key, token):
        parts = urlsplit(endpoint)
        if (not parts.hostname or parts.username is not None or parts.path not in ("", "/")
                or parts.query or parts.fragment or not (
                    parts.scheme == "https" or parts.scheme == "http" and loopback(parts.hostname)
                )):
            raise ValueError("invalid Supabase Storage origin")
        payload = claims(token)
        if (payload.get("role") != ROLE or not isinstance(payload.get("exp"), int)
                or payload["exp"] <= time.time() + 60):
            raise ValueError("an unexpired vision-only Storage JWT is required")
        if not (key.startswith("sb_publishable_") or claims(key).get("role") == "anon"):
            raise ValueError("a publishable Storage key is required")
        self.endpoint = endpoint.rstrip("/")
        self.headers = {"Authorization": "Bearer " + token, "apikey": key}
        self.client = build_opener(NoRedirect)

    def read(self, path, size):
        if len(path) > 300 or PATH.fullmatch(path) is None or not 1 <= size <= MAX_IMAGE_BYTES:
            raise InvalidDerivative("invalid derivative metadata")
        req = Request(self.endpoint + "/storage/v1/object/authenticated/feed-media/" + path, headers=self.headers)
        with self.client.open(req, timeout=8) as response:
            data = response.read(size + 1)
        if len(data) != size:
            raise InvalidDerivative("prepared derivative size changed")
        return data


def connect(dsn):
    import psycopg
    from psycopg.conninfo import conninfo_to_dict

    values = conninfo_to_dict(dsn)
    host = values.get("host")
    addr = values.get("hostaddr", host)
    if not host or not addr or "," in host or "," in addr or host.startswith("/") or (
        not (loopback(host) and loopback(addr)) and values.get("sslmode") != "verify-full"
    ):
        raise ValueError("database requires an explicit host and verify-full for remote TLS")
    db = psycopg.connect(dsn, autocommit=True, connect_timeout=5, prepare_threshold=None,
                         application_name="yap_vision_worker")
    try:
        safe = db.execute("""
            select not(rolsuper or rolbypassrls or rolcreaterole or rolcreatedb)
              and pg_has_role(current_user,'yap_feed_vision_worker','USAGE')
            from pg_roles where rolname=current_user
        """).fetchone()
        if safe != (True,):
            raise ValueError("database login must inherit the limited vision role")
        db.execute("select set_config('statement_timeout','5s',false)")
    except BaseException:
        db.close()
        raise
    return db


class Worker:
    def __init__(self, db, storage, encoder, space):
        self.db, self.storage, self.encoder, self.space = db, storage, encoder, space
        self.cache = OrderedDict()

    def _embedding(self, job):
        import onnxruntime as ort

        checksum = job["sha256"]
        if checksum in self.cache:
            self.cache.move_to_end(checksum)
            return self.cache[checksum], True
        image = self.storage.read(job["path"], job["bytes"])
        if hashlib.sha256(image).hexdigest() != checksum:
            raise InvalidDerivative("prepared derivative checksum changed")
        opts = ort.RunOptions()

        def cancel():
            opts.terminate = True

        deadline = threading.Timer(8, cancel)
        deadline.start()
        try:
            output = self.encoder.images([image], opts)[0]
        finally:
            deadline.cancel()
            deadline.join()
        vector = "[" + ",".join(str(number) for number in output) + "]"
        self.cache[checksum] = vector
        if len(self.cache) > 256:
            self.cache.popitem(last=False)
        return vector, False

    def once(self):
        from PIL import UnidentifiedImageError

        lease = uuid.uuid4()
        job = self.db.execute("select recsys.claim_vision_photo(%s)", (lease,)).fetchone()[0]
        if job is None:
            return "idle"
        try:
            if job["space"] != self.space:
                raise RuntimeError("vision space changed")
            vector, cached = self._embedding(job)
            complete = self.db.execute("""
                select recsys.complete_vision_photo(%s::uuid,%s,%s,%s,%s::extensions.vector)
            """, (job["id"], lease, job["sha256"], self.space, vector)).fetchone()[0]
            return ("cached" if cached else "encoded") if complete else "stale"
        except (InvalidDerivative, UnidentifiedImageError):
            self.retry(job["id"], lease, True)
            return "rejected"
        except Exception:
            self.retry(job["id"], lease, False)
            return "retry"

    def retry(self, photo, lease, terminal):
        self.db.execute("select recsys.retry_vision_photo(%s::uuid,%s,%s)", (photo, lease, terminal))


def main():
    from model_artifacts import VISION_SPACE

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--threads", type=int, choices=(1, 2), default=2)
    parser.add_argument("--max-jobs", type=int, help="stop at the given number of jobs or when the queue is idle")
    args = parser.parse_args()
    if args.max_jobs is not None and not 1 <= args.max_jobs <= 128:
        parser.error("--max-jobs must be 1–128")
    os.environ.update(USE_TORCH="0", HF_HUB_OFFLINE="1", HF_HUB_DISABLE_IMPLICIT_TOKEN="1",
                      TRANSFORMERS_VERBOSITY="error", OPENBLAS_NUM_THREADS=str(args.threads))
    model = MODELS["siglip2"]
    files, space = artifacts(model, False)
    if space["id"] != VISION_SPACE:
        raise ValueError("vision artifact does not match the pinned space")
    storage = PreparedStorage(os.environ.get("SUPABASE_URL", ""), os.environ.get("SUPABASE_ANON_KEY", ""),
                              os.environ.get("SUPABASE_VISION_TOKEN", ""))
    with connect(os.environ.get("VISION_DATABASE_URL", "")) as db:
        contract = db.execute("select recsys.vision_contract()").fetchone()[0]
        if contract["space"] != space["id"] or contract["dimension"] != model.dimension:
            raise ValueError("Supabase vision contract mismatch")
        worker = Worker(db, storage, VisionEncoder(model, files, args.threads), space["id"])
        print(json.dumps({"ready": True, "space": space["id"]}), flush=True)
        db.execute("select recsys.reconcile_vision_jobs()")
        last = time.monotonic()
        cleaned = last
        counts = Counter()
        processed = 0
        while True:
            status = worker.once()
            counts[status] += 1
            processed += status != "idle"
            done = args.max_jobs is not None and (status == "idle" or processed >= args.max_jobs)
            if done or time.monotonic() - last >= 5:
                db.execute("select recsys.reconcile_vision_jobs()")
                refreshed = db.execute("select recsys.refresh_vision_pilot()").fetchone()[0]
                print(json.dumps({"jobs": counts, "refreshes": refreshed}), flush=True)
                counts.clear()
                last = time.monotonic()
            if done:
                return
            if time.monotonic() - cleaned >= 30:
                db.execute("select recsys.cleanup_vision_pilot()")
                cleaned = time.monotonic()
            if status == "idle":
                time.sleep(0.5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception as error:
        # Connection errors can include hostnames, paths or SQL values.
        raise SystemExit("vision worker stopped: " + type(error).__name__) from None
