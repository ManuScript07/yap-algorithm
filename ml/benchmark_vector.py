"""Exact and HNSW retrieval against temporary tables in local Supabase."""
from __future__ import annotations

from collections import defaultdict
import random
import time

INDEX_NAME = "yap_benchmark_hnsw"
QUERY = """
    select id from pg_temp.yap_benchmark_vectors
    order by embedding operator(extensions.<=>) %s::extensions.vector limit %s
"""


def percentile_ms(seconds):
    import numpy as np

    return {f"p{p}_ms": float(np.percentile(seconds, p) * 1000) for p in (50, 95, 99)}


def vector_literal(vector):
    return "[" + ",".join(str(value) for value in vector) + "]"


def index_names(plan):
    names = [plan["Index Name"]] if "Index Name" in plan else []
    for child in plan.get("Plans", []):
        names.extend(index_names(child))
    return names


def select_plan(cursor, mode):
    cursor.execute("select set_config('enable_seqscan', %s, true), set_config('enable_indexscan', %s, true)",
                   ("on", "off") if mode == "exact" else ("off", "on"))
    if mode != "exact":
        cursor.execute("select set_config('hnsw.ef_search', %s, true)", (str(mode),))


def explain(cursor, query, k):
    return cursor.execute("explain (format json) " + QUERY, (query, k), prepare=False).fetchone()[0][0]["Plan"]


def benchmark_pgvector(documents, queries, k, port, seed):
    import numpy as np
    import psycopg
    from psycopg import sql

    if not np.isfinite(documents).all() or not np.isfinite(queries).all():
        raise ValueError("pgvector benchmark requires finite vectors")
    if not len(queries) or not 0 < k <= len(documents) or queries.shape[1] != documents.shape[1]:
        raise ValueError("invalid pgvector benchmark dimensions")
    dimension = documents.shape[1]
    query_literals = [vector_literal(vector) for vector in queries]
    modes = ["exact", 10, 40, 100]
    elapsed = defaultdict(list)
    results = defaultdict(dict)
    plans = {}
    rng = random.Random(seed)
    with psycopg.connect(host="127.0.0.1", hostaddr="127.0.0.1", port=port, dbname="postgres",
                         user="postgres", connect_timeout=5, application_name="yap_text_benchmark") as conn:
        with conn.cursor() as cursor:
            cursor.execute("set local statement_timeout = '60s'")
            cursor.execute("set local lock_timeout = '3s'")
            pg_version, vector_version = cursor.execute("""
                select current_setting('server_version'), extversion
                from pg_extension where extname = 'vector' and extnamespace = 'extensions'::regnamespace
            """).fetchone()
            cursor.execute(sql.SQL("""
                create temporary table yap_benchmark_vectors (
                    id integer not null, embedding extensions.vector({}) not null
                ) on commit drop
            """).format(sql.Literal(dimension)))
            with cursor.copy("copy pg_temp.yap_benchmark_vectors (id, embedding) from stdin") as copy:
                for number, vector in enumerate(documents):
                    copy.write_row((number, vector_literal(vector)))
            cursor.execute("analyze pg_temp.yap_benchmark_vectors")
            before = time.perf_counter()
            cursor.execute("""
                create index yap_benchmark_hnsw on pg_temp.yap_benchmark_vectors
                using hnsw (embedding extensions.vector_cosine_ops)
                with (m = 16, ef_construction = 64)
            """)
            build_seconds = time.perf_counter() - before
            table_bytes, index_bytes = cursor.execute("""
                select pg_table_size('pg_temp.yap_benchmark_vectors'),
                       pg_relation_size('pg_temp.yap_benchmark_hnsw')
            """).fetchone()
            automatic = explain(cursor, query_literals[0], k)
            for mode in modes:
                select_plan(cursor, mode)
                plan = explain(cursor, query_literals[0], k)
                indexes = index_names(plan)
                if indexes != ([] if mode == "exact" else [INDEX_NAME]):
                    raise RuntimeError(f"unexpected pgvector plan for {mode}: {indexes}")
                plans[mode] = indexes[0] if indexes else "sequential scan + sort"

            # Warm each path, then interleave them in a repeatable shuffled order.
            for iteration in range(5):
                order = list(range(len(query_literals)))
                rng.shuffle(order)
                for index in order:
                    rng.shuffle(modes)
                    for mode in modes:
                        select_plan(cursor, mode)
                        before = time.perf_counter()
                        rows = cursor.execute(QUERY, (query_literals[index], k), prepare=False).fetchall()
                        latency = time.perf_counter() - before
                        if len(rows) != k:
                            raise RuntimeError(f"pgvector returned {len(rows)} results for k={k}")
                        if iteration:
                            elapsed[mode].append(latency)
                            results[mode][index] = {row[0] for row in rows}
        conn.rollback()

    return {
        "postgres": pg_version, "pgvector": vector_version, "documents": len(documents),
        "queries": len(queries), "dimension": dimension, "k": k,
        "table_bytes": table_bytes, "index_bytes": index_bytes, "index_build_ms": build_seconds * 1000,
        "automatic_indexes": index_names(automatic),
        "paths": {
            str(mode): {
                "plan": plans[mode], "requests": len(elapsed[mode]), **percentile_ms(elapsed[mode]),
                "recall_vs_exact": sum(len(ids & results["exact"][i]) / k for i, ids in results[mode].items()) / len(queries),
            }
            for mode in ("exact", 10, 40, 100)
        },
    }
