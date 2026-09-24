"""Batch loop: dbt build on the Parquet lake -> publish marts to Postgres `analytics` -> reconcile stream vs lake vs
warehouse. `python batch/run.py --once` runs a single cycle."""
import json
import os
import subprocess
import sys
import time

import duckdb
import psycopg
from confluent_kafka import Consumer, TopicPartition
from psycopg import sql

TOPICS = ["mkt.public.sellers", "mkt.public.listings", "mkt.public.listing_interactions", "risk.events"]
PUBLISH = ["mart_seller_health", "mart_listing_quality", "mart_marketplace_funnel", "mart_trust_metrics",
           "fact_risk_events", "dim_listing", "dim_seller"]
RAW = "s3://lake/raw"
WAREHOUSE = os.environ.get("WAREHOUSE_PATH", "warehouse.duckdb")
PG = os.environ["PG_DSN"]


def cutoffs():
    """Per partition: (low watermark, archiver committed offset, high watermark). The archiver commits only after
    an S3 upload, so every offset below `committed` must already be in the lake."""
    c = Consumer({"bootstrap.servers": os.environ["KAFKA"], "group.id": "s3-archiver", "enable.auto.commit": False})
    out = {}
    meta = c.list_topics(timeout=10).topics
    for t in TOPICS:
        if t not in meta:
            continue
        for tp in c.committed([TopicPartition(t, p) for p in meta[t].partitions], timeout=10):
            lo, hi = c.get_watermark_offsets(tp, timeout=5)
            out[(t, tp.partition)] = (lo, max(tp.offset, lo), hi)
    c.close()
    return out


def connect():
    con = duckdb.connect(WAREHOUSE)
    con.execute(f"""CREATE OR REPLACE SECRET lake (TYPE s3, KEY_ID '{os.environ['AWS_ACCESS_KEY_ID']}',
        SECRET '{os.environ['AWS_SECRET_ACCESS_KEY']}', ENDPOINT '{os.environ['S3_HOST']}', URL_STYLE 'path',
        USE_SSL false, REGION 'us-east-1')""")
    return con


def publish(con):
    con.execute(f"ATTACH '{PG}' AS pg (TYPE postgres)")
    for t in PUBLISH:  # ponytail: drop+create leaves a sub-second gap; swap via rename if readers mind
        con.execute(f"DROP TABLE IF EXISTS pg.analytics.{t}")
        con.execute(f"CREATE TABLE pg.analytics.{t} AS SELECT * FROM main.{t}")
    con.execute("DETACH pg")
    nodes = json.load(open("dbt/target/manifest.json"))["nodes"].values()
    docs = {n["name"]: n["description"] for n in nodes if n["resource_type"] == "model"}
    with psycopg.connect(PG, autocommit=True) as pg:  # dbt docs become schema context for Ask-Your-Data
        for t in PUBLISH:
            pg.execute(sql.SQL("COMMENT ON TABLE analytics.{} IS {}").format(sql.Identifier(t), docs.get(t, "")))


def reconcile(con, cut):
    if not cut:
        return []
    values = ", ".join(f"('{t}', {p}, {lo}, {c})" for (t, p), (lo, c, _) in cut.items())
    rows = con.execute(f"""
        WITH cut(topic, kafka_partition, lo, hi) AS (VALUES {values}),
        s3 AS (
            SELECT topic, kafka_partition, count(*) AS n, count(DISTINCT kafka_offset) AS d
            FROM read_parquet('{RAW}/*/*/*/*.parquet', hive_partitioning = true) JOIN cut USING (topic, kafka_partition)
            WHERE kafka_offset >= lo AND kafka_offset < hi GROUP BY ALL),
        wh AS (
            SELECT topic, kafka_partition, count(*) AS n
            FROM stg_cdc_events JOIN cut USING (topic, kafka_partition)
            WHERE kafka_offset >= lo AND kafka_offset < hi GROUP BY ALL)
        SELECT cut.topic, sum(hi - lo), coalesce(sum(s3.d), 0), coalesce(sum(s3.n - s3.d), 0), coalesce(sum(wh.n), 0)
        FROM cut LEFT JOIN s3 USING (topic, kafka_partition) LEFT JOIN wh USING (topic, kafka_partition)
        GROUP BY 1 ORDER BY 1""").fetchall()
    pending = {}
    for (t, _), (_, c, hi) in cut.items():
        pending[t] = pending.get(t, 0) + hi - c
    out = []
    for topic, kafka, s3, dups, wh in rows:
        mismatch = abs(kafka - s3) + abs(s3 - wh)
        out.append((topic, kafka, s3, dups, wh, pending[topic], mismatch, "OK" if mismatch == 0 else "MISMATCH"))
    with psycopg.connect(PG, autocommit=True) as pg:
        pg.cursor().executemany("INSERT INTO recon_results (topic, kafka_events, s3_events, s3_duplicates, "
                                "warehouse_events, pending_events, mismatch, status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                                out)
    return out


def cycle():
    cut = cutoffs()
    if not any(c > lo for lo, c, _ in cut.values()):
        print("lake is empty, waiting", flush=True)
        return
    if subprocess.run(["dbt", "build", "--project-dir", "dbt", "--profiles-dir", "dbt"]).returncode:
        print("dbt build failed (see log above); publishing what built", flush=True)
    con = connect()
    try:
        publish(con)
        for row in reconcile(con, cut):
            print("recon", row, flush=True)
    finally:
        con.close()


if __name__ == "__main__":
    while True:
        try:
            cycle()
        except Exception as e:  # keep the loop alive; next cycle retries
            print("batch cycle failed:", e, flush=True)
        if "--once" in sys.argv:
            break
        time.sleep(int(os.environ.get("BATCH_INTERVAL", "120")))
