"""Kafka consumers.

detect : CDC events -> trust signals (Redis state) -> risk_events (Postgres) + risk:{id} (Redis) + risk.events topic
archive: CDC + risk topics -> Parquet on S3 (raw lake), offsets committed only after upload (at-least-once)
"""
import io
import json
import os
import statistics
import sys
import time
from datetime import datetime, timezone

import boto3
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import redis
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from detectors import (
    CHURN_MAX,
    DUP_JACCARD,
    PHASH_MAX_DIST,
    VELOCITY_MAX,
    VELOCITY_WINDOW_S,
    hamming,
    jaccard,
    lsh_bands,
    minhash,
    phash_chunks,
    price_flag,
    score,
)

KAFKA = os.environ["KAFKA"]
CDC_TOPICS = ["mkt.public.sellers", "mkt.public.listings", "mkt.public.listing_interactions"]
RISK_TOPIC, DLQ_TOPIC = "risk.events", "mkt.dlq"
BUCKET = "lake"


def consumer(group, topics):
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": group, "enable.auto.commit": False,
                  "auto.offset.reset": "earliest", "allow.auto.create.topics": True,
                  "topic.metadata.refresh.interval.ms": 10000})
    c.subscribe(topics)
    return c


def lag(c):
    """Consumer-group lag: high watermark - committed offset, summed over assigned partitions."""
    total = 0
    for tp in c.committed(c.assignment(), timeout=2):
        lo, hi = c.get_watermark_offsets(tp, timeout=2)
        total += hi - max(tp.offset, lo)
    return total


class Metrics:
    """Flushes throughput/lag/latency to pipeline_metrics every 5 s (Grafana reads it)."""

    def __init__(self, service, pg):
        self.service, self.pg, self.t0 = service, pg, time.time()
        self.events, self.errors, self.latencies = 0, 0, []

    def maybe_flush(self, c):
        dt = time.time() - self.t0
        if dt < 5:
            return
        lat = self.latencies
        p50 = statistics.median(lat) if lat else None
        p95 = statistics.quantiles(lat, n=20)[18] if len(lat) >= 2 else p50
        self.pg.execute("INSERT INTO pipeline_metrics (service, events, events_per_sec, lag, latency_p50_ms, "
                        "latency_p95_ms, errors) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        (self.service, self.events, self.events / dt, lag(c), p50, p95, self.errors))
        self.t0, self.events, self.errors, self.latencies = time.time(), 0, 0, []


# ---------------------------------------------------------------- detect

def ts(iso):
    return datetime.fromisoformat(iso).timestamp()


class Detector:
    def __init__(self, r, benchmark):
        self.r, self.benchmark = r, benchmark

    def seller(self, v):
        row = v["after"]
        if row:  # phone -> accounts index (shared phone across accounts)
            self.r.sadd(f"phone:{row['phone']}", row["seller_id"])
            self.r.set(f"seller:{row['seller_id']}:phone", row["phone"])

    def listing(self, v):
        """Returns (flags, row) for scored events, None for events that aren't scored."""
        op, row = v["op"], v["after"] or v["before"]
        lid, sid, r = row["listing_id"], row["seller_id"], self.r
        if op == "d":
            r.incr(f"deletes:{sid}")
            return None
        flags = []
        if op in ("c", "r"):
            flags += self._duplicates(row)
            created = ts(row["created_at"])
            r.zadd(f"vel:{sid}", {lid: created})
            r.zremrangebyscore(f"vel:{sid}", 0, created - 3600)
            if r.zcount(f"vel:{sid}", created - VELOCITY_WINDOW_S, created) >= VELOCITY_MAX:
                flags.append("seller_velocity")
        elif v["before"]["price"] == row["price"]:
            return None  # text-only edit: nothing to rescore
        else:
            n = r.incr(f"churn:{lid}")
            r.expire(f"churn:{lid}", 3600, nx=True)
            if n >= CHURN_MAX:
                flags.append("price_churn")
        flag = price_flag(row["price"], self._median(row))
        if flag:
            flags.append(flag)
        phone = r.get(f"seller:{sid}:phone")
        if phone and r.scard(f"phone:{phone}") > 1:
            flags.append("shared_phone")
        return flags, row

    def _median(self, row):
        if row["category"] == "property":
            return self.benchmark.get(row["location"])
        key = f"px:{row['category']}:{row['subcategory']}"
        prices = [float(p) for p in self.r.lrange(key, 0, -1)]
        self.r.lpush(key, row["price"])
        self.r.ltrim(key, 0, 199)
        return statistics.median(prices) if len(prices) >= 20 else None

    def _duplicates(self, row):
        """MinHash-LSH on text + multi-index search on perceptual image hash. Deleted listings stay indexed
        on purpose: delete-and-repost is a classic pattern."""
        lid, cat, r, flags = row["listing_id"], row["category"], self.r, []
        sig = minhash(f"{row['title']} {row['description']}")
        bands = [f"lsh:{cat}:{i}:{b}" for i, b in enumerate(lsh_bands(sig))]
        pipe = r.pipeline()
        for b in bands:
            pipe.smembers(b)
        cands = set().union(*pipe.execute()) - {str(lid)}
        if cands:
            sigs = r.mget([f"sig:{c}" for c in cands])
            if any(s and jaccard(sig, json.loads(s)) >= DUP_JACCARD for s in sigs):
                flags.append("near_duplicate")
        ph = row.get("image_phash")
        pipe = r.pipeline()
        for b in bands:
            pipe.sadd(b, lid)
        pipe.set(f"sig:{lid}", json.dumps(sig))
        if ph is not None:
            chunks = [f"ph:{i}:{c}" for i, c in enumerate(phash_chunks(ph))]
            cands = set().union(*(r.smembers(k) for k in chunks)) - {str(lid)}
            others = r.mget([f"phash:{c}" for c in cands]) if cands else []
            if any(o and hamming(ph, int(o)) <= PHASH_MAX_DIST for o in others):
                flags.append("duplicate_image")
            for k in chunks:
                pipe.sadd(k, lid)
            pipe.set(f"phash:{lid}", ph)
        pipe.execute()
        return flags


def detect():
    pg = psycopg.connect(os.environ["PG_DSN"], autocommit=True)
    r = redis.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    benchmark = {loc: float(p) for loc, p in pg.execute(
        "SELECT location, median_price FROM benchmark_prices WHERE category = 'property'")}
    det, prod = Detector(r, benchmark), Producer({"bootstrap.servers": KAFKA})
    c, m = consumer("risk-detector", CDC_TOPICS), Metrics("detector", pg)
    while True:
        rows, msgs = [], c.consume(500, timeout=0.05)
        for msg in msgs:
            if msg.error():
                if msg.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    print("kafka error", msg.error(), flush=True)
                continue
            try:
                if msg.value() is None:
                    continue
                v = json.loads(msg.value())
                if msg.topic() == "mkt.public.sellers":
                    det.seller(v)
                elif msg.topic() == "mkt.public.listings" and (res := det.listing(v)):
                    flags, row = res
                    s, status = score(flags)
                    now_ms = time.time() * 1000
                    src_ms = v["source"]["ts_ms"]
                    risk = {"listing_id": row["listing_id"], "seller_id": row["seller_id"], "risk_score": s,
                            "flags": flags, "status": status, "op": v["op"], "source_partition": msg.partition(),
                            "source_offset": msg.offset(), "event_ts_ms": src_ms, "processed_ts_ms": int(now_ms),
                            "latency_ms": int(now_ms - src_ms)}
                    rows.append(risk)
                    if v["op"] != "r":  # snapshot rows have old commit times; keep latency honest
                        m.latencies.append(risk["latency_ms"])
                m.events += 1
            except Exception as e:  # poison message -> DLQ, keep the stream moving
                m.errors += 1
                prod.produce(DLQ_TOPIC, msg.value(), headers={"error": str(e)[:200]})
        if rows:
            with pg.transaction():
                pg.cursor().executemany(
                    "INSERT INTO risk_events (listing_id, seller_id, risk_score, flags, status, source_partition, "
                    "source_offset, event_ts, latency_ms) VALUES (%s,%s,%s,%s,%s,%s,%s,to_timestamp(%s/1000.0),%s) "
                    "ON CONFLICT DO NOTHING",
                    [(x["listing_id"], x["seller_id"], x["risk_score"], x["flags"], x["status"],
                      x["source_partition"], x["source_offset"], x["event_ts_ms"], x["latency_ms"]) for x in rows])
            pipe = r.pipeline()
            for x in rows:
                pipe.set(f"risk:{x['listing_id']}", json.dumps(x))
                prod.produce(RISK_TOPIC, json.dumps(x), key=str(x["listing_id"]))
            pipe.execute()
        if msgs:
            prod.flush()
            try:
                c.commit(asynchronous=False)
            except KafkaException:  # batch held only error events -> nothing to commit
                pass
        m.maybe_flush(c)


# ---------------------------------------------------------------- archive

SCHEMA = pa.schema([("kafka_partition", pa.int32()), ("kafka_offset", pa.int64()), ("kafka_ts_ms", pa.int64()),
                    ("op", pa.string()), ("key_json", pa.string()), ("source_ts_ms", pa.int64()),
                    ("before_json", pa.string()), ("after_json", pa.string())])


def to_row(msg):
    v = json.loads(msg.value())
    cdc = "source" in v  # risk.events values aren't Debezium envelopes
    dump = lambda x: None if x is None else json.dumps(x)  # noqa: E731
    return {"kafka_partition": msg.partition(), "kafka_offset": msg.offset(), "kafka_ts_ms": msg.timestamp()[1],
            "op": v.get("op") if cdc else None, "key_json": msg.key().decode() if msg.key() else None,
            "source_ts_ms": v["source"]["ts_ms"] if cdc else v.get("event_ts_ms"),
            "before_json": dump(v.get("before")) if cdc else None, "after_json": dump(v["after"]) if cdc else dump(v)}


def archive():
    s3 = boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT"))
    try:
        s3.create_bucket(Bucket=BUCKET)
    except (s3.exceptions.BucketAlreadyOwnedByYou, s3.exceptions.BucketAlreadyExists):
        pass
    pg = psycopg.connect(os.environ["PG_DSN"], autocommit=True)
    c, m = consumer("s3-archiver", CDC_TOPICS + [RISK_TOPIC]), Metrics("archiver", pg)
    buf, n, t0 = {}, 0, time.time()
    while True:
        for msg in c.consume(1000, timeout=0.5):
            if msg.error() or msg.value() is None:
                continue
            hour = datetime.fromtimestamp(msg.timestamp()[1] / 1000, timezone.utc)
            buf.setdefault((msg.topic(), hour.strftime("%Y-%m-%d"), hour.hour), []).append(to_row(msg))
            n += 1
        if n and (n >= 5000 or time.time() - t0 >= 30):
            for (topic, d, h), rows in buf.items():
                out = io.BytesIO()
                pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), out)
                key = f"raw/topic={topic}/dt={d}/hour={h:02d}/{int(time.time() * 1000)}-{rows[0]['kafka_offset']}.parquet"
                s3.put_object(Bucket=BUCKET, Key=key, Body=out.getvalue())
            c.commit(asynchronous=False)
            m.events += n
            buf, n, t0 = {}, 0, time.time()
        m.maybe_flush(c)


if __name__ == "__main__":
    {"detect": detect, "archive": archive}[sys.argv[1]]()
