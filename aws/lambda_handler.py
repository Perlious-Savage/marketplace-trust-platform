"""AWS Lambda: Kinesis (Debezium Server CDC) -> Parquet raw lake on S3 + stateless price check -> risk JSONL.

ponytail: stateless - duplicate/velocity signals need shared state (Redis in the local path); add ElastiCache or
DynamoDB if the cloud path must run every detector.
"""
import base64
import csv
import io
import json
import os
import time
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from detectors import price_flag, score

HERE = Path(__file__).parent
BENCH_CSV = next(p for p in (HERE / "benchmark_prices.csv", HERE.parent / "postgres" / "benchmark_prices.csv")
                 if p.exists())
BENCHMARK = {r["location"]: float(r["median_price"]) for r in csv.DictReader(open(BENCH_CSV))}
s3 = boto3.client("s3")


def dump(x):
    return None if x is None else json.dumps(x)


def handler(event, context):
    rows, risks = [], []
    for rec in event["Records"]:
        k = rec["kinesis"]
        v = json.loads(base64.b64decode(k["data"]))
        rows.append({"shard_id": rec["eventID"].split(":")[0], "sequence_number": k["sequenceNumber"],
                     "op": v.get("op"), "source_ts_ms": (v.get("source") or {}).get("ts_ms"),
                     "before_json": dump(v.get("before")), "after_json": dump(v.get("after"))})
        after = v.get("after")
        if after and after.get("category") == "property" and v.get("op") in ("c", "u"):
            if flag := price_flag(after["price"], BENCHMARK.get(after["location"])):
                s, status = score([flag])
                risks.append({"listing_id": after["listing_id"], "seller_id": after["seller_id"], "risk_score": s,
                              "flags": [flag], "status": status, "sequence_number": k["sequenceNumber"]})
    prefix = time.strftime("dt=%Y-%m-%d/hour=%H", time.gmtime())
    bucket, rid = os.environ["BUCKET"], context.aws_request_id
    if rows:
        out = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(rows), out)
        s3.put_object(Bucket=bucket, Key=f"raw/listings/{prefix}/{rid}.parquet", Body=out.getvalue())
    if risks:
        s3.put_object(Bucket=bucket, Key=f"risk/{prefix}/{rid}.jsonl", Body="\n".join(map(json.dumps, risks)))
    print(json.dumps({"records": len(rows), "flagged": len(risks)}))
    return {"records": len(rows), "flagged": len(risks)}
