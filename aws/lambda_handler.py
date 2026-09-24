"""AWS Lambda: Debezium CDC events -> Parquet raw lake on S3 + stateless price check -> risk JSONL.

Two triggers, same code:
  - Kinesis event source mapping (Debezium Server kinesis sink)       event = {"Records": [...]}
  - Lambda function URL (Debezium Server http sink, AWS Free plan)    event = {"body": ..., "queryStringParameters": ...}

ponytail: stateless - duplicate/velocity signals need shared state (Redis in the local path); add ElastiCache or
DynamoDB if the cloud path must run every detector.
"""
import base64
import csv
import hmac
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
with open(BENCH_CSV) as f:
    BENCHMARK = {r["location"]: float(r["median_price"]) for r in csv.DictReader(f)}
s3 = boto3.client("s3")


def dump(x):
    return None if x is None else json.dumps(x)


def events(event, context):
    """Yield (sequence_number, debezium_value) for either trigger."""
    if "Records" in event:
        for rec in event["Records"]:
            yield rec["kinesis"]["sequenceNumber"], json.loads(base64.b64decode(rec["kinesis"]["data"]))
    else:
        body = event.get("body") or "null"
        yield context.aws_request_id, json.loads(base64.b64decode(body) if event.get("isBase64Encoded") else body)


def handler(event, context):
    if "Records" not in event:  # public function URL: require the shared token deploy.sh gave Debezium
        token = (event.get("queryStringParameters") or {}).get("token", "")
        if not hmac.compare_digest(token, os.environ["TOKEN"]):
            return {"statusCode": 403, "body": "forbidden"}
    rows, risks = [], []
    for seq, v in events(event, context):
        if isinstance(v, dict) and "payload" in v and "schema" in v:  # converter left schemas on
            v = v["payload"]
        if not v:
            continue
        rows.append({"sequence_number": seq, "op": v.get("op"), "source_ts_ms": (v.get("source") or {}).get("ts_ms"),
                     "before_json": dump(v.get("before")), "after_json": dump(v.get("after"))})
        after = v.get("after")
        flag = (after and after.get("category") == "property" and v.get("op") in ("c", "u")
                and price_flag(after["price"], BENCHMARK.get(after["location"])))
        if flag:
            s, status = score([flag])
            risks.append({"listing_id": after["listing_id"], "seller_id": after["seller_id"], "risk_score": s,
                          "flags": [flag], "status": status, "sequence_number": seq})
    prefix = time.strftime("dt=%Y-%m-%d/hour=%H", time.gmtime())
    bucket, rid = os.environ["BUCKET"], context.aws_request_id
    if rows:
        out = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(rows), out)
        s3.put_object(Bucket=bucket, Key=f"raw/listings/{prefix}/{rid}.parquet", Body=out.getvalue())
    if risks:
        s3.put_object(Bucket=bucket, Key=f"risk/{prefix}/{rid}.jsonl", Body="\n".join(map(json.dumps, risks)))
    result = {"records": len(rows), "flagged": len(risks)}
    print(json.dumps(result))
    return {"statusCode": 200, "body": json.dumps(result)}
