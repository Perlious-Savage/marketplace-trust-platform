import base64
import io
import json
import os
from types import SimpleNamespace

import pyarrow.parquet as pq

os.environ.setdefault("AWS_DEFAULT_REGION", "ap-south-1")
os.environ.update(BUCKET="test-bucket", TOKEN="secret")
import lambda_handler as lh  # noqa: E402

LISTING = {"listing_id": 7, "seller_id": 3, "category": "property", "location": "Dubai Marina", "price": 150_000.0}
CTX = SimpleNamespace(aws_request_id="req1")


def fake_s3(monkeypatch):
    puts = {}
    monkeypatch.setattr(lh, "s3", SimpleNamespace(put_object=lambda **kw: puts.__setitem__(kw["Key"], kw["Body"])))
    return puts


def rec(seq, payload):
    return {"eventID": f"shardId-000000000000:{seq}",
            "kinesis": {"sequenceNumber": seq, "data": base64.b64encode(json.dumps(payload).encode()).decode()}}


def test_kinesis_batch_writes_parquet_and_flags_cheap_listing(monkeypatch):
    puts = fake_s3(monkeypatch)
    event = {"Records": [rec("1", {"op": "c", "before": None, "after": LISTING, "source": {"ts_ms": 1}}),
                         rec("2", {"op": "c", "before": None, "after": {**LISTING, "listing_id": 8, "price": 2e6},
                                   "source": {"ts_ms": 2}})]}
    assert json.loads(lh.handler(event, CTX)["body"]) == {"records": 2, "flagged": 1}
    parquet = next(v for k, v in puts.items() if k.endswith(".parquet"))
    assert pq.read_table(io.BytesIO(parquet)).num_rows == 2
    risk = json.loads(next(v for k, v in puts.items() if k.endswith(".jsonl")))
    assert risk["listing_id"] == 7 and risk["flags"] == ["price_anomaly_low"]


def test_function_url_requires_token(monkeypatch):
    puts = fake_s3(monkeypatch)
    body = json.dumps({"op": "c", "before": None, "after": LISTING, "source": {"ts_ms": 1}})
    assert lh.handler({"body": body, "queryStringParameters": {"token": "nope"}}, CTX)["statusCode"] == 403
    assert not puts
    ok = lh.handler({"body": body, "queryStringParameters": {"token": "secret"}}, CTX)
    assert json.loads(ok["body"]) == {"records": 1, "flagged": 1}
