"""Marketplace app + trust data product API."""
import json
import os
import time
from typing import Literal

import psycopg
import redis
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

PG = os.environ["PG_DSN"]
r = redis.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
app = FastAPI(title="Marketplace Trust API", description="Listings write path (CDC source) + trust & analytics reads")


def q(sql, params=(), one=False):
    with psycopg.connect(PG, row_factory=dict_row, autocommit=True) as con:
        cur = con.execute(sql, params)
        rows = cur.fetchall() if cur.description else []
    return (rows[0] if rows else None) if one else rows


@app.exception_handler(psycopg.errors.UndefinedTable)
def warehouse_not_ready(_: Request, e: Exception):
    return JSONResponse({"detail": "analytics marts not published yet - the batch job runs every 2 minutes"}, 503)


# ------------------------------------------------------------ marketplace (writes -> Postgres -> Debezium)

class NewListing(BaseModel):
    seller_id: int
    category: Literal["property", "car", "classified"]
    subcategory: str
    location: str
    title: str = Field(min_length=3, max_length=200)
    description: str = Field(min_length=3, max_length=5000)
    price: float = Field(gt=0)
    image_phash: int | None = Field(None, ge=0, lt=2**63)


class ListingUpdate(BaseModel):
    title: str | None = Field(None, min_length=3, max_length=200)
    description: str | None = Field(None, min_length=3, max_length=5000)
    price: float | None = Field(None, gt=0)


@app.post("/listings", status_code=201)
def create_listing(body: NewListing):
    return q("INSERT INTO listings (seller_id, category, subcategory, location, title, description, price, image_phash)"
             " VALUES (%(seller_id)s, %(category)s, %(subcategory)s, %(location)s, %(title)s, %(description)s,"
             " %(price)s, %(image_phash)s) RETURNING listing_id", body.model_dump(), one=True)


@app.patch("/listings/{listing_id}")
def update_listing(listing_id: int, body: ListingUpdate):
    fields = body.model_dump(exclude_none=True)
    if not fields:
        raise HTTPException(400, "nothing to update")
    sets = ", ".join(f"{k} = %({k})s" for k in fields)  # keys come from the model, not the client
    row = q(f"UPDATE listings SET {sets}, updated_at = now() WHERE listing_id = %(id)s RETURNING listing_id",
            {**fields, "id": listing_id}, one=True)
    if not row:
        raise HTTPException(404, "listing not found")
    return row


@app.delete("/listings/{listing_id}", status_code=204)
def delete_listing(listing_id: int):
    if not q("DELETE FROM listings WHERE listing_id = %s RETURNING listing_id", (listing_id,), one=True):
        raise HTTPException(404, "listing not found")


@app.post("/listings/{listing_id}/interactions", status_code=201)
def add_interaction(listing_id: int, interaction_type: Literal["view", "favorite", "lead"]):
    return q("INSERT INTO listing_interactions (listing_id, interaction_type) VALUES (%s, %s) RETURNING interaction_id",
             (listing_id, interaction_type), one=True)


# ------------------------------------------------------------ trust & analytics

@app.get("/listings/{listing_id}/risk")
def listing_risk(listing_id: int):
    if cached := r.get(f"risk:{listing_id}"):  # written by the stream processor within ms of the DB commit
        x = json.loads(cached)
        return {k: x[k] for k in ("listing_id", "risk_score", "flags", "status", "latency_ms")} | {"source": "redis"}
    row = q("SELECT listing_id, risk_score, flags, status, latency_ms FROM risk_events WHERE listing_id = %s "
            "ORDER BY processed_at DESC LIMIT 1", (listing_id,), one=True)
    if not row:
        raise HTTPException(404, "listing not scored yet")
    return row | {"source": "postgres"}


@app.get("/sellers/{seller_id}/health")
def seller_health(seller_id: int):
    row = q("SELECT * FROM analytics.mart_seller_health WHERE seller_id = %s", (seller_id,), one=True)
    if not row:
        raise HTTPException(404, "seller not found in warehouse yet")
    return row | {"live_listings_last_10m": r.zcount(f"vel:{seller_id}", f"({time.time() - 600}", "+inf")}


@app.get("/marketplace/metrics")
def marketplace_metrics():
    return {
        "pipeline": q("SELECT DISTINCT ON (service) service, ts, events_per_sec, lag, latency_p50_ms, latency_p95_ms, "
                      "errors FROM pipeline_metrics ORDER BY service, ts DESC"),
        "risk": q("SELECT count(*) AS scored, count(*) FILTER (WHERE status = 'REVIEW') AS review, "
                  "count(*) FILTER (WHERE status = 'WATCH') AS watch, "
                  "round(avg(('near_duplicate' = ANY(flags))::int), 4) AS duplicate_rate FROM risk_events", one=True),
        "reconciliation": q("SELECT DISTINCT ON (topic) topic, run_at, kafka_events, s3_events, warehouse_events, "
                            "s3_duplicates, pending_events, mismatch, status FROM recon_results ORDER BY topic, run_at DESC"),
    }


@app.get("/trust/events")
def trust_events(status: Literal["OK", "WATCH", "REVIEW"] | None = None, limit: int = Query(50, le=500)):
    return q("SELECT listing_id, seller_id, risk_score, flags, status, latency_ms, processed_at FROM risk_events "
             "WHERE %(s)s::text IS NULL OR status = %(s)s ORDER BY processed_at DESC LIMIT %(l)s",
             {"s": status, "l": limit})


@app.get("/funnel/summary")
def funnel_summary(days: int = 7):
    return q("SELECT category, sum(listings_created) AS listings_created, sum(views) AS views, "
             "sum(favorites) AS favorites, sum(leads) AS leads, "
             "round(sum(leads)::numeric / nullif(sum(views), 0), 4) AS view_to_lead_rate "
             "FROM analytics.mart_marketplace_funnel WHERE activity_date >= current_date - %s "
             "GROUP BY 1 ORDER BY 1", (days,))


class Question(BaseModel):
    question: str = Field(min_length=5, max_length=500)


@app.post("/ask")
def ask_data(body: Question):
    if not os.environ.get("LLM_MODEL"):
        raise HTTPException(503, "set LLM_MODEL and an API key in .env to enable Ask-Your-Data")
    from ask import ask
    return ask(body.question)
