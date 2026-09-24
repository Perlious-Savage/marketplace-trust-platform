"""Fake Dubizzle-style marketplace traffic with injected trust problems:
duplicate reposts, listing bursts, phone numbers shared across accounts, price outliers, price churn."""
import os
import random
import time
from datetime import datetime, timedelta, timezone

import psycopg

RATE = float(os.environ.get("RATE", "25"))  # actions / sec
R = random.Random()
CARS = {"Toyota Land Cruiser": 250_000, "Nissan Patrol": 220_000, "Toyota Camry": 85_000, "Honda Civic": 70_000,
        "Mercedes G-Class": 650_000, "BMW X5": 280_000, "Kia Sportage": 75_000, "Tesla Model 3": 140_000}
ITEMS = {"iPhone 16 Pro": 3_800, "PlayStation 5": 1_700, "Sofa set": 1_500, "Dining table": 900,
         "Mountain bike": 1_100, "MacBook Pro": 6_500, "Washing machine": 1_000, "Baby stroller": 600}
P_FEAT = ["sea view", "maid room", "gym", "pool", "balcony", "study", "covered parking", "chiller free",
          "furnished", "high floor", "near metro", "vacant on transfer", "upgraded kitchen", "walk-in closet",
          "burj view", "private garden", "concierge", "kids play area", "pet friendly", "storage room"]
C_FEAT = ["full service history", "single owner", "GCC specs", "sunroof", "leather seats", "under warranty",
          "accident free", "360 camera", "adaptive cruise", "new tyres", "agency maintained", "low mileage"]
I_FEAT = ["like new", "barely used", "with box", "all accessories", "pickup only", "can deliver",
          "original receipt", "minor scratches", "price negotiable", "moving sale"]
NAMES = ["Ahmed", "Fatima", "Omar", "Aisha", "Rahul", "Priya", "John", "Maria", "Ali", "Sara", "Wei", "Yusuf"]


def phone():
    return f"+9715{R.randint(0, 9)}{R.randint(1_000_000, 9_999_999)}"


def make_listing(seller_id, locations, when):
    cat = R.choices(["property", "car", "classified"], [0.45, 0.25, 0.30])[0]
    loc = R.choice(list(locations))
    ref = f"Ref {R.randint(1000, 9999)}"
    if cat == "property":
        beds = R.randint(0, 5)
        sub = "apartment" if beds < 4 else "villa"
        title = f"{beds or 'Studio'} BR {sub} in {loc}"
        desc = (f"{beds} bedroom {sub} in {loc}, {', '.join(R.sample(P_FEAT, 5))}, {R.randint(450, 6000)} sqft, "
                f"{R.randint(1, 60)}th floor. {ref}")
        price = locations[loc] * R.uniform(0.6, 1.5)
    elif cat == "car":
        sub = R.choice(list(CARS))
        year = R.randint(2014, 2025)
        title = f"{year} {sub}"
        desc = f"{year} {sub}, {R.randint(5, 220) * 1000} km, {', '.join(R.sample(C_FEAT, 4))}. {ref}"
        price = CARS[sub] * R.uniform(0.6, 1.5)
    else:
        sub = R.choice(list(ITEMS))
        title = f"{sub} for sale"
        desc = f"{sub} for sale in {loc}, {', '.join(R.sample(I_FEAT, 3))}, colour {R.randint(1, 50)}. {ref}"
        price = ITEMS[sub] * R.uniform(0.5, 1.6)
    return dict(seller_id=seller_id, category=cat, subcategory=sub, location=loc, title=title, description=desc,
                price=round(price, 2), image_phash=R.getrandbits(63), created_at=when, updated_at=when)


INSERT = ("INSERT INTO listings (seller_id, category, subcategory, location, title, description, price, image_phash, "
          "created_at, updated_at) VALUES (%(seller_id)s, %(category)s, %(subcategory)s, %(location)s, %(title)s, "
          "%(description)s, %(price)s, %(image_phash)s, %(created_at)s, %(updated_at)s) RETURNING listing_id")


def seed(db, locations):
    now = datetime.now(timezone.utc)
    sellers = [(f"{R.choice(NAMES)} {R.randint(1, 999)}", phone(), R.choice(["individual", "agency", "dealer"]))
               for _ in range(400)]
    for _ in range(8):  # fraud rings: 3 accounts, one phone
        p = phone()
        sellers += [(f"{R.choice(NAMES)} {R.randint(1, 999)}", p, "individual") for _ in range(3)]
    with db.cursor() as cur:
        cur.executemany("INSERT INTO sellers (name, phone, seller_type) VALUES (%s, %s, %s)", sellers)
        for _ in range(3000):
            created = now - timedelta(days=R.uniform(0, 45))
            row = make_listing(R.randint(1, 400), locations, created)
            row["updated_at"] = created + (now - created) * R.random() * R.random()
            cur.execute(INSERT, row)
    print("seeded", flush=True)


def main():
    db = psycopg.connect(os.environ["PG_DSN"], autocommit=True)
    locations = {loc: float(p) for loc, p in db.execute(
        "SELECT location, median_price FROM benchmark_prices WHERE category = 'property'")}
    if not db.execute("SELECT count(*) FROM sellers").fetchone()[0]:
        seed(db, locations)
    n_sellers = db.execute("SELECT max(seller_id) FROM sellers").fetchone()[0]
    rings = [r[0] for r in db.execute("SELECT seller_id FROM sellers WHERE phone IN "
                                      "(SELECT phone FROM sellers GROUP BY phone HAVING count(*) > 1)")]
    active = [r[0] for r in db.execute("SELECT listing_id FROM listings")]
    burst_sellers = list(range(1, 11))

    def insert(row):
        active.append(db.execute(INSERT, row).fetchone()[0])

    while True:
        now = datetime.now(timezone.utc)
        act = R.choices(["interaction", "new", "update", "delete", "dup", "burst", "outlier", "churn"],
                        [55, 20, 10, 4, 4, 0.15, 4, 2])[0]
        if act == "interaction" and active:
            kind = R.choices(["view", "favorite", "lead"], [80, 12, 8])[0]
            db.execute("INSERT INTO listing_interactions (listing_id, interaction_type) VALUES (%s, %s)",
                       (R.choice(active[-2000:]), kind))
        elif act == "new":
            insert(make_listing(R.randint(11, n_sellers), locations, now))
        elif act == "update" and active:
            db.execute("UPDATE listings SET price = round(price * %s::numeric, 2), updated_at = now() WHERE listing_id = %s",
                       (R.uniform(0.88, 1.08), R.choice(active)))
        elif act == "delete" and len(active) > 100:
            db.execute("DELETE FROM listings WHERE listing_id = %s", (active.pop(R.randrange(len(active))),))
        elif act == "dup" and active:  # repost: same text with new ref, near-identical photo, maybe another account
            src = db.execute("SELECT seller_id, category, subcategory, location, title, description, price, "
                             "image_phash FROM listings WHERE listing_id = %s", (R.choice(active),)).fetchone()
            if src:
                sid = R.choice(rings) if rings and R.random() < 0.5 else src[0]
                desc = src[5].rsplit("Ref", 1)[0] + f"Ref {R.randint(1000, 9999)}"
                ph = src[7] ^ (1 << R.randrange(63)) ^ (1 << R.randrange(63))
                insert(dict(seller_id=sid, category=src[1], subcategory=src[2], location=src[3], title=src[4],
                            description=desc, price=round(float(src[6]) * R.uniform(0.97, 1.0), 2),
                            image_phash=ph, created_at=now, updated_at=now))
        elif act == "burst":
            sid = R.choice(burst_sellers)
            for _ in range(R.randint(12, 20)):
                insert(make_listing(sid, locations, datetime.now(timezone.utc)))
        elif act == "outlier":
            row = make_listing(R.randint(11, n_sellers), locations, now)
            row["price"] = round(row["price"] * R.choice([0.15, 0.2, 5, 6]), 2)
            insert(row)
        elif act == "churn" and active:
            lid = R.choice(active)
            for _ in range(5):
                db.execute("UPDATE listings SET price = round(price * %s::numeric, 2), updated_at = now() "
                           "WHERE listing_id = %s", (R.uniform(0.9, 1.1), lid))
        time.sleep(1 / RATE)


if __name__ == "__main__":
    main()
