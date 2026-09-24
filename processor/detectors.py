"""Pure trust-signal functions (no I/O), shared by the stream processor and the AWS Lambda."""
import hashlib
import random
import re

NUM_PERM, BANDS = 64, 16  # 16 bands x 4 rows -> LSH candidate threshold ~0.5
ROWS = NUM_PERM // BANDS
DUP_JACCARD = 0.6          # estimated Jaccard to call a near-duplicate
PHASH_MAX_DIST = 6         # hamming bits on a 64-bit perceptual hash
VELOCITY_WINDOW_S, VELOCITY_MAX = 600, 10
PRICE_LOW, PRICE_HIGH = 0.4, 3.0   # price / median
CHURN_MAX = 4              # price edits per listing per hour
WEIGHTS = {
    "near_duplicate": 0.6, "duplicate_image": 0.4, "seller_velocity": 0.35, "shared_phone": 0.25,
    "price_anomaly_low": 0.45, "price_anomaly_high": 0.25, "price_churn": 0.2,
}

_P = (1 << 61) - 1
_rng = random.Random(42)
_PERMS = [(_rng.randrange(1, _P), _rng.randrange(0, _P)) for _ in range(NUM_PERM)]


def shingles(text, k=3):
    words = re.findall(r"[a-z0-9]+", text.lower())
    return {" ".join(words[i:i + k]) for i in range(max(1, len(words) - k + 1))}


def minhash(text):
    hs = [int.from_bytes(hashlib.blake2b(s.encode(), digest_size=8).digest(), "big") for s in shingles(text)]
    return [min((a * h + b) % _P for h in hs) for a, b in _PERMS]


def lsh_bands(sig):
    return [hashlib.blake2b(repr(sig[i * ROWS:(i + 1) * ROWS]).encode(), digest_size=8).hexdigest()
            for i in range(BANDS)]


def jaccard(a, b):
    return sum(x == y for x, y in zip(a, b)) / len(a)


def phash_chunks(h):
    """4 x 16-bit chunks: two hashes within 3 bits share at least one chunk (pigeonhole)."""
    return [(h >> (16 * i)) & 0xFFFF for i in range(4)]


def hamming(a, b):
    return (a ^ b).bit_count()


def price_flag(price, median):
    if not median:
        return None
    ratio = price / median
    return "price_anomaly_low" if ratio < PRICE_LOW else "price_anomaly_high" if ratio > PRICE_HIGH else None


def score(flags):
    """Noisy-OR of flag weights -> (score, status). Flags mean 'review', never 'fraud'."""
    p = 1.0
    for f in flags:
        p *= 1 - WEIGHTS[f]
    s = round(1 - p, 3)
    return s, "REVIEW" if s >= 0.6 else "WATCH" if s >= 0.3 else "OK"
