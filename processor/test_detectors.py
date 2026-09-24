from detectors import DUP_JACCARD, hamming, jaccard, lsh_bands, minhash, phash_chunks, price_flag, score

A = "3 BR apartment in Dubai Marina. 3 bedroom apartment in Dubai Marina, sea view, maid room, gym, pool, 1800 sqft, 12th floor. Ref 4821"
B = A.replace("4821", "9377")
C = "2019 Toyota Camry. 2019 Toyota Camry, 45000 km, full service history, single owner, GCC specs, sunroof. Ref 1111"


def test_near_duplicate():
    a, b, c = minhash(A), minhash(B), minhash(C)
    assert jaccard(a, b) >= DUP_JACCARD > jaccard(a, c)
    assert set(lsh_bands(a)) & set(lsh_bands(b))
    assert not set(lsh_bands(a)) & set(lsh_bands(c))


def test_phash():
    h = 0x1234_5678_9ABC_DEF0
    near = h ^ 0b1010_0000_0000_0000_0001  # 3 bits flipped
    assert hamming(h, near) == 3
    assert any(x == y for x, y in zip(phash_chunks(h), phash_chunks(near)))


def test_price():
    assert price_flag(100_000, 1_000_000) == "price_anomaly_low"
    assert price_flag(5_000_000, 1_000_000) == "price_anomaly_high"
    assert price_flag(1_100_000, 1_000_000) is None
    assert price_flag(1, None) is None


def test_score():
    assert score([]) == (0.0, "OK")
    assert score(["near_duplicate"])[1] == "REVIEW"
    assert score(["seller_velocity"])[1] == "WATCH"
    assert score(["near_duplicate", "price_anomaly_low"])[0] == 0.78
