import pytest
from ask import validate


def test_select_gets_limit():
    assert "LIMIT 200" in validate("SELECT * FROM analytics.mart_trust_metrics")


def test_cte_allowed():
    validate("WITH t AS (SELECT * FROM analytics.dim_seller) SELECT count(*) FROM t")


@pytest.mark.parametrize("bad", [
    "DROP TABLE analytics.dim_seller",
    "DELETE FROM analytics.dim_seller",
    "UPDATE analytics.dim_seller SET seller_name = 'x'",
    "INSERT INTO analytics.dim_seller VALUES (1)",
    "SELECT 1; DROP TABLE analytics.dim_seller",
    "SELECT * FROM sellers",
    "SELECT * FROM pg_catalog.pg_user",
    "SELECT pg_sleep(10) FROM analytics.dim_seller",
    "not sql at all (",
])
def test_rejected(bad):
    with pytest.raises(ValueError):
        validate(bad)
