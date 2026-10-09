import pytest

from src.flow import check_reject_rate


def summary(rate):
    return {"data_quality": {"reject_rate_pct": rate}}


def test_gate_passes_below_the_limit_and_returns_the_rate():
    assert check_reject_rate(summary(8.48), 15.0) == 8.48


def test_gate_passes_exactly_at_the_limit():
    assert check_reject_rate(summary(15.0), 15.0) == 15.0


def test_gate_stops_the_flow_above_the_limit():
    with pytest.raises(ValueError, match="above the allowed"):
        check_reject_rate(summary(15.01), 15.0)
