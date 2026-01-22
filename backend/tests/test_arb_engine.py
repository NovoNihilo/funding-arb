import pytest
from datetime import datetime, timedelta
from app.arb_engine import (
    compute_arbs,
    filter_by_min_spread,
    make_cooldown_key,
    is_cooldown_passed,
    format_telegram_message,
    format_funding_rate,
    estimate_apr,
    ArbOpportunity,
)


def test_compute_arbs_picks_correct_venues():
    venue_funding_map = {
        "venue_a": {"BTC": 0.0005, "ETH": 0.0001},
        "venue_b": {"BTC": -0.0002, "ETH": 0.0003},
        "venue_c": {"BTC": 0.0001, "ETH": -0.0001},
    }

    opps = compute_arbs(["BTC", "ETH"], venue_funding_map, fee_buffer=0.0001)

    btc_opp = next(o for o in opps if o.symbol == "BTC")
    assert btc_opp.short_venue == "venue_a"
    assert btc_opp.long_venue == "venue_b"

    eth_opp = next(o for o in opps if o.symbol == "ETH")
    assert eth_opp.short_venue == "venue_b"
    assert eth_opp.long_venue == "venue_c"


def test_compute_arbs_spread_calculation():
    venue_funding_map = {
        "venue_a": {"BTC": 0.0005},
        "venue_b": {"BTC": -0.0002},
    }

    opps = compute_arbs(["BTC"], venue_funding_map, fee_buffer=0.0001)

    assert len(opps) == 1
    opp = opps[0]
    assert opp.spread == pytest.approx(0.0007)
    assert opp.net_spread == pytest.approx(0.0006)


def test_compute_arbs_skips_single_venue():
    venue_funding_map = {
        "venue_a": {"BTC": 0.0005},
    }

    opps = compute_arbs(["BTC"], venue_funding_map, fee_buffer=0.0001)
    assert len(opps) == 0


def test_filter_by_min_spread():
    opps = [
        ArbOpportunity("BTC", "a", 0.0005, "b", -0.0002, 0.0007, 0.0006),
        ArbOpportunity("ETH", "a", 0.0002, "b", 0.0001, 0.0001, 0.0000),
    ]

    filtered = filter_by_min_spread(opps, min_net_spread=0.0003)
    assert len(filtered) == 1
    assert filtered[0].symbol == "BTC"


def test_cooldown_key_format():
    key = make_cooldown_key("BTC", "venue_a", "venue_b", "free")
    assert key == "BTC:venue_a:venue_b:free"


def test_cooldown_passed():
    now = datetime(2024, 1, 1, 12, 0, 0)

    assert is_cooldown_passed(None, 1800, now) is True

    last = now - timedelta(minutes=30)
    assert is_cooldown_passed(last, 1800, now) is True

    last = now - timedelta(minutes=29)
    assert is_cooldown_passed(last, 1800, now) is False


def test_format_funding_rate():
    assert format_funding_rate(0.0005) == "0.0500%"
    assert format_funding_rate(-0.0002) == "-0.0200%"


def test_estimate_apr():
    apr = estimate_apr(0.0007, interval_hours=8)
    assert apr == pytest.approx(0.7665)


def test_format_telegram_message():
    opp = ArbOpportunity(
        symbol="BTC",
        short_venue="venue_a",
        short_funding=0.0005,
        long_venue="venue_b",
        long_funding=-0.0002,
        spread=0.0007,
        net_spread=0.0006,
    )

    msg = format_telegram_message(opp)
    assert "BTC" in msg
    assert "venue_a" in msg
    assert "venue_b" in msg
    assert "0.0500%" in msg
    assert "-0.0200%" in msg
    assert "0.0700%" in msg
    assert "76.6%" in msg
