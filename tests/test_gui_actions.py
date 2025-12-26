from datetime import datetime

import pytest
from wallet.gui.actions import parse_schedule_target


def test_parse_schedule_height() -> None:
    assert parse_schedule_target("12345") == 12345


def test_parse_schedule_iso_date() -> None:
    ts = parse_schedule_target("2025-12-25 20:20 UTC")
    assert ts == int(datetime(2025, 12, 25, 20, 20).timestamp())


def test_parse_schedule_iso_with_z() -> None:
    ts = parse_schedule_target("2025-12-25 20:20Z")
    assert ts == int(datetime(2025, 12, 25, 20, 20).timestamp())


def test_parse_schedule_date_only() -> None:
    ts = parse_schedule_target("2025-12-25")
    assert ts == int(datetime(2025, 12, 25, 0, 0).timestamp())


def test_parse_schedule_invalid() -> None:
    with pytest.raises(ValueError):
        parse_schedule_target("tomorrow")
