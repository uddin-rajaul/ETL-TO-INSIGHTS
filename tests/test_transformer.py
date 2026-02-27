"""
Tests for the transform step.
"""

import pytest
from datetime import datetime
from db.base import SessionLocal
from etl.transform.transformer import Transformer
from db.models_bronze import RawTimesheet


@pytest.fixture
def transformer():
    """Create a transformer instance with a database session."""
    session = SessionLocal()
    yield Transformer(session)
    session.close()

def test_parse_datetime_keeps_time(transformer):
    """_parse_datetime should preserve time component, not just the date"""
    result = transformer._parse_datetime("2025-05-28 10:53:00.000")
    assert isinstance(result, datetime)
    assert result.year == 2025
    assert result.month == 5
    assert result.day == 28
    assert result.hour == 10
    assert result.minute == 53

def test_flag_logic_late_arrival(transformer):
    """Employee punching in after scheduled start + grace should be flagged as late arrival."""
    raw = RawTimesheet(
        client_employee_id="TEST01",
        punch_in_datetime="2025-05-28 09:16:00.000", # 16 minutes after 9:00 start
        punch_out_datetime="2025-05-28 17:00:00.000",
        scheduled_start_datetime="2025-05-28 09:00:00.000",
        scheduled_end_datetime="2025-05-28 17:00:00.000"
    )

    flags = transformer._compute_flags(raw)
    
    assert flags["is_late_arrival"] == True
    assert flags["is_early_departure"] == False
    assert flags["is_overtime"] == False
    assert flags["is_unscheduled"] == False


def test_flag_logic_overtime(transformer):
    """Employee punching out after scheduled end + grace should be flagged as overtime."""
    raw = RawTimesheet(
        client_employee_id="TEST02",
        punch_in_datetime="2025-05-28 09:00:00.000",
        punch_out_datetime="2025-05-28 17:16:00.000", # 16 minutes after 17:00 end
        scheduled_start_datetime="2025-05-28 09:00:00.000",
        scheduled_end_datetime="2025-05-28 17:00:00.000"
    )

    flags = transformer._compute_flags(raw)
    
    assert flags["is_late_arrival"] == False
    assert flags["is_early_departure"] == False
    assert flags["is_overtime"] == True
    assert flags["is_unscheduled"] == False


def test_flag_logic_unscheduled(transformer):
    """Employee punching in on a day they are not scheduled should be flagged as unscheduled."""
    raw = RawTimesheet(
        client_employee_id="TEST03",
        punch_in_datetime="2025-05-28 09:00:00.000",
        punch_out_datetime="2025-05-28 17:00:00.000",
        scheduled_start_datetime=None,
        scheduled_end_datetime=None
    )

    flags = transformer._compute_flags(raw)
    
    assert flags["is_late_arrival"] == False
    assert flags["is_early_departure"] == False
    assert flags["is_overtime"] == False
    assert flags["is_unscheduled"] == True