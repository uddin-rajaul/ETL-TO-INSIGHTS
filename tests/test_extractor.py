"""
Tests for the extraction step.
Run with: pytest tests/test_extractor.py -v
"""

import pytest
from db.base import SessionLocal
from db.models_bronze import RawEmployee, RawTimesheet
from etl.extract.extractor import Extractor


@pytest.fixture
def session():
    """
    Create a database session for testing.
    Clears bronze tables before each test to ensure a clean state.
    """
    db = SessionLocal()
    import sqlalchemy as sa
    db.execute(sa.text("TRUNCATE TABLE bronze.raw_employee RESTART IDENTITY"))
    db.execute(sa.text("TRUNCATE TABLE bronze.raw_timesheet RESTART IDENTITY"))
    db.commit()
    yield db
    db.close()


@pytest.fixture
def extractor(session):
    """Create an Extractor instance for testing."""
    return Extractor(session)


def test_extract_employees(extractor, session):
    """Should insert 50 employee rows into bronze.raw_employee."""
    result = extractor.extract_employees()
    assert result == 50
    count = session.query(RawEmployee).count()
    assert count == 50


def test_extract_timesheets(extractor, session):
    """Should insert rows from all 4 timesheet files."""
    result = extractor.extract_timesheets()
    assert result > 0

    # verify all 4 source files are represented
    from sqlalchemy import func
    file_counts = (
        session.query(RawTimesheet.source_file, func.count())
        .group_by(RawTimesheet.source_file)
        .all()
    )
    assert len(file_counts) == 4, f"Expected 4 source files, got {len(file_counts)}"


def test_no_missing_source_file(extractor, session):
    """Every row must have source_file populated."""
    extractor.extract_employees()
    missing = session.query(RawEmployee).filter(
        RawEmployee.source_file == None
    ).count()
    assert missing == 0


def test_rerun_does_not_duplicate(extractor, session):
    """Running extraction twice should not duplicate rows."""
    extractor.run()
    extractor.run()
    count = session.query(RawEmployee).count()
    assert count == 50, f"Expected 50 employees, got {count} after rerun"