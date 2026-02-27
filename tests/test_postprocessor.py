"""
Tests for the post-processing step.
"""

import pytest
import sqlalchemy as sa
from db.base import SessionLocal
from etl.postprocess.postprocessor import PostProcessor


@pytest.fixture
def postprocessor():
    """Create a postprocessor instance with a database session."""
    session = SessionLocal()
    yield PostProcessor(session)
    session.close()


def test_postprocessor_populates_gold_tables(postprocessor):
    """All 6 gold tables should have rows after postprocessing."""
    postprocessor.run()

    session = postprocessor.session

    tables = [
        "gold.kpi_headcount",
        "gold.kpi_turnover",
        "gold.kpi_tenure_by_department",
        "gold.kpi_attendance",
        "gold.kpi_rolling_hours",
        "gold.kpi_early_attrition",
    ]
    for table in tables:
        count = session.execute(
            sa.text(f"SELECT COUNT(*) FROM {table}")
        ).scalar()
        assert count > 0, f"{table} is empty after postprocessor run"

def test_postprocessor_idempotent(postprocessor):
    """Running the postprocessor multiple times should not create duplicate records."""
    # First run
    postprocessor.run()  
    session = postprocessor.session

    # Capture counts after first run
    initial_counts = {}
    tables = [
        "gold.kpi_headcount",
        "gold.kpi_turnover",
        "gold.kpi_tenure_by_department",
        "gold.kpi_attendance",
        "gold.kpi_rolling_hours",
        "gold.kpi_early_attrition",
    ]
    for table in tables:
        initial_counts[table] = session.execute(
            sa.text(f"SELECT COUNT(*) FROM {table}")
        ).scalar()
    
    # Second run
    postprocessor.run()  

    # Verify counts are unchanged
    for table in tables:
        count = session.execute(
            sa.text(f"SELECT COUNT(*) FROM {table}")
        ).scalar()
        assert count == initial_counts[table], f"{table} has duplicate records after second run"