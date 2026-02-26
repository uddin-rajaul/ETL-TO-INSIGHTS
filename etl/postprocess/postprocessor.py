"""
Post-processing step - computes KPI aggregates from silver layer
and loads them into gold tables.
"""

from pathlib import Path

from loguru import logger
from sqlalchemy import text
from sqlalchemy.orm import Session

from db.models_gold import (
    KpiAttendance,
    KpiEarlyAttrition,
    KpiHeadcount,
    KpiRollingHours,
    KpiTenureByDepartment,
    KpiTurnover,
)


QUERIES_DIR = Path(__file__).resolve().parents[2] / "analytics" / "queries"


def load_query(filename: str) -> str:
    """Read a SQL file from analytics/queries."""
    return (QUERIES_DIR / filename).read_text()


class PostProcessor:
    """Build and load gold KPI tables from silver-layer analytics."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def _clear_gold_tables(self) -> None:
        """Truncate gold tables so each post-processing run is a full rebuild."""
        logger.info("Clearing gold tables...")
        self.session.execute(text("TRUNCATE TABLE gold.kpi_attendance RESTART IDENTITY"))
        self.session.execute(text("TRUNCATE TABLE gold.kpi_early_attrition RESTART IDENTITY"))
        self.session.execute(text("TRUNCATE TABLE gold.kpi_rolling_hours RESTART IDENTITY"))
        self.session.execute(text("TRUNCATE TABLE gold.kpi_tenure_by_department RESTART IDENTITY"))
        self.session.execute(text("TRUNCATE TABLE gold.kpi_turnover RESTART IDENTITY"))
        self.session.execute(text("TRUNCATE TABLE gold.kpi_headcount RESTART IDENTITY"))
        self.session.commit()
        logger.info("Gold tables cleared.")

    def _load_headcount(self) -> int:
        rows = self.session.execute(text(load_query("01_active_headcount.sql"))).fetchall()
        objects = [
            KpiHeadcount(
                snapshot_date=r[0],
                active_count=r[1],
            )
            for r in rows
        ]
        self.session.bulk_save_objects(objects)
        self.session.commit()
        return len(objects)

    def _load_turnover(self) -> int:
        rows = self.session.execute(text(load_query("02_turnover_trend.sql"))).fetchall()
        objects = [
            KpiTurnover(
                year=r[0],
                month=r[1],
                terminations=r[2],
                turnover_rate_pct=r[4],
            )
            for r in rows
        ]
        self.session.bulk_save_objects(objects)
        self.session.commit()
        return len(objects)

    def _load_tenure_by_department(self) -> int:
        rows = self.session.execute(text(load_query("03_avg_tenure_by_dept.sql"))).fetchall()
        objects = [
            KpiTenureByDepartment(
                department_id=None,
                department_name=r[0],
                employee_count=r[1],
                avg_tenure_years=r[2],
            )
            for r in rows
        ]
        self.session.bulk_save_objects(objects)
        self.session.commit()
        return len(objects)

    def _load_attendance(self) -> int:
        rows = self.session.execute(
            text(load_query("10_kpi_attendance_consolidated.sql"))
        ).fetchall()
        objects = [
            KpiAttendance(
                client_employee_id=r[0],
                department_name=r[1],
                total_shifts=r[2],
                late_arrival_count=r[3],
                early_departure_count=r[4],
                overtime_count=r[5],
                late_arrival_rate_pct=r[6],
            )
            for r in rows
        ]
        self.session.bulk_save_objects(objects)
        self.session.commit()
        return len(objects)

    def _load_rolling_hours(self) -> int:
        rows = self.session.execute(text(load_query("08_rolling_avg_hours.sql"))).fetchall()
        objects = [
            KpiRollingHours(
                client_employee_id=r[0],
                punch_apply_date=r[3],
                hours_worked=r[4],
                rolling_avg_7d=r[5],
            )
            for r in rows
        ]
        self.session.bulk_save_objects(objects)
        self.session.commit()
        return len(objects)

    def _load_early_attrition(self) -> int:
        rows = self.session.execute(text(load_query("09_early_attrition_rate.sql"))).fetchall()
        objects = [
            KpiEarlyAttrition(
                department_name=r[0],
                total_hires=r[1],
                early_attrition_count=r[2],
                early_attrition_rate_pct=r[3],
            )
            for r in rows
        ]
        self.session.bulk_save_objects(objects)
        self.session.commit()
        return len(objects)

    def run(self) -> dict:
        """Execute full post-processing and return row counts for each gold table."""
        logger.info("Starting post-processing step...")
        self._clear_gold_tables()

        summary = {
            "kpi_headcount": self._load_headcount(),
            "kpi_turnover": self._load_turnover(),
            "kpi_tenure_by_department": self._load_tenure_by_department(),
            "kpi_attendance": self._load_attendance(),
            "kpi_rolling_hours": self._load_rolling_hours(),
            "kpi_early_attrition": self._load_early_attrition(),
        }

        logger.info(f"Post-processing complete: {summary}")
        return summary
