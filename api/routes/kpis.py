"""
KPI routes.
All endpoints are protected by JWT authentication.
Reads SQL queries from analytics/queries/ directory.
"""

from pathlib import Path
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from db.base import get_db
from api.auth.dependencies import get_current_user

router = APIRouter(prefix="/kpis", tags=["KPIs"])

QUERIES_DIR = Path(__file__).resolve().parents[2] / "analytics" / "queries"


def load_query(filename: str) -> str:
    """Read a SQL file from the analytics/queries directory."""
    return (QUERIES_DIR / filename).read_text()


@router.get("/headcount")
def get_headcount(
    session: Session = Depends(get_db),
    _: object = Depends(get_current_user),
):
    """Active headcount snapshot per day."""
    rows = session.execute(text(load_query("01_active_headcount.sql"))).fetchall()
    return [{"snapshot_date": str(r[0]), "active_count": r[1]} for r in rows]


@router.get("/turnover")
def get_turnover(
    session: Session = Depends(get_db),
    _: object = Depends(get_current_user),
):
    """Monthly terminations and turnover rate."""
    rows = session.execute(text(load_query("02_turnover_trend.sql"))).fetchall()
    return [
        {
            "year": r[0], "month": r[1],
            "terminations": r[2], "avg_headcount": r[3],
            "turnover_rate_pct": float(r[4]),
        }
        for r in rows
    ]


@router.get("/tenure-by-department")
def get_tenure_by_department(
    session: Session = Depends(get_db),
    _: object = Depends(get_current_user),
):
    """Average tenure in years per department."""
    rows = session.execute(text(load_query("03_avg_tenure_by_dept.sql"))).fetchall()
    return [
        {
            "department_name": r[0],
            "employee_count": r[1],
            "avg_tenure_years": float(r[2]),
        }
        for r in rows
    ]


@router.get("/attendance")
def get_attendance(
    session: Session = Depends(get_db),
    _: object = Depends(get_current_user),
):
    """Late arrivals, early departures and overtime per employee."""
    rows = session.execute(text(load_query("05_late_arrival_frequency.sql"))).fetchall()
    return [
        {
            "client_employee_id": r[0],
            "first_name": r[1], "last_name": r[2],
            "department_name": r[3],
            "total_shifts": r[4],
            "late_arrival_count": r[5],
            "late_arrival_rate_pct": float(r[6]),
        }
        for r in rows
    ]


@router.get("/early-attrition")
def get_early_attrition(
    session: Session = Depends(get_db),
    _: object = Depends(get_current_user),
):
    """Early attrition rate per department."""
    rows = session.execute(text(load_query("09_early_attrition_rate.sql"))).fetchall()
    return [
        {
            "department_name": r[0],
            "total_hires": r[1],
            "early_attrition_count": r[2],
            "early_attrition_rate_pct": float(r[3]),
        }
        for r in rows
    ]