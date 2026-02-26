"""
Timesheet API routes.
Provides read-only access to timesheet entries with filtering.
"""
# List import kept for potential future use
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from datetime import date
from db.base import get_db
from db.models_silver import Timesheet, Employee
from api.auth.dependencies import get_current_user
from api.schemas.timesheet import (
    TimesheetResponse,
    TimesheetListResponse,
)

router = APIRouter(prefix="/timesheets", tags=["Timesheets"])


@router.get("/", response_model=TimesheetListResponse)
def get_timesheets(
    employee_id: str | None = Query(
        None, description="Filter by employee ID (client_employee_id)"
    ),
    start_date: date | None = Query(None, description="Start date for punch_apply_date"),
    end_date: date | None = Query(None, description="End date for punch_apply_date"),
    session: Session = Depends(get_db),
    current_user: Employee = Depends(get_current_user),
):
    """
    List timesheet entries with optional filters.
    - `employee_id`: Filter by specific employee
    - `start_date`: Filter timesheets on or after this date
    - `end_date`: Filter timesheets on or before this date
    """
    query = session.query(Timesheet)

    if employee_id:
        query = query.filter(Timesheet.client_employee_id == employee_id)

    if start_date:
        query = query.filter(Timesheet.punch_apply_date >= start_date)

    if end_date:
        query = query.filter(Timesheet.punch_apply_date <= end_date)

    total = query.count()
    timesheets = [TimesheetResponse.model_validate(t) for t in query.all()]

    return {"total": total, "items": timesheets}


@router.get("/employee/{employee_id}", response_model=TimesheetListResponse)
def get_employee_timesheets(
    employee_id: str,
    start_date: date | None = Query(None, description="Start date for punch_apply_date"),
    end_date: date | None = Query(None, description="End date for punch_apply_date"),
    session: Session = Depends(get_db),
    current_user: Employee = Depends(get_current_user),
):
    """
    Get timesheets for a specific employee.
    """
    # Verify employee exists
    employee = session.query(Employee).filter(
        Employee.client_employee_id == employee_id
    ).first()
    if not employee:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee with ID {employee_id} not found.",
        )

    query = session.query(Timesheet).filter(
        Timesheet.client_employee_id == employee_id
    )

    if start_date:
        query = query.filter(Timesheet.punch_apply_date >= start_date)

    if end_date:
        query = query.filter(Timesheet.punch_apply_date <= end_date)

    total = query.count()
    timesheets = [TimesheetResponse.model_validate(t) for t in query.all()]

    return {"total": total, "items": timesheets}


@router.get("/{timesheet_id}", response_model=TimesheetResponse)
def get_timesheet(
    timesheet_id: int,
    session: Session = Depends(get_db),
    current_user: Employee = Depends(get_current_user),
):
    """
    Get a specific timesheet by ID.
    """
    timesheet = session.query(Timesheet).filter(Timesheet.id == timesheet_id).first()
    if not timesheet:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Timesheet with ID {timesheet_id} not found.",
        )
    return TimesheetResponse.model_validate(timesheet)
