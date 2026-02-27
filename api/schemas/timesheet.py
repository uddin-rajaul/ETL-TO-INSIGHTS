"""
Pydantic schemas for Timesheet API.
"""
from datetime import date, datetime
from typing import Optional, List, Any
from pydantic import BaseModel, ConfigDict
import math


class TimesheetBase(BaseModel):
    """Base timesheet schema."""
    client_employee_id: str
    department_id: Optional[str] = None
    department_name: Optional[str] = None
    home_department_id: Optional[str] = None
    home_department_name: Optional[str] = None
    pay_code: Optional[str] = None
    punch_apply_date: date
    punch_in_datetime: Optional[datetime] = None
    punch_out_datetime: Optional[datetime] = None
    scheduled_start_datetime: Optional[datetime] = None
    scheduled_end_datetime: Optional[datetime] = None
    hours_worked: Optional[float] = None
    punch_in_comment: Optional[str] = None
    punch_out_comment: Optional[str] = None
    is_late_arrival: Optional[bool] = False
    is_early_departure: Optional[bool] = False
    is_overtime: Optional[bool] = False
    is_unscheduled: Optional[bool] = False
    source_file: Optional[str] = None


class TimesheetResponse(TimesheetBase):
    """Schema for timesheet response."""
    id: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TimesheetListResponse(BaseModel):
    """Schema for timesheet list response."""
    total: int
    items: List[TimesheetResponse]


class TimesheetFilter(BaseModel):
    """Query parameters for filtering timesheets."""
    employee_id: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None


def remove_nan(obj: Any) -> Any:
    """Recursively remove NaN/Inf values from objects for JSON serialization."""
    if isinstance(obj, dict):
        return {k: remove_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [remove_nan(item) for item in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    return obj
