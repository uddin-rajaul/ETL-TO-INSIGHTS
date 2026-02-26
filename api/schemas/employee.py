"""
Pydantic schemas for Employee API.
"""
from datetime import date, datetime
from typing import Optional, List, Any
from pydantic import BaseModel, Field
import math


class EmployeeBase(BaseModel):
    """Base employee schema with common fields."""
    client_employee_id: str = Field(..., description="Unique employee identifier")
    first_name: Optional[str] = Field(None, description="First name")
    middle_name: Optional[str] = Field(None, description="Middle name")
    last_name: Optional[str] = Field(None, description="Last name")
    preferred_name: Optional[str] = Field(None, description="Preferred name")
    job_code: Optional[str] = Field(None, description="Job code")
    job_title: Optional[str] = Field(None, description="Job title")
    job_start_date: Optional[date] = Field(None, description="Job start date")
    fte_status: Optional[str] = Field(None, description="Full-time equivalent status")
    scheduled_weekly_hour: Optional[float] = Field(None, description="Scheduled weekly hours")
    clinical_level: Optional[str] = Field(None, description="Clinical level")
    is_per_deim: Optional[bool] = Field(None, description="Is per diem employee")
    dob: Optional[date] = Field(None, description="Date of birth")
    hire_date: Optional[date] = Field(..., description="Hire date (required for analytics)")
    recent_hire_date: Optional[date] = Field(None, description="Recent hire date")
    anniversary_date: Optional[date] = Field(None, description="Anniversary date")
    term_date: Optional[date] = Field(None, description="Termination date")
    termination_reason: Optional[str] = Field(None, description="Reason for termination")
    years_of_experience: Optional[float] = Field(None, description="Years of experience")
    active_status: Optional[bool] = Field(True, description="Is employee active")
    work_email: Optional[str] = Field(None, description="Work email")
    address: Optional[str] = Field(None, description="Street address")
    city: Optional[str] = Field(None, description="City")
    state: Optional[str] = Field(None, description="State")
    zip: Optional[str] = Field(None, description="ZIP code")
    country: Optional[str] = Field(None, description="Country")
    cell_phone: Optional[str] = Field(None, description="Cell phone")
    work_phone: Optional[str] = Field(None, description="Work phone")
    organization_id: Optional[int] = Field(None, description="Organization ID")
    department_id: Optional[int] = Field(None, description="Department ID")
    manager_employee_id: Optional[str] = Field(None, description="Manager's employee ID")
    manager_employee_name: Optional[str] = Field(None, description="Manager's name")


class EmployeeCreate(EmployeeBase):
    """Schema for creating a new employee."""
    pass


class EmployeeUpdate(BaseModel):
    """Schema for updating an existing employee (all fields optional)."""
    first_name: Optional[str] = Field(None, description="First name")
    middle_name: Optional[str] = Field(None, description="Middle name")
    last_name: Optional[str] = Field(None, description="Last name")
    preferred_name: Optional[str] = Field(None, description="Preferred name")
    job_code: Optional[str] = Field(None, description="Job code")
    job_title: Optional[str] = Field(None, description="Job title")
    job_start_date: Optional[date] = Field(None, description="Job start date")
    fte_status: Optional[str] = Field(None, description="Full-time equivalent status")
    scheduled_weekly_hour: Optional[float] = Field(None, description="Scheduled weekly hours")
    clinical_level: Optional[str] = Field(None, description="Clinical level")
    is_per_deim: Optional[bool] = Field(None, description="Is per diem employee")
    dob: Optional[date] = Field(None, description="Date of birth")
    hire_date: Optional[date] = Field(None, description="Hire date")
    recent_hire_date: Optional[date] = Field(None, description="Recent hire date")
    anniversary_date: Optional[date] = Field(None, description="Anniversary date")
    term_date: Optional[date] = Field(None, description="Termination date")
    termination_reason: Optional[str] = Field(None, description="Reason for termination")
    years_of_experience: Optional[float] = Field(None, description="Years of experience")
    active_status: Optional[bool] = Field(None, description="Is employee active")
    work_email: Optional[str] = Field(None, description="Work email")
    address: Optional[str] = Field(None, description="Street address")
    city: Optional[str] = Field(None, description="City")
    state: Optional[str] = Field(None, description="State")
    zip: Optional[str] = Field(None, description="ZIP code")
    country: Optional[str] = Field(None, description="Country")
    cell_phone: Optional[str] = Field(None, description="Cell phone")
    work_phone: Optional[str] = Field(None, description="Work phone")
    organization_id: Optional[int] = Field(None, description="Organization ID")
    department_id: Optional[int] = Field(None, description="Department ID")
    manager_employee_id: Optional[str] = Field(None, description="Manager's employee ID")
    manager_employee_name: Optional[str] = Field(None, description="Manager's name")


class EmployeeResponse(EmployeeBase):
    """Schema for employee response."""
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class EmployeeListResponse(BaseModel):
    """Schema for employee list response."""
    total: int
    items: List[EmployeeResponse]


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
