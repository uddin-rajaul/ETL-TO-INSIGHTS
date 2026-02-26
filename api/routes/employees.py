"""
Employee API routes.
Provides CRUD operations for employees in the silver layer.
"""
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import func
from db.base import get_db
from db.models_silver import Employee
from api.auth.dependencies import get_current_user, get_admin_user
from api.schemas.employee import (
    EmployeeCreate,
    EmployeeUpdate,
    EmployeeResponse,
    EmployeeListResponse,
    remove_nan,
)

router = APIRouter(prefix="/employees", tags=["Employees"])


def _safe_employee_response(employee: Employee) -> EmployeeResponse:
    """
    Convert ORM object to schema and normalize NaN/Inf floats to None
    so JSON serialization does not fail.
    """
    payload = EmployeeResponse.model_validate(employee).model_dump()
    cleaned = remove_nan(payload)
    return EmployeeResponse.model_validate(cleaned)


@router.get("/", response_model=EmployeeListResponse)
def get_employees(
    active_only: bool = True,
    department_id: int | None = None,
    session: Session = Depends(get_db),
    current_user: Employee = Depends(get_current_user),
):
    """
    List employees with optional filters.
    - `active_only`: Filter by active status (default: true)
    - `department_id`: Filter by department ID (optional)
    """
    query = session.query(Employee)

    if active_only:
        query = query.filter(Employee.active_status == True)

    if department_id:
        query = query.filter(Employee.department_id == department_id)

    total = query.count()
    employees = [_safe_employee_response(e) for e in query.all()]

    return {"total": total, "items": employees}


@router.get("/{employee_id}", response_model=EmployeeResponse)
def get_employee(
    employee_id: int,
    session: Session = Depends(get_db),
    current_user: Employee = Depends(get_current_user),
):
    """
    Get employee by ID.
    """
    employee = session.query(Employee).filter(Employee.id == employee_id).first()
    if not employee:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee with ID {employee_id} not found.",
        )
    return _safe_employee_response(employee)


@router.post("/", response_model=EmployeeResponse, status_code=status.HTTP_201_CREATED)
def create_employee(
    employee: EmployeeCreate,
    session: Session = Depends(get_db),
    current_user: Employee = Depends(get_admin_user),
):
    """
    Create a new employee. Admin-only endpoint.
    """
    # Check if employee ID already exists
    existing = session.query(Employee).filter(
        Employee.client_employee_id == employee.client_employee_id
    ).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Employee with ID {employee.client_employee_id} already exists.",
        )

    db_employee = Employee(**employee.model_dump())
    session.add(db_employee)
    session.commit()
    session.refresh(db_employee)
    return _safe_employee_response(db_employee)


@router.put("/{employee_id}", response_model=EmployeeResponse)
def update_employee(
    employee_id: int,
    employee: EmployeeUpdate,
    session: Session = Depends(get_db),
    current_user: Employee = Depends(get_admin_user),
):
    """
    Update an existing employee. Admin-only endpoint.
    """
    db_employee = session.query(Employee).filter(Employee.id == employee_id).first()
    if not db_employee:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee with ID {employee_id} not found.",
        )

    update_data = employee.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_employee, field, value)

    session.commit()
    session.refresh(db_employee)
    return _safe_employee_response(db_employee)


@router.delete("/{employee_id}", response_model=EmployeeResponse)
def delete_employee(
    employee_id: int,
    session: Session = Depends(get_db),
    current_user: Employee = Depends(get_admin_user),
):
    """
    Soft-delete an employee. Admin-only endpoint.
    Sets active_status = false and term_date = today.
    """
    db_employee = session.query(Employee).filter(Employee.id == employee_id).first()
    if not db_employee:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee with ID {employee_id} not found.",
        )

    session.query(Employee).filter(Employee.id == employee_id).update(
        {
            Employee.active_status: False,
            Employee.term_date: func.now(),
        }
    )
    session.commit()
    # Refresh the employee object
    db_employee = session.query(Employee).filter(Employee.id == employee_id).first()
    return _safe_employee_response(db_employee)
