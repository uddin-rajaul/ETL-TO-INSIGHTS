from sqlalchemy import (
    Column,
    Integer,
    String,
    UniqueConstraint,
    ForeignKey,
    Date,
    DateTime,
    Boolean,
    Numeric,
    Text,
    Float,
    Index,
)
from sqlalchemy.orm import relationship
from db.base import Base
from sqlalchemy.sql import func


class Organization(Base):
    __tablename__ = "organization"
    __table_args__ = {"schema": "silver"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(50), unique=True, nullable=False)
    name = Column(String(255), nullable=False)

    departments = relationship("Department", back_populates="organization")
    employees = relationship("Employee", back_populates="organization")


class Department(Base):
    __tablename__ = "department"
    __table_args__ = (
        UniqueConstraint("code", "organization_id", name="uq_dept_code_org"),
        {"schema": "silver"},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(50), nullable=False)
    name = Column(String(255), nullable=False)
    organization_id = Column(Integer, ForeignKey("silver.organization.id"), nullable=False)

    organization = relationship("Organization", back_populates="departments")
    employees = relationship("Employee", back_populates="department")


class Employee(Base):
    __tablename__ = "employee"
    __table_args__ = (
        Index("ix_employee_dept", "department_id"),
        Index("ix_employee_active", "active_status"),
        Index("ix_employee_hire_date", "hire_date"),
        {"schema": "silver"},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_employee_id = Column(String(50), unique=True, nullable=False)
    first_name = Column(String(100))
    middle_name = Column(String(100))
    last_name = Column(String(100))
    preferred_name = Column(String(100))
    job_code = Column(String(50))
    job_title = Column(String(255))
    job_start_date = Column(Date)
    fte_status = Column(String(50))
    scheduled_weekly_hour = Column(Numeric(5, 2))
    clinical_level = Column(String(100))
    is_per_deim = Column(Boolean, default=False)
    dob = Column(Date)
    hire_date = Column(Date)
    recent_hire_date = Column(Date)
    anniversary_date = Column(Date)
    term_date = Column(Date)
    termination_reason = Column(String(255))
    years_of_experience = Column(Float)
    active_status = Column(Boolean, default=True, nullable=False)
    work_email = Column(String(255))
    address = Column(Text)
    city = Column(String(100))
    state = Column(String(100))
    zip = Column(String(20))
    country = Column(String(100))
    cell_phone = Column(String(50))
    work_phone = Column(String(50))
    organization_id = Column(Integer, ForeignKey("silver.organization.id"))
    department_id = Column(Integer, ForeignKey("silver.department.id"))
    manager_employee_id = Column(String(50), ForeignKey("silver.employee.client_employee_id"))
    manager_employee_name = Column(String(255))
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    organization = relationship("Organization", back_populates="employees")
    department = relationship("Department", back_populates="employees")
    timesheets = relationship("Timesheet", back_populates="employee",
                                foreign_keys="Timesheet.client_employee_id",
                                primaryjoin="Employee.client_employee_id == Timesheet.client_employee_id")


class Timesheet(Base):
    __tablename__ = "timesheet"
    __table_args__ = (
        Index("ix_timesheet_employee", "client_employee_id"),
        Index("ix_timesheet_date", "punch_apply_date"),
        Index("ix_timesheet_employee_date", "client_employee_id", "punch_apply_date"),
        {"schema": "silver"},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_employee_id       = Column(String(50), ForeignKey("silver.employee.client_employee_id"), nullable=False)
    department_id = Column(String(50))
    department_name = Column(String(255))
    home_department_id = Column(String(50))
    home_department_name = Column(String(255))
    pay_code = Column(String(100))
    punch_apply_date = Column(Date, nullable=False)
    punch_in_datetime = Column(DateTime)
    punch_out_datetime = Column(DateTime)
    scheduled_start_datetime = Column(DateTime)
    scheduled_end_datetime = Column(DateTime)
    hours_worked = Column(Numeric(6, 2))
    punch_in_comment = Column(String(100))
    punch_out_comment = Column(String(100))
    is_late_arrival = Column(Boolean, default=False)
    is_early_departure = Column(Boolean, default=False)
    is_overtime = Column(Boolean, default=False)
    is_unscheduled = Column(Boolean, default=False)
    source_file = Column(Text)
    created_at = Column(DateTime, server_default=func.now())

    employee = relationship("Employee", back_populates="timesheets",
                            foreign_keys=[client_employee_id],
                            primaryjoin="Timesheet.client_employee_id == Employee.client_employee_id")