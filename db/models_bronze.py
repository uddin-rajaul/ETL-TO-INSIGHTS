"""
Bronze layer — raw tables.

Data is loaded here directly from CSV files with no transformation.
All columns are stored as Text regardless of their actual type.
This layer is append-only and serves as a full audit trail.
If anything breaks in the transform step, this data is always safe to reprocess.
"""


from sqlalchemy import Column, Integer, Text, DateTime
from sqlalchemy.sql import func
from db.base import Base


class RawEmployee(Base):
    """
    Raw employee data loaded directly from the employee CSV.
    Column names match CSV headers exactly.
    source_file tracks which file this row came from.
    """

    __tablename__ = "raw_employee"
    __table_args__ = {"schema": "bronze"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_employee_id = Column(Text)
    first_name = Column(Text)
    middle_name = Column(Text)
    last_name = Column(Text)
    preferred_name = Column(Text)
    job_code = Column(Text)
    job_title = Column(Text)
    job_start_date = Column(Text)
    organization_id = Column(Text)
    organization_name = Column(Text)
    department_id = Column(Text)
    department_name = Column(Text)
    dob = Column(Text)
    hire_date = Column(Text)
    recent_hire_date = Column(Text)
    anniversary_date = Column(Text)
    term_date = Column(Text)
    years_of_experience = Column(Text)
    work_email = Column(Text)
    address = Column(Text)
    city = Column(Text)
    state = Column(Text)
    zip = Column(Text)
    country = Column(Text)
    manager_employee_id = Column(Text)
    manager_employee_name = Column(Text)
    fte_status = Column(Text)
    is_per_deim = Column(Text)
    cell_phone = Column(Text)
    work_phone = Column(Text)
    scheduled_weekly_hour = Column(Text)
    active_status = Column(Text)
    termination_reason = Column(Text)
    clinical_level = Column(Text)
    loaded_at = Column(DateTime, server_default=func.now())
    source_file = Column(Text)


class RawTimesheet(Base):
    """
    Raw timesheet data loaded from all three timesheet CSV files.
    All three files share the same structure and load into this single table.
    source_file distinguishes which file each row came from.
    """

    __tablename__ = "raw_timesheet"
    __table_args__ = { "schema": "bronze" }

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_employee_id = Column(Text)
    department_id = Column(Text)
    department_name = Column(Text)
    home_department_id = Column(Text)
    home_department_name = Column(Text)
    pay_code = Column(Text)
    punch_in_comment = Column(Text)
    punch_out_comment = Column(Text)
    hours_worked = Column(Text)
    punch_apply_date = Column(Text)
    punch_in_datetime = Column(Text)
    punch_out_datetime = Column(Text)
    scheduled_start_datetime = Column(Text)
    scheduled_end_datetime = Column(Text)
    loaded_at = Column(DateTime, server_default=func.now())
    source_file = Column(Text)


class RejectedTimesheet(Base):
    """
    Dead letter table for timesheet rows that failed validation.
    Rows land here instead of being silently dropped.
    reason column explains why the row was rejected.
    """
    __tablename__ = "rejected_timesheet"
    __table_args__ = {"schema": "bronze"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_employee_id = Column(Text)
    punch_apply_date = Column(Text)
    hours_worked = Column(Text)
    source_file = Column(Text)
    reason = Column(Text, nullable=False)
    rejected_at = Column(DateTime, server_default=func.now())