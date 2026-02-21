from sqlalchemy import Column, Integer, Text, DateTime
from sqlalchemy.sql import func
from db.base import Base


class RawEmployee(Base):
    __tablename__ = "raw_employee"
    __table_args__ = {"schema": "bronze"}

    id                    = Column(Integer, primary_key=True, autoincrement=True)
    client_employee_id    = Column(Text)
    first_name            = Column(Text)
    middle_name           = Column(Text)
    last_name             = Column(Text)
    preferred_name        = Column(Text)
    job_code              = Column(Text)
    job_title             = Column(Text)
    job_start_date        = Column(Text)
    organization_id       = Column(Text)
    organization_name     = Column(Text)
    department_id         = Column(Text)
    department_name       = Column(Text)
    dob                   = Column(Text)
    hire_date             = Column(Text)
    recent_hire_date      = Column(Text)
    anniversary_date      = Column(Text)
    term_date             = Column(Text)
    years_of_experience   = Column(Text)
    work_email            = Column(Text)
    address               = Column(Text)
    city                  = Column(Text)
    state                 = Column(Text)
    zip                   = Column(Text)
    country               = Column(Text)
    manager_employee_id   = Column(Text)
    manager_employee_name = Column(Text)
    fte_status            = Column(Text)
    is_per_deim           = Column(Text)
    cell_phone            = Column(Text)
    work_phone            = Column(Text)
    scheduled_weekly_hour = Column(Text)
    active_status         = Column(Text)
    termination_reason    = Column(Text)
    clinical_level        = Column(Text)
    loaded_at             = Column(DateTime, server_default=func.now())
    source_file           = Column(Text)


class RawTimesheet(Base):
    __tablename__ = "raw_timesheet"
    __table_args__ = { "schema": "bronze" }

    id                       = Column(Integer, primary_key=True, autoincrement=True)
    client_employee_id       = Column(Text)
    department_id            = Column(Text)
    department_name          = Column(Text)
    home_department_id       = Column(Text)
    home_department_name     = Column(Text)
    pay_code                 = Column(Text)
    punch_in_comment         = Column(Text)
    punch_out_comment        = Column(Text)
    hours_worked             = Column(Text)
    punch_apply_date         = Column(Text)
    punch_in_datetime        = Column(Text)
    punch_out_datetime       = Column(Text)
    scheduled_start_datetime = Column(Text)
    scheduled_end_datetime   = Column(Text)
    loaded_at                = Column(DateTime, server_default=func.now())
    source_file              = Column(Text)