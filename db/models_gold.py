"""
Gold layer — pre-aggregated KPI tables.

These tables store analytics results computed by the post-processing
step of the ETL pipeline. Power BI connects directly to these tables.
Rebuilding them is safe — they are always fully recomputed on each run.
"""


from sqlalchemy import Column, Integer, String, Numeric, Date, DateTime
from sqlalchemy.sql import func
from db.base import Base

class KpiHeadcount(Base):
    """
    Active headcount snapshot per date.
    Populated by the post-processing step after each ETL run.
    Used for the 'Active Headcount Over Time' KPI.
    """

    __tablename__ = "kpi_headcount"
    __table_args__ = {"schema": "gold"}

    id = Column(Integer, primary_key= True, autoincrement=True)
    snapshot_date = Column(Date, nullable=False, unique= True)
    active_count = Column(Integer, nullable= False)
    computed_at = Column(DateTime, server_default= func.now())


class KpiTurnover(Base):
    """
    Monthly employee termination counts and turnover rate.
    One row per year-month.
    Used for the 'Turnover Trend' KPI.
    """
    __tablename__ = "kpi_turnover"
    __table_args__ = {"schema": "gold"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    terminations = Column(Integer, nullable=False)
    turnover_rate_pct = Column(Numeric(5,2))
    computed_at = Column(DateTime, server_default=func.now())


class KpiTenureByDepartment(Base):
    """
    Average employment duration grouped by department.
    One row per department.
    Used for the 'Average Tenure by Department' KPI.
    """

    __tablename__ = "kpi_tenure_by_department"
    __table_args__ = {"schema": "gold"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    department_id = Column(String(50))
    department_name = Column(String(255))
    avg_tenure_years = Column(Numeric(6,2))
    employee_count = Column(Integer)
    computed_at = Column(DateTime, server_default= func.now())


class KpiAttendance(Base):
    """
    Attendance summary per employee — late arrivals, early departures, overtime.
    One row per employee across all their timesheet records.
    Used for 'Late Arrival Frequency', 'Early Departure Count',
    and 'Total Overtime Count' KPIs.
    """

    __tablename__ = "kpi_attendance"
    __table_args__ = {"schema": "gold"}

    id = Column(Integer, primary_key= True, autoincrement=True)
    client_employee_id = Column(String(50), nullable= False)
    department_name = Column(String(255))
    total_shifts = Column(Integer)
    late_arrival_count = Column(Integer)
    early_departure_count = Column(Integer)
    overtime_count = Column(Integer)
    late_arrival_rate_pct = Column(Numeric(5,2))
    computed_at = Column(DateTime, server_default=func.now())


class KpiRollingHours(Base):
    """
    Daily hours worked with 7-day rolling average per employee.
    One row per employee per day.
    Used for the 'Rolling Average Working Hours' KPI.
    """

    __tablename__ = "kpi_rolling_hours"
    __table_args__ = {"schema": "gold"}

    id = Column(Integer, primary_key=True, autoincrement= True)
    client_employee_id = Column(String(50), nullable=False)
    punch_apply_date = Column(Date, nullable=False)
    hours_worked = Column(Numeric(6,2))
    rolling_avg_7d = Column(Numeric(6,2))
    computed_at = Column(DateTime, server_default=func.now())
    

class KpiEarlyAttrition(Base):
    """
    Early attrition rate grouped by department.
    Counts employees who left within 6 months of joining.
    One row per department.
    Used for the 'Early Attrition Rate' KPI.
    """

    __tablename__ = "kpi_early_attrition"
    __table_args__ = {"schema": "gold"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    department_name = Column(String(255))
    total_hires = Column(Integer)
    early_attrition_count = Column(Integer)
    early_attrition_rate_pct =  Column(Numeric(5,2))
    computed_at = Column(DateTime, server_default=func.now())
