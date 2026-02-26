"""
Transform step - reads raw data from bronze layer, cleans and type-casts it.
Then loads it into the silver layer tables.
"""

import re
import yaml
import pandas as pd
from datetime import date, datetime
from loguru import logger
from sqlalchemy.orm import Session
from sqlalchemy import text

from db.models_bronze import RawEmployee, RawTimesheet, RejectedTimesheet
from db.models_silver import Organization, Department, Employee, Timesheet


def load_config():
    """Load settings from config/settings.yaml."""
    with open("config/settings.yaml", "r") as f:
        return yaml.safe_load(f)
    

class Transformer:
    """
    Reads from bronze layer, cleans and transforms the data,
    then writes to silver layer tables.
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self.config = load_config()
        self.grace_minutes = self.config["etl"]["grace_time_minutes"]
    
    def _clean_string(self, value) -> str | None:
        """Strip whitespace and return None if empty or NaN."""
        if value is None:
            return None
        if pd.isna(value):
            return None
        cleaned = str(value).strip()
        if cleaned.lower() in ("nan", "nat", "none", "null", ""):
            return None
        return cleaned
    
    def _parse_date(self, value) -> date | None:
        """Convert a date string to a Python date object."""
        if value is None:
            return None
        try:
            parsed = pd.to_datetime(str(value).strip())
            if pd.isna(parsed):
                return None
            return parsed.date()
        except Exception:
            logger.warning(f"Could not parse date: {value}")
            return None

    def _parse_datetime(self, value) -> datetime | None:
        """Convert a datetime string to a Python datetime object."""
        if value is None:
            return None
        try:
            parsed = pd.to_datetime(str(value).strip())
            if pd.isna(parsed):
                return None
            return parsed.to_pydatetime()
        except Exception:
            logger.warning(f"Could not parse datetime: {value}")
            return None
    
    def _parse_float(self, value) -> float | None:
        """Convert a string to float, return None if invalid."""
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
            return float(str(value).strip())
        except Exception:
            logger.warning(f"Could not parse float: {value}")
            return None
    
    def _parse_bool(self, value) -> bool | None:
        """
        Convert various truthy string representations to boolean True/False.
        Example: 'true', 'yes', '1' -> True; 'false', 'no', '0' -> False.
        """

        if pd.isna(value) or value is None:
            return False
        return str(value).strip().lower() in ("1", "true", "yes")
    
    def _get_or_create_organization(self, code: str | None, name: str | None) -> Organization | None:
        """
        Get existing organization or create a new one.
        Avoid duplicate organiations in the silver layer.
        """

        if not code or not name:
            return None
        
        org = self.session.query(Organization).filter_by(code=code).first()
        if not org:
            org = Organization(code=code, name=name)
            self.session.add(org)
            self.session.flush()  # Get the ID assigned
            logger.debug(f"Created organization {name}")
        
        return org
    
    def _get_or_create_department(self, code: str | None, name: str | None, org_id: int | None) -> Department | None:
        """
        get existing department or create a new one.
        Scoped to organization - same code can exist in different orgs.
        """

        if not code or not name or not org_id:
            return None

        dept = self.session.query(Department).filter_by(
            code=code,
            organization_id=org_id
        ).first()

        if not dept:
            dept = Department(
                code=code,
                name=name,
                organization_id=org_id
            )
            self.session.add(dept)
            self.session.flush()
            logger.debug(f"Created department {name} in org_id {org_id}")
        
        return dept

    
    def transform_employees(self) -> int:
        """
        Read from bronze.raw_employee and insert into silver.employee.
        Uses two passes to handle self-referencing manager FK:
        - Pass 1: insert all employees without manager_employee_id
        - Pass 2: update manager_employee_id after all employees exist
        """
        logger.info("Transforming employees...")
        raw_rows = self.session.query(RawEmployee).all()

        if not raw_rows:
            logger.warning("No raw employee data found in bronze layer.")
            return 0

        # --- Pass 1: insert all employees without manager FK ---
        for raw in raw_rows:
            org = self._get_or_create_organization(
                code=self._clean_string(raw.organization_id),
                name=self._clean_string(raw.organization_name),
            )
            dept = self._get_or_create_department(
                code=self._clean_string(raw.department_id),
                name=self._clean_string(raw.department_name),
                org_id=int(org.id) if org else None,  # type: ignore[arg-type]
            ) if org else None

            employee = Employee(
                client_employee_id    = self._clean_string(raw.client_employee_id),
                first_name            = self._clean_string(raw.first_name),
                middle_name           = self._clean_string(raw.middle_name),
                last_name             = self._clean_string(raw.last_name),
                preferred_name        = self._clean_string(raw.preferred_name),
                job_code              = self._clean_string(raw.job_code),
                job_title             = self._clean_string(raw.job_title),
                job_start_date        = self._parse_date(raw.job_start_date),
                fte_status            = self._clean_string(raw.fte_status),
                scheduled_weekly_hour = self._parse_float(raw.scheduled_weekly_hour),
                clinical_level        = self._clean_string(raw.clinical_level),
                is_per_deim           = self._parse_bool(raw.is_per_deim),
                dob                   = self._parse_date(raw.dob),
                hire_date             = self._parse_date(raw.hire_date),
                recent_hire_date      = self._parse_date(raw.recent_hire_date),
                anniversary_date      = self._parse_date(raw.anniversary_date),
                term_date             = self._parse_date(raw.term_date),
                termination_reason    = self._clean_string(raw.termination_reason),
                years_of_experience   = self._parse_float(raw.years_of_experience),
                active_status         = self._parse_bool(raw.active_status),
                work_email            = self._clean_string(raw.work_email),
                address               = self._clean_string(raw.address),
                city                  = self._clean_string(raw.city),
                state                 = self._clean_string(raw.state),
                zip                   = self._clean_string(raw.zip),
                country               = self._clean_string(raw.country),
                cell_phone            = self._clean_string(raw.cell_phone),
                work_phone            = self._clean_string(raw.work_phone),
                manager_employee_id   = None,  # set in pass 2
                manager_employee_name = self._clean_string(raw.manager_employee_name),
                organization_id       = int(org.id) if org else None,  # type: ignore[arg-type]
                department_id         = int(dept.id) if dept else None,  # type: ignore[arg-type]
            )
            self.session.add(employee)

        self.session.commit()
        logger.info("Pass 1 complete — all employees inserted without manager FK.")

        # --- Pass 2: update manager_employee_id only if manager exists ---
        valid_ids = set(
            row[0] for row in self.session.query(Employee.client_employee_id).all()
        )

        for raw in raw_rows:
            manager_id = self._clean_string(raw.manager_employee_id)
            if not manager_id or manager_id not in valid_ids:
                if manager_id:
                    logger.warning(f"Manager {manager_id} not found in dataset, skipping FK.")
                continue

            self.session.query(Employee).filter_by(
                client_employee_id=self._clean_string(raw.client_employee_id)
            ).update({"manager_employee_id": manager_id})

        self.session.commit()

        logger.info(f"Pass 2 complete — manager FKs updated.")
        logger.info(f"Transformed and inserted {len(raw_rows)} employees.")
        return len(raw_rows)
    
    def _compute_flags(self, row: RawTimesheet) -> dict:
        """
        Compute attendance boolean flags using grace_time_minutes from config.
        Compares actual punch times against scheduled times.
        Returns a dict with four boolean flags.
        """
        from datetime import datetime, timedelta

        grace = timedelta(minutes=self.grace_minutes)

        def parse_dt(value):
            if not value or pd.isna(value):
                return None
            try:
                return pd.to_datetime(str(value).strip()).to_pydatetime()
            except Exception:
                return None

        punch_in = parse_dt(row.punch_in_datetime)
        punch_out = parse_dt(row.punch_out_datetime)
        sched_start = parse_dt(row.scheduled_start_datetime)
        sched_end = parse_dt(row.scheduled_end_datetime)

        is_unscheduled    = sched_start is None or sched_end is None
        is_late_arrival   = False
        is_early_departure = False
        is_overtime       = False

        if not is_unscheduled and sched_start and sched_end:
            if punch_in and punch_in > sched_start + grace:
                is_late_arrival = True
            if punch_out and punch_out < sched_end - grace:
                is_early_departure = True
            if punch_out and punch_out > sched_end + grace:
                is_overtime = True

        return {
            "is_late_arrival":   is_late_arrival,
            "is_early_departure": is_early_departure,
            "is_overtime":       is_overtime,
            "is_unscheduled":    is_unscheduled,
        }
    
    def transform_timesheets(self) -> int:
        """
        Read from bronze.raw_timesheet, clean and type-cast each row,
        compute attendance flags, then insert into silver.timesheet.
        Processes in batches to handle 400k+ rows efficiently.
        Returns number of rows inserted.
        """
        logger.info("Transforming timesheets...")

        # get all valid employee ids from silver for FK validation
        valid_ids = set(
            row[0] for row in self.session.query(Employee.client_employee_id).all()
        )
        logger.info(f"Found {len(valid_ids)} valid employee IDs in silver layer.")

        batch_size = self.config["etl"]["batch_size"]
        offset = 0
        total_inserted = 0
        skipped = 0

        while True:
            raw_batch = (
                self.session.query(RawTimesheet)
                .offset(offset)
                .limit(batch_size)
                .all()
            )

            if not raw_batch:
                break

            rows = []
            for raw in raw_batch:
                emp_id = self._clean_string(raw.client_employee_id)

                # skip rows with no matching employee in silver
                if not emp_id or emp_id not in valid_ids:
                    skipped += 1
                    self.session.add(RejectedTimesheet(
                        client_employee_id = emp_id,
                        punch_apply_date = self._clean_string(raw.punch_apply_date),
                        hours_worked = self._clean_string(raw.hours_worked),
                        source_file = self._clean_string(raw.source_file),
                        reason = "MISSING_EMPLOYEE_RECORD",
                    ))
                    continue

                flags = self._compute_flags(raw)

                rows.append(Timesheet(
                    client_employee_id = emp_id,
                    department_id = self._clean_string(raw.department_id),
                    department_name = self._clean_string(raw.department_name),
                    home_department_id = self._clean_string(raw.home_department_id),
                    home_department_name = self._clean_string(raw.home_department_name),
                    pay_code = self._clean_string(raw.pay_code),
                    punch_apply_date = self._parse_date(raw.punch_apply_date),
                    punch_in_datetime = self._parse_datetime(raw.punch_in_datetime),
                    punch_out_datetime = self._parse_datetime(raw.punch_out_datetime),
                    scheduled_start_datetime = self._parse_datetime(raw.scheduled_start_datetime),
                    scheduled_end_datetime = self._parse_datetime(raw.scheduled_end_datetime),
                    hours_worked = self._parse_float(raw.hours_worked),
                    punch_in_comment = self._clean_string(raw.punch_in_comment),
                    punch_out_comment = self._clean_string(raw.punch_out_comment),
                    source_file = self._clean_string(raw.source_file),
                    **flags,
                ))

            self.session.bulk_save_objects(rows)
            self.session.commit()  # commits both valid and rejected rows
            total_inserted += len(rows)
            offset += batch_size
            logger.info(f"Transformed batch — total so far: {total_inserted}, skipped: {skipped}")

        logger.info(f"Timesheet transform complete. Inserted: {total_inserted}, Skipped: {skipped}")
        return total_inserted
    
    def _clear_silver_tables(self) -> None:
        """
        Truncate silver tables before each run to prevent duplicate rows.
        Order matters — child tables must be truncated before parent tables.
        """
        logger.info("Clearing silver tables...")
        self.session.execute(text("TRUNCATE TABLE silver.timesheet RESTART IDENTITY CASCADE"))
        self.session.execute(text("TRUNCATE TABLE silver.employee RESTART IDENTITY CASCADE"))
        self.session.execute(text("TRUNCATE TABLE silver.department RESTART IDENTITY CASCADE"))
        self.session.execute(text("TRUNCATE TABLE silver.organization RESTART IDENTITY CASCADE"))
        self.session.commit()
        logger.info("Silver tables cleared.")

    def run(self) -> dict:
        """
        Run the full transform step.
        Returns a summary dict with row counts for logging.
        """
        logger.info("Starting transform step...")
        self._clear_silver_tables()
        employee_count = self.transform_employees()
        timesheet_count = self.transform_timesheets()

        summary = {
            "employees_transformed": employee_count,
            "timesheets_transformed": timesheet_count,
        }

        logger.info(f"Transform complete: {summary}")
        return summary
