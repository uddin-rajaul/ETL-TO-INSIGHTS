"""
Extract step - reads CSV files from local disk or MinIO and loads raw data into the bronze layer tables.
"""

import os
import glob
import pandas as pd
from loguru import logger
from pathlib import Path
import sqlalchemy
from sqlalchemy.orm import Session


import yaml
from db.models_bronze import RawEmployee, RawTimesheet

def load_config():
    """Load settings from config/settings.yaml."""
    with open("config/settings.yaml", "r") as f:
        return yaml.safe_load(f)


class Extractor:
    """
    Reads employess and timnesheet CSV files and raw rows into bronze.raw_employee and bronze.raw_timesheet.
    Supports loading from local disk and  MinIO (S3-Compatible Storage).
    """

    def __init__(self, session: Session):
        self.session = session
        self.config = load_config()
        self.data_source = os.getenv("DATA_SOURCE", "local")
        self.raw_data_path = os.getenv("RAW_DATA_PATH", "data/raw")
    

    def _get_local_files(self, pattern: str) -> list[str]:
        """
        Find all CSV files matching a pattern in the raw data filder.
        Example: pattern = 'timesheet' finds all timesheet_*.csv files.
        """

        search_path = os.path.join(self.raw_data_path, f"*{pattern}*.csv")
        files = glob.glob(search_path)
        if not files:
            logger.warning(f"No files found matching pattern:{search_path}")
        else:
            logger.info(f"Found {len(files)} files for pattern '{pattern}'")
        return files
    
    def _read_csv(self, filepath: str) -> pd.DataFrame:
        """
        Read a pipe-delimited CSV file into a dataframe.
        Replaces [NULL] strings with actual NaN values (missing values).
        """

        logger.info(f"Reading file: {filepath}")
        df = pd.read_csv(
            filepath,
            delimiter="|",
            dtype=str,
            na_values=["[NULL]", "NULL", ""],
            keep_default_na=True,
        )

        df.columns = df.columns.str.strip().str.lower()
        logger.info(f"Loaded {len(df)} rows from {Path(filepath).name}")
        return df
    

    def extract_employees(self) -> int:
        """
        Read employee CSV and insert rows into bronze.raw_employee.
        Returns the number of rows inserted.
        """

        files = self._get_local_files("employee")
        if not files:
            logger.error("No employee file found. Aborting.")
            return 0
        
        total_inserted = 0

        for filepath in files:
            df = self._read_csv(filepath)
            filename = Path(filepath).name

            rows = []
            for _, row in df.iterrows():
                rows.append(RawEmployee(
                    client_employee_id = row.get("client_employee_id"),
                    first_name = row.get("first_name"),
                    middle_name = row.get("middle_name"),
                    last_name = row.get("last_name"),
                    preferred_name = row.get("preferred_name"),
                    job_code = row.get("job_code"),
                    job_title = row.get("job_title"),
                    job_start_date = row.get("job_start_date"),
                    organization_id = row.get("organization_id"),
                    organization_name = row.get("organization_name"),
                    department_id = row.get("department_id"),
                    department_name = row.get("department_name"),
                    dob = row.get("dob"),
                    hire_date = row.get("hire_date"),
                    recent_hire_date = row.get("recent_hire_date"),
                    anniversary_date = row.get("anniversary_date"),
                    term_date = row.get("term_date"),
                    years_of_experience = row.get("years_of_experience"),
                    work_email = row.get("work_email"),
                    address = row.get("address"),
                    city = row.get("city"),
                    state = row.get("state"),
                    zip = row.get("zip"),
                    country = row.get("country"),
                    manager_employee_id = row.get("manager_employee_id"),
                    manager_employee_name = row.get("manager_employee_name"),
                    fte_status = row.get("fte_status"),
                    is_per_deim = row.get("is_per_deim"),
                    cell_phone = row.get("cell_phone"),
                    work_phone = row.get("work_phone"),
                    scheduled_weekly_hour = row.get("scheduled_weekly_hour"),
                    active_status = row.get("active_status"),
                    termination_reason = row.get("termination_reason"),
                    clinical_level = row.get("clinical_level"),
                    source_file = filename,
                ))

            self.session.bulk_save_objects(rows)
            self.session.commit()
            total_inserted += len(rows)
            logger.info(f"Inserted {len(rows)} employee rows from {filename}")
        
        return total_inserted
    

    def extract_timesheets(self) -> int:
        """
        Read all timesheet CSVs and insert rows into bronze.raw_timesheet.
        Handles multiple files automatically.
        Returns the number of rows inserted.
        """

        files = self._get_local_files("timesheet")
        if not files:
            logger.error("No timesheet files found. Aborting.")
            return 0
        
        total_inserted = 0
        config = self.config["etl"]
        batch_size = config["batch_size"]

        for filepath in files:
            df = self._read_csv(filepath)
            filename = Path(filepath).name 
            rows = []

            for _, row in df.iterrows():
                rows.append(RawTimesheet(
                    client_employee_id = row.get("client_employee_id"),
                    department_id = row.get("department_id"),
                    department_name = row.get("department_name"),
                    home_department_id = row.get("home_department_id"),
                    home_department_name = row.get("home_department_name"),
                    pay_code = row.get("pay_code"),
                    punch_in_comment = row.get("punch_in_comment"),
                    punch_out_comment = row.get("punch_out_comment"),
                    hours_worked = row.get("hours_worked"),
                    punch_apply_date = row.get("punch_apply_date"),
                    punch_in_datetime = row.get("punch_in_datetime"),
                    punch_out_datetime = row.get("punch_out_datetime"),
                    scheduled_start_datetime = row.get("scheduled_start_datetime"),
                    scheduled_end_datetime = row.get("scheduled_end_datetime"),
                    source_file = filename,
                ))

                # Insert in batches to avoid memory issues with large files
                if len(rows) >= batch_size:
                    self.session.bulk_save_objects(rows)
                    self.session.commit()
                    total_inserted += len(rows)
                    logger.info(f"Inserted {len(rows)} timesheet rows from {filename}")
                    rows = []

            # Insert any remaining rows after the loop
            if rows:
                self.session.bulk_save_objects(rows)
                self.session.commit()
                total_inserted += len(rows)
                logger.info(f"Inserted {len(rows)} timesheet rows from {filename}")
        
        logger.info(f"Total timesheet rows inserted: {total_inserted}")
        return total_inserted


    def _clear_bronze_tables(self) -> None:
        """
        Truncate bronze tables before each run to prevent duplicate rows.
        Bronze data is always reloaded fresh from source files.
        """
        logger.info("Clearing bronze tables before extraction...")
        self.session.execute(sqlalchemy.text("TRUNCATE TABLE bronze.raw_timesheet RESTART IDENTITY"))
        self.session.execute(sqlalchemy.text("TRUNCATE TABLE bronze.raw_employee RESTART IDENTITY"))
        self.session.commit()
        logger.info("Bronze tables cleared.")


    def run(self):
        """
        Run the full extraction step.
        Returns a summary dict with row counts for logging."""

        logger.info("Starting extraction step...")
        self._clear_bronze_tables()

        employee_count = self.extract_employees()
        timesheet_count = self.extract_timesheets()

        summary = {
            "employee_extracted": employee_count,
            "timesheets_extracted": timesheet_count,
        }
        logger.info(f"Extraction completed. Summary: {summary}")
        return summary
    
