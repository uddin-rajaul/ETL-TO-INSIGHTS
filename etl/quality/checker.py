"""
Quality check step - validates silver layer data after transform.
Generates a quality report and logs findings.
"""
import json
import os
import yaml
from pathlib import Path
from datetime import datetime
from loguru import logger
from sqlalchemy import text
from sqlalchemy.orm import Session


def load_config():
    """Load settings from config/settings.yaml."""
    with open("config/settings.yaml", "r") as f:
        return yaml.safe_load(f)


class QualityChecker:
    """
    Runs data quality checks on the silver layer after transformation.
    Generates a report summarising the results of the checks.
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self.config = load_config()
        self.orphan_rate_max_pct = self.config["etl"]["orphan_rate_max_pct"]
        self.attendance_flag_max_overlap = self.config["etl"]["attendance_flag_max_overlap"]
        self.report = {
            "run_at": datetime.now().isoformat(),
            "checks": []
        }
    
    def _add_result(self, check_name: str, passed: bool, details: str) -> None:
        """ Record a check result into the report. """
        status = "PASS" if passed else "FAIL"
        logger.info(f"[{status}] {check_name}: {details}")
        self.report["checks"].append({
            "check": check_name,
            "status": status,
            "details": details
        })
    
    def check_nulls(self) -> None:
        """ Check critical fields for null values."""

        checks = [
            ("silver.employee", "client_employee_id"),
            ("silver.employee", "hire_date"),
            ("silver.employee", "first_name"),
            ("silver.timesheet", "client_employee_id"),
            ("silver.timesheet", "punch_apply_date"),
            ("silver.timesheet", "hours_worked"),
        ]

        for table, column in checks:
            query = text(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
            result = self.session.execute(query).scalar()

            self._add_result(
                check_name=f"null_check_{table.split('.')[1]}_{column}",
                passed=result == 0,
                details=f"{result} null values found in {table}.{column}",
            )

    def check_duplicates(self) -> None:
        """
        Check for duplicate employee IDs in the silver layer.
        """

        result = self.session.execute(text("""
        SELECT COUNT(*) FROM (
            SELECT client_employee_id
            FROM silver.employee
            GROUP BY client_employee_id
            HAVING COUNT(*) > 1
        ) duplicates
    """)).scalar()
        
        self._add_result(
            check_name="duplicate_check_employee_id",
            passed=result == 0,
            details=f"{result} duplicate employee IDs found in silver.employee",
        )

    def check_date_logic(self)-> None:
        """ Check that hire_date is not in the future. """
        result = self.session.execute(text("""
            SELECT COUNT(*) 
            FROM silver.employee
            WHERE term_date IS NOT NULL
            AND hire_date >= term_date
        """)).scalar()

        self._add_result(
            check_name="date_logic_check_hire_date",
            passed=result == 0,
            details=f"{result} records with hire_date in the future found in silver.employee",
        )

    def check_hours_worked(self) -> None:
        """ Check that hours_worked is between 0 and 24. """
        result = self.session.execute(text("""
            SELECT COUNT(*) 
            FROM silver.timesheet
            WHERE hours_worked IS NOT NULL
            AND (hours_worked < 0 OR hours_worked > 24)
        """)).scalar()

        self._add_result(
            check_name="hours_worked_check",
            passed=result == 0,
            details=f"{result} records with invalid hours_worked found in silver.timesheet",
        )
    
    def check_orphan_rate(self) -> None:
        """Check the percentage of rejected timesheet rows."""

        total = self.session.execute(
            text("SELECT COUNT(*) FROM bronze.raw_timesheet")
        ).scalar() or 0

        rejected = self.session.execute(
            text("SELECT COUNT(*) FROM bronze.rejected_timesheet")
        ).scalar() or 0

        if total == 0:
            self._add_result(
                check_name="orphan_rate_check",
                passed=False,
                details="No rows found in bronze.raw_timesheet",
            )
            return

        rate = round((rejected / total) * 100, 2)
        passed = rate < self.orphan_rate_max_pct

        self._add_result(
            check_name="orphan_rate_check",
            passed=passed,
            details=f"{rejected} of {total} rows rejected ({rate}% orphan rate)",
        )

    def check_attendance_flag(self) -> None:
        """Check that no row has both is_late_arrival and is_early_departure set to True."""

        result = self.session.execute(text("""
            SELECT COUNT(*)
            FROM silver.timesheet
            WHERE is_late_arrival = TRUE
            AND is_early_departure = TRUE
        """)).scalar() or 0

        self._add_result(
            check_name="attendance_flag_check",
            passed=result <= self.attendance_flag_max_overlap,
            details=f"{result} records with both is_late_arrival and is_early_departure set to True found in silver.timesheet",
        )

    def save_report(self) -> None:
        """Save the quality report to a json file in the logs directory."""

        os.makedirs("logs", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f"logs/quality_report_{timestamp}.json"

        with open(filepath, "w") as f:
            json.dump(self.report, f, indent=4)
        
        logger.info(f"Quality report saved to {filepath}")

    def run(self)-> dict:
        """ Execute all quality checks and return the report. """
        logger.info("Starting quality checks...")
        self.check_nulls()
        self.check_duplicates()
        self.check_date_logic()
        self.check_hours_worked()
        self.check_orphan_rate()
        self.check_attendance_flag()
        self.save_report()
        logger.info("Quality checks complete.")
        return self.report
    
