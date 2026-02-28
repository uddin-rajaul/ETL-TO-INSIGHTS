"""
Export step - reads gold kpi tables and writes Parquet files to MinIO.
Acts as data warehouse layer - gold data available outside postgresql.
"""

import io
import pandas as pd
from loguru import logger
from sqlalchemy.orm import Session
from minio import Minio
from minio.error import S3Error

class Exporter:
    """
    Reads gold KPI tables and exports them as Parquet files to MinIO.
    Each gold table becomes one parquet file in the "warehouse/" bucket.
    """

    WAREHOUSE_PREFIX = "warehouse"

    GOLD_TABLES = [
        "gold.kpi_headcount",
        "gold.kpi_turnover",
        "gold.kpi_tenure_by_department",
        "gold.kpi_attendance",
        "gold.kpi_rolling_hours",
        "gold.kpi_early_attrition",
    ]

    def __init__(self, session: Session, minio_client: Minio, bucket: str) -> None:
        self.session = session
        self.minio_client = minio_client
        self.bucket = bucket
    
    def _ensure_bucket(self) -> None:
        """Create the bucket if it does not exist."""
        if not self.minio_client.bucket_exists(self.bucket):
            self.minio_client.make_bucket(self.bucket)
            logger.info(f"Created bucket: {self.bucket}")

    def _export_table(self, table: str) -> int:
        """
        Read a gold table into a DataFrame, convert to Parquet,
        and upload to MinIO under warehouse/ prefix.
        Returns number of rows exported.
        """

        if self.session.bind is None:
            raise ValueError("Session is not bound to an engine or connection.")
        
        logger.info(f"Exporting {table}...")
        
        df = pd.read_sql(f"SELECT * FROM {table}", self.session.bind)
        table_name = table.split(".")[1]
        object_name = f"{self.WAREHOUSE_PREFIX}/{table_name}.parquet"

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        self.minio_client.put_object(
            bucket_name=self.bucket,
            object_name=object_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream",
        )

        logger.info(f"Uploaded {object_name} ({len(df)} rows) to MinIO")
        return len(df)

    def run(self) -> dict:
        """Export all gold tables to MinIO as Parquet files."""
        logger.info("Starting export to MinIO warehouse...")
        self._ensure_bucket()

        summary = {}
        for table in self.GOLD_TABLES:
            try:
                rows = self._export_table(table)
                summary[table] = rows
            except S3Error as e:
                logger.error(f"Failed to export {table}: {e}")
                summary[table] = 0

        logger.info(f"Export complete: {summary}")
        return summary