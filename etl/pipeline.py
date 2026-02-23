"""
ETL pipeline orchestration using Prefect.
Defines task and flows afor the end-to-end ETL process.

Run with: python -m etl/pipeline.py
Monitor at: http://localhost:4300
"""

from prefect import flow, task, get_run_logger
from db.base import SessionLocal
from etl.extract.extractor import Extractor
from etl.transform.transformer import Transformer

@task(name="Extract", retries = 2, retry_delay_seconds=30)
def extract_task():
    """
    Extract step - reads CSV files and loads into bronze tables.
    Retries up to 2 times with a 30 second delay if it fails.
    """

    logger = get_run_logger()
    session = SessionLocal()

    try:
        extractor = Extractor(session)
        result = extractor.run()
        logger.info(f"Extract complete: {result}")

        return result
    except Exception as e:
        logger.error(f"Extract failed: {e}")
        raise
    finally:
        session.close()
    

@task(name="Transform", retries=1, retry_delay_seconds=60)
def transform_task():
    """
    Transform step — cleans bronze data and loads into silver layer.
    Retries once if it fails — longer delay since transform is heavy.
    """
    logger = get_run_logger()
    session = SessionLocal()
    try:
        transformer = Transformer(session)
        result = transformer.run()
        logger.info(f"Transform complete: {result}")
        return result
    except Exception as e:
        logger.error(f"Transform failed: {e}")
        raise
    finally:
        session.close()


@flow(name="ETL Pipeline")
def etl_pipeline():
    """
    Main ETL flow - runs extract and transform in order.
    Prefect ensures that transform only runs after extract succeeds.
    """

    logger = get_run_logger()
    logger.info("Starting ETL pipeline")

    extract_result = extract_task()
    transform_result = transform_task(wait_for=[extract_result])

    logger.info(f"Pipeline complete. Extract: {extract_result}, Transform: {transform_result}")

    return {
        "extract": extract_result,
        "transform": transform_result,
    }

if __name__ == "__main__":
    etl_pipeline()