"""
ETL pipeline orchestration using Prefect.
Defines task and flows afor the end-to-end ETL process.

Run with: python -m etl/pipeline.py
Monitor at: http://localhost:4300
"""

from prefect import flow, task, get_run_logger
from db.base import SessionLocal
from etl.extract.extractor import Extractor
from etl.postprocess.postprocessor import PostProcessor
from etl.transform.transformer import Transformer
from etl.quality.checker import QualityChecker

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

@task(name="Quality Check", retries=1, retry_delay_seconds=30)
def quality_check_task():
    """
    Quality check step - validates silver layer data after transform.
    Generates a quality report and logs findings.
    Retries once if it fails — should be quick to run, so short delay.
    """
    logger = get_run_logger()
    session = SessionLocal()
    try:
        checker = QualityChecker(session)
        report = checker.run()
        logger.info(f"Quality check complete: {report}")
        return report
    except Exception as e:
        logger.error(f"Quality check failed: {e}")
        raise
    finally:
        session.close()

@task(name="Post-Process", retries=1, retry_delay_seconds=60)
def postprocess_task():
    """
    Post-processing step — computes KPI aggregates from silver
    and populates gold tables.
    """
    logger = get_run_logger()
    session = SessionLocal()
    try:
        postprocessor = PostProcessor(session)
        result = postprocessor.run()
        logger.info(f"Post-processing complete: {result}")
        return result
    except Exception as e:
        logger.error(f"Post-processing failed: {e}")
        raise
    finally:
        session.close()


@flow(name="ETL Pipeline")
def etl_pipeline():
    """
    Main ETL flow - runs extract, transform, and post-process in order.
    Prefect ensures each stage starts only when the previous stage succeeds.
    """

    logger = get_run_logger()
    logger.info("Starting ETL pipeline")

    extract_result = extract_task()
    transform_result = transform_task(wait_for=[extract_result])
    quality_result = quality_check_task(wait_for=[transform_result])
    postprocess_result = postprocess_task(wait_for=[quality_result])

    logger.info(
        "Pipeline complete. "
        f"Extract: {extract_result}, "
        f"Transform: {transform_result}, "
        f"Quality Check: {quality_result}, "
        f"Post-Process: {postprocess_result}"
    )

    return {
        "extract": extract_result,
        "transform": transform_result,
        "quality_check": quality_result,
        "postprocess": postprocess_result,
    }

if __name__ == "__main__":
    etl_pipeline.serve("daily_etl_pipeline", cron = "0 2 * * *")  # Schedule to run daily at 2 AM
