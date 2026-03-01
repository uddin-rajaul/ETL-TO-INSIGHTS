#!/bin/sh
set -e

echo "========================================"
echo " ETL to Insights — startup"
echo "========================================"

# 1. Run Alembic migrations
echo "[1/3] Running database migrations..."
uv run alembic upgrade head
echo "      Migrations done."

# 2. Run ETL pipeline (extract → bronze → silver → quality → gold → export)
echo "[2/3] Running ETL pipeline..."
uv run python -m etl.pipeline --now
echo "      Pipeline done."

# 3. Start the API
echo "[3/3] Starting FastAPI..."
exec uv run uvicorn main:app --host 0.0.0.0 --port 8000
