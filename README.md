# ETL to Insights - HR Analytics Pipeline

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql)
![Prefect](https://img.shields.io/badge/Prefect-3.x-blue?logo=prefect)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.x-red)
![Alembic](https://img.shields.io/badge/Alembic-migrations-orange)

## 1. Assignment Context

This project is my implementation of the **ETL to Insights** assignment.

The assignment asked for:
- end-to-end ETL with orchestration
- normalized database design
- SQL analytics for HR KPIs
- API with auth, employee CRUD, and read-only timesheets
- quality checks and reporting
- visualizations

This repository covers all of those pieces in one pipeline.

## 2. What I Built

I designed a Medallion-style pipeline in PostgreSQL:
- **Bronze**: raw CSV ingestion (`employee`, `timesheet`, and rejected timesheets)
- **Silver**: cleaned/typed tables with attendance flags
- **Gold**: KPI aggregates for reporting
- **Auth**: API users and role-based access

End-to-end flow:
1. Extract (local files or MinIO/S3-compatible storage)
2. Transform (cleaning + typing + FK handling + attendance flags)
3. Quality checks (rule-based validation + JSON report)
4. Post-processing (populate gold KPI tables)

## 3. Architecture

![Architecture](docs/architecture.png)

## 4. Repository Structure

```text
etl-to-insights/
├── etl/
│   ├── extract/extractor.py
│   ├── transform/transformer.py
│   ├── quality/checker.py
│   ├── postprocess/postprocessor.py
│   └── pipeline.py
├── db/
│   ├── models_bronze.py
│   ├── models_silver.py
│   ├── models_gold.py
│   ├── models_auth.py
│   └── migrations/
├── analytics/
│   └── queries/                # 10 SQL files
├── api/
│   ├── routes/
│   ├── auth/
│   └── schemas/
├── visualizations/
│   ├── charts.py
│   └── output/
├── tests/
├── config/settings.yaml
├── main.py
├── Dockerfile
└── pyproject.toml
```

## 5. Data Inputs

| File | Rows | Notes |
|------|------|------|
| `employee_202510161125.csv` | 50 | Employee master sample |
| `timesheet_202509151540.csv` | 269,921 | Timesheet batch |
| `timesheet_202510161121.csv` | 48,258 | Timesheet batch |
| `timesheet_202510161122.csv` | 55,435 | Timesheet batch |
| `timesheet_202510161124.csv` | 38,957 | Timesheet batch |

Format notes:
- pipe-delimited (`|`)
- missing values represented as `[NULL]`

## 6. Key Data Findings and Decisions

### 6.1 Employee-Timesheet mismatch

Only a small overlap exists between employee master IDs and timesheet IDs. To avoid silent data loss, I used a dead-letter pattern:
- matched rows -> `silver.timesheet`
- unmatched rows -> `bronze.rejected_timesheet`

### 6.2 Self-referencing manager FK

`silver.employee.manager_employee_id` references `silver.employee.client_employee_id`.
I handled this in two passes:
1. insert employees without manager FK
2. update manager FK only when manager exists in loaded sample

### 6.3 NaN / NaT handling

I normalized null-like values during transform to prevent invalid inserts and API serialization issues.

## 7. Analytics (SQL KPIs)

KPI queries are in `analytics/queries/`.

Implemented KPI set:
1. Active headcount over time
2. Turnover trend (fixed to use monthly average of **daily** headcount)
3. Average tenure by department
4. Average working hours per employee
5. Late arrival frequency
6. Early departure count
7. Overtime count
8. 7-day rolling average of working hours
9. Early attrition rate
10. Consolidated attendance summary for gold load

## 8. API

FastAPI + JWT bearer auth + role-based authorization.

### Auth
- `POST /auth/register`
- `POST /auth/login`

### Employee (CRUD)
- `GET /employees`
- `GET /employees/{id}`
- `POST /employees` (admin)
- `PUT /employees/{id}` (admin)
- `DELETE /employees/{id}` (admin, soft delete)

### Timesheet (read-only)
- `GET /timesheets`
- `GET /timesheets/employee/{employee_id}`
- `GET /timesheets/{timesheet_id}`

### KPI endpoints
- `GET /kpis/headcount`
- `GET /kpis/turnover`
- `GET /kpis/tenure-by-department`
- `GET /kpis/attendance`
- `GET /kpis/early-attrition`

### System
- `GET /health`
- `GET /docs`

## 9. Quality Checks

`etl/quality/checker.py` runs these checks after transform:
- null checks on critical columns
- duplicate employee ID check
- date logic check (`hire_date < term_date`)
- hours worked range check
- orphan/rejected timesheet rate check
- attendance flag consistency check

Each run saves a JSON report in `logs/quality_report_*.json`.

## 10. Visualizations

Charts are generated with **Matplotlib** via `visualizations/charts.py` and saved to `visualizations/output/`.

Current charts:
- `01_headcount_over_time.png`
- `02_attendance_breakdown.png`
- `03_tenure_by_department.png`

### Add visuals to README

Use this section to place final screenshots:

#### Dashboard / Visual 1
![Visual 1](visualizations/output/01_headcount_over_time.png)

#### Dashboard / Visual 2
![Visual 2](visualizations/output/02_attendance_breakdown.png)

#### Dashboard / Visual 3
![Visual 3](visualizations/output/03_tenure_by_department.png)

## 11. Test Coverage

Run:

```bash
pytest tests/ -v
```

Current coverage includes:
- auth and role behavior (`test_api_auth.py`)
- extraction behavior (`test_extractor.py`)
- transform parsing/flags (`test_transformer.py`)
- postprocess loads/idempotency (`test_postprocessor.py`)

## 12. Setup and Run

### 12.1 Prerequisites
- Python 3.12+
- PostgreSQL 16
- uv

### 12.2 Install

```bash
uv sync
```

### 12.3 Environment

Create `.env` and set database + auth values.

Example (local files):

```env
DATA_SOURCE=local
RAW_DATA_PATH=data/raw
```

Example (MinIO):

```env
DATA_SOURCE=minio
RAW_DATA_PATH=
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=etl-raw
MINIO_SECURE=false
```

> If your files are in a MinIO subfolder (example `data/raw/`), set `RAW_DATA_PATH=data/raw`.

### 12.4 Database setup

```sql
CREATE DATABASE etl_insights;
\c etl_insights
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
CREATE SCHEMA auth;
```

Run migrations:

```bash
alembic upgrade head
```

### 12.5 Run ETL pipeline

```bash
python -m etl.pipeline
```

### 12.6 Run API

```bash
uvicorn main:app --reload
```

## 13. Docker (API)

Build:

```bash
docker build -t etl-to-insights .
```

Run:

```bash
docker run --rm -p 8000:8000 --env-file .env etl-to-insights
```

Then open:
- `http://localhost:8000/docs`

## 14. Assignment Mapping (Quick View)

| Assignment Requirement | Status |
|---|---|
| ETL + orchestration | Done |
| Local + MinIO ingestion | Done |
| Cleaning + derived columns | Done |
| Post-processing + gold tables | Done |
| Data quality validation/reporting | Done |
| API with auth + employee CRUD + read-only timesheet | Done |
| SQL KPI analytics | Done |
| Visualizations | Done |
| Tests | Done |
| Dockerization | Done (API container) |

## 15. Notes

The strongest part of this project was handling imperfect source data safely:
- preserving raw truth in bronze
- keeping silver clean and typed
- rejecting bad joins explicitly instead of silently dropping rows

That made downstream KPIs and API responses stable and explainable.
