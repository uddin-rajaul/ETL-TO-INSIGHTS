"""
FastAPI application entry point.
Mounts all routers and configures the app.

Run with: uvicorn main:app --reload
Docs at:  http://localhost:8000/docs

## Endpoints

### Authentication
- `POST /auth/register` - Create user account
- `POST /auth/login` - Returns JWT token

### Employee Management (CRUD)
- `GET /employees` - List employees (viewer+)
- `GET /employees/{id}` - Get employee by ID (viewer+)
- `POST /employees` - Create employee (admin only)
- `PUT /employees/{id}` - Update employee (admin only)
- `DELETE /employees/{id}` - Soft-delete employee (admin only)

### Timesheets (Read-only)
- `GET /timesheets` - List timesheets with filters (viewer+)
- `GET /timesheets/employee/{employee_id}` - Timesheets for specific employee (viewer+)
- `GET /timesheets/{timesheet_id}` - Get timesheet by ID (viewer+)

### KPI Analytics
- `GET /kpis/headcount` - Active headcount per day
- `GET /kpis/turnover` - Monthly turnover rate
- `GET /kpis/tenure-by-department` - Average tenure by department
- `GET /kpis/attendance` - Late arrivals, early departures, overtime
- `GET /kpis/early-attrition` - Early attrition rate by department

### System
- `GET /health` - Health check
- `GET /docs` - Swagger UI
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routes import auth, kpis, employees, timesheets

app = FastAPI(
    title="ETL to Insights API",
    description="HR analytics API with employee CRUD and timesheet read-only access.",
    version="1.0.0",
)

# allow Power BI and local frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(kpis.router)
app.include_router(employees.router)
app.include_router(timesheets.router)


@app.get("/")
def root():
    return {"message": "ETL to Insights API is running."}


@app.get("/health")
def health():
    return {"status": "ok"}