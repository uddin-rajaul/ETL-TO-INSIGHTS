"""
Visualization script - generates 3 kpi charts from the gold layer tables.
Charts are saved as PNG files in the visualizations/ directory.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from db.base import engine
from pathlib import Path

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

sns.set_theme(style="whitegrid")


def chart_headcount_over_time():
    """Generate a line chart of active employee headcount."""

    query = """
        SELECT snapshot_date, active_count
        FROM gold.kpi_headcount
        ORDER BY snapshot_date
    """
    df = pd.read_sql(query, engine)

    df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])

    flg, ax = plt.subplots(figsize=(14, 6))
    ax.plot(df['snapshot_date'], df['active_count'], color='#4a9eed', linewidth=1.5)
    ax.fill_between(df['snapshot_date'], df["active_count"], alpha=0.15, color='#4a9eed')

    ax.set_title('Active Employee Headcount Over Time', fontsize=16, weight='bold', pad=15)
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Active Employees', fontsize=12)

    plt.tight_layout()
    filepath = OUTPUT_DIR / '01_headcount_over_time.png'
    plt.savefig(filepath, dpi=150)
    plt.close()
    print(f"Saved chart: {filepath}")

def chart_attendance_breakdown() -> None:
    """Horizontal bar chart showing late arrival, early departure rates and overtime per employee."""

    query = """
        SELECT client_employee_id, late_arrival_count, early_departure_count, overtime_count
        FROM gold.kpi_attendance
        ORDER BY overtime_count DESC
    """
    df = pd.read_sql(query, engine)
    df = df.set_index('client_employee_id')

    fig, ax = plt.subplots(figsize=(14, 10))
    df.plot(
        kind="barh",
        ax=ax,
        color=['#ff9999', '#66b3ff', '#99ff99'],
        width=0.8,
    )

    ax.set_title('Attendance Breakdown by Employee', fontsize=16, weight='bold', pad=15)
    ax.set_xlabel("Count", fontsize=12)
    ax.set_ylabel("Employee ID", fontsize=12)
    ax.legend(["Late Arrivals", "Early Departures", "Overtime"], fontsize = 10)

    plt.tight_layout()
    filepath = OUTPUT_DIR / '02_attendance_breakdown.png'
    plt.savefig(filepath, dpi=150)
    plt.close()
    print(f"Saved chart: {filepath}")

def chart_tenure_by_department() -> None:
    """Horizontal bar chart showing average tenure in years per department."""

    query = """
        SELECT department_name, avg_tenure_years, employee_count
        FROM gold.kpi_tenure_by_department
        ORDER BY avg_tenure_years DESC
        """
    df = pd.read_sql(query, engine)
    

    fig, ax = plt.subplots(figsize=(12, 12))
    bars = ax.barh(
        df["department_name"],
        df["avg_tenure_years"],
        color="#22c55e",
        alpha=0.85,
    )

    # annotate each bar with employee count
    for bar, count in zip(bars, df["employee_count"]):
        ax.text(
            bar.get_width() + 0.2,
            bar.get_y() + bar.get_height() / 2,
            f"n={count}",
            va="center",
            fontsize=11,
            color="#555555",
        )

    ax.set_title("Average Tenure by Department", fontsize=16, weight='bold', pad=15)
    ax.set_xlabel("Average Tenure (Years)", fontsize=12)
    ax.set_ylabel("Department", fontsize=12)

    plt.tight_layout()
    filepath = OUTPUT_DIR / "03_tenure_by_department.png"
    plt.savefig(filepath, dpi=150)
    plt.close()
    print(f"Saved: {filepath}")

if __name__ == "__main__":
    print("Generating charts...")
    chart_headcount_over_time()
    chart_attendance_breakdown()
    chart_tenure_by_department()
    print("All charts saved to visualizations/output/")