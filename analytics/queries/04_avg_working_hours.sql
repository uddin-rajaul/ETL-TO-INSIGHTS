-- KPI 4: Average Working Hours per Employee
-- Mean hours worked per day and per week for each employee.
-- Only includes employees with matching records in silver.employee

WITH daily_hours AS (
    SELECT
        t.client_employee_id,
        e.first_name,
        e.last_name,
        d.name AS department_name,
        t.punch_apply_date,
        SUM(t.hours_worked) AS daily_hours
    FROM silver.timesheet t
    JOIN silver.employee e
        ON t.client_employee_id = e.client_employee_id
    LEFT JOIN silver.department d
        ON e.department_id = d.id
    WHERE t.hours_worked IS NOT NULL
    GROUP BY
        t.client_employee_id,
        e.first_name,
        e.last_name,
        d.name,
        t.punch_apply_date
)

SELECT
    client_employee_id,
    first_name,
    last_name,
    department_name,
    COUNT(punch_apply_date) AS total_days_worked,
    ROUND(SUM(daily_hours), 2) AS total_hours,
    ROUND(AVG(daily_hours), 2) AS avg_hours_per_day,
    ROUND(SUM(daily_hours) /
          NULLIF(COUNT(punch_apply_date) / 5.0, 0), 2) AS avg_hours_per_week
FROM daily_hours
GROUP BY
    client_employee_id,
    first_name,
    last_name,
    department_name
ORDER BY avg_hours_per_day DESC;