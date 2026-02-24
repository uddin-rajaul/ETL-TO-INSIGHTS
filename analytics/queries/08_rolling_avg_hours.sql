-- KPI 8: Rolling Average Working Hours
-- 7-day moving average of hours worked per employee.
-- Helps detect trends like increasing overtime or fatigue.

WITH daily_hours AS (
    SELECT 
        t.client_employee_id,
        e.first_name,
        e.last_name,
        t.punch_apply_date,
        SUM(t.hours_worked) AS daily_hours
    FROM silver.timesheet t
    JOIN silver.employee e
        ON t.client_employee_id = e.client_employee_id
    WHERE t.hours_worked IS NOT NULL
    GROUP BY
        t.client_employee_id,
        e.first_name,
        e.last_name,
        t.punch_apply_date
)

SELECT
    client_employee_id,
    first_name,
    last_name,
    punch_apply_date,
    daily_hours AS hours_worked,
    ROUND(AVG(daily_hours) OVER (
        PARTITION BY client_employee_id
        ORDER BY punch_apply_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_avg_7d
FROM daily_hours
ORDER BY client_employee_id, punch_apply_date;