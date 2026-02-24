-- KPI 7: Total Overtime Count
-- Number of shifts where employees exceeded standard shift
-- duration, considering ±5 minute grace period.
-- is_overtime flag was computed during the transform step.

WITH overtime_summary AS (
    SELECT
        t.client_employee_id,
        e.first_name,
        e.last_name,
        d.name AS department_name,
        COUNT(*) AS total_shifts,
        COUNT(*) FILTER (WHERE t.is_overtime) AS overtime_count,
        ROUND( SUM( CASE WHEN t.is_overtime THEN t.hours_worked ELSE 0 END), 2) AS total_overtime_hours
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
        d.name
)

SELECT
    client_employee_id,
    first_name,
    last_name,
    department_name,
    total_shifts,
    overtime_count,
    total_overtime_hours,
    ROUND(overtime_count::numeric /
          NULLIF(total_shifts, 0) * 100, 2) AS overtime_rate_pct
FROM overtime_summary
WHERE overtime_count > 0
ORDER BY overtime_count DESC;