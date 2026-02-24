-- KPI 5: Late Arrival Frequency
-- Number of times each employee clocked in later than scheduled
-- start time, considering ±5 minute grace period.
-- is_late_arrival flag was computed during the transform step.

WITH late_arrivals AS (
    SELECT
        t.client_employee_id,
        e.first_name,
        e.last_name,
        d.name AS department_name,
        COUNT(*) AS total_shifts,
        COUNT(*) FILTER (WHERE t.is_late_arrival = true) AS late_arrival_count
    FROM silver.timesheet t
    JOIN silver.employee e
        ON t.client_employee_id = e.client_employee_id
    LEFT JOIN silver.department d
        ON e.department_id = d.id
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
    late_arrival_count,
    ROUND(late_arrival_count::numeric / NULLIF(total_shifts, 0) * 100, 2)  AS late_arrival_rate_pct
FROM late_arrivals
WHERE late_arrival_count > 0
ORDER BY late_arrival_count DESC;
