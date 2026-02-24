-- KPI 6: Early Departure Count
-- Number of days each employee left earlier than scheduled
-- end time, considering ±5 minute grace period.
-- is_early_departure flag was computed during the transform step.

WITH early_departures AS (
    SELECT
        t.client_employee_id,
        e.first_name,
        e.last_name,
        d.name AS department_name,
        COUNT(*) AS total_shifts,
        COUNT(*) FILTER (WHERE t.is_early_departure = true) AS early_departure_count
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
    early_departure_count,
    ROUND(early_departure_count::numeric / NULLIF(total_shifts, 0) * 100, 2) AS early_departure_rate_pct
FROM early_departures
WHERE early_departure_count > 0
ORDER BY early_departure_count DESC;