-- Gold KPI load query: consolidated attendance summary per employee
-- Includes late arrival, early departure, and overtime counts/rates.

WITH attendance AS (
    SELECT
        t.client_employee_id,
        d.name AS department_name,
        COUNT(*) AS total_shifts,
        COUNT(*) FILTER (WHERE t.is_late_arrival = true) AS late_arrival_count,
        COUNT(*) FILTER (WHERE t.is_early_departure = true) AS early_departure_count,
        COUNT(*) FILTER (WHERE t.is_overtime = true) AS overtime_count
    FROM silver.timesheet t
    JOIN silver.employee e
        ON t.client_employee_id = e.client_employee_id
    LEFT JOIN silver.department d
        ON e.department_id = d.id
    GROUP BY
        t.client_employee_id,
        d.name
)
SELECT
    client_employee_id,
    department_name,
    total_shifts,
    late_arrival_count,
    early_departure_count,
    overtime_count,
    ROUND(late_arrival_count::numeric / NULLIF(total_shifts, 0) * 100, 2) AS late_arrival_rate_pct
FROM attendance
ORDER BY client_employee_id;

