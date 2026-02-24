-- KPI 1: Active Headcount Over Time
-- Shows number of actively employed staff on each date.
-- An employee is considered active on a given date if:
-- 1. Their hire date is on or before that date, AND
-- 2. Their term_date is NULL (still employed) OR their term_date is after that date.

WITH date_range AS(
    SELECT generate_series(
        (SELECT MIN(hire_date) FROM silver.employee),
        CURRENT_DATE,
        INTERVAL '1 day'
    )::date AS snapshot_date
)

SELECT 
    d.snapshot_date,
    COUNT(e.id) AS active_count
FROM date_range d
LEFT JOIN silver.employee e
    ON e.hire_date <= d.snapshot_date
    AND (e.term_date IS NULL OR e.term_date > d.snapshot_date)
GROUP BY d.snapshot_date
ORDER BY d.snapshot_date;