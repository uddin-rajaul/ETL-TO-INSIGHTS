-- ============================================================
-- KPI 2: Turnover Trend
-- Monthly count of employee terminations and turnover rate.
-- Turnover rate = terminations / avg headcount that month * 100
-- Only includes employees who have a term_date (were terminated).
-- ============================================================

WITH monthly_terminations AS (
    -- count terminations per month
    SELECT
        EXTRACT(YEAR  FROM term_date)::int AS year,
        EXTRACT(MONTH FROM term_date)::int AS month,
        COUNT(*) AS terminations
    FROM silver.employee
    WHERE term_date IS NOT NULL
    GROUP BY
        EXTRACT(YEAR  FROM term_date),
        EXTRACT(MONTH FROM term_date)
),

monthly_headcount AS (
    -- average headcount per month using hire and term dates
    SELECT
        EXTRACT(YEAR  FROM d)::int AS year,
        EXTRACT(MONTH FROM d)::int AS month,
        COUNT(e.id) AS active_count
    FROM generate_series(
        (SELECT MIN(hire_date) FROM silver.employee),
        CURRENT_DATE,
        INTERVAL '1 day'
    ) AS d(snapshot_date)
    LEFT JOIN silver.employee e
        ON e.hire_date <= d.snapshot_date::date
        AND (e.term_date IS NULL OR e.term_date > d.snapshot_date::date)
    GROUP BY
        EXTRACT(YEAR  FROM d),
        EXTRACT(MONTH FROM d)
)

SELECT
    t.year,
    t.month,
    t.terminations,
    h.active_count AS avg_headcount,
    ROUND(t.terminations::numeric / 
          NULLIF(h.active_count, 0) * 100, 2) AS turnover_rate_pct
FROM monthly_terminations t
JOIN monthly_headcount h
    ON t.year  = h.year
    AND t.month = h.month
ORDER BY t.year, t.month;