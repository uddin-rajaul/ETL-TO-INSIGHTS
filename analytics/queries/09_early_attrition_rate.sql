-- KPI 9: Early Attrition Rate
-- Proportion of employees who left within the first 6 months
-- of joining. Identifies onboarding or job satisfaction issues.
-- 6 months threshold is configured in settings.yaml.

WITH employee_attrition AS (
    SELECT
        e.client_employee_id,
        d.name AS department_name,
        e.hire_date,
        e.term_date,
        CASE
            WHEN e.term_date IS NOT NULL AND
                 e.term_date <= e.hire_date + INTERVAL '6 months'
            THEN true
            ELSE false
        END AS is_early_attrition
    FROM silver.employee e
    LEFT JOIN silver.department d
        ON e.department_id = d.id
    WHERE e.hire_date IS NOT NULL
)

SELECT
    department_name,
    COUNT(*) AS total_hires,
    COUNT(*) FILTER (WHERE is_early_attrition) AS early_attrition_count,
    ROUND(
        COUNT(*) FILTER (WHERE is_early_attrition)::numeric /
        NULLIF(COUNT(*), 0) * 100
    , 2) AS early_attrition_rate_pct
FROM employee_attrition
GROUP BY department_name
ORDER BY early_attrition_rate_pct DESC;