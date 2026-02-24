-- KPI 3: Average Tenure by Department
-- Average employment duration of staff within each department.
-- Tenure = from hire_date to term_date (or today if still active).

WITH employee_tenure AS (
    SELECT
        e.department_id,
        d.name AS department_name,
        e.client_employee_id,
        e.hire_date,
        COALESCE(e.term_date, CURRENT_DATE) AS end_date,
        ROUND(
            (COALESCE(e.term_date, CURRENT_DATE) - e.hire_date) / 365.25
        , 2) AS tenure_years
    FROM silver.employee e
    LEFT JOIN silver.department d
        ON e.department_id = d.id
    WHERE e.hire_date IS NOT NULL
)

SELECT
    department_name,
    COUNT(client_employee_id) AS employee_count,
    ROUND(AVG(tenure_years), 2) AS avg_tenure_years,
    ROUND(MIN(tenure_years), 2) AS min_tenure_years,
    ROUND(MAX(tenure_years), 2) AS max_tenure_years
FROM employee_tenure
GROUP BY department_name
ORDER BY avg_tenure_years DESC;