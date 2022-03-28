WITH hired_people AS (
    SELECT d.department AS `Department`,
        j.job AS `Job`,
        EXTRACT(
            QUARTER
            FROM PARSE_TIMESTAMP('%Y-%m-%dT%XZ', he.`datetime`)
        ) as `Quarter`
    FROM `test_globant.hiring_employees` as he
        join `test_globant.departments` d on d.id = he.department_id
        join `test_globant.jobs` j on j.id = he.job_id
),
CTE1 AS (
    SELECT Department,
        Job,
        CASE
            Quarter
            WHEN 1 THEN COUNT(Department)
            ELSE 0
        END as `Q1`,
        CASE
            Quarter
            WHEN 2 THEN COUNT(Department)
            ELSE 0
        END as `Q2`,
        CASE
            Quarter
            WHEN 3 THEN COUNT(Department)
            ELSE 0
        END as `Q3`,
        CASE
            Quarter
            WHEN 4 THEN COUNT(Department)
            ELSE 0
        END as `Q4`
    FROM hired_people
    group by Job,
        Quarter,
        Department
    order by Job
)
SELECT Department,
    Job,
    MAX(Q1) AS Q1,
    MAX(Q2) AS Q2,
    MAX(Q3) AS Q3,
    MAX(Q4) AS Q4
From CTE1
GROUP BY Department,
    Job