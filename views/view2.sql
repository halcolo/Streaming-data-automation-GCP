WITH mean2021 AS (
    Select ID,
        avg(Hired) AS `meanHired`
    from (
            SELECT d.id AS `ID`,
                count(j.job) as `Hired`
            FROM `test_globant.hiring_employees` as he
                JOIN `test_globant.departments` d on d.id = he.department_id
                JOIN `test_globant.jobs` j on j.id = he.job_id
            WHERE EXTRACT(
                    YEAR
                    FROM PARSE_TIMESTAMP('%Y-%m-%dT%XZ', he.`datetime`)
                ) = 2021
            GROUP BY ID,
                Department,
                j.job
            ORDER BY Department
        ) as view1
    group by ID,
        view1.Hired
)
Select ID,
    Department,
    Hired
from (
        SELECT d.id AS `ID`,
            d.department AS `Department`,
            count(*) AS `Hired`,
            view1.meanHired AS `meanHired2021`
        FROM `test_globant.hiring_employees` as he
            JOIN `test_globant.departments` d on d.id = he.department_id
            JOIN mean2021 as view1 ON view1.ID = d.id
        WHERE EXTRACT(
                YEAR
                FROM PARSE_TIMESTAMP('%Y-%m-%dT%XZ', he.`datetime`)
            ) = 2022
        GROUP BY ID,
            Department,
            view1.meanHired
    )
WHERE hired > meanHired2021