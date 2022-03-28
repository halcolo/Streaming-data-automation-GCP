WITH deps2022 AS (
    SELECT d.id AS `ID`,
        AVG(he.id) as `Hired`
    FROM `test_globant.hiring_employees` as he
        JOIN `test_globant.departments` d on d.id = he.department_id
    WHERE EXTRACT(
            YEAR
            FROM PARSE_TIMESTAMP('%Y-%m-%dT%XZ', he.`datetime`)
        ) = 2022
    GROUP BY d.id,
        d.department
)
SELECT d.id AS `ID`,
    d.department AS `Department`,
    count(he.id) as `Hired`
FROM `test_globant.hiring_employees` as he
    JOIN `test_globant.departments` d on d.id = he.department_id
    JOIN deps2022 as view1 ON view1.ID = d.id
WHERE EXTRACT(
        YEAR
        FROM PARSE_TIMESTAMP('%Y-%m-%dT%XZ', he.`datetime`)
    ) = 2021
    AND view1.Hired > Hired
GROUP BY d.id,
    d.department