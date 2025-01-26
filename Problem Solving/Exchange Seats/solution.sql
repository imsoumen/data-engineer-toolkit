
select * from seats;

WITH CTE AS
(
    SELECT
        id,
        student,
        row_number() over () as rn
    FROM seats
    ORDER BY id
)
SELECT
    *,
    CASE WHEN MOD(rn, 2) = 0 THEN LAG(id) OVER (ORDER BY rn)
         WHEN MOD(rn, 2) = 1 THEN COALESCE(LEAD(id) OVER (ORDER BY rn), id)
    END as new_id
FROM CTE;

