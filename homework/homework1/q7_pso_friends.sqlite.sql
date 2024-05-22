WITH t AS (
    SELECT *
        from artist_credit
        WHERE artist_credit.name like '%Pittsburgh Symphony Orchestra%'
)
SELECT DISTINCT artist_credit_name.name AS NAME
FROM t
    JOIN artist_credit_name on artist_credit_name.artist_credit = t.id
WHERE artist_credit_name.name <> 'Pittsburgh Symphony Orchestra'
ORDER BY artist_credit_name.name ASC;
