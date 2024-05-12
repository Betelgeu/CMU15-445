SELECT DISTINCT artist_credit_name.name AS NAME
FROM (
        SELECT *
        from artist_credit
        WHERE artist_credit.name like '%Pittsburgh Symphony Orchestra%'
    ) as artist_credit
    JOIN artist_credit_name on artist_credit_name.artist_credit = artist_credit.id
WHERE artist_credit_name.name <> 'Pittsburgh Symphony Orchestra'
ORDER BY artist_credit_name.name ASC;