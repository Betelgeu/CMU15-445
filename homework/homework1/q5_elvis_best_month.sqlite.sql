WITH t as(
    SELECT artist.name AS ARTIST_NAME,
        release_info.date_month AS RELEASE_MONTH,
        count(distinct release.id) as NUM_RELEASES
    FROM release
        JOIN artist_credit_name ON artist_credit_name.artist_credit = release.artist_credit
        JOIN artist ON artist.id = artist_credit_name.artist
        JOIN artist_type ON artist_type.id = artist.type
        JOIN release_info ON release_info.release = release.id
    WHERE artist.name like 'Elvis%'
        AND artist_type.name = 'Person'
        AND RELEASE_MONTH is NOT NULL
    GROUP BY ARTIST_NAME, RELEASE_MONTH
)
SELECT
    ARTIST_NAME,
    RELEASE_MONTH,
    NUM_RELEASES
FROM (
    SELECT ARTIST_NAME,
        RELEASE_MONTH,
        NUM_RELEASES,
        row_number() OVER (PARTITION BY ARTIST_NAME ORDER BY NUM_RELEASES DESC, RELEASE_MONTH ASC) as rnk
    FROM t
)
where rnk = 1
order by NUM_RELEASES DESC, ARTIST_NAME ASC;