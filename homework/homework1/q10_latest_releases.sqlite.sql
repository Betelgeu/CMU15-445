-- Find the latest three releases of male artists born in 1991 with exactly four artists appear in the credit.
WITH artist_d AS (
    select artist.id as artist_id,
        artist.name AS ARTIST_NAME
    FROM artist
        JOIN artist_type ON artist_type.id = artist.type
        JOIN gender ON gender.id = artist.gender
    WHERE artist_type.name = 'Person'
        AND gender.name = 'Male'
        AND artist.begin_date_year = 1991
),
release_d AS (
    select artist_credit_name.artist AS artist_id,
        release.name AS RELEASE_NAME,
        release_info.date_year AS RELEASE_YEAR,
        release_info.date_month AS RELEASE_MONTH,
        release_info.date_day AS RELEASE_DAY,
        artist_credit_name.name AS ARTIST,
        row_number() OVER (
            PARTITION BY artist_credit_name.artist
            ORDER BY release_info.date_year DESC,
                release_info.date_month DESC,
                release_info.date_day DESC
        ) AS number
    FROM release
        JOIN artist_credit ON artist_credit.id = release.artist_credit
        JOIN release_info ON release_info.release = release.id
        JOIN artist_credit_name ON artist_credit_name.artist_credit = artist_credit.id
    WHERE artist_credit.artist_count = 4
        AND release_info.date_year IS NOT NULL
    group by artist_credit_name.artist,
        release_info.date_year,
        release_info.date_month,
        release_info.date_day
)
SELECT artist_d.ARTIST_NAME as ARTIST_NAME,
    release_d.RELEASE_NAME AS RELEASE_NAME,
    release_d.RELEASE_YEAR AS RELEASE_YEAR
FROM release_d
    JOIN artist_d ON artist_d.artist_id = release_d.artist_id
WHERE release_d.number <= 3
ORDER BY ARTIST_NAME ASC,
    RELEASE_YEAR DESC,
    release_d.RELEASE_MONTH DESC,
    release_d.RELEASE_DAY DESC;