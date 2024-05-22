-- Find all the 12" Vinyl releases of the Beatles in the United Kingdoms up until the year they broke up.
select RELEASE_NAME,
    RELEASE_YEAR
from (
        select *,
            row_number() over (
                partition by t.RELEASE_NAME
                order by RELEASE_YEAR ASC
            ) as group_idx
        from (
                SELECT DISTINCT release.name AS RELEASE_NAME,
                    release_info.date_year AS RELEASE_YEAR
                FROM release
                    JOIN artist_credit ON artist_credit.id = release.artist_credit
                    JOIN artist_credit_name ON artist_credit_name.artist_credit = artist_credit.id
                    JOIN medium ON medium.release = release.id
                    JOIN medium_format ON medium_format.id = medium.format
                    JOIN release_info ON release_info.release = release.id
                    JOIN area ON area.id = release_info.area
                    JOIN artist ON artist.id = artist_credit_name.artist
                WHERE artist.name = 'The Beatles'
                    AND medium_format.name = '12" Vinyl'
                    AND area.name = 'United Kingdom'
                    AND release_info.date_year < artist.end_date_year
            ) as t
    ) as s
WHERE s.group_idx = 1
ORDER BY RELEASE_YEAR ASC,
    RELEASE_NAME;