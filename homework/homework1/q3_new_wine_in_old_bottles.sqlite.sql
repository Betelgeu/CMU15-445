SELECT release.name as RELEASE_NAME,
    artist_credit.name AS ARTIST_NAME,
    release_info.date_year AS RELEASE_YEAR
FROM release
    JOIN (
        SELECT release,
            format
        from medium
    ) as medium ON medium.release = release.id
    JOIN (
        SELECT id,
            name
        from medium_format
    ) as medium_format ON medium_format.id = medium.format
    JOIN (
        SELECT id,
            name
        FROM artist_credit
    ) as artist_credit ON artist_credit.id = release.artist_credit
    JOIN (
        SELECT release,
            date_year,
            date_month,
            date_day
        FROM release_info
    ) as release_info ON release_info.release = release.id
WHERE medium_format.name = 'Cassette'
ORDER BY release_info.date_year DESC,
    release_info.date_month DESC,
    release_info.date_day DESC,
    release.name,
    artist_credit.name
LIMIT 10;