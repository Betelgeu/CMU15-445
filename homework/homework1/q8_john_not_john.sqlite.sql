WITH exclude_id AS (
    select artist
    from artist_alias
    where lower(artist_alias.name) like '%john%'
)
SELECT
    ARTIST AS ARTIST_NAME,
    count(*) AS NUM_ALIASES,
    group_concat(ALIAS, ', ') AS COMMA_SEPARATED_LIST_OF_ALIASES
FROM (
        SELECT artist.name AS ARTIST,
            artist_alias.name as ALIAS
        FROM artist
            JOIN artist_alias ON artist_alias.artist = artist.id
        WHERE
            artist.name like '%John'
            AND artist.id not in exclude_id
        ORDER BY artist.name,
            artist_alias.name
    )
GROUP BY ARTIST;