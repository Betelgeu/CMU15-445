SELECT
    ARTIST,
    count(*) as NUM_ALIASES,
    group_concat(ALIAS, ',')
FROM (
        SELECT artist.name AS ARTIST,
            artist_alias.name as ALIAS
        FROM artist
            JOIN artist_alias
        WHERE artist_alias.artist = artist.id
            AND artist.name like '%John'
            AND lower(artist_alias.name) not like '%john%'
        ORDER BY artist.name,
            artist_alias.name
    )
GROUP BY ARTIST;