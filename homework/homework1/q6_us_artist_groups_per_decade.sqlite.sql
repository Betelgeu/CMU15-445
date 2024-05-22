SELECT
    cast(floor(begin_date_year / 10)  * 10 as int) || 's' as DECADE,
    count(*) as NUM_ARTIST_GROUP
FROM artist
    JOIN area ON area.id = artist.area
    JOIN artist_type ON artist_type.id = artist.type
WHERE area.name = 'United States'
    AND artist_type.name = 'Group'
    AND artist.begin_date_year >= 1900
    AND artist.begin_date_year < 2023
GROUP BY DECADE
ORDER by DECADE;