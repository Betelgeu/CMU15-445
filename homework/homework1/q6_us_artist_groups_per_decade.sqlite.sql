SELECT
    begin_date_year / 10 * 10,
    count(*) as count
FROM artist
    JOIN area ON area.id = artist.area
WHERE area.name = 'United States'
    AND artist.begin_date_year >= 1900
    AND artist.begin_date_year < 2023
GROUP BY begin_date_year / 10;