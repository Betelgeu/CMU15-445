WITH market_1950 AS (
    SELECT *,
        CAST(count AS FLOAT) / (
            SELECT count(*)
            FROM release
                JOIN release_info ON release_info.release = release.id
            WHERE release_info.date_year / 10 * 10 = 1950
        ) AS rate
    FROM (
            SELECT language.name AS language,
                count(*) as count
            FROM release
                JOIN release_info ON release_info.release = release.id
                JOIN language ON language.id = release.language
            WHERE release_info.date_year / 10 * 10 = 1950
            GROUP BY release.language
        )
),
market_2010 AS (
    SELECT *,
        CAST(count AS FLOAT) / (
            SELECT count(*)
            FROM release
                JOIN release_info ON release_info.release = release.id
            WHERE release_info.date_year / 10 * 10 = 2010
        ) AS rate
    FROM (
            SELECT language.name AS language,
                count(*) as count
            FROM release
                JOIN release_info ON release_info.release = release.id
                JOIN language ON language.id = release.language
            WHERE release_info.date_year / 10 * 10 = 2010
            GROUP BY release.language
        )
)
SELECT
    market_1950.language,
    market_1950.count as NUM_RELEASES_IN_1950S,
    market_2010.count as NUM_RELEASES_IN_2010s,
    ROUND((market_2010.rate - market_1950.rate) * 1000) / 1000.0 AS INCREASE
FROM market_1950
    JOIN market_2010 ON market_2010.language = market_1950.language
WHERE market_2010.rate > market_1950.rate
ORDER BY INCREASE DESC;