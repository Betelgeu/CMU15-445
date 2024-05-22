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
                SUM(case when release_info.date_year / 10 * 10 = 1950 then 1 else 0 end) as count
            FROM language
                LEFT JOIN release ON release.language = language.id
                LEFT JOIN release_info ON release_info.release = release.id
            GROUP BY language.id
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
                SUM(case when release_info.date_year / 10 * 10 = 2010 then 1 else 0 end) as count
            FROM language
                LEFT JOIN release ON release.language = language.id
                LEFT JOIN release_info ON release_info.release = release.id
            GROUP BY language.id
        )
)
SELECT
    market_1950.language,
    market_1950.count as NUM_RELEASES_IN_1950S,
    market_2010.count as NUM_RELEASES_IN_2010s,
    ROUND(market_2010.rate - market_1950.rate, 3) AS INCREASE
FROM market_1950
    JOIN market_2010 ON market_2010.language = market_1950.language
WHERE market_2010.rate > market_1950.rate
ORDER BY INCREASE DESC, market_1950.language ASC;