SELECT ARTIST_NAME, RELEASE_MONTH, max(NUM_RELEASES) as NUM_RELEASES
FROM (
	SELECT ARTIST_NAME, RELEASE_MONTH, count as NUM_RELEASES
	FROM (
			SELECT artist_credit_name.name AS ARTIST_NAME, release_info.date_month AS RELEASE_MONTH, count(*) as count
			FROM release
			JOIN artist_credit_name ON artist_credit_name.artist_credit = release.artist_credit
			JOIN (SELECT id, type FROM artist) AS artist ON artist.id = artist_credit_name.artist
			JOIN artist_type ON artist_type.id = artist.type
			JOIN release_info ON release_info.release = release.id
			WHERE artist_credit_name.name like 'Elvis%' AND
			artist_type.name = 'Person'
			GROUP BY artist_credit_name.name, release_info.date_month
	)
	WHERE RELEASE_MONTH is NOT NULL
	order by NUM_RELEASES DESC, ARTIST_NAME ASC
)
GROUP BY ARTIST_NAME
;