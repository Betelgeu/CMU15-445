with exclude_id as (
    select artist
    from artist_alias
    where lower(artist_alias.name) like '%john%'
),
t as (
        select
            artist.name as ARTIST,
            artist_alias.name as ALIAS
        from
            artist
        left join
            artist_alias on artist.id = artist_alias.artist
        where
            lower(artist.name) like '%john'
            and artist.id not in (select * from exclude_id)
        order by
            artist.name, artist_alias.name
)
select
    ARTIST as ARTIST_NAME,
    count(ALIAS) as NUM_ALIASES,
    group_concat(ALIAS, ', ') as COMMA_SEPARATED_LIST_OF_ALIASES
from
    t
group by
    ARTIST
having
    count(ALIAS) > 0
order by
    ARTIST;
