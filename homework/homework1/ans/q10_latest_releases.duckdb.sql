with t as (
    select
        a.name as artist_name,
        lr.release_name as release_name,
        lr.release_year as release_year,
        lr.release_month as release_month,
        lr.release_day as release_day,
        row_number() over (partition by a.name order by lr.release_year desc, lr.release_month desc, lr.release_day desc) as rnk
    from artist a
    join lateral (
        select distinct
            r.name as release_name, 
            ri.date_year as release_year,
            ri.date_month as release_month,
            ri.date_day as release_day
        from 
            artist_credit ac
        join 
            artist_credit_name acn on ac.id = acn.artist_credit
        join 
            release r on ac.id = r.artist_credit
        join 
            release_info ri on r.id = ri.release
        where 
            acn.artist = a.id
            and ac.artist_count = 4
            and ri.date_year is not null
        order by 
            ri.date_year desc, 
            ri.date_month desc, 
            ri.date_day desc
    ) lr on true
    where a.gender = 1 and a.begin_date_year = 1991
    order by 
        a.name, 
        lr.release_year desc
)
select artist_name AS ARTIST_NAME,
    release_name AS RELEASE_NAME,
    release_year AS RELEASE_YEAR
from t
where rnk <= 3;
