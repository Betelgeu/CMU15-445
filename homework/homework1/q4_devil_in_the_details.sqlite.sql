-- Find works with the longest comment for each work type.
SELECT WORK_TYPE,
    WORK_NAME,
    COMMENT_LENGTH,
    COMMENT
FROM (
        SELECT work_type.name AS WORK_TYPE,
            work.name AS WORK_NAME,
            length(work.comment) AS COMMENT_LENGTH,
            work.comment AS COMMENT,
            rank() over (
                partition by work.type
                order by length(work.comment) desc
            ) as rank
        FROM work
            JOIN work_type ON work_type.id = work.type
        WHERE COMMENT_LENGTH > 0
    )
WHERE rank = 1;