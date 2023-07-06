WITH src_comment_data AS (
    SELECT * FROM raw_data.comment_data
)
SELECT
    comment_id,
    game_id,
    user_id,
    total_playtime,
    last_two_weeks_playtime,
    comment_content,
    language
FROM
    src_comment_data