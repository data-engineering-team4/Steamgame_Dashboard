WITH src_game_status AS (
    SELECT * FROM raw_data.game_status
)
SELECT
    game_id,
    create_dt,
    user_cnt,
    review_cnt
FROM
    src_game_status