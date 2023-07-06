WITH src_game_info AS (
    SELECT * FROM raw_data.game_info
)
SELECT
    game_id,
    game_name,
    price,
    discount_percentage,
    release_date,
    thumbnail_path,
    genre,
    negative_cnt,
    positive_cnt
FROM
    src_game_info