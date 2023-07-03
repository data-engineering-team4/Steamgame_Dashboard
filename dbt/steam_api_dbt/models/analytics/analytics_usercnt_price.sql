WITH latest_status AS (
  SELECT game_id, MAX(create_dt) AS max_datetime
  FROM raw_data.game_status
  GROUP BY game_id
)
SELECT a.game_id, a.game_name, price / (100.0 - discount_percentage) * 100 AS original_price, a.price, a.discount_percentage,
       a.negative_cnt, a.positive_cnt, ROUND(negative_cnt / (negative_cnt + positive_cnt) * 100, 2) AS n_ratio,
       b.user_cnt
FROM raw_data.game_info a
JOIN latest_status ls ON a.game_id = ls.game_id
JOIN raw_data.game_status b ON ls.game_id = b.game_id AND ls.max_datetime = b.create_dt
WHERE (negative_cnt + positive_cnt) > 0 AND price != -1