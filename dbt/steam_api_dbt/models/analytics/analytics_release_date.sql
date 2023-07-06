WITH gi AS (
    SELECT game_id, release_date, negative_cnt, positive_cnt FROM {{ref("src_game_info")}}
), cm AS (
    SELECT game_id, AVG(total_playtime) total_playtime, AVG(last_two_weeks_playtime) last_two_weeks_playtime FROM {{ref("src_comment_data")}} GROUP BY 1
), gs AS (
    SELECT game_id, AVG(user_cnt) user_cnt, AVG(review_cnt) review_cnt FROM {{ref("src_game_status")}} GROUP BY 1
)
SELECT
    SUBSTR(release_date,6,2) month,
    ROUND(AVG(total_playtime)) total_playtime_avg,
    ROUND(AVG(last_two_weeks_playtime)) last_two_weeks_playtime_avg,
    ROUND(AVG(positive_cnt)) positive_cnt_avg,
    ROUND(AVG(negative_cnt)) negative_cnt_avg,
    ROUND(AVG(user_cnt)) current_user_cnt_avg,
    ROUND(AVG(review_cnt)) review_cnt_avg,
    COUNT(*) game_cnt
FROM gi 
    JOIN cm ON gi.game_id = cm.game_id
    JOIN gs ON gi.game_id = gs.game_id
WHERE LENGTH(release_date) = 7 -- 연도 및 월까지 저장된 데이터만
GROUP BY 1