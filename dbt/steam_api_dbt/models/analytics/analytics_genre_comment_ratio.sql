WITH gi AS (
    SELECT genre, negative_cnt, positive_cnt FROM {{ref("src_game_info")}}
)
SELECT '액션' AS genre, SUM(positive_cnt) AS total_positive_cnt, SUM(negative_cnt) AS total_negative_cnt, COUNT(*) AS game_cnt
FROM gi
WHERE genre LIKE '%액션%'
UNION ALL
SELECT '어드벤처' AS genre, SUM(positive_cnt) AS total_positive_cnt, SUM(negative_cnt) AS total_negative_cnt, COUNT(*) AS game_cnt
FROM gi
WHERE genre LIKE '%어드벤처%'
UNION ALL
SELECT '대규모멀티플레이어' AS genre, SUM(positive_cnt) AS total_positive_cnt, SUM(negative_cnt) AS total_negative_cnt, COUNT(*) AS game_cnt
FROM gi
WHERE genre LIKE '%대규모멀티플레이어%'
UNION ALL
SELECT 'RPG' AS genre, SUM(positive_cnt) AS total_positive_cnt, SUM(negative_cnt) AS total_negative_cnt, COUNT(*) AS game_cnt
FROM gi
WHERE genre LIKE '%RPG%'
UNION ALL
SELECT '캐주얼' AS genre, SUM(positive_cnt) AS total_positive_cnt, SUM(negative_cnt) AS total_negative_cnt, COUNT(*) AS game_cnt
FROM gi
WHERE genre LIKE '%캐주얼%'
UNION ALL
SELECT '인디' AS genre, SUM(positive_cnt) AS total_positive_cnt, SUM(negative_cnt) AS total_negative_cnt, COUNT(*) AS game_cnt
FROM gi
WHERE genre LIKE '%인디%'
UNION ALL
SELECT '전략' AS genre, SUM(positive_cnt) AS total_positive_cnt, SUM(negative_cnt) AS total_negative_cnt, COUNT(*) AS game_cnt
FROM gi
WHERE genre LIKE '%전략%'
UNION ALL
SELECT '시뮬레이션' AS genre, SUM(positive_cnt) AS total_positive_cnt, SUM(negative_cnt) AS total_negative_cnt, COUNT(*) AS game_cnt
FROM gi
WHERE genre LIKE '%시뮬레이션%'
UNION ALL
SELECT '스포츠' AS genre, SUM(positive_cnt) AS total_positive_cnt, SUM(negative_cnt) AS total_negative_cnt, COUNT(*) AS game_cnt
FROM gi
WHERE genre LIKE '%스포츠%'
UNION ALL
SELECT '레이싱' AS genre, SUM(positive_cnt) AS total_positive_cnt, SUM(negative_cnt) AS total_negative_cnt, COUNT(*) AS game_cnt
FROM gi
WHERE genre LIKE '%레이싱%'