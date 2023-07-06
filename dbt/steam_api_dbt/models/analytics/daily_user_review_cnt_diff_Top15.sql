SELECT *
  FROM (
        SELECT A.GAME_ID
             , C.GAME_NAME 
             , A.CREATE_DT                  AS START_DT
             , B.CREATE_DT                  AS END_DT
             , B.REVIEW_CNT - A.REVIEW_CNT  AS REVIEW_DIFF 
             , ROW_NUMBER() OVER(PARTITION BY A.CREATE_DT ORDER BY B.REVIEW_CNT - A.REVIEW_CNT DESC) AS RN
             , C.GENRE
             , C.PRICE
             , C.DISCOUNT_PERCENTAGE
             , C.THUMBNAIL_PATH
          FROM {{ref("src_game_status")}} A
          JOIN {{ref("src_game_status")}} B 
            ON A.GAME_ID = B.GAME_ID 
          JOIN {{ref("src_game_info")}} C
            ON A.GAME_ID = C.GAME_ID
           AND B.CREATE_DT = DATEADD(DAY, 1, A.CREATE_DT) 
    ) G
WHERE RN <= 15 
ORDER BY START_DT, RN