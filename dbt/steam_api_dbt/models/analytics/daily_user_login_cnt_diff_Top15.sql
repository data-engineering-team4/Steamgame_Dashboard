SELECT *
  FROM (
        SELECT A.GAME_ID
             , C.GAME_NAME
             , A.CREATE_DT              AS START_DT
             , B.CREATE_DT              AS END_DT
             , B.USER_CNT - A.USER_CNT  AS USER_DIFF 
             , ROW_NUMBER() OVER(PARTITION BY A.CREATE_DT ORDER BY B.USER_CNT - A.USER_CNT ASC) AS RN
             , C.GENRE
             , C.PRICE
             , C.DISCOUNT_PERCENTAGE
             , C.THUMBNAIL_PATH
             , 'DECREASE'               AS GUBUN
          FROM {{ref("src_game_status")}} A
          JOIN {{ref("src_game_status")}} B 
            ON A.GAME_ID = B.GAME_ID 
          JOIN {{ref("src_game_info")}} C
            ON A.GAME_ID = C.GAME_ID
           AND B.CREATE_DT = DATEADD(DAY, 1, A.CREATE_DT) 
) G
WHERE RN <= 15 
UNION ALL
SELECT *
  FROM (
        SELECT A.GAME_ID
             , C.GAME_NAME
             , A.CREATE_DT              AS START_DT
             , B.CREATE_DT              AS END_DT
             , B.USER_CNT - A.USER_CNT  AS USER_DIFF 
             , ROW_NUMBER() OVER(PARTITION BY A.CREATE_DT ORDER BY B.USER_CNT - A.USER_CNT DESC) AS RN
             , C.GENRE
             , C.PRICE
             , C.DISCOUNT_PERCENTAGE
             , C.THUMBNAIL_PATH
             , 'INCREASE'              AS GUBUN
          FROM {{ref("src_game_status")}} A
          JOIN {{ref("src_game_status")}} B 
            ON A.GAME_ID = B.GAME_ID 
          JOIN {{ref("src_game_info")}} C
            ON A.GAME_ID = C.GAME_ID
           AND B.CREATE_DT = DATEADD(DAY, 1, A.CREATE_DT) 
) G
WHERE RN <= 15
ORDER BY GUBUN, START_DT, RN