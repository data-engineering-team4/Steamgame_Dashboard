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
          FROM RAW_DATA.GAME_STATUS A
          JOIN RAW_DATA.GAME_STATUS B 
            ON A.GAME_ID = B.GAME_ID 
          JOIN RAW_DATA.GAME_INFO C
            ON A.GAME_ID = C.GAME_ID
           AND B.CREATE_DT = DATEADD(DAY, 1, A.CREATE_DT) 
) G
WHERE RN <= 20 
ORDER BY START_DT, RN