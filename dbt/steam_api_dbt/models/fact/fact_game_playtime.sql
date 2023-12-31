WITH av AS (
    SELECT GAME_ID, COUNT(GAME_ID) COUNT_GAME_ID, SUM(TOTAL_PLAYTIME) SUM_TOTAL_PLAYTIME, SUM(LAST_TWO_WEEKS_PLAYTIME) SUM_LAST_TWO_WEEKS_PLAYTIME,
    AVG(TOTAL_PLAYTIME) AVG_TOTAL_PLAYTIME, AVG(LAST_TWO_WEEKS_PLAYTIME) AVG_LAST_TWO_WEEKS_PLAYTIME
    FROM {{ref("src_comment_data")}}
    GROUP BY 1
)
    SELECT av.GAME_ID, av.COUNT_GAME_ID, G.GAME_NAME, av.SUM_TOTAL_PLAYTIME, av.SUM_LAST_TWO_WEEKS_PLAYTIME, 
    av.AVG_TOTAL_PLAYTIME, av.AVG_LAST_TWO_WEEKS_PLAYTIME, G.GENRE
    FROM av
    JOIN RAW_DATA.GAME_INFO G ON av.GAME_ID = G.GAME_ID