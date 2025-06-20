create or replace view DE6_PROJECT_III.ANALYTICS.RANK_SUMMARY(
       DATA_SOURCE,
       BASEDT,
       BASE_DT_STR,
       MUSIC_NAME,
       ARTIST_NAME,
       MUSIC_RANK,
       MUSIC_RANK_POINT,
       MUSIC_RANK_STR
) as 
WITH RECURSIVE DATE_RANGE AS ( -- 데이터 조회 범위 설정
    SELECT DATE_TRUNC('WEEK', DATEADD(MONTH, -6, CURRENT_DATE)) AS MONDAY
    UNION ALL
    SELECT DATEADD(WEEK, 1, MONDAY)
    FROM DATE_RANGE
    WHERE MONDAY < DATE_TRUNC('WEEK', CURRENT_DATE)
), BASE_DATE_RANGE AS (
    SELECT --주차 구하기 및 조인을 위한 컬럼명 선언
        MONDAY ,
        TO_CHAR(MONDAY,'YYYYMMDD') AS BASEDT,
        TO_CHAR(MONDAY,'YYYY') || '년 ' ||
        TO_CHAR(MONDAY,'MM') || '월 ' ||
        ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('MONTH',MONDAY) ORDER BY MONDAY) || '주차' AS BASE_DT_STR
    FROM DATE_RANGE
), RANK_SUMMARY AS (
SELECT 'GENIE' AS DATA_SOURCE ,
       B.BASEDT,
       B.BASE_DT_STR,
       A.MUSIC_NAME,
       A.ARTIST_NAME,
       A.MUSIC_RANK
  FROM RAW_DATA.GENIE_WEEKLY_CHART A
  JOIN BASE_DATE_RANGE B
  ON A.BASEDT = B.BASEDT
 AND A.MUSIC_RANK <= 100 
UNION ALL
SELECT 'BUGS' AS DATA_SOURCE ,
       B.BASEDT,
       B.BASE_DT_STR,
       A.MUSIC_NAME,
       A.ARTIST_NAME,
       A.MUSIC_RANK
  FROM RAW_DATA.BUGS_WEEKLY_CHART A
  JOIN BASE_DATE_RANGE B
  ON A.BASEDT = B.BASEDT
 AND A.MUSIC_RANK <= 100 
UNION ALL
SELECT 'BILLBOARD' AS DATA_SOURCE ,
       B.BASEDT,
       B.BASE_DT_STR,
       A.MUSIC_NAME,
       A.ARTIST_NAME,
       A.MUSIC_RANK
  FROM RAW_DATA.BILLBOARD_WEEKLY_CHART A
  JOIN BASE_DATE_RANGE B
  ON A.BASEDT = B.BASEDT
 AND A.MUSIC_RANK <= 100 
UNION ALL
SELECT 'SPOTIFY' AS DATA_SOURCE ,
       B.BASEDT,
       B.BASE_DT_STR,
       A.MUSIC_NAME,
       A.ARTIST_NAME,
       A.MUSIC_RANK
  FROM RAW_DATA.SPOTIFY_WEEKLY_CHART A
  JOIN BASE_DATE_RANGE B
  ON A.BASEDT = B.BASEDT
 AND A.MUSIC_RANK <= 100 
 AND COUNTRY_CODE = 'KR'
)
SELECT DATA_SOURCE,
       CAST(BASEDT AS VARCHAR2(8)) BASEDT,
       CAST(BASE_DT_STR AS VARCHAR2(30)) BASE_DT_STR,
       MUSIC_NAME,
       ARTIST_NAME,
       MUSIC_RANK,
       101 - MUSIC_RANK AS MUSIC_RANK_POINT,
       CAST(MUSIC_RANK || '등' AS VARCHAR2(30)) MUSIC_RANK_STR
  FROM RANK_SUMMARY;