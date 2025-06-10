create or replace view DE6_PROJECT_III.ANALYTICS.TOP1_MONTHLY_CHART(
	MONTH,
	MUSIC_NAME,
	ARTIST_NAME,
	TOP1_COUNT,
	MONTH6,
	RECENT_MONTH6
) as 
    WITH all_charts AS (
      -- Billboard
      SELECT TO_DATE(BASEDT, 'YYYYMMDD') AS basedt, MUSIC_NAME, ARTIST_NAME, 101 - MUSIC_RANK AS score
      FROM DE6_PROJECT_III.RAW_DATA.BILLBOARD_WEEKLY_CHART
      WHERE MUSIC_RANK <= 100
    
      UNION ALL
      -- Bugs
      SELECT TO_DATE(BASEDT, 'YYYYMMDD') AS basedt, MUSIC_NAME, ARTIST_NAME, 101 - MUSIC_RANK AS score
      FROM DE6_PROJECT_III.RAW_DATA.BUGS_WEEKLY_CHART
      WHERE MUSIC_RANK <= 100
    
      UNION ALL
      -- Genie
      SELECT TO_DATE(BASEDT, 'YYYYMMDD') AS basedt, MUSIC_NAME, ARTIST_NAME, 101 - MUSIC_RANK AS score
      FROM DE6_PROJECT_III.RAW_DATA.GENIE_WEEKLY_CHART
      WHERE MUSIC_RANK <= 100
    
      UNION ALL
      -- Spotify
      SELECT TO_DATE(BASEDT, 'YYYYMMDD') AS basedt, MUSIC_NAME, ARTIST_NAME, 101 - MUSIC_RANK AS score
      FROM DE6_PROJECT_III.RAW_DATA.SPOTIFY_WEEKLY_CHART
      WHERE MUSIC_RANK <= 100
    ),
    
    score_avg AS (
      SELECT 
        basedt,
        MUSIC_NAME,
        ARTIST_NAME,
        ROUND(AVG(score), 2) AS avg_score
      FROM all_charts
      GROUP BY basedt, MUSIC_NAME, ARTIST_NAME
    ),
    
    ranked_chart AS (
      SELECT
        basedt,
        MUSIC_NAME,
        ARTIST_NAME,
        avg_score,
        DENSE_RANK() OVER (PARTITION BY basedt ORDER BY avg_score DESC) AS integrated_rank
      FROM score_avg
    ),
    
    top1_per_day AS (
      SELECT 
        basedt,
        MUSIC_NAME,
        ARTIST_NAME
      FROM ranked_chart
      WHERE integrated_rank = 1
    ),
    
    monthly_top1_count AS (
      SELECT 
        TO_CHAR(basedt, 'YYYY-MM') AS month,
        MIN(basedt) AS min_basedt,
        MUSIC_NAME,
        ARTIST_NAME,
        COUNT(*) AS top1_count
      FROM top1_per_day
      GROUP BY TO_CHAR(basedt, 'YYYY-MM'), MUSIC_NAME, ARTIST_NAME
    ),
    
    monthly_top1_labeled AS (
      SELECT *,
             -- 기준일자
             DATE_TRUNC('MONTH', min_basedt) AS month_date,
    
             -- 6개월 단위 인덱스 (0부터 시작)
             FLOOR(DATEDIFF(MONTH, (SELECT MIN(DATE_TRUNC('MONTH', basedt)) FROM top1_per_day), DATE_TRUNC('MONTH', min_basedt)) / 6) AS group_idx,
    
             -- 시작월
             TO_CHAR(DATEADD(MONTH, group_idx * 6, (SELECT MIN(DATE_TRUNC('MONTH', basedt)) FROM top1_per_day)), 'YYYY-MM') AS period_start,
    
             -- 종료월
             TO_CHAR(DATEADD(MONTH, group_idx * 6 + 5, (SELECT MIN(DATE_TRUNC('MONTH', basedt)) FROM top1_per_day)), 'YYYY-MM') AS period_end
      FROM monthly_top1_count
    ),
    
    ranked_monthly AS (
      SELECT *,
             period_start || '~' || period_end AS month6,
             RANK() OVER (PARTITION BY month ORDER BY top1_count DESC) AS rnk
      FROM monthly_top1_labeled
    ),

recent_month6_val AS (
  SELECT period_start || '~' || period_end AS recent_month6
  FROM (
    SELECT DISTINCT
      period_start,
      period_end,
      ABS(DATEDIFF(DAY, CURRENT_DATE, DATEADD(MONTH, 3, TO_DATE(period_start || '-01')))) AS diff -- 중간지점을 기준으로 거리 측정
    FROM monthly_top1_labeled
  )
  ORDER BY diff
  LIMIT 1
)

SELECT month, music_name, artist_name, top1_count,month6,  (SELECT recent_month6 FROM recent_month6_val) AS recent_month6
FROM ranked_monthly
WHERE rnk = 1
ORDER BY month;