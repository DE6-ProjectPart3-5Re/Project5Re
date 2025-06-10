create or replace view DE6_PROJECT_III.ANALYTICS.INTEGRATED_CHART(
	BASEDT,
	MUSIC_NAME,
	MUSIC_RANK,
	ARTIST_NAME,
	SCORE,
	SITE_NAME
) as
-- bugs
SELECT
    TO_DATE(basedt, 'YYYYMMDD') AS basedt,
    CASE
        WHEN music_name = '모르시나요(PROD.로코베리)' THEN '모르시나요 (Prod. by 로코베리)'
        WHEN music_name = 'HOME SWEET HOME (feat. 태양, 대성)' THEN 'HOME SWEET HOME (Feat. 태양 & 대성)'
        ELSE REPLACE(music_name, 'feat', 'Feat')
    END AS music_name,
    music_rank,
    CASE
        WHEN artist_name = 'Lady GaGa(레이디 가가)' AND music_name = 'Die With A Smile' THEN 'Lady Gaga & Bruno Mars'
        WHEN artist_name = '로제(ROSÉ)' AND music_name = 'APT.' THEN '로제 (ROSÉ) & Bruno Mars'
        WHEN artist_name = '로제(ROSÉ)' THEN '로제 (ROSÉ)'
        WHEN artist_name = '지민' THEN 'Jimin'
        ELSE artist_name
    END AS artist_name,
    (101 - music_rank) AS score,
    'bugs' AS site_name
FROM
    raw_data.bugs_weekly_chart

UNION ALL
-- genie
SELECT
    TO_DATE(basedt, 'YYYYMMDD') AS basedt,
    music_name,
    music_rank,
    CASE
        WHEN artist_name = '지민' THEN 'Jimin'
        ELSE artist_name
    END AS artist_name,
    (101 - music_rank) AS score,
    'genie' AS site_name
FROM
    raw_data.genie_weekly_chart

UNION ALL
-- spotify (only top 100)
SELECT
    TO_DATE(basedt, 'YYYYMMDD') AS basedt,
    REPLACE(music_name, 'feat', 'Feat') AS music_name,
    music_rank,
    CASE
        WHEN artist_name = 'Lady Gaga, Bruno Mars' THEN 'Lady Gaga & Bruno Mars'
        WHEN artist_name = 'ROSÉ, Bruno Mars' THEN '로제 (ROSÉ) & Bruno Mars'
        WHEN artist_name = 'ROSÉ' THEN '로제 (ROSÉ)'
        WHEN artist_name = 'JENNIE' THEN '제니 (JENNIE)'
        WHEN artist_name = 'DAY6' THEN 'DAY6 (데이식스)'
        WHEN artist_name = 'LE SSERAFIM' THEN 'LE SSERAFIM (르세라핌)'
        ELSE artist_name
    END AS artist_name,
    (101 - music_rank) AS score,
    'spotify' AS site_name
FROM
    raw_data.spotify_weekly_chart
WHERE
    music_rank <= 100
    AND country_code = 'KR'

UNION ALL
-- billboard
SELECT
    TO_DATE(basedt, 'YYYYMMDD') AS basedt,
    music_name,
    music_rank,
    CASE
        WHEN artist_name = 'ROSE & Bruno Mars' AND music_name = 'APT.' THEN '로제 (ROSÉ) & Bruno Mars'
        WHEN artist_name = 'JENNIE' THEN '제니 (JENNIE)'
        ELSE artist_name
    END AS artist_name,
    (101 - music_rank) AS score,
    'billboard' AS site_name
FROM
    raw_data.billboard_weekly_chart;