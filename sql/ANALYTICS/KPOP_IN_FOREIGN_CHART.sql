create or replace view DE6_PROJECT_III.ANALYTICS.KPOP_IN_FOREIGN_CHART(
	BASEDT,
	COUNTRY_CODE,
	MUSIC_ID,
	MUSIC_NAME,
	MUSIC_RANK,
	ORIGINAL_ARTIST_NAME
) as
WITH 
kr_distinct_artists_in_period AS (
    SELECT DISTINCT
        TRIM(f.value::string) AS kr_artist_name_normalized
    FROM
        raw_data.spotify_weekly_chart kr_chart,
        LATERAL SPLIT_TO_TABLE(kr_chart.artist_name, ',') f -- 쉼표로 구분된 경우 가정
    WHERE
        kr_chart.country_code = 'KR'
        AND kr_chart.music_rank <= 200
        AND TO_DATE(kr_chart.basedt, 'YYYYMMDD') BETWEEN DATEADD(year, -2, CURRENT_DATE()) AND CURRENT_DATE() 
        AND kr_artist_name_normalized IS NOT NULL AND kr_artist_name_normalized != ''
),
foreign_chart_with_individual_artists AS (
    SELECT
        s.basedt,
        s.country_code,
        s.music_id,
        s.music_name,
        s.music_rank,
        s.artist_name AS original_artist_name, -- 원본 전체 아티스트 이름
        TRIM(f.value::string) AS individual_artist_name_normalized
    FROM
        raw_data.spotify_weekly_chart s,
        LATERAL SPLIT_TO_TABLE(s.artist_name, ',') f
    WHERE
        s.country_code != 'KR'
        AND s.artist_name IS NOT NULL AND s.artist_name != ''
        AND s.music_id IS NOT NULL
)
SELECT DISTINCT -- 각 곡이 여러 아티스트로 인해 중복될 수 있으므로 DISTINCT 추가
    fca.basedt,
    fca.country_code,
    fca.music_id,
    fca.music_name,
    fca.music_rank,
    fca.original_artist_name
FROM
    foreign_chart_with_individual_artists fca
WHERE
    EXISTS (
        SELECT 1
        FROM kr_distinct_artists_in_period kra
        WHERE UPPER(fca.individual_artist_name_normalized) = UPPER(kra.kr_artist_name_normalized) 
    );