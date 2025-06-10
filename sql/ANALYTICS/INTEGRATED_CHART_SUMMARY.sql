create or replace view DE6_PROJECT_III.ANALYTICS.INTEGRATED_CHART_SUMMARY(
	MUSIC_NAME,
	ARTIST_NAME,
	TOTAL_SCORE,
	TOTAL_RANK
) as
SELECT
    music_name,
    artist_name,
    SUM(score) AS total_score,
    RANK() OVER (ORDER BY SUM(score) DESC) AS total_rank
FROM
    analytics.integrated_chart
GROUP BY
    music_name,
    artist_name;