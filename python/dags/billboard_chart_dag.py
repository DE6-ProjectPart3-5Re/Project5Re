from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import time
import logging

@task
def extract_and_transform(execution_date=None):
    chart_date = execution_date.date()  # ← 월요일 실행일이 곧 크롤링 기준일
    chart_date_str = chart_date.strftime("%Y-%m-%d")
    basedt_str = chart_date.strftime("%Y%m%d")

    def scrape_billboard(date):
        url = f"https://www.billboard.com/charts/hot-100/{date}"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        chart_items = soup.select("div.o-chart-results-list-row-container")

        result = []
        for item in chart_items[:100]:
            try:
                rank = int(item.select_one("span.c-label.a-font-primary-bold-l").text.strip())
                title = item.select_one("h3.c-title").text.strip()
                artist = item.select_one("span.c-label.a-no-trucate.a-font-primary-s").text.strip()

                result.append({
                    "basedt": basedt_str,
                    "music_rank": rank,
                    "music_name": title,
                    "artist_name": artist
                })
            except Exception:
                continue
        return result

    logging.info(f"{chart_date_str} 기준 주간 차트 수집 중...")
    return scrape_billboard(chart_date_str)

@task
def load_to_snowflake(records):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_project_db")
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")

        if records:
            basedt = records[0]["basedt"]
            cur.execute(f"DELETE FROM DE6_PROJECT_III.RAW_DATA.BILLBOARD_WEEKLY_CHART WHERE BASEDT = '{basedt}'")

        for row in records:
            sql = """
                INSERT INTO DE6_PROJECT_III.RAW_DATA.BILLBOARD_WEEKLY_CHART 
                (BASEDT, MUSIC_ID, MUSIC_NAME, MUSIC_RANK, ARTIST_NAME)
                VALUES (%s, %s, %s, %s, %s)
            """
            cur.execute(sql, (
                row["basedt"],
                None,
                row["music_name"],
                row["music_rank"],
                row["artist_name"]
            ))

        cur.execute("COMMIT")
        logging.info(f"{len(records)}건 적재 완료 (basedt = {records[0]['basedt']})")
    except Exception as e:
        logging.error(f"적재 실패: {e}")
        cur.execute("ROLLBACK")
        raise
    finally:
        cur.close()

# DAG 정의
with DAG(
    dag_id="Billboard_Chart_Dag",
    start_date=datetime(2025, 1, 5),  # 첫 월요일
    schedule='0 13 * * 1',  # 매주 월요일 13:00
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=1)},
    tags=["billboard", "snowflake"]
) as dag:

    records = extract_and_transform()
    load_to_snowflake(records)
