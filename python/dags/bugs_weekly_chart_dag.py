from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup
from snowflake.connector.pandas_tools import write_pandas
import re

# 벅스 주간차트 크롤링 함수
def crawl_bugs_weekly_chart_by_date(date_str: str) -> pd.DataFrame:
    base_url = f"https://music.bugs.co.kr/chart/track/week/total?chartdate={date_str}"
    print(f"크롤링 주간 날짜: {date_str}")

    try:
        res = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0"})
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")

        rows = soup.select("table.list.trackList tbody tr")

        chart_data = []
        for row in rows:
            rank_tag = row.select_one("div.ranking > strong")
            title_tag = row.select_one("p.title > a")
            artist_tag = row.select_one("p.artist > a")

            if rank_tag and title_tag and artist_tag:
                onclick_attr = title_tag.get("onclick", "")
                music_id_match = re.search(r"bugs\.music\.listen\('(\d+)'", onclick_attr)
                music_id = music_id_match.group(1) if music_id_match else ""

                chart_data.append({
                    "basedt": date_str,
                    "music_id": music_id,
                    "music_name": title_tag.text.strip(),
                    "music_rank": int(rank_tag.text.strip()),
                    "artist_name": artist_tag.text.strip()
                })

        return pd.DataFrame(chart_data)

    except Exception as e:
        print(f"[에러] {date_str} 주간차트 크롤링 실패: {e}")
        return pd.DataFrame()


# 적재 함수 (Snowflake)
def upload_to_snowflake(df: pd.DataFrame, table_name: str):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn_rawdata_db")
    conn = hook.get_conn()

    df.columns = [col.upper() for col in df.columns]
    success, nchunks, nrows, _ = write_pandas(conn, df, table_name.upper())

    if success:
        print(f"Snowflake에 {nrows}개 행 적재 완료 (Table: {table_name})")
    else:
        print("Snowflake 적재 실패")

from airflow.decorators import dag, task

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# DAG 정의
@dag(
    dag_id='bugs_weekly_etl',
    default_args=default_args,
    schedule_interval="0 13 * * 1",  # 매주 월요일 오후 1시
    start_date=datetime(2023, 1, 1),
    catchup=True,
    tags=['bugs', 'chart', 'weekly'],
)
def bugs_weekly_etl():
    
    @task()
    def format_date(ds: str) -> str:
        print(f"[format_date] ds값 확인. DAG 논리 실행 날짜: {ds}")
        return ds.replace("-", "")  # 예: 2025-06-05 → 20250605

    @task()
    def extract_chart(date_str: str) -> pd.DataFrame:
        print(f"[추출 시작] {date_str}")
        return crawl_bugs_weekly_chart_by_date(date_str)

    @task()
    def load_to_snowflake(df: pd.DataFrame):
        if not df.empty:
            print("[적재 시작]")
            upload_to_snowflake(df, table_name="bugs_weekly_chart")
        else:
            print("데이터 없음: 적재 생략")

    #raw_date = get_last_week_monday()
    raw_date = format_date()
    df = extract_chart(raw_date)
    load_to_snowflake(df)

dag_instance = bugs_weekly_etl()
