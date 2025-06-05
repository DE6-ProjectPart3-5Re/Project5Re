from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta

import logging
import psycopg2
from selenium import webdriver
from bs4 import BeautifulSoup

class crawling_driver:
    
    options = webdriver.ChromeOptions()

    def __init__(self):
        logging.info("initialize the Selenium driver")
        self.options = webdriver.ChromeOptions()
        self.options.add_argument("--headless") # 브라우저 창을 띄우지 않고 실행
        self.options.add_argument("--no-sandbox") # 샌드박스 보안 기능 비활성화 
        self.options.add_argument("--disable-dev-shm-usage") # /dev/shm (공유 메모리) 대신 디스크를 사용하게 하여 메모리 부족 문제 해결
        self.options.set_capability('browserName', 'chrome') # 브라우저 이름을 명시적으로 설정
        self.options.add_argument('--disable-gpu') # GPU 가속 기능 비활성화 
        self.options.add_argument('--disable-blink-features=AutomationControlled') # Selenium을 통해 자동 제어 중이라는 Blink 엔진의 탐지 기능 비활성화
        self.options.add_experimental_option('excludeSwitches', ['enable-automation']) # "Chrome is being controlled by automated software" 메시지 제거 및 봇 탐지 우회

    def __enter__(self):
        # driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        self.driver = webdriver.Remote(command_executor="http://selenium:4444/wd/hub", options=self.options)
        self.driver.implicitly_wait(10) # seconds
        return self.driver
    def __exit__(self, exc_type, exc_value, traceback):
        logging.info("Closing the Selenium driver")
        self.driver.quit()

def getGenieChart(ditc, date):
    #지니 뮤직 top 200 (https://www.genie.co.kr/chart/top200) 파라미터 정리
    # rtm=Y/N 실시간 여부
    # rtm이N일때, ditc: D= 일간차트, W= 주간차트, M= 월간차트, S= 누적차트
    # rtm이N일때, ditc이 D이면, ymd=기준일자(전일부터 존제)
    # rtm이N일때, ditc이 W이면, ymd=기준일자 (전주 월요일)
    # rtm이N일때, ditc이 M이면, ymd=기준일자(전월 1일)
    # pg: 페이지 번호 (1부터 시작, 4페이지 까지 존재)
    # ed) https://www.genie.co.kr/chart/top200?ditc=D&rtm=N&ymd=20250603&pg=1
    
    url = f"https://www.genie.co.kr/chart/top200?ditc={ditc}&rtm=N"
    
    if ditc == "D":   #일간차트, 선택한 일자 그대로 삽입.
        url += f"&ymd={date.strftime('%Y%m%d')}"
    elif ditc == "W": # 주간차트. 작업 기중일자의 월요일 구하기.
        date = date - timedelta(days=date.weekday())
        url += f"&ymd={date.strftime('%Y%m%d')}"
    elif ditc == "M": #월간 차트 선택한 일자의 1일자 구하기.
        url += f"&ymd={date.replace(day=1).strftime('%Y%m%d')}"
    
    logging.info(f"Extracting data from {url}")

    with crawling_driver() as driver:
        list = []
        for i in range(1, 5):
            driver.get(url + f"&pg={i}")
            basedt = driver.find_element("xpath", "//*[@id=\"inc_date\"]")
            bs = BeautifulSoup(driver.find_element("xpath", "//*[@id=\"body-content\"]/div[4]/div/table/tbody").get_attribute('outerHTML'),"html.parser")
            
            for tr in bs.select("tr"):
                songId = tr.get("songid")
                song_rank = tr.find(class_="number").find(text=True, recursive=False).strip()
                info = tr.find(class_="info")
                title = info.find(class_="title")
                span = title.find("span")
                if span :
                    span.decompose()  # 19금 span 태그 있으면 제거
                song_name = title.text.replace("\n", "").strip()  # 개행문자 제거 및 양쪽 공백 제거
                song_artist = info.find(class_="artist").text.strip()
                list.append({
                    "songId": songId,
                    "song_rank": song_rank,
                    "song_name": song_name,
                    "song_artist": song_artist
                })
                
        return list


def get_snowflake_connection(autocommit=True):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_project_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


def extract(**context):

    logging.basicConfig(level=logging.DEBUG)
    logging.info("get Genie chart data")

    # link = context["params"]["url"]
    # task_instance = context['task_instance']
    execution_date = context['execution_date']

    logging.info(execution_date)
    result = getGenieChart("D", execution_date)
    
    return result


def load(**context):
    execution_date = context['execution_date'].strftime('%Y%m%d')
    logging.info("load started")    
    records = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")    
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    cur = get_snowflake_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM RAW_DATA.GENIE_DAILY_CHART WHERE BASEDT = {execution_date};") 
        # DELETE FROM을 먼저 수행 -> FULL REFRESH을 하는 형태
        for r in records:
            songId = r["songId"]
            song_rank = r["song_rank"]
            song_name = r["song_name"].replace("'", "''")
            song_artist = r["song_artist"].replace("'", "''")
            sql = f"INSERT INTO RAW_DATA.GENIE_DAILY_CHART(BASEDT,MUSIC_ID,MUSIC_NAME,MUSIC_RANK,ARTIST_NAME) VALUES ('{execution_date}', '{songId}', '{song_name}', {song_rank}, '{song_artist}')"
            logging.info(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;") 
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    logging.info("load done")


dag = DAG(
    dag_id = 'GetGenieDaliyChart',
    start_date = datetime(2025,5,1), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 13 * * *',  # 적당히 조절
    max_active_runs = 1,
    concurrency = 1,
    catchup = False,
    default_args = {
        'retries': 0
    }
)


extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {},
    dag = dag)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {},
    dag = dag)

extract >> load
