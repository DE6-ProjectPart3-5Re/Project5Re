import time
import json
import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago # start_date 설정에 사용

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import os

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logger = logging.getLogger("airflow.task")

def parse_spotify_charts_html_structural(html_content, chart_date_for_db, country_code_for_db):
    soup = BeautifulSoup(html_content, 'html.parser')
    chart_items = []
    charts_table_div = soup.find('div', attrs={'data-testid': 'charts-table'})
    if not charts_table_div:
        logger.error(f"차트 div 못찾음 ({chart_date_for_db}, {country_code_for_db})")
        return chart_items
    table = charts_table_div.find('table')
    if not table:
        logger.error(f"차트 테이블 못찾음 ({chart_date_for_db}, {country_code_for_db})")
        return chart_items
    tbody = table.find('tbody')
    if not tbody:
        logger.error(f"테이블 바디 못찾음 ({chart_date_for_db}, {country_code_for_db})")
        return chart_items
    rows = tbody.find_all('tr')
    logger.info(f"날짜 {chart_date_for_db}, 국가 {country_code_for_db}: 테이블에서 {len(rows)}개 행 찾음.")
    if not rows:
        logger.warning(f"차트 데이터 행 없음 ({chart_date_for_db}, {country_code_for_db})")
        return chart_items
    for idx, row in enumerate(rows):
        cells = row.find_all('td')
        if len(cells) < 3: continue
        try:
            rank_text = "N/A"; rank_cell = cells[1]; rank_div = rank_cell.find('div')
            if rank_div:
                current_pos_span = rank_div.find('span', attrs={'aria-label': 'Current position'})
                if current_pos_span: rank_text = current_pos_span.text.strip()
                else:
                    all_spans_in_rank_div = rank_div.find_all('span', recursive=False)
                    if all_spans_in_rank_div and all_spans_in_rank_div[0].text.strip().isdigit(): rank_text = all_spans_in_rank_div[0].text.strip()
            if not rank_text.isdigit(): continue
            music_rank_val = int(rank_text)
            track_cell = cells[2]; music_name_val = "N/A"; artist_name_val = "N/A"; music_id_val = None
            title_span_tag = track_cell.find('span', class_=lambda x: x and 'StyledTruncatedTitle' in x)
            if title_span_tag: music_name_val = title_span_tag.text.strip()[:100]
            else:
                strong_title = track_cell.find('strong')
                if strong_title: music_name_val = strong_title.text.strip()[:100]
                else:
                    div_texts = [div.text.strip() for div in track_cell.find_all('div') if div.text.strip()]
                    if div_texts: music_name_val = div_texts[0][:100]
            track_link_tag = track_cell.find('a', href=lambda href: href and "open.spotify.com/track/" in href)
            if track_link_tag:
                track_full_url = track_link_tag['href']
                try: music_id_val = track_full_url.split('/track/')[-1].split('?')[0][:50]
                except Exception: logger.debug(f"ID 파싱 불가: {track_full_url}")
            artists_container = track_cell.find('div', class_=lambda x: x and 'StyledArtistsTruncatedDiv' in x)
            temp_artist_names = []
            if artists_container:
                artist_tags = artists_container.find_all(['a', 'span'])
                for tag in artist_tags:
                    if tag.name == 'a' or (tag.name == 'span' and not tag.find('a')):
                        artist_text = tag.text.strip()
                        if artist_text and artist_text not in temp_artist_names: temp_artist_names.append(artist_text)
                if temp_artist_names: artist_name_val = ', '.join(temp_artist_names)[:100]
            elif music_name_val != "N/A":
                title_element = title_span_tag if title_span_tag else track_cell.find(text=music_name_val)
                if title_element:
                    parent_of_title = title_element.find_parent()
                    sibling_artist_info = parent_of_title.find_next_sibling() if parent_of_title else None
                    if sibling_artist_info and sibling_artist_info.text.strip(): artist_name_val = sibling_artist_info.text.strip()[:100]
            if music_name_val == "N/A" or artist_name_val == "N/A": continue
            chart_items.append({'basedt': chart_date_for_db, 'country_code': country_code_for_db, 'music_id': music_id_val, 'music_name': music_name_val, 'music_rank': music_rank_val, 'artist_name': artist_name_val})
        except IndexError: logger.warning(f"Row {idx+1} 파싱 중 셀 개수 부족. Skipping.")
        except Exception as e: logger.error(f"Row {idx+1} 파싱 중 오류: {e}", exc_info=True)
    logger.info(f"파싱 완료: {len(chart_items)}개 항목 ({chart_date_for_db}, {country_code_for_db}).")
    return chart_items

def initialize_driver_and_login_once(google_email, google_password, initial_url="https://charts.spotify.com/home", retries=1):
    """WebDriver를 초기화하고 Spotify에 한 번 로그인합니다. 성공 시 드라이버 객체를 반환합니다."""
    logger.info(f"WebDriver 초기화 및 단일 로그인 시도 (초기 URL: {initial_url})")
    options = webdriver.ChromeOptions()
    options.add_argument('--headless'); options.add_argument('--disable-gpu'); options.add_argument('--no-sandbox'); options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--window-size=1366,768")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
    options.add_argument("accept-language=ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7")
    options.add_argument("--disable-blink-features=AutomationControlled"); options.add_experimental_option("excludeSwitches", ["enable-automation"]); options.add_experimental_option('useAutomationExtension', False)

    driver = None
    for attempt in range(retries + 1):
        try:
            logger.info(f"WebDriver 서비스 시작 (시도 {attempt + 1}/{retries + 1})...")
            service = ChromeService(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            driver.implicitly_wait(15)
            driver.set_page_load_timeout(120)

            logger.info(f"초기 URL로 이동: {initial_url}"); driver.get(initial_url); time.sleep(7)
            
            try:
                driver.get("https://charts.spotify.com/charts/view/regional-global-daily/latest")
                time.sleep(5)
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-testid='charts-table'] table tbody tr")))
                logger.info("이미 로그인되어 있거나 공개 차트 접근 가능하여 로그인 과정 생략 가능성 있음.")
                return driver 
            except:
                logger.info("로그인 필요 상태로 판단. 로그인 과정 시작.")
                driver.get(initial_url); time.sleep(5)

            if not login_to_spotify_with_google_v3(driver, google_email, google_password):
                raise AirflowException("Spotify Google 로그인 실패")
            
            logger.info("로그인 성공. WebDriver 인스턴스 반환 준비 완료.")
            return driver

        except Exception as e:
            logger.error(f"드라이버 초기화 또는 로그인 시도 {attempt + 1} 실패: {e}", exc_info=True)
            if driver: driver.quit()
            if attempt < retries: logger.info(f"Retrying driver init/login in 30s..."); time.sleep(30)
            else: logger.error("모든 드라이버 초기화/로그인 시도 실패."); raise AirflowException(f"Driver init/login failed: {e}")
    return None

def fetch_chart_page_html_with_driver(driver, chart_url, target_date_str, country_code):
    chart_type = 'weekly'
    logger.info(f"로그인된 드라이버로 차트 페이지 요청: {chart_url}")
    try:
        driver.get(chart_url)
        logger.info(f"'{chart_url}' 페이지 로드 후 15초 대기...")
        time.sleep(15) 

        main_content_locator = (By.CSS_SELECTOR, "div[data-testid='charts-table'] table tbody tr")
        WebDriverWait(driver, 90).until(EC.presence_of_all_elements_located(main_content_locator))
        logger.info(f"차트 컨텐츠 ({country_code}, {chart_type}, {target_date_str}) 감지됨.")
        time.sleep(7) 

        html_content = driver.page_source
        if "데이터를 사용할 수 없습니다" in html_content or "No data available" in html_content:
            logger.warning(f"페이지에 '{target_date_str}' ({country_code}, {chart_type})에 대한 데이터가 없다고 표시됩니다.")
            return None
        logger.info(f"페이지 소스 가져오기 성공 (길이: {len(html_content)}).")
        return html_content
    except Exception as e:
        logger.error(f"차트 페이지({chart_url}) HTML 가져오기 중 오류: {e}", exc_info=True)
        return None

def login_to_spotify_with_google_v3(driver, google_email, google_password):
    logger.info("Spotify 로그인 과정 시작 (v3 - 현재 창에서 구글 로그인)...")
    try:
        current_url = driver.current_url
        if "charts.spotify.com/home" in current_url:
            login_with_spotify_link_xpath = "//a[contains(text(), 'Log in with Spotify')]"
            try:
                logger.info(f"Spotify Charts 홈에서 '{login_with_spotify_link_xpath}' 링크 클릭 시도...")
                WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, login_with_spotify_link_xpath))).click()
                logger.info("'Log in with Spotify' 링크 클릭 성공."); time.sleep(3)
            except Exception as e_link:
                logger.debug(f"'Log in with Spotify' 링크 찾기/클릭 실패: {e_link}. 우상단 버튼 또는 다른 버튼 시도.")
                spotify_main_login_button_xpath = "//button[normalize-space()='Log in']"
                try:
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, spotify_main_login_button_xpath))).click()
                    logger.info("우상단 'Log in' 버튼 클릭 성공."); time.sleep(3)
                except:
                    logger.warning(f"Spotify 로그인 시작 버튼을 찾을 수 없으나 계속 진행. 현재 URL: {driver.current_url}")
                    pass
        
        logger.info("현재 페이지에서 'Google로 계속하기' 버튼 대기 중...")
        google_login_button_xpath = ("//button[@data-testid='google-login'] | //button[contains(@aria-label, 'Google') and (contains(., 'Google') or .//span[contains(text(),'Google')])] | //button[.//span[contains(text(),'Google로 계속하기')] or contains(text(),'Google로 계속하기')] | //button[.//span[contains(text(),'Continue with Google')] or contains(text(),'Continue with Google')] | //button[contains(@data-testid, 'google-button')]")
        google_button_present = False
        try:
            google_button = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, google_login_button_xpath)))
            logger.info("'Google로 계속하기' 버튼 찾음. 클릭 시도..."); driver.execute_script("arguments[0].click();", google_button)
            logger.info("'Google로 계속하기' 버튼 클릭 성공."); google_button_present = True
        except Exception as e_google_btn:
            logger.warning(f"'Google로 계속하기' 버튼 찾기/클릭 실패. 현재 URL: {driver.current_url}. 오류: {e_google_btn}")
            if "accounts.google.com" in driver.current_url: logger.info("이미 구글 로그인 페이지로 보임.")
            else: return False
        
        if google_button_present:
            logger.info("구글 로그인 페이지로 전환 대기 중 (최대 20초)...")
            WebDriverWait(driver, 20).until(lambda d: "accounts.google.com" in d.current_url)
            logger.info(f"구글 로그인 페이지로 전환됨. URL: {driver.current_url}")
        time.sleep(3)

        logger.info("구글 이메일 입력 필드 대기 중...")
        email_field_xpath = "//input[@type='email' or @name='identifier' or @aria-label='이메일 또는 휴대전화']"
        email_input = WebDriverWait(driver, 45).until(EC.visibility_of_element_located((By.XPATH, email_field_xpath)))
        logger.info("구글 이메일 입력 필드 찾음. 이메일 입력 시도..."); email_input.send_keys(google_email)
        logger.info(f"구글 이메일 '{google_email}' 입력 완료."); time.sleep(1)
        
        next_button_email_xpath = "//button[.//span[normalize-space()='다음'] or normalize-space()='다음' or .//span[normalize-space()='Next'] or normalize-space()='Next']"
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, next_button_email_xpath))).click()
        logger.info("이메일 입력 후 '다음' 버튼 클릭됨."); time.sleep(5)
        
        logger.info("구글 비밀번호 입력 필드 대기 중...")
        password_field_xpath = "//input[@type='password' or @name='Passwd' or @name='password']"
        password_input = WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By.XPATH, password_field_xpath)))
        logger.info("구글 비밀번호 입력 필드 찾음. 비밀번호 입력 시도..."); password_input.send_keys(google_password)
        logger.info("구글 비밀번호 입력 완료."); time.sleep(1)
        
        next_button_password_xpath = "//button[.//span[normalize-space()='다음'] or normalize-space()='다음' or .//span[normalize-space()='Next'] or normalize-space()='Next']"
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, next_button_password_xpath))).click()
        logger.info("비밀번호 입력 후 '다음' 버튼 클릭됨.")

        logger.info("로그인 완료 및 Spotify 페이지로 리디렉션 대기 중 (최대 75초)...")
        WebDriverWait(driver, 75).until(lambda d: "charts.spotify.com" in d.current_url or "spotify.com/us/authorize" in d.current_url or EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-testid='charts-table']"))(d))
        logger.info(f"Spotify 페이지로 돌아왔습니다. 현재 URL: {driver.current_url}")

        if "spotify.com/us/authorize" in driver.current_url:
            try:
                logger.info("Spotify 권한 부여 페이지 감지됨. 동의 버튼 클릭 시도...")
                authorize_button_xpath = "//button[@data-testid='auth-accept'] | //button[normalize-space()='동의하고 계속하기'] | //button[normalize-space()='Allow']"
                WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, authorize_button_xpath))).click()
                logger.info("Spotify 권한 부여 완료.")
                WebDriverWait(driver, 45).until(lambda d: "charts.spotify.com" in d.current_url)
                logger.info(f"차트 페이지로 최종 이동 완료. URL: {driver.current_url}")
            except Exception as e_auth: logger.warning(f"Spotify 권한 부여 페이지 처리 중 오류 또는 버튼 없음: {e_auth}")
        time.sleep(10); return True
    except Exception as e:
        logger.error(f"구글 로그인 과정 중 오류 발생: {e}", exc_info=True)
        return False

def spotify_crawl(target_date_str, country_code, google_email, google_password, retries=0):
    chart_type = 'weekly'
    if country_code.upper() == 'GLOBAL':
        chart_segment = f"regional-global-{chart_type.lower()}"
    else:
        chart_segment = f"regional-{country_code.lower()}-{chart_type.lower()}"
    
    chart_url = f"https://charts.spotify.com/charts/view/{chart_segment}/{target_date_str}"
    initial_url = "https://charts.spotify.com/home"
    logger.info(f"Selenium (login v3)으로 URL 요청: {chart_url} (초기 진입: {initial_url})")
    
    options = webdriver.ChromeOptions()
    options.add_argument('--headless'); options.add_argument('--disable-gpu'); options.add_argument('--no-sandbox'); options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--window-size=1366,768")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
    options.add_argument("accept-language=ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7")
    options.add_argument("--disable-blink-features=AutomationControlled"); options.add_experimental_option("excludeSwitches", ["enable-automation"]); options.add_experimental_option('useAutomationExtension', False)

    html_content = None
    for attempt in range(retries + 1):
        driver = None
        try:
            logger.info(f"WebDriver 서비스 시작 (시도 {attempt + 1}/{retries + 1})..."); service = ChromeService(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options); driver.implicitly_wait(15); driver.set_page_load_timeout(120)
            logger.info(f"초기 URL로 이동: {initial_url}"); driver.get(initial_url); time.sleep(7)
            logger.info(f"차트 페이지로 직접 이동 시도하여 로그인 상태 확인: {chart_url}"); driver.get(chart_url); time.sleep(5)
            try:
                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-testid='charts-table'] table tbody tr")))
                logger.info("차트 데이터가 바로 보여 이미 로그인된 것으로 간주합니다.")
            except:
                logger.info("차트 데이터가 바로 보이지 않음. 로그인 과정 필요.")
                if "accounts.spotify.com" not in driver.current_url and "charts.spotify.com/home" not in driver.current_url:
                    logger.info(f"로그인 시작점으로 이동: {initial_url}"); driver.get(initial_url); time.sleep(5)
                if not login_to_spotify_with_google_v3(driver, google_email, google_password):
                    logger.error("구글 계정으로 Spotify 로그인 실패."); raise AirflowException("Spotify Login Failed")
                logger.info(f"로그인 성공 후 다시 차트 페이지로 이동: {chart_url}"); driver.get(chart_url); time.sleep(15)
                WebDriverWait(driver, 75).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-testid='charts-table'] table tbody tr")))
                logger.info("로그인 후 차트 데이터 로드 확인됨.")
            html_content = driver.page_source
            if "데이터를 사용할 수 없습니다" in html_content or "No data available" in html_content:
                logger.warning(f"페이지에 '{target_date_str}' ({country_code}, {chart_type})에 대한 데이터가 없다고 표시됩니다."); return None
            logger.info(f"페이지 소스 가져오기 성공 (로그인 후, 길이: {len(html_content)}).")
            return html_content
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed for {chart_url}: {e}", exc_info=True)
            if attempt < retries: logger.info(f"Retrying in 40 seconds..."); time.sleep(40)
            else: logger.error(f"All {retries + 1} attempts failed for {chart_url}."); raise AirflowException(f"All attempts failed: {e}")
        finally:
            if driver: driver.quit(); logger.info("WebDriver 최종 종료됨 (login v3).")
    return None

def get_snowflake_hook():
    return SnowflakeHook(snowflake_conn_id='snowflake_default')

def save_data(snowflake_hook: SnowflakeHook, chart_data_list):
    if not chart_data_list: logger.warning("저장할 차트 데이터 없음."); return 0
    target_table_name = "raw_data.spotify_weekly_chart"
    staging_table_name_base = f"SPOTIFY_CHART_STAGING_{datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    conn = None
    try:
        conn = snowflake_hook.get_conn(); logger.info("Snowflake 연결 성공.")
        with conn.cursor() as cur:
            create_staging_sql = f"""
            CREATE OR REPLACE TEMPORARY TABLE {staging_table_name_base} (
                BASEDT VARCHAR(8), COUNTRY_CODE VARCHAR(10),
                MUSIC_ID VARCHAR(50), MUSIC_NAME VARCHAR(100),
                MUSIC_RANK NUMBER(8,0), ARTIST_NAME VARCHAR(100)
            );"""
            cur.execute(create_staging_sql); logger.info(f"임시 스테이징 테이블 {staging_table_name_base} 생성됨.")
            rows_to_insert = []
            for item in chart_data_list:
                rows_to_insert.append((
                    item['basedt'], item['country_code'], 
                    item.get('music_id'), item.get('music_name'),
                    int(item['music_rank']), item.get('artist_name')
                ))
            if not rows_to_insert: logger.warning("스테이징할 유효한 행 없음."); return 0
            insert_sql = f"INSERT INTO {staging_table_name_base} (BASEDT, COUNTRY_CODE, MUSIC_ID, MUSIC_NAME, MUSIC_RANK, ARTIST_NAME) VALUES (%s, %s, %s, %s, %s, %s)"
            cur.executemany(insert_sql, rows_to_insert); logger.info(f"{cur.rowcount}개 행 스테이징 테이블에 삽입됨.")
            merge_sql = f"""
            MERGE INTO {target_table_name} TGT
            USING {staging_table_name_base} STG
            ON TGT.BASEDT = STG.BASEDT AND TGT.COUNTRY_CODE = STG.COUNTRY_CODE AND TGT.MUSIC_RANK = STG.MUSIC_RANK
            WHEN NOT MATCHED THEN INSERT (BASEDT, COUNTRY_CODE, MUSIC_ID, MUSIC_NAME, MUSIC_RANK, ARTIST_NAME)
                VALUES (STG.BASEDT, STG.COUNTRY_CODE, STG.MUSIC_ID, STG.MUSIC_NAME, STG.MUSIC_RANK, STG.ARTIST_NAME)
            WHEN MATCHED THEN UPDATE SET 
                TGT.MUSIC_ID = STG.MUSIC_ID, TGT.MUSIC_NAME = STG.MUSIC_NAME, TGT.ARTIST_NAME = STG.ARTIST_NAME;"""
            cur.execute(merge_sql); num_affected = 0; merge_res = cur.fetchone()
            if merge_res and len(merge_res) >=2: num_affected = merge_res[0] + merge_res[1]; logger.info(f"MERGE 완료. 삽입: {merge_res[0]}, 업데이트: {merge_res[1]}")
            else: num_affected = cur.rowcount if cur.rowcount is not None else len(rows_to_insert); logger.info(f"MERGE 완료. 영향받은 행(추정): {num_affected}")
            conn.commit(); return num_affected
    except Exception as e:
        logger.error(f"Snowflake 작업 중 오류: {e}", exc_info=True)
        if conn: conn.rollback()
        raise AirflowException(f"Snowflake 작업 실패: {e}")
    finally:
        if conn: conn.close(); logger.info("Snowflake 연결 종료.")

def spotify_charts_selenium_etl_weekly_task(ds, **kwargs):
    execution_date_obj = datetime.datetime.strptime(ds, '%Y-%m-%d')
    previous_monday_obj = execution_date_obj - datetime.timedelta(days=7)
    basedt_for_db = previous_monday_obj.strftime('%Y%m%d')
    target_spotify_date_obj = previous_monday_obj + datetime.timedelta(days=3) 
    target_date_str_for_url = target_spotify_date_obj.strftime('%Y-%m-%d')

    logger.info(f"주간 차트 ETL 시작. DAG 실행일(월): {ds}, DB기준일(지난주월): {basedt_for_db}, Spotify조회일(지난주목): {target_date_str_for_url}")

    countries_to_crawl = ['KR', 'US', 'GB', 'GLOBAL'] 

    try:
        google_email = Variable.get("spotify_google_email")
        google_password = Variable.get("spotify_google_password", deserialize_json=False)
    except KeyError:
        logger.error("Airflow Variables 'spotify_google_email' 또는 'spotify_google_password'가 설정되지 않았습니다.")
        raise AirflowException("Missing login credentials.")

    drive = None
    all_parsed_data = []
    try:
        driver = initialize_driver_and_login_once(google_email, google_password, retries=1)
        if not driver:
            raise AirflowException("WebDriver 초기화 및 로그인 최종 실패")
        for country_code in countries_to_crawl:
            chart_type_for_url = 'weekly'
            if country_code.upper() == 'GLOBAL':
                chart_segment = f"regional-global-{chart_type_for_url.lower()}"
            else:
                chart_segment = f"regional-{country_code.lower()}-{chart_type_for_url.lower()}"
            chart_url = f"https://charts.spotify.com/charts/view/{chart_segment}/{target_date_str_for_url}"
            
            logger.info(f"--- {country_code} {chart_type_for_url} 차트 크롤링 시작 (Spotify 기준일: {target_date_str_for_url}) ---")
            html_content = fetch_chart_page_html_with_driver(driver, chart_url, target_date_str_for_url, country_code)
            if html_content:
                parsed_country_data = parse_spotify_charts_html_structural(html_content, basedt_for_db, country_code)
                if parsed_country_data:
                    all_parsed_data.extend(parsed_country_data)
                    logger.info(f"{country_code} {chart_type_for_url} 차트에서 {len(parsed_country_data)}개 항목 파싱 완료.")
                else:
                    logger.warning(f"{country_code} {chart_type_for_url} 차트 파싱 결과 없음 (Spotify 기준일: {target_date_str_for_url}).")
            else:
                logger.warning(f"{country_code} {chart_type_for_url} 차트 HTML 가져오기 실패 (Spotify 기준일: {target_date_str_for_url}).")
            
            logger.info(f"{country_code} 크롤링 후 15초 대기...")
            time.sleep(15)

        if all_parsed_data:
            logger.info(f"총 {len(all_parsed_data)}개의 주간 차트 항목을 Snowflake에 저장합니다 (DB 기준일: {basedt_for_db}).")
            snowflake_hook = get_snowflake_hook()
            affected_rows = save_data(snowflake_hook, all_parsed_data)
            logger.info(f"{affected_rows}개의 주간 차트 항목이 Snowflake에 반영/처리되었습니다.")
        else:
            logger.warning("모든 국가에 대해 파싱된 주간 차트 데이터가 없습니다.")

    except Exception as e:
        logger.error(f"주간 차트 ETL 태스크 중 오류 발생: {e}", exc_info=True)
        raise AirflowException(f"Weekly chart ETL task failed: {e}")
    finally:
        if driver:
            driver.quit()
            logger.info("WebDriver 최종 종료됨.")
    
    logger.info(f"주간 차트 ETL 태스크 완료 (DAG 실행일: {ds}).")

# --- Airflow DAG 정의 ---
default_args_weekly_selenium = {
    'owner': 'cjw',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, 
    'retry_delay': datetime.timedelta(minutes=30), 
    'execution_timeout': datetime.timedelta(hours=3, minutes=0), 
}

start_date_for_dag_weekly = datetime.datetime(2023, 1, 2, 9, 0, 0, tzinfo=datetime.timezone.utc)

with DAG(
    dag_id='spotify_weekly_charts',
    default_args=default_args_weekly_selenium,
    description='Weekly Spotify charts (KR, US, GB, GLOBAL via Selenium) to Snowflake',
    schedule_interval='0 9 * * 1', 
    start_date=start_date_for_dag_weekly,
    catchup=True, 
    tags=['spotify', 'charts', 'selenium', 'snowflake', 'weekly', 'multi-country'],
    max_active_runs=1, 
) as dag_weekly_final_no_chart_type:

    start_pipeline = EmptyOperator(task_id='start_weekly_pipeline')

    crawl_and_load_weekly_task = PythonOperator(
        task_id='crawl_and_load_all_weekly_charts',
        python_callable=spotify_charts_selenium_etl_weekly_task,
    )

    end_pipeline = EmptyOperator(task_id='end_weekly_pipeline')

    start_pipeline >> crawl_and_load_weekly_task >> end_pipeline