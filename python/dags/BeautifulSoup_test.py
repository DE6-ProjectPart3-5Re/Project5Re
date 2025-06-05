import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

class crawling_driver:
    
    options = webdriver.ChromeOptions()

    def __init__(self):
        logging.info("initialize the Selenium driver")
        self.options = webdriver.ChromeOptions()
        # self.options.add_argument("--headless") # 브라우저 창을 띄우지 않고 실행
        # self.options.add_argument("--no-sandbox") # 샌드박스 보안 기능 비활성화 
        # self.options.add_argument("--disable-dev-shm-usage") # /dev/shm (공유 메모리) 대신 디스크를 사용하게 하여 메모리 부족 문제 해결
        # self.options.set_capability('browserName', 'chrome') # 브라우저 이름을 명시적으로 설정
        # self.options.add_argument('--disable-gpu') # GPU 가속 기능 비활성화 
        # self.options.add_argument('--disable-blink-features=AutomationControlled') # Selenium을 통해 자동 제어 중이라는 Blink 엔진의 탐지 기능 비활성화
        # self.options.add_experimental_option('excludeSwitches', ['enable-automation']) # "Chrome is being controlled by automated software" 메시지 제거 및 봇 탐지 우회

    def __enter__(self):
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=self.options)
        # self.driver = webdriver.Remote(command_executor="http://selenium:4444/wd/hub", options=self.options)
        self.driver.implicitly_wait(10) # seconds
        return self.driver
    def __exit__(self, exc_type, exc_value, traceback):
        logging.info("Closing the Selenium driver")
        self.driver.quit()

def getGenieChart():
    #지니 뮤직 top 200 (https://www.genie.co.kr/chart/top200) 파라미터 정리
    # rtm=Y/N 실시간 여부
    # rtm이N일때, ditc: D= 일간차트, W= 주간차트, M= 월간차트, S= 누적차트
    # rtm이N일때, ditc이 D이면, ymd=기준일자(전일부터 존제)
    # rtm이N일때, ditc이 W이면, ymd=기준일자 (전주 월요일)
    # rtm이N일때, ditc이 M이면, ymd=기준일자(전월 1일)
    # pg: 페이지 번호 (1부터 시작, 4페이지 까지 존재)
    # ed) https://www.genie.co.kr/chart/top200?ditc=D&rtm=N&ymd=20250603&pg=1
    
    url = f"https://www.genie.co.kr/chart/top200?ditc=D&rtm=N&ymd=20250603"
    
    # if ditc == "D":   #일간차트, 선택한 일자 그대로 삽입.
    #     url += f"&ymd={date.strftime('%Y%m%d')}"
    # elif ditc == "W": # 주간차트. 작업 기중일자의 월요일 구하기.
    #     date = date - timedelta(days=date.weekday())
    #     url += f"&ymd={date.strftime('%Y%m%d')}"
    # elif ditc == "M": #월간 차트 선택한 일자의 1일자 구하기.
    #     url += f"&ymd={date.replace(day=1).strftime('%Y%m%d')}"
    
    logging.info(f"Extracting data from {url}")

    with crawling_driver() as driver:
        list = []
        for i in range(4, 5):
            driver.get(url + f"&pg={i}")
            bs = BeautifulSoup(driver.find_element("xpath", "//*[@id=\"body-content\"]/div[4]/div/table/tbody").get_attribute('outerHTML'),"html.parser")
            
            for tr in bs.select("tr"):
                songId = tr.get("songid")
                song_rank = tr.find(class_="number").find(text=True, recursive=False).strip()
                info = tr.find(class_="info")
                title = info.find(class_="title")
                span = title.find("span")
                span and span.decompose()  # 19금 span 태그 있으면 제거
                song_name = title.text.replace("\n", "").strip()  # 개행문자 제거 및 양쪽 공백 제거
                song_artist = info.find(class_="artist").text.strip()
                list.append({
                    "songId": songId,
                    "song_rank": song_rank,
                    "song_name": song_name,
                    "song_artist": song_artist
                })
                
        return list

print(getGenieChart())


# <a href="#" class="title ellipsis" title="Peaches (Feat. Daniel Caesar &amp; Giveon)" onclick="fnPlaySong('92682943;','1'); return false;" ontouchend="fnPlaySong('92682943;','1'); return false;">
#           <span class="icon icon-19">19<span class="hide">금</span></span>
# Peaches (Feat. Daniel Caesar &amp; Giveon)</a>