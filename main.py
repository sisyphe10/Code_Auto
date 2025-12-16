import time
import schedule
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import os
import csv  # 엑셀 대신 CSV 사용
import yfinance as yf
import re

# === 상수 정의 ===
# [수정] 파일 확장자를 csv로 변경
CSV_FILE = 'dataset.csv'

# DRAM 제품명
TARGET_DRAM_ITEMS = {
    'DDR5 16G (2Gx8) 4800/5600': 'DDR5 16G (2Gx8) 4800/5600',
    'DDR4 16Gb (1Gx16)3200': 'DDR4 16Gb (1Gx16)3200',
    'DDR4 16Gb (2Gx8)3200': 'DDR4 16Gb (2Gx8)3200',
    'DDR4 8Gb (1Gx8) 3200': 'DDR4 8Gb (1Gx8) 3200',
    'DDR4 8Gb (512Mx16) 3200': 'DDR4 8Gb (512Mx16) 3200'
}

# NAND 제품명
TARGET_NAND_ITEMS = {
    'SLC 2Gb 256MBx8': 'SLC 2Gb 256MBx8',
    'SLC 1Gb 128MBx8': 'SLC 1Gb 128MBx8',
    'MLC 64Gb 8GBx8': 'MLC 64Gb 8GBx8',
    'MLC 32Gb 4GBx8': 'MLC 32Gb 4GBx8'
}

# yfinance 및 금리 티커 목록
YFINANCE_TICKERS = {
    'Bitcoin': {'ticker': 'BTC-USD', 'type': 'CRYPTO'},
    'Ethereum': {'ticker': 'ETH-USD', 'type': 'CRYPTO'},
    'Binance Coin': {'ticker': 'BNB-USD', 'type': 'CRYPTO'},
    'Ripple': {'ticker': 'XRP-USD', 'type': 'CRYPTO'},
    'Solana': {'ticker': 'SOL-USD', 'type': 'CRYPTO'},
    'Dogecoin': {'ticker': 'DOGE-USD', 'type': 'CRYPTO'},
    'WTI Crude Oil': {'ticker': 'CL=F', 'type': 'COMMODITY'},
    'Brent Crude Oil': {'ticker': 'BZ=F', 'type': 'COMMODITY'},
    'Natural Gas': {'ticker': 'NG=F', 'type': 'COMMODITY'},
    'Gold': {'ticker': 'GC=F', 'type': 'COMMODITY'},
    'Silver': {'ticker': 'SI=F', 'type': 'COMMODITY'},
    'Copper': {'ticker': 'HG=F', 'type': 'COMMODITY'},
    'Uranium ETF (URA)': {'ticker': 'URA', 'type': 'COMMODITY'},
    'Wheat Futures': {'ticker': 'ZW=F', 'type': 'COMMODITY'},
    'VIX Index': {'ticker': '^VIX', 'type': 'INDEX'},
    'Dollar Index (DXY)': {'ticker': 'DX-Y.NYB', 'type': 'FX'},
    'KRW/USD': {'ticker': 'KRW=X', 'type': 'FX'},
    'CNY/USD': {'ticker': 'CNY=X', 'type': 'FX'},
    'EUR/USD': {'ticker': 'EURUSD=X', 'type': 'FX'},
    'TWD/USD': {'ticker': 'TWD=X', 'type': 'FX'},
    'JPY/USD': {'ticker': 'JPY=X', 'type': 'FX'},
    'US 2 Year Treasury Yield': {'ticker': '^IRX', 'type': 'INTEREST_RATE'},
    'US 10 Year Treasury Yield': {'ticker': '^TNX', 'type': 'INTEREST_RATE'},
    'US 30 Year Treasury Yield': {'ticker': '^TYX', 'type': 'INTEREST_RATE'}
}

# === 유틸리티 함수 ===
def setup_csv():
    """CSV 파일 초기 설정"""
    if not os.path.exists(CSV_FILE):
        # utf-8-sig를 써야 엑셀에서 한글이 안 깨집니다.
        with open(CSV_FILE, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(['날짜', '제품명', '가격', '데이터 타입'])
        print(f"✅ CSV 파일 생성 완료: {CSV_FILE}")
    else:
        print(f"✅ 기존 CSV 파일 사용: {CSV_FILE}")

def setup_driver(headless=True):
    """Selenium 웹드라이버 설정"""
    chrome_options = Options()
    if headless:
        chrome_options.add_argument('--headless')
    
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def save_to_csv(data):
    """CSV에 데이터 추가 저장"""
    try:
        # a 모드(append)로 열어서 뒤에 이어 붙이기
        with open(CSV_FILE, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            for row in data:
                writer.writerow(row) # 튜플(날짜, 이름, 가격, 타입) 저장
        return True
    except PermissionError:
        print(f"\n❌ 파일 저장 실패! '{CSV_FILE}' 파일이 열려있습니다.")
        return False
    except Exception as e:
        print(f"\n❌ 저장 중 오류: {str(e)}")
        return False

def get_last_scfi_date():
    """CSV에서 마지막으로 저장된 SCFI 날짜 가져오기"""
    try:
        if not os.path.exists(CSV_FILE):
            return None
            
        last_date = None
        with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            next(reader, None) # 헤더 건너뛰기
            for row in reader:
                if len(row) >= 4 and row[3] == 'OCEAN_FREIGHT' and 'SCFI' in row[1]:
                    last_date = row[0]
        return last_date
    except Exception as e:
        print(f"⚠️  마지막 SCFI 날짜 확인 중 오류: {str(e)}")
        return None

def save_debug_html(page_source, filename):
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(page_source)
    except:
        pass

# === 크롤링 함수 ===
def crawl_dram_nand(data_type, debug_mode=False):
    """DRAM 및 NAND 가격 크롤링"""
    print(f"\n{'=' * 60}")
    print(f"📊 {data_type} 가격 크롤링 시작")
    print(f"{'=' * 60}")

    driver = None
    success = False

    try:
        driver = setup_driver(headless=not debug_mode)
        url = f'https://www.dramexchange.com/#{data_type.lower()}'
        print(f"🌐 웹사이트 접속 중: {url}")
        driver.get(url)

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, 'table'))
        )

        now = datetime.now()
        current_date = now.strftime('%Y-%m-%d')
        
        collected_data = []
        target_items = TARGET_DRAM_ITEMS if data_type == 'DRAM' else TARGET_NAND_ITEMS
        tables = driver.find_elements(By.TAG_NAME, 'table')

        for table in tables:
            rows = table.find_elements(By.TAG_NAME, 'tr')
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, 'td')
                if not cells: cells = row.find_elements(By.TAG_NAME, 'th')
                if len(cells) < 2: continue

                item_name = cells[0].text.strip()
                if item_name in target_items:
                    try:
                        daily_high = cells[1].text.strip()
                        if daily_high and daily_high.replace('.', '').replace(',', '').isdigit():
                            price_numeric = float(daily_high.replace(',', ''))
                            collected_data.append((current_date, item_name, price_numeric, data_type))
                            print(f"✓ {item_name}: ${daily_high}")
                    except:
                        pass

        if collected_data:
            if save_to_csv(collected_data):
                print(f"\n✅ {data_type}: {len(collected_data)}개 항목 저장 완료!")
                success = True
        else:
            print(f"\n⚠️  {data_type} 데이터 없음")

    except Exception as e:
        print(f"\n❌ {data_type} 오류: {str(e)}")
        if debug_mode:
            save_debug_html(driver.page_source, f"{data_type.lower()}_debug.html")
    finally:
        if driver: driver.quit()

    return success

def crawl_scfi_index(debug_mode=False):
    """SCFI 지수 크롤링"""
    print(f"\n{'=' * 60}")
    print(f"🚢 SCFI 지수 크롤링 시작")
    print(f"{'=' * 60}")

    driver = None
    success = False

    try:
        driver = setup_driver(headless=not debug_mode)
        url = 'https://en.sse.net.cn/indices/scfinew.jsp'
        driver.get(url)
        
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'currdate')))

        scfi_value = None
        scfi_date = None

        try:
            scfi_date = driver.find_element(By.ID, 'currdate').text.strip()
            tables = driver.find_elements(By.TAG_NAME, 'table')
            for table in tables:
                if 'Comprehensive Index' in table.text:
                    rows = table.find_elements(By.TAG_NAME, 'tr')
                    for row in rows:
                        if 'Comprehensive Index' in row.text:
                            idx4 = row.find_elements(By.CSS_SELECTOR, 'span.idx4')
                            if idx4:
                                scfi_value = idx4[0].text.strip()
                                break
                    if scfi_value: break
        except:
            pass

        if scfi_value and scfi_date:
            last_saved_date = get_last_scfi_date()
            if last_saved_date == scfi_date:
                print(f"💡 SCFI 최신 상태임 ({scfi_date})")
                success = True
            else:
                collected_data = [(scfi_date, 'SCFI Comprehensive Index', float(scfi_value), 'OCEAN_FREIGHT')]
                if save_to_csv(collected_data):
                    print(f"✅ SCFI 저장 완료: {scfi_value}")
                    success = True
        else:
            print("⚠️  SCFI 데이터 못 찾음")

    except Exception as e:
        print(f"❌ SCFI 오류: {str(e)}")
    finally:
        if driver: driver.quit()

    return success

def crawl_yfinance_data(debug_mode=False):
    """yfinance 데이터 크롤링"""
    print(f"\n{'=' * 60}")
    print(f"📈 yfinance 크롤링 시작")
    print(f"{'=' * 60}")

    current_date = datetime.now().strftime('%Y-%m-%d')
    collected_data = []
    
    try:
        for name, info in YFINANCE_TICKERS.items():
            try:
                ticker = yf.Ticker(info['ticker'])
                hist = ticker.history(period='1d')
                if not hist.empty:
                    close_price = float(hist['Close'].iloc[0])
                    d_date = hist.index[0].strftime('%Y-%m-%d') if info['type'] != 'CRYPTO' else current_date
                    collected_data.append((d_date, name, close_price, info['type']))
                    print(f"✓ {name}: {close_price:.2f}")
            except:
                print(f"⚠️  {name} 실패")

        if collected_data:
            if save_to_csv(collected_data):
                print(f"✅ {len(collected_data)}개 저장 완료")
                return True
    except:
        return False
    return False

def main():
    print("🚀 크롤링 시작 (CSV 저장)")
    setup_csv()
    
    crawl_dram_nand('DRAM')
    crawl_dram_nand('NAND')
    crawl_scfi_index()
    crawl_yfinance_data()
    
    print(f"\n📁 결과 파일: {CSV_FILE}")

if __name__ == "__main__":
    main()
