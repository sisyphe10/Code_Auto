import time
import schedule
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import os
import csv
import yfinance as yf
import warnings
from pykrx import stock

# 경고 메시지 무시
warnings.simplefilter(action='ignore', category=FutureWarning)

# === 상수 정의 ===
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

# yfinance 티커 목록
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
    chrome_options.add_argument('--user-agent=Mozilla/5.0')

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver


def save_to_csv(data):
    """중복 방지 기능이 추가된 CSV 저장"""
    try:
        # 1. 기존 데이터 읽어서 중복 키(날짜, 제품명) 저장
        existing_keys = set()
        if os.path.exists(CSV_FILE):
            with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        # (날짜, 제품명) 조합이 같으면 중복으로 간주
                        key = (row[0], row[1])
                        existing_keys.add(key)

        # 2. 중복되지 않은 새 데이터만 선별
        new_data = []
        for row in data:
            current_key = (row[0], row[1])
            if current_key not in existing_keys:
                new_data.append(row)

        # 3. 저장
        if new_data:
            with open(CSV_FILE, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                for row in new_data:
                    writer.writerow(row)
            print(f"✅ {len(new_data)}건 저장 완료 (중복 제외됨)")
            return True
        else:
            print("💡 새로운 데이터가 없습니다. (모두 중복)")
            return True

    except Exception as e:
        print(f"\n❌ 저장 중 오류: {str(e)}")
        return False


def get_last_scfi_date():
    try:
        if not os.path.exists(CSV_FILE): return None
        last_date = None
        with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) >= 4 and row[3] == 'OCEAN_FREIGHT' and 'SCFI' in row[1]:
                    last_date = row[0]
        return last_date
    except:
        return None


# === [KRX] 지수 및 펀더멘탈 크롤링 ===
def crawl_krx_indices():
    """
    KOSPI, KOSDAQ, KOSPI200의 지수, PER, PBR, [추가] 시가총액 수집
    """
    print(f"\n{'=' * 60}")
    print(f"🇰🇷 KRX 데이터(지수/시총/PER/PBR) 크롤링 시작")
    print(f"{'=' * 60}")

    # 1. 기준일 설정
    target_date = datetime.now()
    valid_date_str = None

    for i in range(7):
        check_date = (target_date - timedelta(days=i)).strftime("%Y%m%d")
        try:
            test_df = stock.get_index_ohlcv_by_date(check_date, check_date, "1001")
            if not test_df.empty:
                valid_date_str = check_date
                break
        except:
            continue

    if not valid_date_str:
        print("❌ 유효한 KRX 데이터를 찾을 수 없습니다.")
        return False

    default_date = datetime.strptime(valid_date_str, "%Y%m%d").strftime("%Y-%m-%d")
    print(f"📅 조회 기준일: {default_date}")

    targets = {"KOSPI": "1001", "KOSDAQ": "2001", "KOSPI 200": "1028"}
    collected_data = []

    start_lookup = (datetime.strptime(valid_date_str, "%Y%m%d") - timedelta(days=5)).strftime("%Y%m%d")

    for name, ticker in targets.items():
        try:
            # --- A. 지수 및 시가총액 (OHLCV API) ---
            df_price = stock.get_index_ohlcv_by_date(valid_date_str, valid_date_str, ticker)
            if not df_price.empty:
                # 1. 지수 (종가)
                price = float(df_price['종가'].iloc[0])
                collected_data.append((default_date, name, price, 'INDEX_KR'))
                print(f"✓ {name}: {price:,.2f}")

                # 2. [추가됨] 상장시가총액 (백만원 단위 -> 원 단위 보정 or 그대로 저장)
                # pykrx의 상장시가총액은 보통 '원' 단위로 나옵니다.
                if '상장시가총액' in df_price.columns:
                    market_cap = float(df_price['상장시가총액'].iloc[0])
                    # 항목명: KOSPI 시가총액
                    collected_data.append((default_date, f"{name} 시가총액", market_cap, 'INDEX_KR'))
                    print(f"  -> 시가총액: {market_cap:,.0f}")

            # --- B. 펀더멘탈 (PER, PBR) ---
            df_fund = stock.get_index_fundamental_by_date(start_lookup, valid_date_str, ticker)
            if not df_fund.empty:
                if 'PER' in df_fund.columns:
                    valid = df_fund[df_fund['PER'] > 0]
                    if not valid.empty:
                        val = float(valid.iloc[-1]['PER'])
                        r_date = valid.iloc[-1].name.strftime("%Y-%m-%d")
                        collected_data.append((r_date, f"{name} PER", val, 'INDEX_KR'))
                        print(f"  -> PER: {val} ({r_date})")

                if 'PBR' in df_fund.columns:
                    valid = df_fund[df_fund['PBR'] > 0]
                    if not valid.empty:
                        val = float(valid.iloc[-1]['PBR'])
                        r_date = valid.iloc[-1].name.strftime("%Y-%m-%d")
                        collected_data.append((r_date, f"{name} PBR", val, 'INDEX_KR'))
                        print(f"  -> PBR: {val} ({r_date})")

        except Exception as e:
            print(f"❌ {name} 오류: {e}")

    if collected_data:
        save_to_csv(collected_data)
        return True
    return False


# === 기존 크롤링 함수들 ===
def crawl_dram_nand(data_type):
    print(f"\n📊 {data_type} 크롤링 시작")
    driver = None
    try:
        driver = setup_driver()
        driver.get(f'https://www.dramexchange.com/#{data_type.lower()}')
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'table')))

        current_date = datetime.now().strftime('%Y-%m-%d')
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
                        price = cells[1].text.strip()
                        if price and price.replace('.', '').replace(',', '').isdigit():
                            val = float(price.replace(',', ''))
                            collected_data.append((current_date, item_name, val, data_type))
                            print(f"✓ {item_name}: ${price}")
                    except:
                        pass

        if collected_data:
            save_to_csv(collected_data)
        else:
            print(f"⚠️ {data_type} 데이터 없음")

    except Exception as e:
        print(f"❌ {data_type} 오류: {e}")
    finally:
        if driver: driver.quit()


def crawl_scfi_index():
    print(f"\n🚢 SCFI 크롤링 시작")
    driver = None
    try:
        driver = setup_driver()
        driver.get('https://en.sse.net.cn/indices/scfinew.jsp')
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'currdate')))

        scfi_date = driver.find_element(By.ID, 'currdate').text.strip()
        scfi_value = None

        tables = driver.find_elements(By.TAG_NAME, 'table')
        for table in tables:
            if 'Comprehensive Index' in table.text:
                rows = table.find_elements(By.TAG_NAME, 'tr')
                for row in rows:
                    if 'Comprehensive Index' in row.text:
                        idx4 = row.find_elements(By.CSS_SELECTOR, 'span.idx4')
                        if idx4: scfi_value = idx4[0].text.strip()

        if scfi_value and scfi_date:
            if get_last_scfi_date() == scfi_date:
                print(f"💡 SCFI 최신 상태 ({scfi_date})")
            else:
                save_to_csv([(scfi_date, 'SCFI Comprehensive Index', float(scfi_value), 'OCEAN_FREIGHT')])
                print(f"✅ SCFI 저장: {scfi_value}")
    except Exception as e:
        print(f"❌ SCFI 오류: {e}")
    finally:
        if driver: driver.quit()


def crawl_yfinance_data():
    print(f"\n📈 yfinance 크롤링 시작")
    current_date = datetime.now().strftime('%Y-%m-%d')
    collected_data = []
    for name, info in YFINANCE_TICKERS.items():
        try:
            t = yf.Ticker(info['ticker'])
            h = t.history(period='1d')
            if not h.empty:
                price = float(h['Close'].iloc[0])
                d = h.index[0].strftime('%Y-%m-%d') if info['type'] != 'CRYPTO' else current_date
                collected_data.append((d, name, price, info['type']))
                print(f"✓ {name}: {price:.2f}")
        except:
            print(f"⚠️ {name} 실패")

    if collected_data: save_to_csv(collected_data)


def main():
    print("🚀 전체 크롤링 시작")
    setup_csv()

    crawl_dram_nand('DRAM')
    crawl_dram_nand('NAND')
    crawl_scfi_index()
    crawl_yfinance_data()
    crawl_krx_indices()  # KRX 추가

    print(f"\n📁 결과 파일: {CSV_FILE}")


if __name__ == "__main__":
    main()
