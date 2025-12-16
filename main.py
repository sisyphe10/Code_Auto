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

# yfinance 티커 목록 (지수는 별도 함수로 뺐음)
# yfinance 티커 목록 (환율 추가됨)
# yfinance 티커 목록
YFINANCE_TICKERS = {
    # --- 암호화폐 ---
    'Bitcoin': {'ticker': 'BTC-USD', 'type': 'CRYPTO'},
    'Ethereum': {'ticker': 'ETH-USD', 'type': 'CRYPTO'},
    'Binance Coin': {'ticker': 'BNB-USD', 'type': 'CRYPTO'},

    # --- 원자재 ---
    'WTI Crude Oil': {'ticker': 'CL=F', 'type': 'COMMODITY'},
    'Brent Crude Oil': {'ticker': 'BZ=F', 'type': 'COMMODITY'},
    'Natural Gas': {'ticker': 'NG=F', 'type': 'COMMODITY'},
    'Gold': {'ticker': 'GC=F', 'type': 'COMMODITY'},
    'Silver': {'ticker': 'SI=F', 'type': 'COMMODITY'},
    'Copper': {'ticker': 'HG=F', 'type': 'COMMODITY'},
    'Uranium ETF (URA)': {'ticker': 'URA', 'type': 'COMMODITY'},

    # --- 지수 및 금리 ---
    'VIX Index': {'ticker': '^VIX', 'type': 'INDEX'},
    'US 10 Year Treasury Yield': {'ticker': '^TNX', 'type': 'INTEREST_RATE'},

    # --- 환율 (FX) ---
    # 이제 모든 환율이 "1달러당 얼마" 기준으로 통일됩니다.
    'Dollar Index (DXY)': {'ticker': 'DX-Y.NYB', 'type': 'FX'},
    'KRW/USD': {'ticker': 'KRW=X', 'type': 'FX'},  # 원/달러 (예: 1450)
    'JPY/USD': {'ticker': 'JPY=X', 'type': 'FX'},  # 엔/달러 (예: 150)
    'CNY/USD': {'ticker': 'CNY=X', 'type': 'FX'},  # 위안/달러 (예: 7.2)
    'TWD/USD': {'ticker': 'TWD=X', 'type': 'FX'},  # 대만달러/달러 (예: 32.5)

    # [변경] EUR=X를 쓰면 '1달러당 유로'가 나옵니다.
    'EUR/USD': {'ticker': 'EUR=X', 'type': 'FX'},  # 유로/달러 (예: 0.96)
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
    """중복 방지 기능이 강화된 CSV 저장 (배치 내 중복까지 제거)"""
    try:
        # 1. 파일에 이미 저장된 데이터 키 로드
        existing_keys = set()
        if os.path.exists(CSV_FILE):
            with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        key = (row[0], row[1])
                        existing_keys.add(key)

        new_data = []
        # [핵심 수정] 이번에 저장할 데이터끼리의 중복도 방지하기 위한 세트
        current_batch_keys = set()

        for row in data:
            current_key = (row[0], row[1])

            # 1) 파일에 없고  AND  2) 지금 저장하려는 리스트에도 없을 때만 추가
            if current_key not in existing_keys and current_key not in current_batch_keys:
                new_data.append(row)
                current_batch_keys.add(current_key)  # 방금 추가했음을 기록

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


# ==========================================
# 1. [KRX] 한국 지수/시총/PER/PBR
# ==========================================
def crawl_krx_indices():
    """
    KOSPI, KOSDAQ, KOSPI200의 지수, 시가총액, PER, PBR, [추가] 순수 종목수 수집
    """
    print(f"\n{'=' * 60}")
    print(f"🇰🇷 KRX 종합 데이터(종목수 포함) 크롤링 시작")
    print(f"{'=' * 60}")

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

    targets = {"KOSPI": "KOSPI", "KOSDAQ": "KOSDAQ", "KOSPI 200": "KOSPI"}
    # 주의: KOSPI 200은 지수지만 종목수는 KOSPI 시장 전체를 세는게 맞는지,
    # 혹은 구성종목(200개)을 세는게 맞는지 애매하지만, 보통 시장 전체(KOSPI/KOSDAQ) 종목수를 봅니다.
    # 여기서는 KOSPI, KOSDAQ 두 시장의 종목수만 집계하겠습니다.

    collected_data = []
    start_lookup = (datetime.strptime(valid_date_str, "%Y%m%d") - timedelta(days=5)).strftime("%Y%m%d")

    # 1. [신규] 순수 상장 종목수 카운트 (KOSPI, KOSDAQ)
    market_map = {"KOSPI": "KOSPI", "KOSDAQ": "KOSDAQ"}

    for market_name, market_code in market_map.items():
        try:
            # 해당 시장의 전체 티커 가져오기 (ETF, ETN은 기본 제외됨)
            tickers = stock.get_market_ticker_list(valid_date_str, market=market_code)

            real_stock_count = 0
            for ticker in tickers:
                # 필터링 로직
                # 1. 티커 끝자리 확인: 보통주는 '0'으로 끝남 (우선주 등 제외)
                if ticker[-1] != '0':
                    continue

                # 2. 이름으로 스팩/리츠 거르기 (확실히 하기 위해 이름 조회)
                name = stock.get_market_ticker_name(ticker)
                if '스팩' in name or '리츠' in name:
                    continue

                real_stock_count += 1

            collected_data.append((default_date, f"{market_name} 상장종목수", real_stock_count, 'INDEX_KR'))
            print(f"✓ {market_name} 순수 종목수: {real_stock_count}개")

        except Exception as e:
            print(f"❌ {market_name} 종목수 집계 실패: {e}")

    # 2. 기존 지수/시총/PER/PBR 로직
    # (티커 매핑용)
    index_targets = {"KOSPI": "1001", "KOSDAQ": "2001", "KOSPI 200": "1028"}

    for name, ticker in index_targets.items():
        try:
            # A. 지수/시총
            df_price = stock.get_index_ohlcv_by_date(valid_date_str, valid_date_str, ticker)
            if not df_price.empty:
                price = float(df_price['종가'].iloc[0])
                collected_data.append((default_date, name, price, 'INDEX_KR'))
                print(f"✓ {name}: {price:,.2f}")

                if '상장시가총액' in df_price.columns:
                    market_cap = float(df_price['상장시가총액'].iloc[0])
                    collected_data.append((default_date, f"{name} 시가총액", market_cap, 'INDEX_KR'))

            # B. PER/PBR
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

        except Exception as e:
            print(f"❌ {name} 지수 데이터 오류: {e}")

    if collected_data:
        save_to_csv(collected_data)
        return True
    return False


# ==========================================
# 2. [US] 미국 지수/PER/PBR (ETF 대용)
# ==========================================
def crawl_us_indices():
    """미국 지수는 Index로 가격을, 대형 ETF로 펀더멘탈(PER/PBR)을 수집합니다."""
    print(f"\n{'=' * 60}")
    print(f"🇺🇸 미국 지수/PER/PBR 크롤링 시작 (yfinance)")
    print(f"{'=' * 60}")

    current_date = datetime.now().strftime('%Y-%m-%d')

    # 지수 티커(가격용) / ETF 티커(펀더멘탈용) 매핑
    targets = {
        "S&P 500": {"idx": "^GSPC", "etf": "SPY"},
        "NASDAQ": {"idx": "^IXIC", "etf": "QQQ"},  # NASDAQ 100 기준
        "RUSSELL 2000": {"idx": "^RUT", "etf": "IWM"}
    }

    collected_data = []

    for name, tickers in targets.items():
        try:
            # 1. 지수 가격 (Index Ticker)
            idx_ticker = yf.Ticker(tickers['idx'])
            hist = idx_ticker.history(period="1d")

            if not hist.empty:
                price = float(hist['Close'].iloc[0])
                d_date = hist.index[0].strftime('%Y-%m-%d')  # 실제 장 마감일
                collected_data.append((d_date, name, price, 'INDEX_US'))
                print(f"✓ {name}: {price:,.2f}")

                # 2. 펀더멘탈 (ETF Ticker)
                # 지수 자체는 PER/PBR 데이터가 없는 경우가 많아 ETF를 Proxy로 사용
                etf_ticker = yf.Ticker(tickers['etf'])
                info = etf_ticker.info

                # PER
                if 'trailingPE' in info and info['trailingPE']:
                    pe = info['trailingPE']
                    collected_data.append((d_date, f"{name} PER", pe, 'INDEX_US'))
                    print(f"  -> PER: {pe:.2f}")

                # PBR
                if 'priceToBook' in info and info['priceToBook']:
                    pbr = info['priceToBook']
                    collected_data.append((d_date, f"{name} PBR", pbr, 'INDEX_US'))
                    print(f"  -> PBR: {pbr:.2f}")

                # 시가총액 (주의: ETF 시총이 아니라 전체 지수 시총은 무료 API로 얻기 매우 힘듭니다)
                # yfinance Index ticker의 info에 marketCap이 있는 경우만 수집
                # (보통 S&P500 같은 지수는 marketCap이 None으로 나옵니다)
                if 'marketCap' in idx_ticker.info and idx_ticker.info['marketCap']:
                    mkt_cap = idx_ticker.info['marketCap']
                    collected_data.append((d_date, f"{name} 시가총액", mkt_cap, 'INDEX_US'))

        except Exception as e:
            print(f"❌ {name} 오류: {e}")

    if collected_data:
        save_to_csv(collected_data)


def crawl_dram_nand(data_type):
    """DRAM 및 NAND 가격 크롤링 (중복 수집 방지 적용)"""
    print(f"\n📊 {data_type} 크롤링 시작")
    driver = None
    try:
        driver = setup_driver()
        driver.get(f'https://www.dramexchange.com/#{data_type.lower()}')
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'table')))

        current_date = datetime.now().strftime('%Y-%m-%d')
        collected_data = []
        target_items = TARGET_DRAM_ITEMS if data_type == 'DRAM' else TARGET_NAND_ITEMS

        # [핵심 수정] 이미 찾은 제품명을 기억하는 세트
        found_items = set()

        tables = driver.find_elements(By.TAG_NAME, 'table')
        for table in tables:
            rows = table.find_elements(By.TAG_NAME, 'tr')
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, 'td')
                if not cells: cells = row.find_elements(By.TAG_NAME, 'th')
                if len(cells) < 2: continue

                item_name = cells[0].text.strip()

                # 타겟 제품이면서 + 아직 수집하지 않은 제품인 경우에만!
                if item_name in target_items and item_name not in found_items:
                    try:
                        price = cells[1].text.strip()
                        if price and price.replace('.', '').replace(',', '').isdigit():
                            val = float(price.replace(',', ''))
                            collected_data.append((current_date, item_name, val, data_type))
                            found_items.add(item_name)  # "나 이거 찾았음" 기록
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

    # 한국 지수
    crawl_krx_indices()
    # 미국 지수
    crawl_us_indices()

    print(f"\n📁 결과 파일: {CSV_FILE}")


if __name__ == "__main__":
    main()
