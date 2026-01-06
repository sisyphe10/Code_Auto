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
import FinanceDataReader as fdr  # [변경] pykrx 대신 fdr 사용
import pandas as pd

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
    'Dollar Index (DXY)': {'ticker': 'DX-Y.NYB', 'type': 'FX'},
    'KRW/USD': {'ticker': 'KRW=X', 'type': 'FX'},
    'JPY/USD': {'ticker': 'JPY=X', 'type': 'FX'},
    'CNY/USD': {'ticker': 'CNY=X', 'type': 'FX'},
    'TWD/USD': {'ticker': 'TWD=X', 'type': 'FX'},
    'EUR/USD': {'ticker': 'EUR=X', 'type': 'FX'},
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
        current_batch_keys = set()

        for row in data:
            current_key = (row[0], row[1])
            if current_key not in existing_keys and current_key not in current_batch_keys:
                new_data.append(row)
                current_batch_keys.add(current_key)

        if new_data:
            with open(CSV_FILE, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerows(new_data)
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
# 1. [KRX] 한국 지수/시총/종목수 (FinanceDataReader 사용)
# ==========================================
def crawl_krx_indices():
    """FinanceDataReader를 사용하여 KOSPI, KOSDAQ의 지수, 시가총액, 종목수를 수집"""
    print(f"\n{'=' * 60}")
    print(f"🇰🇷 KRX 종합 데이터(fdr) 크롤링 시작")
    print(f"{'=' * 60}")

    collected_data = []
    today_str = datetime.now().strftime("%Y-%m-%d")

    try:
        # 1. 전종목 리스트 가져오기
        df_master = fdr.StockListing('KRX')
        
        # 컬럼명 표준화
        col_map = {'MarketCap': 'Marcap', 'MarCap': 'Marcap', 'Name': 'Name', 'Code': 'Code', 'Market': 'Market'}
        df_master = df_master.rename(columns={k: v for k, v in col_map.items() if k in df_master.columns})

        # 2. 시장별 분석
        target_markets = ['KOSPI', 'KOSDAQ']
        
        for market in target_markets:
            try:
                mkt_df = df_master[df_master['Market'] == market]
                
                # A. 순수 상장 종목수
                real_stocks = mkt_df[
                    (mkt_df['Code'].str.endswith('0')) & 
                    (~mkt_df['Name'].str.contains('스팩')) & 
                    (~mkt_df['Name'].str.contains('리츠'))
                ]
                count = len(real_stocks)
                collected_data.append((today_str, f"{market} 상장종목수", count, 'INDEX_KR'))
                print(f"✓ {market} 순수 종목수: {count}개")

                # B. 시가총액 합계
                if 'Marcap' in mkt_df.columns:
                    total_cap = mkt_df['Marcap'].sum()
                    collected_data.append((today_str, f"{market} 시가총액", float(total_cap), 'INDEX_KR'))
                    print(f"✓ {market} 시가총액 합계 집계 완료")

            except Exception as e:
                print(f"⚠️ {market} 종목 분석 실패: {e}")

        # 3. 지수 가격
        index_map = {'KOSPI': 'KS11', 'KOSDAQ': 'KQ11', 'KOSPI 200': 'KS200'}

        for name, symbol in index_map.items():
            try:
                df_idx = fdr.DataReader(symbol, today_str, today_str)
                if df_idx.empty:
                    prev_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
                    df_idx = fdr.DataReader(symbol, prev_date)
                
                if not df_idx.empty:
                    last_row = df_idx.iloc[-1]
                    price = float(last_row['Close'])
                    date_val = last_row.name.strftime("%Y-%m-%d")
                    collected_data.append((date_val, name, price, 'INDEX_KR'))
                    print(f"✓ {name} 지수: {price:,.2f}")
            except Exception as e:
                print(f"⚠️ {name} 지수 조회 실패: {e}")

    except Exception as e:
        print(f"❌ KRX 데이터 수집 전체 실패: {e}")

    if collected_data:
        save_to_csv(collected_data)


# ==========================================
# 2. [US] 미국 지수/PER/PBR (yfinance)
# ==========================================
def crawl_us_indices():
    """미국 지수 및 PER/PBR 수집"""
    print(f"\n{'=' * 60}")
    print(f"🇺🇸 미국 지수/PER/PBR 크롤링 시작 (yfinance)")
    print(f"{'=' * 60}")

    collected_data = []

    targets = {
        "S&P 500": {"idx": "^GSPC", "etf": "SPY"},
        "NASDAQ": {"idx": "^IXIC", "etf": "QQQ"},
        "RUSSELL 2000": {"idx": "^RUT", "etf": "IWM"}
    }

    for name, tickers in targets.items():
        try:
            # 1. 지수 가격
            idx_ticker = yf.Ticker(tickers['idx'])
            hist = idx_ticker.history(period="1d")

            if not hist.empty:
                price = float(hist['Close'].iloc[0])
                d_date = hist.index[0].strftime('%Y-%m-%d')
                collected_data.append((d_date, name, price, 'INDEX_US'))
                print(f"✓ {name}: {price:,.2f}")

                # 2. 펀더멘탈 (ETF 사용)
                etf_ticker = yf.Ticker(tickers['etf'])
                info = etf_ticker.info

                if 'trailingPE' in info and info['trailingPE']:
                    pe = info['trailingPE']
                    collected_data.append((d_date, f"{name} PER", pe, 'INDEX_US'))

                if 'priceToBook' in info and info['priceToBook']:
                    pbr = info['priceToBook']
                    collected_data.append((d_date, f"{name} PBR", pbr, 'INDEX_US'))

        except Exception as e:
            print(f"❌ {name} 오류: {e}")

    if collected_data:
        save_to_csv(collected_data)


# ==========================================
# 3. [DRAM/NAND] 반도체 가격
# ==========================================
def crawl_dram_nand(data_type):
    """DRAM 및 NAND 가격 크롤링"""
    print(f"\n📊 {data_type} 크롤링 시작")
    driver = None
    try:
        driver = setup_driver()
        driver.get(f'https://www.dramexchange.com/#{data_type.lower()}')
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'table')))

        current_date = datetime.now().strftime('%Y-%m-%d')
        collected_data = []
        target_items = TARGET_DRAM_ITEMS if data_type == 'DRAM' else TARGET_NAND_ITEMS
        found_items = set()

        tables = driver.find_elements(By.TAG_NAME, 'table')
        for table in tables:
            rows = table.find_elements(By.TAG_NAME, 'tr')
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, 'td')
                if not cells: cells = row.find_elements(By.TAG_NAME, 'th')
                if len(cells) < 2: continue

                item_name = cells[0].text.strip()
                if item_name in target_items and item_name not in found_items:
                    try:
                        price = cells[1].text.strip()
                        if price and price.replace('.', '').replace(',', '').isdigit():
                            val = float(price.replace(',', ''))
                            collected_data.append((current_date, item_name, val, data_type))
                            found_items.add(item_name)
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


# ==========================================
# 4. [SCFI] 해상운임지수
# ==========================================
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


# ==========================================
# 5. [yfinance] 기타 자산
# ==========================================
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


# ==========================================
# Main Execution
# ==========================================
def main():
    print("🚀 전체 크롤링 시작")
    setup_csv()

    # 순차적 실행
    crawl_dram_nand('DRAM')
    crawl_dram_nand('NAND')
    crawl_scfi_index()
    crawl_yfinance_data()
    crawl_krx_indices()  # FinanceDataReader 버전
    crawl_us_indices()

    print(f"\n📁 결과 파일: {CSV_FILE}")


if __name__ == "__main__":
    main()
