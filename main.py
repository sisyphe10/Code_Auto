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
import FinanceDataReader as fdr  # [교체] pykrx 제거 및 fdr 추가
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
# 1. [KRX] 한국 지수/시총/종목수 (fdr로 교체)
# ==========================================
def crawl_krx_indices():
    """
    FinanceDataReader를 사용하여 KOSPI, KOSDAQ의 지수, 시가총액, 종목수를 수집.
    (주의: fdr은 지수별 PER/PBR 데이터를 제공하지 않아 해당 부분은 삭제됨)
    """
    print(f"\n{'=' * 60}")
    print(f"🇰🇷 KRX 종합 데이터(fdr) 크롤링 시작")
    print(f"{'=' * 60}")

    collected_data = []
    # fdr은 별도 날짜 지정 없이 호출 시 최신 데이터 스냅샷을 가져옵니다.
    today_str = datetime.now().strftime("%Y-%m-%d")

    try:
        # --- 1. 종목수 및 시가총액 계산 ---
        # KRX 전체 상장 종목 리스트 가져오기
        df_master = fdr.StockListing('KRX')
        
        # 컬럼명 유연화 (MarCap, MarketCap 등 버전별 차이 대응)
        col_map = {'MarketCap': 'Marcap', 'MarCap': 'Marcap', 'Name': 'Name', 'Code': 'Code', 'Market': 'Market'}
        df_master = df_master.rename(columns={k: v for k, v in col_map.items() if k in df_master.columns})

        target_markets = ['KOSPI', 'KOSDAQ']
        
        for market in target_markets:
            try:
                # 해당 시장만 필터링
                mkt_df = df_master[df_master['Market'] == market]
                
                # A. 순수 상장 종목수 필터링 (기존 로직 유지: 스팩/리츠 제외, 보통주만)
                # 보통주: 종목코드가 '0'으로 끝남
                real_stocks = mkt_df[
                    (mkt_df['Code'].str.endswith('0')) & 
                    (~mkt_df['Name'].str.contains('스팩')) & 
                    (~mkt_df['Name'].str.contains('리츠'))
                ]
                count = len(real_stocks)
                collected_data.append((today_str, f"{market} 상장종목수", count, 'INDEX_KR'))
                print(f"✓ {market} 순수 종목수: {count}개")

                # B. 시가총액 합계 (단위: 원)
                if 'Marcap' in mkt_df.columns:
                    total_cap = mkt_df['Marcap'].sum()
                    collected_data.append((today_str, f"{market} 시가총액", float(total_cap), 'INDEX_KR'))
                    print(f"✓ {market} 시가총액 합계 집계 완료")

            except Exception as e:
                print(f"⚠️ {market} 분석 실패: {e}")

        # --- 2. 지수 가격 (Index Price) ---
        # 심볼 매핑: KOSPI -> KS11, KOSDAQ -> KQ11, KOSPI 200 -> KS200
        index_map = {
            'KOSPI': 'KS11',
            'KOSDAQ': 'KQ11',
            'KOSPI 200': 'KS200'
        }

        for name, symbol in index_map.items():
            try:
                # 오늘 기준 최근 데이터 조회 (주말/휴일 고려하여 최근 5일치 중 마지막 값)
                start_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
                df_idx = fdr.DataReader(symbol, start_date)
                
                if not df_idx.empty:
                    last_row = df_idx.iloc[-1]
                    price = float(last_row['Close'])
                    # 실제 데이터 날짜
                    date_val = last_row.name.strftime("%Y-%m-%d")
                    
                    collected_data.append((date_val, name, price, 'INDEX_KR'))
                    print(f"✓ {name} 지수: {price:,.2f} ({date_val})")
            except Exception as e:
                print(f"⚠️ {name} 지수 조회 실패: {e}")

    except Exception as e:
        print(f"❌ KRX 데이터 수집 전체 실패: {e}")

    if collected_data:
        save_to_csv(collected_data)


# ==========================================
# 2. [US] 미국 지수/PER/PBR (ETF 대용)
# ==========================================
def crawl_us_indices():
    """미국 지수는 Index로 가격을, 대형 ETF로 펀더멘탈(PER/PBR)을 수집합니다."""
    print(f"\n{'=' * 60}")
    print(f"🇺🇸 미국 지수/PER/PBR 크롤링 시작 (yfinance)")
    print(f"{'=' * 60}")

    collected_data = []

    targets = {
        "S&P 500": {"idx": "^GSPC", "etf": "SPY"},
        "NASDAQ": {"idx": "^IXIC", "etf": "QQQ"},
        "RUSSELL 2000": {"idx": "^RUT", "etf": "IWM"}
    }

    for name, tickers in
