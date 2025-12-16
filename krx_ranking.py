# 파일명: krx_ranking.py
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import csv
import os
import time

# 저장할 파일명
CSV_FILE = 'krx_ranking.csv'

def setup_csv():
    """CSV 파일이 없으면 헤더 생성"""
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            # CSV 헤더 정의
            writer.writerow(['날짜', '투자자', '순위', '종목명', '시가총액', '순매수수량', '순매수대금', '비율(%)'])

def save_to_csv(data_list):
    """중복 방지 기능이 추가된 CSV 저장"""
    try:
        existing_keys = set()
        if os.path.exists(CSV_FILE):
            with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                next(reader, None) # 헤더 건너뛰기
                for row in reader:
                    if len(row) >= 4:
                        # 중복 체크 키: (날짜, 투자자/카테고리, 종목명)
                        key = (row[0], row[1], row[3])
                        existing_keys.add(key)

        new_records = []
        for row in data_list:
            current_key = (row[0], row[1], row[3])
            if current_key not in existing_keys:
                new_records.append(row)

        if new_records:
            with open(CSV_FILE, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                for row in new_records:
                    writer.writerow(row)
            print(f"✅ 데이터 {len(new_records)}건 저장 완료 (중복 제외)")
        else:
            print("💡 이미 모든 데이터가 저장되어 있습니다.")

    except Exception as e:
        print(f"❌ 저장 실패: {e}")

def find_latest_valid_data_date():
    """가장 최근 영업일 찾기 (주말/휴일 고려)"""
    target_date = datetime.now()
    for i in range(7):
        check_date = (target_date - timedelta(days=i)).strftime("%Y%m%d")
        try:
            # KOSPI 지수 데이터가 있으면 영업일로 간주
            df = stock.get_market_ohlcv(check_date, market="KOSPI")
            if not df.empty:
                return check_date
        except:
            continue
    return None

# ==========================================
# 1. 투자자별 순매수 상위 (개인, 기관 등)
# ==========================================
def get_top20_by_investor(target_date, investor_name):
    cap_df = stock.get_market_cap(target_date)
    inv_code = "기관합계" if investor_name == "기관" else investor_name
    
    try:
        df = stock.get_market_net_purchases_of_equities_by_ticker(target_date, target_date, "ALL", inv_code)
    except:
        return pd.DataFrame()

    # 순매수 관련 컬럼 찾기 (거래대금 or 거래금액)
    col_candidates = [c for c in df.columns if "순매수" in c and ("대금" in c or "금액" in c)]
    if not col_candidates: return pd.DataFrame()
    target_col = col_candidates[0]

    # 시가총액 데이터와 병합
    merged = df.join(cap_df[['시가총액']], how='inner')
    
    # 정렬 및 상위 20개 추출
    top20 = merged.sort_values(by=target_col, ascending=False).head(20).copy()
    
    # 비율 계산 (순매수대금 / 시가총액)
    top20['비율(%)'] = (top20[target_col] / top20['시가총액']) * 100
    qty_col = '순매수거래량' if '순매수거래량' in top20.columns else '순매수수량'

    # 결과 포맷팅
    result = pd.DataFrame()
    result['종목명'] = top20['종목명']
    result['시가총액'] = (top20['시가총액'] / 100000000).apply(lambda x: f"{x:,.0f}")
    result['순매수수량'] = top20[qty_col].apply(lambda x: f"{x:,}")
    result['순매수대금'] = (top20[target_col] / 100000000).apply(lambda x: f"{x:,.0f}")
    result['비율(%)'] = top20['비율(%)'].apply(lambda x: f"{x:.2f}%")

    return result.reset_index(drop=True)

# ==========================================
# 2. 시장 데이터 상위 (시가총액, 등락률, 거래대금)
# ==========================================
def get_market_ranking(target_date, rank_type, market="ALL"):
    """
    rank_type: 'CAP'(시총), 'CHANGE'(등락률), 'AMOUNT'(거래대금)
    """
    df = stock.get_market_ohlcv(target_date, market=market)
    cap = stock.get_market_cap(target_date, market=market)
    
    # [중요] 컬럼 이름 충돌 방지 (OHLCV에도 시가총액이 있을 수 있음)
    if '시가총액' in df.columns:
        df = df.drop(['시가총액'], axis=1)
        
    # 시가총액 정보 병합
    df = df.join(cap[['시가총액']], how='inner')
    
    # 정렬 기준 설정
    if rank_type == 'CAP':
        df = df.sort_values(by='시가총액', ascending=False)
    elif rank_type == 'CHANGE':
        df = df.sort_values(by='등락률', ascending=False)
    elif rank_type == 'AMOUNT':
        df = df.sort_values(by='거래대금', ascending=False)
        
    top20 = df.head(20).copy()
    
    # 종목명 가져오기
    top20['종목명'] = [stock.get_market_ticker_name(ticker) for ticker in top20.index]

    # 결과 포맷팅
    result = pd.DataFrame()
    result['종목명'] = top20['종목명']
    result['시가총액'] = (top20['시가총액'] / 100000000).apply(lambda x: f"{x:,.0f}")
    result['순매수수량'] = top20['거래량'].apply(lambda x: f"{x:,}") # 여기선 거래량
    result['순매수대금'] = (top20['거래대금'] / 100000000).apply(lambda x: f"{x:,.0f}") # 여기선 거래대금
    result['비율(%)'] = top20['등락률'].apply(lambda x: f"{x:.2f}%") # 등락률

    return result.reset_index(drop=True)

def main():
    print("🚀 KRX 순매수 및 시장 데이터 크롤링 시작")
    setup_csv()
    
    # 최근 영업일 조회
    latest_date = find_latest_valid_data_date()

    if latest_date:
        # 날짜 포맷 변환 (20251216 -> 2025-12-16)
        display_date = datetime.strptime(latest_date, "%Y%m%d").strftime("%Y-%m-%d")
        print(f"### 분석 기준일: {display_date} ###")
        
        all_data = []

        # ------------------------------------------------
        # A. 투자자별 순매수 상위 (개인, 기관, 외국인, 연기금)
        # ------------------------------------------------
        investors = ["개인", "기관", "외국인", "연기금"]
        for inv in investors:
            print(f"Analyzing Investor: {inv}...")
            try:
                top_stocks = get_top20_by_investor(latest_date, inv)
                if not top_stocks.empty:
                    for idx, row in top_stocks.iterrows():
                        all_data.append([
                            display_date, inv, idx + 1, row['종목명'], 
                            row['시가총액'], row['순매수수량'], row['순매수대금'], row['비율(%)']
                        ])
            except Exception as e:
                print(f"Pass {inv}: {e}")

        # ------------------------------------------------
        # B. 시가총액 상위 20 (코스피, 코스닥)
        # ------------------------------------------------
        markets = ["KOSPI", "KOSDAQ"]
        for mkt in markets:
            print(f"Analyzing Market Cap: {mkt}...")
            try:
                top_stocks = get_market_ranking(latest_date, 'CAP', market=mkt)
                if not top_stocks.empty:
                    for idx, row in top_stocks.iterrows():
                        all_data.append([
                            display_date, f"{mkt}시총상위", idx + 1, row['종목명'], 
                            row['시가총액'], row['순매수수량'], row['순매수대금'], row['비율(%)']
                        ])
            except Exception as e:
                print(f"Pass {mkt}: {e}")

        # ------------------------------------------------
        # C. 상승률 상위 20 (코스피, 코스닥)
        # ------------------------------------------------
        for mkt in markets:
            print(f"Analyzing Gainers: {mkt}...")
            try:
                top_stocks = get_market_ranking(latest_date, 'CHANGE', market=mkt)
                if not top_stocks.empty:
                    for idx, row in top_stocks.iterrows():
                        all_data.append([
                            display_date, f"{mkt}상승률", idx + 1, row['종목명'], 
                            row['시가총액'], row['순매수수량'], row['순매수대금'], row['비율(%)']
                        ])
            except Exception as e:
                print(f"Pass Gainers {mkt}: {e}")

        # ------------------------------------------------
        # D. 거래대금 상위 20 (시장 전체)
        # ------------------------------------------------
        print(f"Analyzing Trading Value (ALL)...")
        try:
            top_stocks = get_market_ranking(latest_date, 'AMOUNT', market="ALL")
            if not top_stocks.empty:
                for idx, row in top_stocks.iterrows():
                    all_data.append([
                        display_date, "거래대금상위", idx + 1, row['종목명'], 
                        row['시가총액'], row['순매수수량'], row['순매수대금'], row['비율(%)']
                    ])
        except Exception as e:
            print(f"Pass Trading Value: {e}")

        # ------------------------------------------------
        # 결과 저장 및 출력
        # ------------------------------------------------
        if all_data:
            # 콘솔에 표 형태로 미리보기 출력
            df_preview = pd.DataFrame(all_data, columns=['날짜', '투자자', '순위', '종목명', '시가총액', '순매수수량', '순매수대금', '비율(%)'])
            print("\n📊 데이터 미리보기:")
            print(df_preview) 
            print("-" * 50)
            
            # 파일 저장
            save_to_csv(all_data)
    else:
        print("데이터를 찾을 수 없습니다.")

if __name__ == "__main__":
    main()
