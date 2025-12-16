# 파일명: krx_ranking.py
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import csv
import os

# 저장할 파일명
CSV_FILE = 'krx_ranking.csv'

def setup_csv():
    """CSV 파일이 없으면 헤더 생성"""
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(['날짜', '투자자', '순위', '종목명', '시가총액', '순매수수량', '순매수대금', '비율(%)'])

def save_to_csv(data_list):
    """데이터 리스트를 CSV에 추가"""
    try:
        with open(CSV_FILE, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            for row in data_list:
                writer.writerow(row)
        print(f"✅ 데이터 {len(data_list)}건 저장 완료")
    except Exception as e:
        print(f"❌ 저장 실패: {e}")

def find_latest_valid_data_date():
    """가장 최근 영업일 찾기 (반환값: YYYYMMDD 문자열)"""
    target_date = datetime.now()
    for i in range(7):
        check_date = (target_date - timedelta(days=i)).strftime("%Y%m%d")
        try:
            # 데이터 존재 여부 확인용 가조회
            df = stock.get_market_net_purchases_of_equities_by_ticker(check_date, check_date, "ALL", "개인")
            if not df.empty:
                return check_date
        except:
            continue
    return None

def get_top20_by_investor(target_date, investor_name):
    cap_df = stock.get_market_cap(target_date)
    inv_code = "기관합계" if investor_name == "기관" else investor_name
    
    # pykrx는 날짜를 'YYYYMMDD'로 받아야 작동하므로 그대로 사용
    df = stock.get_market_net_purchases_of_equities_by_ticker(target_date, target_date, "ALL", inv_code)

    col_candidates = [c for c in df.columns if "순매수" in c and ("대금" in c or "금액" in c)]
    if not col_candidates:
        return pd.DataFrame()
    target_col = col_candidates[0]

    merged = df.join(cap_df[['시가총액']], how='inner')
    top20 = merged.sort_values(by=target_col, ascending=False).head(20).copy()
    top20['비율(%)'] = (top20[target_col] / top20['시가총액']) * 100
    qty_col = '순매수거래량' if '순매수거래량' in top20.columns else '순매수수량'

    result = pd.DataFrame()
    result['종목명'] = top20['종목명']
    result['시가총액'] = (top20['시가총액'] / 100000000).apply(lambda x: f"{x:,.0f}억원")
    result['순매수수량'] = top20[qty_col].apply(lambda x: f"{x:,}")
    result['순매수대금'] = (top20[target_col] / 100000000).apply(lambda x: f"{x:,.0f}억원")
    result['비율(%)'] = top20['비율(%)'].apply(lambda x: f"{x:.2f}%")

    return result.reset_index(drop=True)

def main():
    print("🚀 KRX 순매수 상위 크롤링 시작")
    setup_csv()
    
    # 여기서 받은 latest_date는 '20251215' 형식입니다 (API 조회용)
    latest_date = find_latest_valid_data_date()

    if latest_date:
        # [수정된 부분] 저장할 때는 '2025-12-15'로 예쁘게 변환합니다.
        display_date = datetime.strptime(latest_date, "%Y%m%d").strftime("%Y-%m-%d")
        
        print(f"### 분석 기준일: {display_date} ###")
        investors = ["개인", "기관", "외국인", "연기금"]
        
        all_data = []

        for inv in investors:
            print(f"Analyzing {inv}...")
            try:
                top_stocks = get_top20_by_investor(latest_date, inv)
                if not top_stocks.empty:
                    for idx, row in top_stocks.iterrows():
                        save_row = [
                            display_date,  # <-- 수정된 날짜 형식 사용
                            inv,
                            idx + 1,
                            row['종목명'],
                            row['시가총액'],
                            row['순매수수량'],
                            row['순매수대금'],
                            row['비율(%)']
                        ]
                        all_data.append(save_row)
            except Exception as e:
                print(f"Error in {inv}: {e}")
        
        if all_data:
            save_to_csv(all_data)
    else:
        print("데이터를 찾을 수 없습니다.")

if __name__ == "__main__":
    main()
