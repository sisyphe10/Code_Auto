import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime
import csv
import os

# 저장할 파일명
CSV_FILE = 'krx_ranking.csv'

def setup_csv():
    """CSV 파일 초기 설정 (헤더 생성)"""
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(['날짜', '카테고리', '순위', '종목명', '시가총액(억)', '거래대금(억)', '등락률(%)'])

def save_to_csv(data_list):
    """중복 데이터를 제외하고 CSV에 추가 저장"""
    try:
        existing_keys = set()
        if os.path.exists(CSV_FILE):
            with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                next(reader, None) # 헤더 건너뛰기
                for row in reader:
                    if len(row) >= 4:
                        # 중복 체크 기준: (날짜, 카테고리, 종목명)
                        existing_keys.add((row[0], row[1], row[3]))

        new_records = [row for row in data_list if (row[0], row[1], row[3]) not in existing_keys]

        if new_records:
            with open(CSV_FILE, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerows(new_records)
            print(f"✅ 신규 데이터 {len(new_records)}건 저장 완료")
        else:
            print("💡 이미 최신 데이터가 저장되어 있습니다.")
    except Exception as e:
        print(f"❌ 저장 실패: {e}")

def main():
    print(f"🚀 KRX 시장 데이터 분석 시작 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    setup_csv()
    
    try:
        # 1. 전체 시장 데이터 로드
        df = fdr.StockListing('KRX')
    except Exception as e:
        print(f"❌ 데이터 로드 실패: {e}")
        return

    target_date = datetime.now().strftime("%Y-%m-%d")
    all_final_data = []

    # 분석 대상 설정 (시장명, 카테고리명, 정렬기준 컬럼)
    markets = ['KOSPI', 'KOSDAQ']
    analysis_types = [
        ('Marcap', '시총상위'),
        ('Amount', '거래대금상위'),
        ('ChagesRatio', '상승률상위')
    ]

    for mkt in markets:
        # 해당 시장 데이터만 추출
        mkt_df = df[df['Market'] == mkt].copy()
        
        for col, label in analysis_types:
            category_full_name = f"{mkt}_{label}"
            print(f"📊 {category_full_name} 분석 중...")
            
            # 숫자형 변환 및 정렬
            mkt_df[col] = pd.to_numeric(mkt_df[col], errors='coerce').fillna(0)
            top20 = mkt_df.sort_values(by=col, ascending=False).head(20)
            
            # 데이터 가공 및 리스트 추가
            for i, (_, row) in enumerate(top20.iterrows()):
                try:
                    name = str(row['Name'])
                    m_cap = float(row['Marcap']) / 100000000 # 억 단위 변환
                    amt = float(row['Amount']) / 100000000   # 억 단위 변환
                    chg = float(row['ChagesRatio'])         # 등락률

                    all_final_data.append([
                        target_date, 
                        category_full_name, 
                        i + 1, 
                        name,
                        f"{m_cap:,.0f}", 
                        f"{amt:,.0f}", 
                        f"{chg:.2f}%"
                    ])
                except:
                    continue

    # 결과 출력 및 저장
    if all_final_data:
        # 콘솔 미리보기 (카테고리별 1위만 출력)
        summary_df = pd.DataFrame(all_final_data, columns=['날짜', '카테고리', '순위', '종목명', '시총', '거래대금', '등락률'])
        print("\n[수집 완료 요약 - 각 1위 종목]")
        print(summary_df[summary_df['순위'] == 1].to_string(index=False))
        
        save_to_csv(all_final_data)

if __name__ == "__main__":
    main()
