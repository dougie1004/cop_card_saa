import pandas as pd
from datetime import time
# 지리적 분석 및 DB 연결을 위한 라이브러리는 설정에 따라 변경될 수 있습니다.
# 예를 들어, 실제 환경에서는 psycopg2와 PostGIS 쿼리를 사용합니다.

# --- 1. 데이터 로딩 및 규칙 정의 ---

def load_data(file_path='data/transactions.csv'):
    """예시 CSV 파일을 Pandas DataFrame으로 로드"""
    # 실제 환경에서는 DB에서 직접 데이터를 로드하는 함수로 대체됩니다.
    try:
        df = pd.read_csv(file_path, parse_dates=['transaction_dt'])
        return df
    except FileNotFoundError:
        print("Error: transactions.csv 파일을 찾을 수 없습니다.")
        return pd.DataFrame()

# 규칙에 사용될 상수 정의
PROHIBITED_MCCS = ['5813', '7995', '5814']  # 유흥주점, 카지노, 주점 등
HOLIDAY_LIST = [pd.to_datetime('2025-12-25').date(), pd.to_datetime('2026-01-01').date()]

# --- 2. 탐지 함수 정의 ---

def check_restricted_mcc(df):
    """제한 업종 MCC 코드 탐지 (Critical)"""
    alerts = []
    restricted_tx = df[df['mcc_code'].isin(PROHIBITED_MCCS)]
    
    for _, tx in restricted_tx.iterrows():
        alerts.append({
            'transaction_id': tx['transaction_id'],
            'rule_name': '제한 업종 사용',
            'severity': 'Critical',
            'detail': f"금지된 MCC 코드({tx['mcc_code']}) 사용",
            'alert_dt': pd.Timestamp.now()
        })
    return alerts

def check_irregular_time(df):
    """비정상 시간/휴일 사용 탐지 (High)"""
    alerts = []
    
    for _, tx in df.iterrows():
        tx_time = tx['transaction_dt'].time()
        tx_date = tx['transaction_dt'].date()
        day_of_week = tx['transaction_dt'].weekday()  # 5=Sat, 6=Sun
        
        # 1. 심야 시간 (23:00 ~ 05:59)
        if tx_time >= time(23, 0) or tx_time < time(6, 0):
            alerts.append({
                'transaction_id': tx['transaction_id'],
                'rule_name': '심야 시간 사용',
                'severity': 'High',
                'detail': f"사용 시간: {tx_time}",
                'alert_dt': pd.Timestamp.now()
            })
            
        # 2. 휴일 사용
        if day_of_week >= 5 or tx_date in HOLIDAY_LIST:
            alerts.append({
                'transaction_id': tx['transaction_id'],
                'rule_name': '휴일 사용',
                'severity': 'High',
                'detail': f"사용 일자: {tx_date}",
                'alert_dt': pd.Timestamp.now()
            })
            
    return alerts

def check_sequential_transactions(df):
    """연속/중복 결제 패턴 탐지 (Medium/High)"""
    alerts = []
    
    df_sorted = df.sort_values(by=['card_holder_id', 'transaction_dt']).copy()
    df_sorted['time_diff'] = df_sorted.groupby('card_holder_id')['transaction_dt'].diff().dt.total_seconds() / 60
    df_sorted['prev_merchant'] = df_sorted.groupby('card_holder_id')['merchant_name'].shift(1)
    df_sorted['prev_mcc'] = df_sorted.groupby('card_holder_id')['mcc_code'].shift(1)

    # 1. 동일 가맹점 연속 결제 (10분 이내)
    sequential_mask = (df_sorted['time_diff'] <= 10) & (df_sorted['merchant_name'] == df_sorted['prev_merchant'])

    for _, tx in df_sorted[sequential_mask].iterrows():
        alerts.append({
            'transaction_id': tx['transaction_id'],
            'rule_name': '동일 가맹점 연속 결제',
            'severity': 'Medium',
            'detail': f"이전 거래와의 시간차: {tx['time_diff']:.1f}분",
            'alert_dt': pd.Timestamp.now()
        })

    # 2. 고위험 업종 이동 (30분 이내, 식당(5812) -> 주점(5813))
    transition_mask = (df_sorted['time_diff'] <= 30) & \
                      (df_sorted['prev_mcc'] == '5812') & \
                      (df_sorted['mcc_code'].isin(['5813', '5814'])) 

    for _, tx in df_sorted[transition_mask].iterrows():
        alerts.append({
            'transaction_id': tx['transaction_id'],
            'rule_name': '고위험 업종 이동 결제',
            'severity': 'High',
            'detail': f"이전 업종({tx['prev_mcc']})에서 현재 업종({tx['mcc_code']})으로 전환",
            'alert_dt': pd.Timestamp.now()
        })
    return alerts


def run_all_detection(df):
    """모든 탐지 함수를 실행하고 결과를 통합"""
    if df.empty:
        return []
        
    all_alerts = []
    
    all_alerts.extend(check_restricted_mcc(df))
    all_alerts.extend(check_irregular_time(df))
    all_alerts.extend(check_sequential_transactions(df))
    # 지리적 이상 탐지 (PostGIS)는 DB 연결 및 쿼리가 필요하므로 여기서는 생략합니다.
    
    return all_alerts

# --- 3. 메인 실행 및 경고 출력/저장 ---

if __name__ == '__main__':
    # 1. 데이터 로드
    transactions_df = load_data()
    
    # 2. 탐지 실행
    alerts_result = run_all_detection(transactions_df)
    
    # 3. 경고 출력
    if alerts_result:
        alerts_df = pd.DataFrame(alerts_result)
        print("\n--- CardGuard AI 탐지 경고 (SAA) 결과 ---")
        print(alerts_df[['alert_dt', 'severity', 'rule_name', 'transaction_id', 'detail']].to_string())
        
        # 4. 경고 저장 (실제로는 DB의 Alert Log 테이블에 저장됩니다)
        # alerts_df.to_sql('alert_log', db_engine, if_exists='append', index=False)
    else:
        print("\n탐지된 의심 활동(SAA)이 없습니다.")