import pandas as pd
from datetime import time
import streamlit as st
import numpy as np 

# --- 1. 데이터 로딩 및 규칙 정의 (최종 수정된 load_data 함수 포함) ---

def load_data(file_path='data/transactions.csv'):
    """
    [최종 수정 2] CSV 파일을 로드하고 헤더 표준화 및 디버깅 정보를 추가합니다.
    구분자(delimiter) 문제를 해결하기 위해 쉼표(,)를 명시합니다.
    """
    try:
        # 1. 파일을 읽을 때 인코딩('utf-8'), 공백 제거, 그리고 구분자(delimiter=',')를 명시
        # 💡 만약 이 코드로 실패하면, 아래 줄의 'delimiter=','를 'delimiter=';'' 또는 'delimiter='\t''로 변경해 보세요.
        df = pd.read_csv(file_path, encoding='utf-8', skipinitialspace=True, delimiter=',')
        
        # 2. 모든 컬럼 이름을 소문자로 변환하고 앞뒤 공백을 제거하여 표준화
        df.columns = df.columns.str.lower().str.strip()
        
        # 3. 'transaction_dt' 컬럼이 존재하는지 최종 확인 후, DateTime 형식으로 강제 변환
        if 'transaction_dt' not in df.columns:
            # 🚨 디버깅 정보 출력: 현재 로드된 컬럼 목록을 사용자에게 보여줌
            st.error(f"디버깅 정보: 로드된 컬럼: {list(df.columns)}") 
            raise ValueError("CSV 파일에 'transaction_dt' 컬럼이 존재하지 않습니다. 헤더를 확인하십시오.")

        df['transaction_dt'] = pd.to_datetime(df['transaction_dt'])
        
        return df
    
    except FileNotFoundError:
        st.error(f"🚨 파일을 찾을 수 없습니다: '{file_path}'. 경로를 확인하십시오.")
        return pd.DataFrame()
    except Exception as e:
        # 기타 모든 파싱 및 로딩 오류를 Streamlit에 표시
        st.error(f"데이터 로딩 중 치명적인 오류 발생: {e}")
        return pd.DataFrame()


# 규칙에 사용될 상수 정의 (이후 코드는 변경 없음)
PROHIBITED_MCCS = ['5813', '7995', '5814']  # 유흥주점, 카지노, 주점 등
HOLIDAY_LIST = [pd.to_datetime('2025-12-25').date(), pd.to_datetime('2026-01-01').date()]

# --- 2. 탐지 함수 정의 (변경 없음) ---

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
        day_of_week = tx['transaction_dt'].weekday()  
        
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
    sequential_mask = (df_sorted['time_diff'] <= 10) & \
                      (df_sorted['merchant_name'] == df_sorted['prev_merchant'])

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
    
    return all_alerts

# --- 3. Streamlit 애플리케이션 메인 로직 (변경 없음) ---

def color_severity(val):
    """심각도에 따라 셀 배경색을 지정하는 함수"""
    if val == 'Critical':
        color = '#ffcccc' # Light Red
    elif val == 'High':
        color = '#ffe0b3'  # Light Orange
    elif val == 'Medium':
        color = '#ffffb3'  # Light Yellow
    else:
        color = ''
    return f'background-color: {color}'

# ==============================================================================

if __name__ == '__main__':
    st.set_page_config(layout="wide")
    st.title("🛡️ CardGuard AI: 법인카드 이상 활동 경고 (SAA) 시스템")

    # 1. 데이터 로드 (수정된 load_data 함수 사용)
    transactions_df = load_data('data/transactions.csv') 

    if transactions_df.empty:
        st.info("👈 데이터 로드에 실패했거나, 'data/transactions.csv' 파일이 비어 있습니다.")
    else:
        # 2. 탐지 실행
        alerts_result = run_all_detection(transactions_df)

        st.header("📈 1. 전체 거래 현황")
        st.dataframe(transactions_df, use_container_width=True)
        
        st.header("🔔 2. 탐지 경고 결과 (SAA)")

        # 3. 경고 출력 및 지표 표시
        if alerts_result:
            alerts_df = pd.DataFrame(alerts_result)
            alerts_df = alerts_df.drop_duplicates() 
            
            # 지표 표시
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("총 거래 건수", len(transactions_df))
            col2.metric("총 경고 건수", len(alerts_df))
            col3.metric("Critical 경고", len(alerts_df[alerts_df['severity'] == 'Critical']))
            col4.metric("High 경고", len(alerts_df[alerts_df['severity'] == 'High']))
            
            st.subheader("⚠️ 경고 상세 내역")
            
            # DataFrame 스타일링 적용
            styled_df = alerts_df[['alert_dt', 'severity', 'rule_name', 'transaction_id', 'detail']].style.applymap(color_severity, subset=['severity'])

            st.dataframe(styled_df, use_container_width=True)

        else:
            st.success("🎉 탐지된 의심 활동(SAA)이 없습니다. 모든 거래는 정상입니다.")
