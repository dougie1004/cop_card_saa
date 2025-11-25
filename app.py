import pandas as pd
from datetime import time
import streamlit as st
import numpy as np 
# Pydeck 라이브러리 추가 (지도 툴팁 기능을 위해 필요)
import pydeck as pdk 

# --- 1. 데이터 로딩 및 규칙 정의 (load_data 함수는 변경 없음) ---

def load_data(file_path='data/transactions.csv'):
    """
    [최종 수정 2] CSV 파일을 로드하고 헤더 표준화 및 디버깅 정보를 추가합니다.
    구분자(delimiter) 문제를 해결하기 위해 쉼표(,)를 명시합니다.
    """
    try:
        # delimiter=','를 명시하여 CSV 파일을 로드합니다.
        df = pd.read_csv(file_path, encoding='utf-8', skipinitialspace=True, delimiter=',') 
        
        # 모든 컬럼 이름을 소문자로 변환하고 앞뒤 공백을 제거하여 표준화
        df.columns = df.columns.str.lower().str.strip()
        
        # 'transaction_dt' 컬럼이 존재하는지 최종 확인 후, DateTime 형식으로 강제 변환
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


# 규칙에 사용될 상수 정의
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

# --- 3. Streamlit 애플리케이션 메인 로직 (지도 및 툴팁 추가) ---

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

    # 1. 데이터 로드 
    transactions_df = load_data('data/transactions.csv') 

    if transactions_df.empty:
        st.info("👈 데이터 로드에 실패했거나, 'data/transactions.csv' 파일이 비어 있습니다.")
    else:
        # 2. 탐지 실행
        alerts_result = run_all_detection(transactions_df)

        st.header("📈 1. 전체 거래 현황")
        st.dataframe(transactions_df, use_container_width=True)
        
        st.header("🔔 2. 탐지 경고 결과 (SAA)")

        # 3. 경고 출력, 지도 표시 및 지표 표시
        if alerts_result:
            alerts_df = pd.DataFrame(alerts_result)
            # 중복 경고 제거
            alerts_df = alerts_df.drop_duplicates(subset=['transaction_id', 'rule_name']) 
            
            # --- 지도 생성을 위해 원본 거래 데이터(위치, 사용자, 금액)와 경고 데이터를 병합 ---
            map_data = alerts_df.merge(
                transactions_df[['transaction_id', 'card_holder_id', 'amount', 'merchant_name', 'location_lat', 'location_lon']],
                on='transaction_id',
                how='left'
            )
            
            # pydeck을 위해 컬럼 이름을 'lat'과 'lon'으로 변경
            map_data = map_data.rename(columns={
                'location_lat': 'lat', 
                'location_lon': 'lon'
            })
            
            # 위치 정보가 없는 경고는 지도에서 제외
            map_data = map_data.dropna(subset=['lat', 'lon'])
            
            # --- 툴팁에 사용될 상세 정보 컬럼 생성 ---
            map_data['popup_text'] = (
                "**사용자:** " + map_data['card_holder_id'].astype(str) + 
                "<br>**사용처:** " + map_data['merchant_name'].astype(str) +
                "<br>**금액:** " + map_data['amount'].apply(lambda x: f"{x:,.0f}원") +
                "<br>**위반 사유:** " + map_data['rule_name'].astype(str) +
                "<br>**심각도:** " + map_data['severity'].astype(str)
            )

            # --- 지표 표시 ---
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("총 거래 건수", len(transactions_df))
            col2.metric("총 경고 건수", len(alerts_df))
            col3.metric("Critical 경고", len(alerts_df[alerts_df['severity'] == 'Critical']))
            col4.metric("High 경고", len(alerts_df[alerts_df['severity'] == 'High']))
            
            # --- 지도 표시 (pydeck을 사용) ---
            st.header("🗺️ 3. 위반된 사용처 지도 (경고 정보 표시)")
            
            # 📌 경고 건수와 핀 개수 불일치에 대한 설명
            st.info(f"**총 경고 건수({len(alerts_df)}건)**와 지도에 표시된 핀의 개수가 다를 수 있습니다. 이는 여러 개의 경고가 **동일한 위치**에서 발생했기 때문입니다. 핀 위에 커서를 올려 상세 정보를 확인하세요.")


            if not map_data.empty:
                # 1. 뷰포트 설정 (경고 발생 지점의 평균 위치를 중심으로 설정)
                view_state = pdk.ViewState(
                    latitude=map_data["lat"].mean(),
                    longitude=map_data["lon"].mean(),
                    zoom=11, 
                    pitch=50
                )

                # 2. 산점도 레이어 설정
                layer = pdk.Layer(
                    "ScatterplotLayer",
                    map_data,
                    get_position=["lon", "lat"], # pydeck은 [경도, 위도] 순서
                    get_color=[255, 0, 0, 200], # 빨간색 핀 (Critical)
                    get_radius=500, # 핀 크기 (미터 단위)
                    pickable=True, # 툴팁 활성화
                )

                # 3. PyDeck 맵 렌더링 (툴팁 설정 포함)
                st.pydeck_chart(pdk.Deck(
                    map_style="mapbox://styles/mapbox/light-v9",
                    initial_view_state=view_state,
                    layers=[layer],
                    tooltip={
                        # popup_text 컬럼의 내용을 툴팁으로 표시
                        "html": "{popup_text}", 
                        "style": {
                            "backgroundColor": "red",
                            "color": "white"
                        }
                    }
                ))
                
            else:
                st.info("지도에 표시할 위치 정보(lat, lon)가 있는 경고는 없습니다.")

            # --- 상세 내역 테이블 표시 (병합된 데이터를 사용) ---
            st.subheader("⚠️ 경고 상세 내역 (사용자/사용처/금액 포함)")
            
            # 표시할 컬럼 목록 재정의
            display_cols = ['alert_dt', 'severity', 'rule_name', 'card_holder_id', 'merchant_name', 'amount', 'detail']
            
            # DataFrame 스타일링 적용
            styled_df = map_data[display_cols].style.applymap(color_severity, subset=['severity']).format({'amount': '{:,.0f}원'})

            st.dataframe(styled_df, use_container_width=True)

        else:
            st.success("🎉 탐지된 의심 활동(SAA)이 없습니다. 모든 거래는 정상입니다.")
