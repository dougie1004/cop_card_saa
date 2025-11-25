Python

# app.py 파일 내, 3. PyDeck 맵 렌더링 부분

# ... (중략)

# 3. PyDeck 맵 렌더링 (TypeError 방지 로직 적용)
if not map_data.empty:
    # 1. 뷰포트 설정
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
        get_position=["lon", "lat"], 
        get_color=[255, 0, 0, 200], 
        get_radius=500, 
        pickable=True, 
    )

    # 3. pdk.Deck 생성 시 필요한 인수를 직접 전달
    #    TypeError를 방지하기 위해, Mapbox 키를 전달할지 말지 결정합니다.
    deck = pdk.Deck(
        map_style="mapbox://styles/mapbox/light-v9",
        initial_view_state=view_state,
        layers=[layer],
        tooltip={
            "html": "{popup_text}", 
            "style": {
                "backgroundColor": "red",
                "color": "white"
            }
        }
    )

    # 🚨 Mapbox API 키가 None이 아닐 경우에만 key 속성에 할당합니다.
    # 이 방식이 딕셔너리 언패킹보다 안정적입니다.
    if MAPBOX_API_KEY is not None:
        deck.mapbox_key = MAPBOX_API_KEY
    
    # 4. PyDeck 맵 렌더링
    st.pydeck_chart(deck)
        
else:
    st.info("지도에 표시할 위치 정보(lat, lon)가 있는 경고는 없습니다.")

# ... (이하 상세 내역 테이블 표시 로직 유지)
