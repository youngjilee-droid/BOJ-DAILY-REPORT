import base64
import hashlib
import hmac
import io
import json
import os
import re
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List

import pandas as pd
import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# OpenAI 및 GSpread 라이브러리 체크
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

# =========================================================
# 1. 기본 설정 및 CSS 스타일
# =========================================================
st.set_page_config(page_title="광고 통합 대시보드", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    .main { background-color: #f8f9fa; }
    [data-testid="metric-container"] {
        background-color: white; border: 1px solid #e0e0e0;
        border-radius: 12px; padding: 16px 20px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    }
    .ai-comment-box {
        background: #f0f2f6; border-left: 5px solid #4f46e5;
        padding: 20px; border-radius: 8px; margin: 10px 0;
    }
    .status-pill-connected {
        background: #dcfce7; color: #166534; padding: 4px 10px;
        border-radius: 999px; font-size: 12px; font-weight: 700;
    }
</style>
""", unsafe_allow_html=True)

# 세션 상태 초기화
for key in ["final_report_df", "ai_comment", "ai_chat_history", "manual_upload_dfs"]:
    if key not in st.session_state:
        st.session_state[key] = pd.DataFrame() if "df" in key else ([] if "history" in key else "")

# =========================================================
# 2. 데이터 처리 유틸리티 (수치 보존 핵심)
# =========================================================
FINAL_COLUMNS = [
    "날짜", "매체", "캠페인명", "광고그룹명", "광고명",
    "비용", "실제 비용", "노출", "클릭", "구매", "매출액", 
    "장바구니담기수", "도달", "참여", "팔로우", "동영상조회"
]

def sanitize_numeric(series: pd.Series) -> pd.Series:
    """문자열 혼입 수치를 정밀한 숫자로 변환 (반올림 오차 방지)"""
    s = series.astype(str).str.replace(r'[,%원건\s]', '', regex=True)
    return pd.to_numeric(s, errors='coerce').fillna(0)

def safe_divide(n, d):
    return n / d if d > 0 else 0

def add_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """ROAS, CTR 등 주요 지표 계산"""
    df = df.copy()
    df["CTR"] = df.apply(lambda x: safe_divide(x["클릭"], x["노출"]) * 100, axis=1)
    df["CPC"] = df.apply(lambda x: safe_divide(x["비용"], x["클릭"]), axis=1)
    df["ROAS"] = df.apply(lambda x: safe_divide(x["매출액"], x["비용"]) * 100, axis=1)
    df["CVR"] = df.apply(lambda x: safe_divide(x["구매"], x["클릭"]) * 100, axis=1)
    return df

# =========================================================
# 3. 엑셀 리포트 생성 (스타일 적용)
# =========================================================
def build_excel_report(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='통합리포트')
        # 여기에 추가적인 엑셀 스타일(셀 병합, 색상 등) 로직을 적용할 수 있습니다.
    return output.getvalue()

# =========================================================
# 4. 회사 톤 코멘트 생성기 (핵심 기능)
# =========================================================
def generate_company_comment(df: pd.DataFrame) -> str:
    """수치 훼손 없이 엑셀 데이터를 기반으로 텍스트 생성"""
    if df.empty: return "데이터가 없습니다."
    
    # 날짜별 요약
    df['날짜_dt'] = pd.to_datetime(df['날짜'])
    dates = sorted(df['날짜_dt'].unique(), reverse=True)
    if len(dates) < 2: return "최근 2일간의 데이터가 필요합니다."
    
    curr_df = df[df['날짜_dt'] == dates[0]]
    prev_df = df[df['날짜_dt'] == dates[1]]
    
    c_cost, c_sales = curr_df['비용'].sum(), curr_df['매출액'].sum()
    p_cost, p_sales = prev_df['비용'].sum(), prev_df['매출액'].sum()
    
    c_roas = safe_divide(c_sales, c_cost) * 100
    p_roas = safe_divide(p_sales, p_cost) * 100
    
    comment = f"### 📊 데일리 성과 요약 ({dates[0].strftime('%m/%d')})\n"
    comment += f"- **총 광고비**: {c_cost:,.0f}원 (전일 대비 {safe_divide(c_cost-p_cost, p_cost)*100:+.1f}%)\n"
    comment += f"- **총 매출액**: {c_sales:,.0f}원 (전일 대비 {safe_divide(c_sales-p_sales, p_sales)*100:+.1f}%)\n"
    comment += f"- **ROAS**: {c_roas:.1f}% ({p_roas:.1f}% 대비 {c_roas-p_roas:+.1f}%p)\n\n"
    
    comment += "#### 📺 주요 매체별 현황\n"
    media_summary = curr_df.groupby('매체')[['비용', '매출액']].sum()
    for media, row in media_summary.sort_values('비용', ascending=False).iterrows():
        m_roas = safe_divide(row['매출액'], row['비용']) * 100
        comment += f"- **{media}**: {row['비용']:,.0f}원 소진 / ROAS {m_roas:.1f}%\n"
        
    return comment

# =========================================================
# 5. UI 메인 로직
# =========================================================
def main():
    st.title("🚀 광고 통합 성과 대시보드 & 자동 코멘트")
    
    tab_collect, tab_dash, tab_comment, tab_debug = st.tabs([
        "🔗 데이터 수집", "📊 대시보드", "💬 데일리 코멘트", "🛠 매핑 디버그"
    ])
    
    with tab_collect:
        st.subheader("파일 업로드 (통합 리포트 xlsx)")
        uploaded_file = st.file_uploader("최종 리포트 파일을 업로드하세요", type=['xlsx', 'csv'])
        
        if uploaded_file:
            try:
                df = pd.read_excel(uploaded_file) if uploaded_file.name.endswith('xlsx') else pd.read_csv(uploaded_file)
                # 데이터 표준화 적용
                for col in ["비용", "매출액", "노출", "클릭", "구매"]:
                    if col in df.columns: df[col] = sanitize_numeric(df[col])
                
                st.session_state.final_report_df = df
                st.success("데이터 로드 성공!")
                st.dataframe(df.head())
            except Exception as e:
                st.error(f"오류 발생: {e}")

    with tab_dash:
        if not st.session_state.final_report_df.empty:
            df = st.session_state.final_report_df
            st.subheader("핵심 성과 지표 (전체 기간)")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("총 비용", f"{df['비용'].sum():,.0f}")
            c2.metric("총 매출", f"{df['매출액'].sum():,.0f}")
            c3.metric("평균 ROAS", f"{(safe_divide(df['매출액'].sum(), df['비용'].sum())*100):.1f}%")
            c4.metric("총 구매", f"{df['구매'].sum():,.0f}")
            
            st.line_chart(df.groupby('날짜')[['비용', '매출액']].sum())
        else:
            st.info("수집 탭에서 데이터를 먼저 업로드해주세요.")

    with tab_comment:
        if not st.session_state.final_report_df.empty:
            comment = generate_company_comment(st.session_state.final_report_df)
            st.markdown(f"<div class='ai-comment-box'>{comment.replace(chr(10), '<br>')}</div>", unsafe_allow_html=True)
            st.download_button("코멘트 다운로드", comment, "daily_comment.txt")
        else:
            st.info("데이터가 없습니다.")

    with tab_debug:
        st.write("매핑 오류 및 데이터 정합성 체크 기능을 제공합니다. (기능 확장 예정)")

if __name__ == "__main__":
    main()
