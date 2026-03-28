import io
import re
import pandas as pd
import streamlit as st
from datetime import datetime

# [설정] 페이지 설정
st.set_page_config(page_title="광고 통합 대시보드", layout="wide")

# 스타일 설정
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
</style>
""", unsafe_allow_html=True)

# =========================================================
# 1. 데이터 클리닝 및 표준화 유틸리티
# =========================================================
def sanitize_numeric(series):
    """문자열 혼입 수치를 정밀한 숫자로 변환"""
    if series is None: return 0
    s = series.astype(str).str.replace(r'[,%원건\s]', '', regex=True)
    return pd.to_numeric(s, errors='coerce').fillna(0)

def normalize_columns(df):
    """컬럼명 자동 매핑 로직 (KeyError 방지)"""
    col_map = {
        '비용': ['비용', '광고비', '지출', 'spend', 'amount', '금액', '실제비용'],
        '매출액': ['매출액', '매출', 'revenue', 'sales', '전환금액'],
        '노출': ['노출', '노출수', 'impressions', 'imp'],
        '클릭': ['클릭', '클릭수', 'clicks', 'clk'],
        '구매': ['구매', '구매수', '구매건수', 'conversions', 'orders'],
        '날짜': ['날짜', '일자', 'date', 'day'],
        '매체': ['매체', '매체명', 'media', 'platform', 'channel']
    }
    
    current_cols = {c.replace(" ", ""): c for c in df.columns}
    new_df = df.copy()
    
    for standard_key, aliases in col_map.items():
        for alias in aliases:
            for clean_col in current_cols:
                if alias in clean_col.lower():
                    new_df = new_df.rename(columns={current_cols[clean_col]: standard_key})
                    break
    
    # 필수 컬럼이 없을 경우 0으로 채운 빈 컬럼 생성
    for col in col_map.keys():
        if col not in new_df.columns:
            new_df[col] = 0 if col != '날짜' and col != '매체' else ""
            
    return new_df

def safe_divide(n, d):
    return n / d if d > 0 else 0

# =========================================================
# 2. 분석 및 코멘트 로직
# =========================================================
def generate_company_comment(df):
    try:
        df['날짜_dt'] = pd.to_datetime(df['날짜'], errors='coerce')
        valid_df = df.dropna(subset=['날짜_dt'])
        dates = sorted(valid_df['날짜_dt'].unique(), reverse=True)
        
        if len(dates) < 2: return "💡 분석을 위해 최소 2일 이상의 데이터가 필요합니다."
        
        curr_df = valid_df[valid_df['날짜_dt'] == dates[0]]
        prev_df = valid_df[valid_df['날짜_dt'] == dates[1]]
        
        c_cost, c_sales = curr_df['비용'].sum(), curr_df['매출액'].sum()
        p_cost, p_sales = prev_df['비용'].sum(), prev_df['매출액'].sum()
        
        c_roas = safe_divide(c_sales, c_cost) * 100
        p_roas = safe_divide(p_sales, p_cost) * 100
        
        comment = f"### 📊 데일리 성과 요약 ({dates[0].strftime('%m/%d')})\n"
        comment += f"- **총 광고비**: {c_cost:,.0f}원 (전일 대비 {safe_divide(c_cost-p_cost, p_cost)*100:+.1f}%)\n"
        comment += f"- **총 매출액**: {c_sales:,.0f}원 (전일 대비 {safe_divide(c_sales-p_sales, p_sales)*100:+.1f}%)\n"
        comment += f"- **ROAS**: {c_roas:.1f}% ({p_roas:.1f}% 대비 {c_roas-p_roas:+.1f}%p)\n"
        return comment
    except:
        return "⚠️ 코멘트 생성 중 오류가 발생했습니다. 데이터 형식을 확인해주세요."

# =========================================================
# 3. 메인 앱 실행
# =========================================================
def main():
    st.title("🚀 광고 통합 성과 대시보드")
    
    # 세션 초기화
    if "final_df" not in st.session_state:
        st.session_state.final_df = pd.DataFrame()

    with st.sidebar:
        st.header("📂 데이터 업로드")
        uploaded_file = st.file_uploader("통합 리포트 엑셀 업로드", type=['xlsx', 'csv'])
        
        if uploaded_file:
            try:
                raw_df = pd.read_excel(uploaded_file) if uploaded_file.name.endswith('xlsx') else pd.read_csv(uploaded_file)
                # 컬럼 표준화
                std_df = normalize_columns(raw_df)
                # 수치 데이터 정제
                for col in ["비용", "매출액", "노출", "클릭", "구매"]:
                    std_df[col] = sanitize_numeric(std_df[col])
                
                st.session_state.final_df = std_df
                st.success("데이터 로드 완료!")
            except Exception as e:
                st.error(f"파일을 읽는 중 오류가 발생했습니다: {e}")

    # 메인 화면
    df = st.session_state.final_df
    
    if not df.empty:
        tab1, tab2 = st.tabs(["📈 지표 요약", "💬 자동 코멘트"])
        
        with tab1:
            st.subheader("핵심 성과 지표")
            c1, c2, c3, c4 = st.columns(4)
            
            # .get() 대신 표준화된 컬럼 직접 호출 (normalize_columns에서 생성 보장)
            total_cost = df['비용'].sum()
            total_sales = df['매출액'].sum()
            total_roas = safe_divide(total_sales, total_cost) * 100
            total_purchase = df['구매'].sum()

            c1.metric("총 광고비", f"{total_cost:,.0f}원")
            c2.metric("총 매출액", f"{total_sales:,.0f}원")
            c3.metric("평균 ROAS", f"{total_roas:.1f}%")
            c4.metric("총 구매수", f"{total_purchase:,.0f}건")
            
            st.divider()
            st.subheader("데이터 미리보기")
            st.dataframe(df.head(10), use_container_width=True)

        with tab2:
            st.subheader("수치 기반 자동 코멘트")
            comment_text = generate_company_comment(df)
            st.markdown(f"<div class='ai-comment-box'>{comment_text.replace(chr(10), '<br>')}</div>", unsafe_allow_html=True)
    else:
        st.info("왼쪽 사이드바에서 엑셀 파일을 업로드해주세요.")

if __name__ == "__main__":
    main()
