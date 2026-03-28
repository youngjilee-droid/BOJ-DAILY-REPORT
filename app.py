import pandas as pd
import streamlit as st
import io
import json
import re
from datetime import datetime, timedelta
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

# [설정] 페이지 설정 및 스타일
st.set_page_config(page_title="광고 통합 성과 대시보드", layout="wide")

st.markdown("""
<style>
    .main { background-color: #f4f7f9; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; border: 1px solid #e1e4e8; }
    .report-card { background-color: white; padding: 20px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); }
</style>
""", unsafe_allow_html=True)

# =========================================================
# 1. 핵심 유틸리티 (수치 보존 및 계산)
# =========================================================
def safe_num(val):
    """데이터 훼손 없이 숫자로 변환"""
    if pd.isna(val) or val == "": return 0
    if isinstance(val, (int, float)): return val
    try:
        # 쉼표, 원, % 등 제거 후 float 변환
        cleaned = re.sub(r'[^\d.-]', '', str(val))
        return float(cleaned)
    except:
        return 0

def calculate_metrics(df):
    """수치 계산 로직 통일 (원본 데이터 보존)"""
    res = df.copy()
    # 기본 지표 보정
    for col in ["비용", "매출액", "클릭", "노출", "구매"]:
        if col in res.columns:
            res[col] = res[col].apply(safe_num)
    
    # 계산 지표 (분모가 0인 경우 처리)
    res["CTR"] = res.apply(lambda x: (x["클릭"] / x["노출"] * 100) if x["노출"] > 0 else 0, axis=1)
    res["CPC"] = res.apply(lambda x: (x["비용"] / x["클릭"]) if x["클릭"] > 0 else 0, axis=1)
    res["ROAS"] = res.apply(lambda x: (x["매출액"] / x["비용"] * 100) if x["비용"] > 0 else 0, axis=1)
    res["CVR"] = res.apply(lambda x: (x["구매"] / x["클릭"] * 100) if x["클릭"] > 0 else 0, axis=1)
    return res

# =========================================================
# 2. 엑셀 파일 로드 및 표준화
# =========================================================
def load_integrated_report(file):
    """업로드된 엑셀/CSV를 읽어 표준화"""
    try:
        if file.name.endswith('.csv'):
            df = pd.read_csv(file)
        else:
            # 엑셀의 경우 모든 시트를 검토하거나 특정 시트 지정 가능
            df = pd.read_excel(file)
        
        # 컬럼명 정리
        df.columns = [str(c).strip() for c in df.columns]
        
        # 필수 컬럼 체크 및 변환
        if "날짜" in df.columns:
            df["날짜"] = pd.to_datetime(df["날짜"]).dt.strftime('%Y-%m-%d')
        
        return calculate_metrics(df)
    except Exception as e:
        st.error(f"파일 로드 중 오류 발생: {e}")
        return None

# =========================================================
# 3. 코멘트 생성 로직 (회사 톤)
# =========================================================
def generate_report_comment(df):
    """수치 기반 자동 코멘트 생성"""
    if df is None or df.empty: return "데이터가 없습니다."
    
    # 날짜 정렬 후 최근 2일 추출
    dates = sorted(df["날짜"].unique(), reverse=True)
    if len(dates) < 2: return "비교를 위한 최소 2일치 데이터가 부족합니다."
    
    latest_date = dates[0]
    prev_date = dates[1]
    
    today_total = df[df["날짜"] == latest_date].sum(numeric_only=True)
    prev_total = df[df["날짜"] == prev_date].sum(numeric_only=True)
    
    # 지표 재계산 (합계 기준)
    t_roas = (today_total["매출액"] / today_total["비용"] * 100) if today_total["비용"] > 0 else 0
    p_roas = (prev_total["매출액"] / prev_total["비용"] * 100) if prev_total["비용"] > 0 else 0
    roas_diff = t_roas - p_roas
    
    comment = f"### 📝 데일리 성과 요약 ({latest_date})\n\n"
    comment += f"**1. 전체 총괄**\n"
    comment += f"- 금일 광고비 **{today_total['비용']:,.0f}원** 소진하여 매출 **{today_total['매출액']:,.0f}원** 달성.\n"
    comment += f"- ROAS는 **{t_roas:.1f}%**로 전일({p_roas:.1f}%) 대비 **{roas_diff:+.1f}%p** {('상승' if roas_diff > 0 else '하락')}하였습니다.\n\n"
    
    comment += "**2. 매체별 특이사항**\n"
    media_df = df[df["날짜"] == latest_date].groupby("매체").sum(numeric_only=True)
    for media, row in media_df.sort_values("비용", ascending=False).iterrows():
        m_roas = (row["매출액"] / row["비용"] * 100) if row["비용"] > 0 else 0
        comment += f"- **{media}**: {row['비용']:,.0f}원 지출 / ROAS {m_roas:.1f}%\n"
        
    return comment

# =========================================================
# 4. 메인 화면 구성
# =========================================================
def main():
    st.title("📊 광고 통합 리포트 분석기 v2")
    st.markdown("이미 완성된 `통합리포트.xlsx` 파일을 업로드하면 수치 분석과 코멘트 작성을 자동으로 수행합니다.")
    
    uploaded_file = st.file_uploader("통합 리포트 파일(xlsx, csv) 업로드", type=["xlsx", "csv"])
    
    if uploaded_file:
        data = load_integrated_report(uploaded_file)
        
        if data is not None:
            st.success("데이터 로드 완료!")
            
            # --- 탭 구성 ---
            tab1, tab2, tab3 = st.tabs(["📈 대시보드", "📝 자동 코멘트", "📋 원본 데이터"])
            
            with tab1:
                # 주요 KPI (전체 기간 합계 또는 최근일 기준 선택 가능)
                latest_date = sorted(data["날짜"].unique())[-1]
                latest_data = data[data["날짜"] == latest_date]
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("최근 소진 비용", f"{latest_data['비용'].sum():,.0f}원")
                with col2:
                    st.metric("최근 매출액", f"{latest_data['매출액'].sum():,.0f}원")
                with col3:
                    roas = (latest_data['매출액'].sum() / latest_data['비용'].sum() * 100) if latest_data['비용'].sum() > 0 else 0
                    st.metric("최근 ROAS", f"{roas:.1f}%")
                with col4:
                    st.metric("구매 건수", f"{latest_data['구매'].sum():,.0f}건")
                
                st.markdown("---")
                st.subheader("일자별 성과 추이")
                daily_trend = data.groupby("날짜").sum(numeric_only=True)
                st.line_chart(daily_trend[["비용", "매출액"]])

            with tab2:
                st.subheader("회사 톤 데일리 코멘트")
                report_text = generate_report_comment(data)
                st.text_area("생성된 코멘트 (복사해서 사용하세요)", value=report_text, height=400)
                
                if st.button("AI 인사이트 추가 분석"):
                    st.info("현재 수치를 바탕으로 OpenAI 분석 기능을 실행할 수 있습니다. (Secrets 설정 필요)")

            with tab3:
                st.subheader("업로드 데이터 확인")
                st.dataframe(data, use_container_width=True)

if __name__ == "__main__":
    main()
