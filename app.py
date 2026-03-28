import io
import openpyxl
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import openai

# ─── 페이지 설정 ───────────────────────────────────────────────
st.set_page_config(page_title="BOJ 광고 코멘트 생성기", layout="wide")

st.markdown("""
<style>
    .block-container { padding-top: 2rem; }
    .stTextArea textarea { font-family: 'Malgun Gothic', sans-serif; font-size: 14px; line-height: 1.7; }
    .metric-card { background: #f8f9fa; border-radius: 8px; padding: 12px 16px; border-left: 3px solid #4A90D9; }
    .section-header { font-size: 15px; font-weight: 600; color: #1a1a2e; margin: 16px 0 8px 0; }
    .comment-box { background: #fff; border: 1px solid #e0e0e0; border-radius: 8px; padding: 16px; 
                   font-family: 'Malgun Gothic', sans-serif; font-size: 14px; line-height: 1.8;
                   white-space: pre-wrap; }
    .tag { display: inline-block; background: #e8f0fe; color: #1967d2; 
           border-radius: 4px; padding: 2px 8px; font-size: 12px; margin-right: 4px; }
</style>
""", unsafe_allow_html=True)

# ─── 매체 매핑 ─────────────────────────────────────────────────
MEDIA_MAP = {
    "Meta": {"campaigns": ["Meta"], "label": "Meta"},
    "TikTok": {"campaigns": ["TikTok"], "label": "TikTok"},
    "Kakao": {"campaigns": ["Kakao", "카카오"], "label": "Kakao"},
    "Criteo": {"campaigns": ["Criteo"], "label": "Criteo"},
    "Buzzvil": {"campaigns": ["Buzzvil"], "label": "Buzzvil"},
    "Naver SSA": {"campaigns": ["SSA"], "label": "Naver SSA"},
    "Naver BSA": {"campaigns": ["BSA"], "label": "Naver BSA"},
    "Naver ADVoost": {"campaigns": ["ADVoost"], "label": "Naver ADVoost"},
    "Push": {"campaigns": ["Push"], "label": "Push"},
    "전체": {"campaigns": [], "label": "전체"},
}

# 캠페인 카테고리 매핑 (캠페인(소) 기준)
CATEGORY_MAP = {
    "Naver": ["네이버", "BSA", "SSA", "ADVoost"],
    "OliveYoung": ["올리브영", "OliveYoung"],
}

# ─── 헬퍼 함수 ─────────────────────────────────────────────────
def fmt_won(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "0원"
    v = int(round(v))
    if abs(v) >= 10_000_000:
        return f"약 {v/10_000_000:.0f}천만 원" if v % 10_000_000 == 0 else f"약 {v/10_000:.0f}만 원"
    if abs(v) >= 10_000:
        return f"약 {v/10_000:.0f}만 원"
    return f"{v:,}원"

def fmt_roas(spend, revenue):
    if not spend or spend == 0:
        return "N/A"
    return f"{revenue/spend*100:.0f}%"

def fmt_pct_change(curr, prev):
    if not prev or prev == 0:
        return ""
    diff = (curr - prev) / prev * 100
    sign = "+" if diff >= 0 else ""
    return f"{sign}{diff:.0f}%p" if abs(diff) < 500 else ""

def load_raw_data(file):
    """Total_Raw 시트에서 데이터 로드 (메모리 효율적)"""
    wb = openpyxl.load_workbook(file, data_only=True, read_only=True)
    ws = wb['Total_Raw']
    
    rows = []
    headers = None
    for row in ws.iter_rows(values_only=True):
        if headers is None:
            headers = row
            continue
        if row[0] is None:
            continue
        rows.append(row)
    
    wb.close()
    df = pd.DataFrame(rows, columns=headers)
    df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
    df['비용'] = pd.to_numeric(df['비용'], errors='coerce').fillna(0)
    df['구매'] = pd.to_numeric(df['구매'], errors='coerce').fillna(0)
    df['매출액'] = pd.to_numeric(df['매출액'], errors='coerce').fillna(0)
    df['클릭'] = pd.to_numeric(df['클릭'], errors='coerce').fillna(0)
    df['노출'] = pd.to_numeric(df['노출'], errors='coerce').fillna(0)
    return df

def filter_by_media(df, media_key):
    """매체별 데이터 필터링"""
    if media_key == "전체":
        return df
    
    keywords = MEDIA_MAP[media_key]["campaigns"]
    mask = df['캠페인명'].str.contains('|'.join(keywords), case=False, na=False)
    return df[mask]

def get_daily_agg(df, target_date):
    """특정 날짜 집계"""
    d = df[df['날짜'].dt.date == target_date.date()]
    return {
        "spend": d['비용'].sum(),
        "purchase": d['구매'].sum(),
        "revenue": d['매출액'].sum(),
        "click": d['클릭'].sum(),
        "impression": d['노출'].sum(),
    }

def get_period_agg(df, start_date, end_date):
    """기간 집계"""
    d = df[(df['날짜'].dt.date >= start_date.date()) & (df['날짜'].dt.date <= end_date.date())]
    return {
        "spend": d['비용'].sum(),
        "purchase": d['구매'].sum(),
        "revenue": d['매출액'].sum(),
        "click": d['클릭'].sum(),
        "impression": d['노출'].sum(),
    }

def get_prev_week_avg(df, target_date):
    """전주 같은 요일 기준 7일 평균"""
    week_start = target_date - timedelta(days=target_date.weekday() + 7)
    week_end = week_start + timedelta(days=6)
    d = df[(df['날짜'].dt.date >= week_start.date()) & (df['날짜'].dt.date <= week_end.date())]
    if len(d) == 0:
        return None
    days = d['날짜'].dt.date.nunique()
    return {
        "spend": d['비용'].sum() / days if days else 0,
        "purchase": d['구매'].sum() / days if days else 0,
        "revenue": d['매출액'].sum() / days if days else 0,
        "roas": d['매출액'].sum() / d['비용'].sum() * 100 if d['비용'].sum() > 0 else 0,
    }

def build_topline_comment(media_label, target_date, today_data, prev_day_data, prev_week_avg, 
                           monthly_data, weekly_data, report_type="daily"):
    """탑라인 멘트 생성"""
    
    today_roas = fmt_roas(today_data['spend'], today_data['revenue'])
    
    # 전일 대비
    prev_spend_diff = ""
    prev_roas_prev = ""
    if prev_day_data and prev_day_data['spend'] > 0:
        roas_curr = today_data['revenue'] / today_data['spend'] * 100 if today_data['spend'] > 0 else 0
        roas_prev = prev_day_data['revenue'] / prev_day_data['spend'] * 100 if prev_day_data['spend'] > 0 else 0
        roas_diff = roas_curr - roas_prev
        if abs(roas_diff) > 5:
            sign = "+" if roas_diff >= 0 else ""
            prev_roas_prev = f" (전일 대비 ROAS {sign}{roas_diff:.0f}%p)"
    
    # 전주 대비
    prev_week_note = ""
    if prev_week_avg and prev_week_avg['spend'] > 0:
        roas_curr = today_data['revenue'] / today_data['spend'] * 100 if today_data['spend'] > 0 else 0
        roas_diff = roas_curr - prev_week_avg['roas']
        if abs(roas_diff) > 5:
            sign = "+" if roas_diff >= 0 else ""
            prev_week_note = f", 전주 평균 대비 ROAS {sign}{roas_diff:.0f}%p"
    
    date_str = target_date.strftime("%-m/%-d(%a)").replace(
        "Mon","월").replace("Tue","화").replace("Wed","수").replace(
        "Thu","목").replace("Fri","금").replace("Sat","토").replace("Sun","일")
    
    comment = f"* {media_label}\n"
    
    if report_type in ["daily", "both"]:
        comment += (f"- {date_str} 광고비 {fmt_won(today_data['spend'])} 소진, "
                   f"구매 건수 {int(today_data['purchase']):,}건 및 "
                   f"매출 {fmt_won(today_data['revenue'])} 확보 "
                   f"(ROAS {today_roas}){prev_roas_prev}{prev_week_note}\n")
    
    if report_type in ["weekly", "both"] and weekly_data:
        w_roas = fmt_roas(weekly_data['spend'], weekly_data['revenue'])
        comment += (f"- 주간 광고비 {fmt_won(weekly_data['spend'])} 소진, "
                   f"구매 건수 {int(weekly_data['purchase']):,}건 및 "
                   f"매출 {fmt_won(weekly_data['revenue'])} 확보 "
                   f"(ROAS {w_roas})\n")
    
    if monthly_data:
        m_roas = fmt_roas(monthly_data['spend'], monthly_data['revenue'])
        comment += (f"- {target_date.month}월 누적 광고비 {fmt_won(monthly_data['spend'])} 소진, "
                   f"구매 건수 {int(monthly_data['purchase']):,}건 및 "
                   f"매출 {fmt_won(monthly_data['revenue'])} 확보 "
                   f"(ROAS {m_roas})\n")
    
    return comment

def generate_ai_insight(api_key, today_data, prev_day_data, prev_week_avg, media_label, target_date, extra_note=""):
    """OpenAI로 특이사항 코멘트 생성"""
    try:
        client = openai.OpenAI(api_key=api_key)
        
        roas_today = today_data['revenue'] / today_data['spend'] * 100 if today_data['spend'] > 0 else 0
        roas_prev_day = prev_day_data['revenue'] / prev_day_data['spend'] * 100 if prev_day_data and prev_day_data['spend'] > 0 else 0
        roas_prev_week = prev_week_avg['roas'] if prev_week_avg else 0
        
        date_str = target_date.strftime("%m/%d")
        
        prompt = f"""당신은 디지털 광고 성과 분석 전문가입니다.
아래 데이터를 바탕으로 {media_label} 매체의 {date_str} 성과 특이사항을 한국어로 작성하세요.

[오늘({date_str}) 데이터]
- 광고비: {today_data['spend']:,.0f}원
- 구매건수: {today_data['purchase']:.0f}건
- 매출: {today_data['revenue']:,.0f}원
- ROAS: {roas_today:.0f}%

[전일 데이터]
- 광고비: {prev_day_data['spend']:,.0f}원 (변화: {((today_data['spend']-prev_day_data['spend'])/prev_day_data['spend']*100) if prev_day_data and prev_day_data['spend'] else 0:+.0f}%)
- ROAS: {roas_prev_day:.0f}% (변화: {roas_today-roas_prev_day:+.0f}%p)
- 구매건수: {prev_day_data['purchase']:.0f}건

[전주 일평균]
- 광고비: {prev_week_avg['spend']:,.0f}원
- ROAS: {roas_prev_week:.0f}%
- 구매건수: {prev_week_avg['purchase']:.0f}건

{f'[추가 메모] {extra_note}' if extra_note else ''}

작성 규칙:
1. 2~4줄 bullet point(ㄴ 또는 - 시작)로 핵심 특이사항만 작성
2. 전일 대비, 전주 대비 변화 중 유의미한 것만 언급
3. ROAS 기준으로 효율 개선/저하 원인 추정
4. 수치 표현은 "약 X만 원", "X%p 상승/하락" 형식 사용
5. 불필요한 인사말이나 서론 없이 바로 bullet point 시작
"""
        
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=400,
            temperature=0.3,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return f"(AI 분석 오류: {str(e)[:80]})"

# ─── 메인 UI ──────────────────────────────────────────────────
st.title("📊 BOJ 광고 코멘트 생성기")
st.caption("최종 리포트 xlsx를 업로드하면 탑라인 멘트 + 특이사항을 자동 생성합니다")

# 사이드바 - 설정
with st.sidebar:
    st.header("⚙️ 설정")
    
    api_key = st.text_input("OpenAI API Key", type="password", 
                             help="특이사항 AI 분석에 사용됩니다. 없으면 기본 템플릿만 생성됩니다.")
    
    st.divider()
    st.subheader("📅 날짜 설정")
    
    target_date = st.date_input("리포트 날짜", value=datetime.today() - timedelta(days=1))
    
    st.divider()
    st.subheader("📺 매체 선택")
    
    all_media = list(MEDIA_MAP.keys())
    selected_media = st.multiselect(
        "코멘트 생성할 매체",
        options=all_media,
        default=["Meta", "Naver BSA", "Naver SSA", "Naver ADVoost", "TikTok"],
    )
    
    report_type = st.radio("리포트 유형", ["일간", "주간+누적", "일간+주간+누적"], index=0)
    type_map = {"일간": "daily", "주간+누적": "weekly", "일간+주간+누적": "both"}
    rtype = type_map[report_type]

# 메인 - 파일 업로드
uploaded = st.file_uploader(
    "📂 최종 리포트 xlsx 업로드",
    type=["xlsx"],
    help="Total_Raw 시트가 포함된 통합 리포트 파일을 업로드하세요"
)

if not uploaded:
    st.info("👆 좌측 사이드바에서 설정 후, xlsx 파일을 업로드하세요.")
    
    with st.expander("💡 사용 방법"):
        st.markdown("""
        1. **사이드바**에서 리포트 날짜 및 매체 선택
        2. **xlsx 파일 업로드** (Total_Raw 시트 포함 필수)
        3. **코멘트 생성** 버튼 클릭
        4. 생성된 코멘트를 복사해서 리포트에 붙여넣기
        
        **생성되는 내용:**
        - 탑라인 멘트: 날짜, 광고비, 구매건수, 매출, ROAS
        - 전일 / 전주 평균 대비 변화 표시
        - AI 특이사항 (OpenAI API Key 입력 시)
        """)
    st.stop()

# 데이터 로드
with st.spinner("데이터 로딩 중..."):
    try:
        file_bytes = uploaded.read()
        df_raw = load_raw_data(io.BytesIO(file_bytes))
        
        available_dates = sorted(df_raw['날짜'].dt.date.dropna().unique())
        st.success(f"✅ 데이터 로드 완료 | 총 {len(df_raw):,}행 | 날짜 범위: {min(available_dates)} ~ {max(available_dates)}")
    except Exception as e:
        st.error(f"파일 로드 실패: {e}")
        st.stop()

# 추가 메모
with st.expander("📝 매체별 추가 메모 (선택사항 — AI 분석 힌트용)"):
    media_notes = {}
    cols = st.columns(2)
    for i, m in enumerate(selected_media):
        with cols[i % 2]:
            media_notes[m] = st.text_area(f"{m}", height=80, key=f"note_{m}",
                                           placeholder="특이사항 힌트 (소재명, 이벤트 등)")

# 코멘트 생성
if st.button("🚀 코멘트 생성", type="primary", use_container_width=True):
    if not selected_media:
        st.warning("매체를 하나 이상 선택하세요.")
        st.stop()
    
    target_dt = datetime.combine(target_date, datetime.min.time())
    prev_day = target_dt - timedelta(days=1)
    
    # 월 누적 기간
    month_start = target_dt.replace(day=1)
    
    # 주간 기간 (해당 주 월~target_date)
    week_start = target_dt - timedelta(days=target_dt.weekday())
    
    all_comments = []
    
    progress = st.progress(0)
    
    for idx, media_key in enumerate(selected_media):
        st.markdown(f"### {MEDIA_MAP[media_key]['label']}")
        
        df_media = filter_by_media(df_raw, media_key)
        
        if len(df_media) == 0:
            st.warning(f"{media_key}: 해당 데이터 없음")
            continue
        
        today_data = get_daily_agg(df_media, target_dt)
        prev_day_data = get_daily_agg(df_media, prev_day)
        prev_week_avg = get_prev_week_avg(df_media, target_dt)
        monthly_data = get_period_agg(df_media, month_start, target_dt)
        weekly_data = get_period_agg(df_media, week_start, target_dt) if rtype in ["weekly", "both"] else None
        
        # 지표 미리보기
        c1, c2, c3, c4 = st.columns(4)
        roas_val = today_data['revenue'] / today_data['spend'] * 100 if today_data['spend'] > 0 else 0
        c1.metric("광고비", fmt_won(today_data['spend']))
        c2.metric("구매건수", f"{int(today_data['purchase']):,}건")
        c3.metric("매출", fmt_won(today_data['revenue']))
        c4.metric("ROAS", f"{roas_val:.0f}%",
                  delta=f"{roas_val - (prev_week_avg['roas'] if prev_week_avg else 0):+.0f}%p vs 전주" 
                  if prev_week_avg else None)
        
        # 탑라인 멘트 생성
        topline = build_topline_comment(
            media_label=MEDIA_MAP[media_key]['label'],
            target_date=target_dt,
            today_data=today_data,
            prev_day_data=prev_day_data,
            prev_week_avg=prev_week_avg,
            monthly_data=monthly_data,
            weekly_data=weekly_data,
            report_type=rtype,
        )
        
        # AI 특이사항
        insight = ""
        if api_key and today_data['spend'] > 0:
            with st.spinner(f"{media_key} AI 분석 중..."):
                insight = generate_ai_insight(
                    api_key=api_key,
                    today_data=today_data,
                    prev_day_data=prev_day_data if prev_day_data['spend'] > 0 else {"spend":0,"purchase":0,"revenue":0},
                    prev_week_avg=prev_week_avg if prev_week_avg else {"spend":0,"purchase":0,"revenue":0,"roas":0},
                    media_label=MEDIA_MAP[media_key]['label'],
                    target_date=target_dt,
                    extra_note=media_notes.get(media_key, ""),
                )
        
        full_comment = topline
        if insight:
            full_comment += insight + "\n"
        
        st.text_area(
            "생성된 코멘트",
            value=full_comment,
            height=200,
            key=f"comment_{media_key}",
        )
        
        all_comments.append(f"[{MEDIA_MAP[media_key]['label']}]\n{full_comment}")
        progress.progress((idx + 1) / len(selected_media))
        st.divider()
    
    # 전체 통합 복사용
    if all_comments:
        st.subheader("📋 전체 통합 코멘트 (복사용)")
        st.text_area(
            "전체",
            value="\n\n".join(all_comments),
            height=400,
            key="all_comments",
        )
    
    progress.empty()
