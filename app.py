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

pages/
"""
BOJ 광고 리포트 대시보드
- 매체별 RAW 파일 업로드 → 현재 엑셀 리포트와 동일한 구조의 대시보드 생성
- Sales Overview / 매체별 시트 (그룹별·소재별·주차별·일자별)
"""

import io
import pandas as pd
import streamlit as st
from datetime import datetime

st.set_page_config(page_title="BOJ | 리포트 대시보드", layout="wide", page_icon="📊")

# ─── 공통 스타일 ───────────────────────────────────────────────
st.markdown("""
<style>
  .block-container { padding-top: 1.5rem; }
  .section-title {
    font-size: 13px; font-weight: 600; color: var(--text-color);
    background: #f0f2f6; border-left: 3px solid #4A90D9;
    padding: 6px 12px; border-radius: 0 6px 6px 0; margin: 20px 0 10px 0;
  }
  .kpi-label { font-size: 11px; color: #888; margin-bottom: 2px; }
  .kpi-val   { font-size: 20px; font-weight: 700; color: #1a1a2e; }
  .kpi-sub   { font-size: 11px; color: #aaa; }
  div[data-testid="stDataFrame"] { border: 1px solid #e8e8e8; border-radius: 8px; overflow: hidden; }
  .upload-box { border: 1.5px dashed #c0c8d8; border-radius: 10px; padding: 16px;
                background: #fafbfd; margin-bottom: 12px; }
  .badge { display:inline-block; padding:2px 8px; border-radius:4px; font-size:11px;
           font-weight:600; margin-right:4px; }
  .badge-ok  { background:#e6f4ea; color:#1e7e34; }
  .badge-err { background:#fce8e6; color:#c62828; }
  .badge-warn{ background:#fff8e1; color:#e65100; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
# 1. 매체별 컬럼 매핑 정의
#    각 매체 RAW 파일의 실제 컬럼명 → 표준 컬럼명으로 매핑
#    매체를 추가하거나 컬럼명이 바뀌면 여기만 수정
# ══════════════════════════════════════════════════════════════

# 표준 컬럼 (공통 계산 기준)
STD_COLS = {
    "date":       "날짜",
    "campaign":   "캠페인명",
    "adgroup":    "광고그룹명",
    "ad":         "광고명/소재명",
    "spend":      "비용",
    "impression": "노출",
    "click":      "클릭",
    "purchase":   "구매",
    "revenue":    "매출액",
    "cart":       "장바구니",
}

# 매체별 컬럼 매핑 딕셔너리
# key: 표준 컬럼명, value: 해당 매체 RAW 파일의 실제 컬럼명 (없으면 None)
MEDIA_COL_MAP = {
    "Meta": {
        "date":       ["날짜", "Date", "일자", "기간"],
        "campaign":   ["캠페인 이름", "캠페인명", "Campaign name"],
        "adgroup":    ["광고 세트 이름", "광고세트명", "Ad set name"],
        "ad":         ["광고 이름", "광고명", "Ad name"],
        "spend":      ["금액 (KRW)", "비용", "Amount spent (KRW)", "Spend"],
        "impression": ["노출", "Impressions"],
        "click":      ["링크 클릭수", "클릭수", "Link clicks"],
        "purchase":   ["구매", "웹사이트 구매", "Purchases"],
        "revenue":    ["구매 전환값", "매출액", "Purchase conversion value"],
        "cart":       ["장바구니에 담기", "Add to cart"],
    },
    "TikTok": {
        "date":       ["날짜", "Date", "일자"],
        "campaign":   ["캠페인", "Campaign Name"],
        "adgroup":    ["광고그룹", "Ad Group Name"],
        "ad":         ["광고", "Ad Name"],
        "spend":      ["비용", "Cost", "Spend"],
        "impression": ["노출수", "Impressions"],
        "click":      ["클릭수", "Clicks"],
        "purchase":   ["완료된 결제", "구매", "Complete Payment"],
        "revenue":    ["완료된 결제 가치", "매출", "Complete Payment Value"],
        "cart":       ["장바구니 담기", "Add to Cart"],
    },
    "Kakao": {
        "date":       ["날짜", "일자", "기간"],
        "campaign":   ["캠페인", "캠페인명"],
        "adgroup":    ["광고그룹", "광고그룹명"],
        "ad":         ["소재", "소재명"],
        "spend":      ["비용", "청구금액"],
        "impression": ["노출수", "노출"],
        "click":      ["클릭수", "클릭"],
        "purchase":   ["구매수", "구매"],
        "revenue":    ["구매금액", "매출액"],
        "cart":       ["장바구니", "장바구니수"],
    },
    "Criteo": {
        "date":       ["날짜", "Date"],
        "campaign":   ["캠페인", "Campaign"],
        "adgroup":    ["광고세트", "Ad Set"],
        "ad":         ["광고", "Ad"],
        "spend":      ["비용", "Cost", "Spend"],
        "impression": ["노출", "Displays"],
        "click":      ["클릭", "Clicks"],
        "purchase":   ["구매", "Sales"],
        "revenue":    ["매출", "Revenue"],
        "cart":       ["장바구니", None],
    },
    "Buzzvil": {
        "date":       ["날짜", "일자"],
        "campaign":   ["캠페인명"],
        "adgroup":    ["광고그룹명"],
        "ad":         ["소재명"],
        "spend":      ["비용"],
        "impression": ["노출"],
        "click":      ["클릭"],
        "purchase":   ["구매"],
        "revenue":    ["매출"],
        "cart":       [None],
    },
    "Naver SSA": {
        "date":       ["날짜", "일자"],
        "campaign":   ["캠페인명"],
        "adgroup":    ["광고그룹명"],
        "ad":         ["광고명"],
        "spend":      ["비용", "광고비"],
        "impression": ["노출수", "노출"],
        "click":      ["클릭수", "클릭"],
        "purchase":   ["구매수", "전환수", "구매"],
        "revenue":    ["구매금액", "전환매출", "매출액"],
        "cart":       ["장바구니", None],
    },
    "Naver BSA": {
        "date":       ["날짜", "일자"],
        "campaign":   ["캠페인명"],
        "adgroup":    ["광고그룹명"],
        "ad":         ["광고명"],
        "spend":      ["비용", "광고비"],
        "impression": ["노출수", "노출"],
        "click":      ["클릭수", "클릭"],
        "purchase":   ["구매수", "전환수"],
        "revenue":    ["구매금액", "전환매출"],
        "cart":       ["장바구니", None],
    },
    "Naver ADVoost": {
        "date":       ["날짜", "일자"],
        "campaign":   ["캠페인명"],
        "adgroup":    ["광고그룹명"],
        "ad":         ["광고명"],
        "spend":      ["비용", "광고비"],
        "impression": ["노출수", "노출"],
        "click":      ["클릭수", "클릭"],
        "purchase":   ["구매수", "전환수"],
        "revenue":    ["구매금액", "전환매출"],
        "cart":       [None],
    },
    "Push": {
        "date":       ["날짜", "일자"],
        "campaign":   ["캠페인명"],
        "adgroup":    ["광고그룹명"],
        "ad":         ["광고명"],
        "spend":      ["비용"],
        "impression": ["노출"],
        "click":      ["클릭"],
        "purchase":   ["구매"],
        "revenue":    ["매출"],
        "cart":       [None],
    },
}

MEDIA_LIST = list(MEDIA_COL_MAP.keys())

# ══════════════════════════════════════════════════════════════
# 2. 유틸 함수
# ══════════════════════════════════════════════════════════════

def fmt_krw(v, short=True):
    if v is None or (isinstance(v, float) and pd.isna(v)) or v == 0:
        return "-"
    v = int(round(v))
    if short:
        if abs(v) >= 100_000_000: return f"{v/100_000_000:.1f}억"
        if abs(v) >= 10_000:      return f"{v/10_000:.0f}만"
    return f"{v:,}"

def fmt_pct(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return "-"
    return f"{v:.1f}%"

def fmt_num(v, dec=0):
    if v is None or (isinstance(v, float) and pd.isna(v)): return "-"
    return f"{v:,.{dec}f}"

def safe_div(a, b, pct=False, mult=1):
    try:
        r = a / b * mult
        return r if not pd.isna(r) else None
    except:
        return None

def detect_col(df_cols, candidates):
    """후보 컬럼명 리스트에서 실제로 존재하는 컬럼 반환"""
    for c in candidates:
        if c and c in df_cols:
            return c
    return None

def normalize_df(df_raw, media):
    """매체 RAW DataFrame → 표준 컬럼 DataFrame으로 변환"""
    mapping = MEDIA_COL_MAP.get(media, {})
    cols = df_raw.columns.tolist()
    result = {}
    missing = []

    for std_key, std_name in STD_COLS.items():
        candidates = mapping.get(std_key, [])
        found = detect_col(cols, candidates)
        if found:
            result[std_name] = df_raw[found]
        else:
            result[std_name] = pd.Series([None] * len(df_raw))
            if std_key not in ["cart", "ad"]:  # 없어도 되는 컬럼
                missing.append(std_name)

    df = pd.DataFrame(result)

    # 날짜 파싱
    df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce")

    # 숫자 컬럼 정리
    for col in ["비용", "노출", "클릭", "구매", "매출액", "장바구니"]:
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", "").str.replace("₩", "").str.strip(),
            errors="coerce"
        ).fillna(0)

    return df, missing

def calc_kpis(df):
    """표준 DataFrame → KPI 집계"""
    spend  = df["비용"].sum()
    imp    = df["노출"].sum()
    click  = df["클릭"].sum()
    pur    = df["구매"].sum()
    rev    = df["매출액"].sum()

    return {
        "spend":    spend,
        "imp":      imp,
        "click":    click,
        "pur":      pur,
        "rev":      rev,
        "ctr":      safe_div(click, imp, pct=True, mult=100),
        "cpc":      safe_div(spend, click),
        "cpm":      safe_div(spend, imp, mult=1000),
        "cpa":      safe_div(spend, pur),
        "cvr":      safe_div(pur, click, pct=True, mult=100),
        "aov":      safe_div(rev, pur),
        "roas":     safe_div(rev, spend, pct=True, mult=100),
    }

def kpi_table(df_grp):
    """집계된 그룹 DataFrame에서 KPI 컬럼 추가한 표 반환"""
    rows = []
    for _, row in df_grp.iterrows():
        spend = row.get("비용", 0) or 0
        imp   = row.get("노출", 0) or 0
        click = row.get("클릭", 0) or 0
        pur   = row.get("구매", 0) or 0
        rev   = row.get("매출액", 0) or 0
        r = {k: row[k] for k in df_grp.columns if k not in ["비용","노출","클릭","구매","매출액","장바구니"]}
        r.update({
            "비용":     int(spend),
            "노출":     int(imp),
            "클릭":     int(click),
            "CTR":      f"{click/imp*100:.2f}%" if imp else "-",
            "CPC":      f"{spend/click:,.0f}" if click else "-",
            "CPM":      f"{spend/imp*1000:,.0f}" if imp else "-",
            "구매":     int(pur),
            "CPA":      f"{spend/pur:,.0f}" if pur else "-",
            "CVR":      f"{pur/click*100:.2f}%" if click else "-",
            "매출액":   int(rev),
            "AOV":      f"{rev/pur:,.0f}" if pur else "-",
            "ROAS":     f"{rev/spend*100:.0f}%" if spend else "-",
        })
        rows.append(r)
    return pd.DataFrame(rows)

def section(title):
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)

def kpi_row(kpis):
    cols = st.columns(6)
    items = [
        ("광고비",    fmt_krw(kpis["spend"]) + "만"),
        ("노출",      fmt_num(kpis["imp"])),
        ("클릭",      fmt_num(kpis["click"])),
        ("구매",      fmt_num(kpis["pur"])),
        ("ROAS",      fmt_pct(kpis["roas"])),
        ("매출",      fmt_krw(kpis["rev"]) + "만"),
    ]
    for col, (label, val) in zip(cols, items):
        col.metric(label, val)

def show_table(df, height=320):
    st.dataframe(df, use_container_width=True, height=height, hide_index=True)


# ══════════════════════════════════════════════════════════════
# 3. 세션 상태 초기화
# ══════════════════════════════════════════════════════════════

if "media_data" not in st.session_state:
    st.session_state.media_data = {}   # {매체명: DataFrame(표준화)}
if "media_warnings" not in st.session_state:
    st.session_state.media_warnings = {}  # {매체명: [missing cols]}

# ══════════════════════════════════════════════════════════════
# 4. UI — 사이드바: RAW 파일 업로드
# ══════════════════════════════════════════════════════════════

with st.sidebar:
    st.header("📂 RAW 데이터 업로드")
    st.caption("매체별 RAW 파일을 업로드하면 리포트가 자동 생성됩니다")

    for media in MEDIA_LIST:
        with st.expander(media, expanded=False):
            uploaded = st.file_uploader(
                f"{media} RAW",
                type=["xlsx", "csv"],
                key=f"raw_{media}",
                label_visibility="collapsed",
            )
            if uploaded:
                try:
                    if uploaded.name.endswith(".csv"):
                        df_raw = pd.read_csv(uploaded, encoding="utf-8-sig")
                    else:
                        df_raw = pd.read_excel(uploaded, sheet_name=0)

                    df_norm, missing = normalize_df(df_raw, media)
                    st.session_state.media_data[media] = df_norm
                    st.session_state.media_warnings[media] = missing

                    if missing:
                        st.warning(f"미매핑 컬럼: {', '.join(missing)}")
                    else:
                        st.success(f"✅ {len(df_norm):,}행 로드")
                except Exception as e:
                    st.error(f"오류: {e}")

    st.divider()

    # 업로드 현황 요약
    st.caption("**업로드 현황**")
    for media in MEDIA_LIST:
        if media in st.session_state.media_data:
            warn = st.session_state.media_warnings.get(media, [])
            badge = "🟡" if warn else "🟢"
            cnt = len(st.session_state.media_data[media])
            st.caption(f"{badge} {media} — {cnt:,}행")
        else:
            st.caption(f"⚪ {media} — 미업로드")

    st.divider()
    if st.button("🗑️ 전체 초기화", use_container_width=True):
        st.session_state.media_data = {}
        st.session_state.media_warnings = {}
        st.rerun()


# ══════════════════════════════════════════════════════════════
# 5. 메인 — 탭 구조
# ══════════════════════════════════════════════════════════════

st.title("📊 BOJ 광고 리포트 대시보드")
st.caption("매체별 RAW 파일을 좌측 사이드바에 업로드하세요. Excel / CSV 모두 지원합니다.")

if not st.session_state.media_data:
    st.info("👈 사이드바에서 매체별 RAW 파일을 업로드하면 리포트가 생성됩니다.")

    # 컬럼 매핑 안내 테이블
    with st.expander("📋 매체별 컬럼 매핑 현황 (RAW 파일 준비 시 참고)"):
        rows = []
        for media, mapping in MEDIA_COL_MAP.items():
            for std_key, std_name in STD_COLS.items():
                candidates = mapping.get(std_key, [])
                valid = [c for c in candidates if c]
                rows.append({
                    "매체": media,
                    "표준 컬럼": std_name,
                    "인식 가능한 RAW 컬럼명": " / ".join(valid) if valid else "❌ 미정의",
                })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, height=400, hide_index=True)
    st.stop()

# 전체 데이터 합산
all_dfs = list(st.session_state.media_data.values())
df_all = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()


# ── 탭 생성 ──
loaded_media = list(st.session_state.media_data.keys())
tab_labels = ["📈 Sales Overview"] + [f"📺 {m}" for m in loaded_media] + ["⚙️ 컬럼 매핑 설정"]
tabs = st.tabs(tab_labels)


# ══════════════════════════════════════════════════════════════
# TAB 0 — Sales Overview
# ══════════════════════════════════════════════════════════════

with tabs[0]:
    st.subheader("Sales Campaign Overview")

    if df_all.empty:
        st.warning("업로드된 데이터가 없습니다.")
    else:
        # ── 기간 필터 ──────────────────────────────────────────
        valid_dates = df_all["날짜"].dropna()
        if not valid_dates.empty:
            min_d, max_d = valid_dates.min().date(), valid_dates.max().date()
            col_f1, col_f2, col_f3 = st.columns([2, 2, 4])
            with col_f1:
                sel_start = st.date_input("시작일", value=min_d, min_value=min_d, max_value=max_d, key="s_start")
            with col_f2:
                sel_end   = st.date_input("종료일", value=max_d, min_value=min_d, max_value=max_d, key="s_end")

            mask = (df_all["날짜"].dt.date >= sel_start) & (df_all["날짜"].dt.date <= sel_end)
            df_f = df_all[mask].copy()
        else:
            df_f = df_all.copy()

        # ── 전체 KPI ───────────────────────────────────────────
        section("▣ 전체 KPI")
        kpis_total = calc_kpis(df_f)
        kpi_row(kpis_total)

        st.divider()

        # ── 월별 성과 ──────────────────────────────────────────
        section("▣ Sales Campaign Overview — 월별 성과")
        if not df_f.empty and df_f["날짜"].notna().any():
            df_f["월"] = df_f["날짜"].dt.to_period("M").astype(str)
            grp_month = df_f.groupby("월").agg(
                비용=("비용","sum"), 노출=("노출","sum"), 클릭=("클릭","sum"),
                구매=("구매","sum"), 매출액=("매출액","sum")
            ).reset_index()
            show_table(kpi_table(grp_month))

        st.divider()

        # ── 매체별 월간 성과 ───────────────────────────────────
        section("▣ 매체 별 성과 — 월별")
        media_rows = []
        for media, df_m in st.session_state.media_data.items():
            mask2 = (df_m["날짜"].dt.date >= sel_start) & (df_m["날짜"].dt.date <= sel_end)
            df_mf = df_m[mask2] if not valid_dates.empty else df_m
            if df_mf.empty:
                continue
            df_mf = df_mf.copy()
            df_mf["월"] = df_mf["날짜"].dt.to_period("M").astype(str)
            for month, grp in df_mf.groupby("월"):
                k = calc_kpis(grp)
                media_rows.append({
                    "매체": media, "월": month,
                    "비용": int(k["spend"]), "노출": int(k["imp"]), "클릭": int(k["click"]),
                    "CTR": fmt_pct(k["ctr"]), "CPC": fmt_num(k["cpc"]),
                    "CPM": fmt_num(k["cpm"]),
                    "구매": int(k["pur"]), "CPA": fmt_num(k["cpa"]),
                    "CVR": fmt_pct(k["cvr"]), "매출액": int(k["rev"]),
                    "AOV": fmt_num(k["aov"]), "ROAS": fmt_pct(k["roas"]),
                })
        if media_rows:
            show_table(pd.DataFrame(media_rows))

        st.divider()

        # ── 매체별 주차별 성과 ────────────────────────────────
        section("▣ 매체 별 성과 — 주차별")
        week_rows = []
        for media, df_m in st.session_state.media_data.items():
            mask2 = (df_m["날짜"].dt.date >= sel_start) & (df_m["날짜"].dt.date <= sel_end)
            df_mf = df_m[mask2] if not valid_dates.empty else df_m
            if df_mf.empty:
                continue
            df_mf = df_mf.copy()
            df_mf["주차"] = df_mf["날짜"].dt.to_period("W").apply(
                lambda p: f"{p.start_time.strftime('%m/%d')}~{p.end_time.strftime('%m/%d')}"
            )
            df_mf["주차정렬"] = df_mf["날짜"].dt.to_period("W").astype(str)
            for (week_sort, week_label), grp in df_mf.groupby(["주차정렬", "주차"]):
                k = calc_kpis(grp)
                week_rows.append({
                    "매체": media, "주차": week_label,
                    "비용": int(k["spend"]), "노출": int(k["imp"]), "클릭": int(k["click"]),
                    "CTR": fmt_pct(k["ctr"]), "CPC": fmt_num(k["cpc"]), "CPM": fmt_num(k["cpm"]),
                    "구매": int(k["pur"]), "CPA": fmt_num(k["cpa"]),
                    "CVR": fmt_pct(k["cvr"]), "매출액": int(k["rev"]),
                    "AOV": fmt_num(k["aov"]), "ROAS": fmt_pct(k["roas"]),
                    "_sort": week_sort,
                })
        if week_rows:
            df_week = pd.DataFrame(week_rows).sort_values("_sort").drop(columns=["_sort"])
            show_table(df_week)

        st.divider()

        # ── 매체별 일자별 성과 ────────────────────────────────
        section("▣ 매체 별 성과 — 일자별")
        day_rows = []
        for media, df_m in st.session_state.media_data.items():
            mask2 = (df_m["날짜"].dt.date >= sel_start) & (df_m["날짜"].dt.date <= sel_end)
            df_mf = df_m[mask2] if not valid_dates.empty else df_m
            if df_mf.empty:
                continue
            df_mf = df_mf.copy()
            df_mf["날짜str"] = df_mf["날짜"].dt.strftime("%Y-%m-%d")
            for date_str, grp in df_mf.groupby("날짜str"):
                k = calc_kpis(grp)
                day_rows.append({
                    "날짜": date_str, "매체": media,
                    "비용": int(k["spend"]), "노출": int(k["imp"]), "클릭": int(k["click"]),
                    "CTR": fmt_pct(k["ctr"]), "CPC": fmt_num(k["cpc"]), "CPM": fmt_num(k["cpm"]),
                    "구매": int(k["pur"]), "CPA": fmt_num(k["cpa"]),
                    "CVR": fmt_pct(k["cvr"]), "매출액": int(k["rev"]),
                    "AOV": fmt_num(k["aov"]), "ROAS": fmt_pct(k["roas"]),
                })
        if day_rows:
            df_day = pd.DataFrame(day_rows).sort_values(["날짜", "매체"])

            # 매체 필터 (슬라이서 역할)
            all_media_opts = ["전체"] + sorted(df_day["매체"].unique().tolist())
            sel_media = st.selectbox("매체 선택", all_media_opts, key="day_media_filter")
            if sel_media != "전체":
                df_day = df_day[df_day["매체"] == sel_media]
            show_table(df_day, height=400)


# ══════════════════════════════════════════════════════════════
# TAB 1~N — 매체별 탭
# ══════════════════════════════════════════════════════════════

for tab_idx, media in enumerate(loaded_media):
    with tabs[tab_idx + 1]:
        df_m = st.session_state.media_data[media].copy()
        warn = st.session_state.media_warnings.get(media, [])

        st.subheader(f"{media} 성과 리포트")
        if warn:
            st.warning(f"⚠️ 미매핑 컬럼 (데이터 없음): {', '.join(warn)}")

        # 기간 필터
        valid_d = df_m["날짜"].dropna()
        if not valid_d.empty:
            min_d2, max_d2 = valid_d.min().date(), valid_d.max().date()
            c1, c2, _ = st.columns([2, 2, 4])
            with c1: s = st.date_input("시작일", min_d2, min_value=min_d2, max_value=max_d2, key=f"{media}_s")
            with c2: e = st.date_input("종료일", max_d2, min_value=min_d2, max_value=max_d2, key=f"{media}_e")
            mask = (df_m["날짜"].dt.date >= s) & (df_m["날짜"].dt.date <= e)
            df_mf = df_m[mask].copy()
        else:
            df_mf = df_m.copy()

        # KPI 헤더
        kpis_m = calc_kpis(df_mf)
        kpi_row(kpis_m)
        st.divider()

        # 매체별 내부 탭
        inner_tabs = st.tabs(["그룹별 성과", "소재별 성과", "주차별 성과", "일자별 성과"])

        # ── 그룹별 ─────────────────────────────────────────────
        with inner_tabs[0]:
            section(f"▣ {media} — 그룹별 성과")
            grp_col = "광고그룹명"
            if grp_col in df_mf.columns and df_mf[grp_col].notna().any():
                grp = df_mf.groupby(grp_col).agg(
                    비용=("비용","sum"), 노출=("노출","sum"), 클릭=("클릭","sum"),
                    구매=("구매","sum"), 매출액=("매출액","sum")
                ).reset_index()
                show_table(kpi_table(grp))
            else:
                st.info("광고그룹명 컬럼이 매핑되지 않았습니다.")

        # ── 소재별 ─────────────────────────────────────────────
        with inner_tabs[1]:
            section(f"▣ {media} — 소재별 성과")
            ad_col = "광고명/소재명"
            if ad_col in df_mf.columns and df_mf[ad_col].notna().any():
                grp = df_mf.groupby(ad_col).agg(
                    비용=("비용","sum"), 노출=("노출","sum"), 클릭=("클릭","sum"),
                    구매=("구매","sum"), 매출액=("매출액","sum")
                ).reset_index()
                show_table(kpi_table(grp), height=400)
            else:
                st.info("소재명 컬럼이 매핑되지 않았습니다.")

        # ── 주차별 ─────────────────────────────────────────────
        with inner_tabs[2]:
            section(f"▣ {media} — 주차별 성과")
            if df_mf["날짜"].notna().any():
                df_mf["주차"] = df_mf["날짜"].dt.to_period("W").apply(
                    lambda p: f"{p.start_time.strftime('%m/%d')}~{p.end_time.strftime('%m/%d')}"
                )
                df_mf["_wsort"] = df_mf["날짜"].dt.to_period("W").astype(str)
                grp = df_mf.groupby(["_wsort", "주차"]).agg(
                    비용=("비용","sum"), 노출=("노출","sum"), 클릭=("클릭","sum"),
                    구매=("구매","sum"), 매출액=("매출액","sum")
                ).reset_index().sort_values("_wsort").drop(columns=["_wsort"])
                show_table(kpi_table(grp))
            else:
                st.info("날짜 데이터가 없습니다.")

        # ── 일자별 ─────────────────────────────────────────────
        with inner_tabs[3]:
            section(f"▣ {media} — 일자별 성과")
            if df_mf["날짜"].notna().any():
                df_mf["날짜str"] = df_mf["날짜"].dt.strftime("%Y-%m-%d")
                grp = df_mf.groupby("날짜str").agg(
                    비용=("비용","sum"), 노출=("노출","sum"), 클릭=("클릭","sum"),
                    구매=("구매","sum"), 매출액=("매출액","sum")
                ).reset_index().rename(columns={"날짜str": "날짜"})
                show_table(kpi_table(grp), height=400)
            else:
                st.info("날짜 데이터가 없습니다.")


# ══════════════════════════════════════════════════════════════
# TAB 마지막 — 컬럼 매핑 설정 (RAW 컬럼 확인 & 수동 매핑)
# ══════════════════════════════════════════════════════════════

with tabs[-1]:
    st.subheader("⚙️ 컬럼 매핑 확인 및 커스텀 업로드")
    st.markdown("""
    RAW 파일의 컬럼명이 자동 인식되지 않을 때 여기서 직접 매핑할 수 있습니다.  
    매핑 후 **적용** 버튼을 누르면 해당 매체 데이터가 재처리됩니다.
    """)

    if not st.session_state.media_data:
        st.info("먼저 사이드바에서 RAW 파일을 업로드하세요.")
    else:
        target_media = st.selectbox("수정할 매체", loaded_media, key="map_media")
        df_sample = st.session_state.media_data[target_media]
        warn_cols = st.session_state.media_warnings.get(target_media, [])

        st.markdown(f"**현재 미매핑 컬럼:** {', '.join(warn_cols) if warn_cols else '없음 (정상)'}")

        st.caption("현재 데이터 미리보기 (표준화 후):")
        st.dataframe(df_sample.head(5), use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("**RAW 파일 재업로드 + 수동 컬럼 지정**")
        re_uploaded = st.file_uploader(
            f"{target_media} RAW 재업로드",
            type=["xlsx", "csv"],
            key=f"remap_{target_media}",
        )
        if re_uploaded:
            if re_uploaded.name.endswith(".csv"):
                df_re = pd.read_csv(re_uploaded, encoding="utf-8-sig")
            else:
                df_re = pd.read_excel(re_uploaded, sheet_name=0)

            raw_cols = ["(없음)"] + df_re.columns.tolist()
            st.caption("RAW 파일 컬럼:")
            st.write(df_re.columns.tolist())

            st.markdown("**컬럼 직접 매핑:**")
            manual_map = {}
            col_pairs = st.columns(2)
            for i, (std_key, std_name) in enumerate(STD_COLS.items()):
                with col_pairs[i % 2]:
                    sel = st.selectbox(
                        f"{std_name}",
                        raw_cols,
                        key=f"manual_{target_media}_{std_key}",
                    )
                    if sel != "(없음)":
                        manual_map[std_key] = sel

            if st.button("✅ 매핑 적용", type="primary"):
                result = {}
                missing2 = []
                for std_key, std_name in STD_COLS.items():
                    if std_key in manual_map:
                        result[std_name] = df_re[manual_map[std_key]]
                    else:
                        result[std_name] = pd.Series([None] * len(df_re))
                        if std_key not in ["cart", "ad"]:
                            missing2.append(std_name)

                df_new = pd.DataFrame(result)
                df_new["날짜"] = pd.to_datetime(df_new["날짜"], errors="coerce")
                for col in ["비용","노출","클릭","구매","매출액","장바구니"]:
                    df_new[col] = pd.to_numeric(
                        df_new[col].astype(str).str.replace(",","").str.replace("₩","").str.strip(),
                        errors="coerce"
                    ).fillna(0)

                st.session_state.media_data[target_media] = df_new
                st.session_state.media_warnings[target_media] = missing2
                st.success("✅ 매핑 적용 완료! 리포트 탭을 확인하세요.")
                st.rerun()

        # 전체 컬럼 매핑 현황 테이블
        st.divider()
        st.markdown("**전체 매체 컬럼 매핑 현황**")
        map_rows = []
        for m, mapping in MEDIA_COL_MAP.items():
            for std_key, std_name in STD_COLS.items():
                candidates = [c for c in mapping.get(std_key, []) if c]
                map_rows.append({
                    "매체": m,
                    "표준 컬럼": std_name,
                    "인식 가능한 컬럼명": " / ".join(candidates) if candidates else "❌",
                })
        st.dataframe(pd.DataFrame(map_rows), use_container_width=True, height=400, hide_index=True)
