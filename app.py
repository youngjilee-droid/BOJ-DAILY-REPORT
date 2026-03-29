import io
import openpyxl
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# ══════════════════════════════════════════════════════════════
# 페이지 설정 & 공통 스타일
# ══════════════════════════════════════════════════════════════
st.set_page_config(page_title="BOJ 광고 대시보드", layout="wide", page_icon="📊")

st.markdown("""
<style>
  .block-container { padding-top: 1.5rem; }
  .stTextArea textarea { font-family: 'Malgun Gothic', sans-serif; font-size: 14px; line-height: 1.7; }
  .section-title {
    font-size: 13px; font-weight: 600;
    background: #f0f2f6; border-left: 3px solid #4A90D9;
    padding: 6px 12px; border-radius: 0 6px 6px 0; margin: 20px 0 10px 0;
  }
  div[data-testid="stDataFrame"] { border: 1px solid #e8e8e8; border-radius: 8px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
# 공통 상수
# ══════════════════════════════════════════════════════════════

# 코멘트 생성기용 매체 매핑 (Total_Raw 캠페인명 키워드 기준)
COMMENT_MEDIA_MAP = {
    "Meta":          {"campaigns": ["Meta"],           "label": "Meta"},
    "TikTok":        {"campaigns": ["TikTok"],          "label": "TikTok"},
    "Kakao":         {"campaigns": ["Kakao", "카카오"], "label": "Kakao"},
    "Criteo":        {"campaigns": ["Criteo"],          "label": "Criteo"},
    "Buzzvil":       {"campaigns": ["Buzzvil"],         "label": "Buzzvil"},
    "Naver SSA":     {"campaigns": ["SSA"],             "label": "Naver SSA"},
    "Naver BSA":     {"campaigns": ["BSA"],             "label": "Naver BSA"},
    "Naver ADVoost": {"campaigns": ["ADVoost"],         "label": "Naver ADVoost"},
    "Push":          {"campaigns": ["Push"],            "label": "Push"},
    "전체":           {"campaigns": [],                 "label": "전체"},
}

# 대시보드용 표준 컬럼
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

# 대시보드용 매체별 RAW 컬럼 매핑
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

DASHBOARD_MEDIA_LIST = list(MEDIA_COL_MAP.keys())

# ══════════════════════════════════════════════════════════════
# 세션 상태 초기화
# ══════════════════════════════════════════════════════════════
if "media_data" not in st.session_state:
    st.session_state.media_data = {}
if "media_warnings" not in st.session_state:
    st.session_state.media_warnings = {}

# ══════════════════════════════════════════════════════════════
# ① 코멘트 생성기 전용 함수
# ══════════════════════════════════════════════════════════════

def fmt_won(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "0원"
    v = int(round(v))
    if abs(v) >= 10_000_000:
        return f"약 {v/10_000:.0f}만 원"
    if abs(v) >= 10_000:
        return f"약 {v/10_000:.0f}만 원"
    return f"{v:,}원"

def fmt_roas(spend, revenue):
    if not spend or spend == 0:
        return "N/A"
    return f"{revenue / spend * 100:.0f}%"

def load_report_raw(file):
    wb = openpyxl.load_workbook(file, data_only=True, read_only=True)
    ws = wb["Total_Raw"]
    rows, headers = [], None
    for row in ws.iter_rows(values_only=True):
        if headers is None:
            headers = row
            continue
        if row[0] is None:
            continue
        rows.append(row)
    wb.close()
    df = pd.DataFrame(rows, columns=headers)
    df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce")
    for c in ["비용", "구매", "매출액", "클릭", "노출"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df

def filter_by_media(df, media_key):
    if media_key == "전체":
        return df
    kws = COMMENT_MEDIA_MAP[media_key]["campaigns"]
    return df[df["캠페인명"].str.contains("|".join(kws), case=False, na=False)]

def daily_agg(df, dt):
    d = df[df["날짜"].dt.date == dt.date()]
    return {k: d[v].sum() for k, v in
            [("spend","비용"),("purchase","구매"),("revenue","매출액"),("click","클릭"),("impression","노출")]}

def period_agg(df, s, e):
    d = df[(df["날짜"].dt.date >= s.date()) & (df["날짜"].dt.date <= e.date())]
    return {k: d[v].sum() for k, v in
            [("spend","비용"),("purchase","구매"),("revenue","매출액"),("click","클릭"),("impression","노출")]}

def prev_week_avg(df, dt):
    ws = dt - timedelta(days=dt.weekday() + 7)
    we = ws + timedelta(days=6)
    d = df[(df["날짜"].dt.date >= ws.date()) & (df["날짜"].dt.date <= we.date())]
    if len(d) == 0:
        return None
    days = d["날짜"].dt.date.nunique()
    return {
        "spend":    d["비용"].sum() / days,
        "purchase": d["구매"].sum() / days,
        "revenue":  d["매출액"].sum() / days,
        "roas":     d["매출액"].sum() / d["비용"].sum() * 100 if d["비용"].sum() > 0 else 0,
    }

def build_topline(media_label, target_dt, today, prev_day, prev_week, monthly, weekly, rtype):
    today_roas = fmt_roas(today["spend"], today["revenue"])

    prev_note = ""
    if prev_day and prev_day["spend"] > 0 and today["spend"] > 0:
        diff = today["revenue"] / today["spend"] * 100 - prev_day["revenue"] / prev_day["spend"] * 100
        if abs(diff) > 5:
            prev_note = f" (전일 대비 ROAS {diff:+.0f}%p)"

    week_note = ""
    if prev_week and prev_week["spend"] > 0 and today["spend"] > 0:
        diff = today["revenue"] / today["spend"] * 100 - prev_week["roas"]
        if abs(diff) > 5:
            week_note = f", 전주 평균 대비 ROAS {diff:+.0f}%p"

    date_str = (target_dt.strftime("%-m/%-d(%a)")
                .replace("Mon","월").replace("Tue","화").replace("Wed","수")
                .replace("Thu","목").replace("Fri","금").replace("Sat","토").replace("Sun","일"))

    out = f"* {media_label}\n"
    if rtype in ["daily", "both"]:
        out += (f"- {date_str} 광고비 {fmt_won(today['spend'])} 소진, "
                f"구매 건수 {int(today['purchase']):,}건 및 "
                f"매출 {fmt_won(today['revenue'])} 확보 "
                f"(ROAS {today_roas}){prev_note}{week_note}\n")
    if rtype in ["weekly", "both"] and weekly:
        out += (f"- 주간 광고비 {fmt_won(weekly['spend'])} 소진, "
                f"구매 건수 {int(weekly['purchase']):,}건 및 "
                f"매출 {fmt_won(weekly['revenue'])} 확보 (ROAS {fmt_roas(weekly['spend'], weekly['revenue'])})\n")
    if monthly:
        out += (f"- {target_dt.month}월 누적 광고비 {fmt_won(monthly['spend'])} 소진, "
                f"구매 건수 {int(monthly['purchase']):,}건 및 "
                f"매출 {fmt_won(monthly['revenue'])} 확보 (ROAS {fmt_roas(monthly['spend'], monthly['revenue'])})\n")
    return out

def gen_ai_insight(api_key, today, prev_day, pw_avg, media_label, target_dt, note=""):
    if not OPENAI_AVAILABLE:
        return "(openai 패키지 미설치)"
    try:
        client = openai.OpenAI(api_key=api_key)
        roas_t = today["revenue"] / today["spend"] * 100 if today["spend"] else 0
        roas_p = prev_day["revenue"] / prev_day["spend"] * 100 if prev_day and prev_day["spend"] else 0
        roas_w = pw_avg["roas"] if pw_avg else 0
        prompt = f"""당신은 디지털 광고 성과 분석 전문가입니다.
아래 데이터로 {media_label} 매체의 {target_dt.strftime('%m/%d')} 성과 특이사항을 한국어로 작성하세요.

[오늘] 광고비:{today['spend']:,.0f}원 / 구매:{today['purchase']:.0f}건 / 매출:{today['revenue']:,.0f}원 / ROAS:{roas_t:.0f}%
[전일] 광고비:{prev_day['spend']:,.0f}원 / ROAS:{roas_p:.0f}% / 구매:{prev_day['purchase']:.0f}건
[전주평균] 광고비:{pw_avg['spend']:,.0f}원 / ROAS:{roas_w:.0f}% / 구매:{pw_avg['purchase']:.0f}건
{f'[메모] {note}' if note else ''}

규칙: 2~4줄 bullet(ㄴ/-), 유의미한 변화만, ROAS 효율 원인 추정, "약 X만 원"/"X%p 상승/하락" 형식, 인사말 없이 바로 시작."""
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=400, temperature=0.3,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return f"(AI 오류: {str(e)[:80]})"


# ══════════════════════════════════════════════════════════════
# ② 대시보드 전용 함수
# ══════════════════════════════════════════════════════════════

def fmt_krw(v):
    if v is None or (isinstance(v, float) and pd.isna(v)) or v == 0:
        return "-"
    v = int(round(v))
    if abs(v) >= 100_000_000: return f"{v/100_000_000:.1f}억"
    if abs(v) >= 10_000:      return f"{v/10_000:.0f}만"
    return f"{v:,}"

def fmt_pct(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return "-"
    return f"{v:.1f}%"

def fmt_num(v, dec=0):
    if v is None or (isinstance(v, float) and pd.isna(v)): return "-"
    return f"{v:,.{dec}f}"

def safe_div(a, b, mult=1):
    try:
        r = a / b * mult
        return r if not pd.isna(r) else None
    except:
        return None

def detect_col(df_cols, candidates):
    for c in candidates:
        if c and c in df_cols:
            return c
    return None

def normalize_df(df_raw, media):
    mapping = MEDIA_COL_MAP.get(media, {})
    cols = df_raw.columns.tolist()
    result, missing = {}, []
    for std_key, std_name in STD_COLS.items():
        found = detect_col(cols, mapping.get(std_key, []))
        if found:
            result[std_name] = df_raw[found]
        else:
            result[std_name] = pd.Series([None] * len(df_raw))
            if std_key not in ["cart", "ad"]:
                missing.append(std_name)
    df = pd.DataFrame(result)
    df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce")
    for col in ["비용", "노출", "클릭", "구매", "매출액", "장바구니"]:
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", "").str.replace("₩", "").str.strip(),
            errors="coerce"
        ).fillna(0)
    return df, missing

def calc_kpis(df):
    sp = df["비용"].sum(); im = df["노출"].sum()
    cl = df["클릭"].sum(); pu = df["구매"].sum(); rv = df["매출액"].sum()
    return {
        "spend": sp, "imp": im, "click": cl, "pur": pu, "rev": rv,
        "ctr":  safe_div(cl, im, 100),
        "cpc":  safe_div(sp, cl),
        "cpm":  safe_div(sp, im, 1000),
        "cpa":  safe_div(sp, pu),
        "cvr":  safe_div(pu, cl, 100),
        "aov":  safe_div(rv, pu),
        "roas": safe_div(rv, sp, 100),
    }

def kpi_table(df_grp):
    rows = []
    for _, row in df_grp.iterrows():
        sp = row.get("비용", 0) or 0; im = row.get("노출", 0) or 0
        cl = row.get("클릭", 0) or 0; pu = row.get("구매", 0) or 0; rv = row.get("매출액", 0) or 0
        r = {k: row[k] for k in df_grp.columns if k not in ["비용","노출","클릭","구매","매출액","장바구니"]}
        r.update({
            "비용":   int(sp), "노출": int(im), "클릭": int(cl),
            "CTR":    f"{cl/im*100:.2f}%" if im else "-",
            "CPC":    f"{sp/cl:,.0f}"    if cl else "-",
            "CPM":    f"{sp/im*1000:,.0f}" if im else "-",
            "구매":   int(pu),
            "CPA":    f"{sp/pu:,.0f}"    if pu else "-",
            "CVR":    f"{pu/cl*100:.2f}%" if cl else "-",
            "매출액": int(rv),
            "AOV":    f"{rv/pu:,.0f}"    if pu else "-",
            "ROAS":   f"{rv/sp*100:.0f}%" if sp else "-",
        })
        rows.append(r)
    return pd.DataFrame(rows)

def section(title):
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)

def kpi_row(kpis):
    c = st.columns(6)
    items = [
        ("광고비",   fmt_krw(kpis["spend"]) + "만"),
        ("노출",     fmt_num(kpis["imp"])),
        ("클릭",     fmt_num(kpis["click"])),
        ("구매",     fmt_num(kpis["pur"])),
        ("ROAS",     fmt_pct(kpis["roas"])),
        ("매출",     fmt_krw(kpis["rev"]) + "만"),
    ]
    for col, (label, val) in zip(c, items):
        col.metric(label, val)

def show_table(df, height=320):
    st.dataframe(df, use_container_width=True, height=height, hide_index=True)


# ══════════════════════════════════════════════════════════════
# 사이드바: 페이지 네비게이션
# ══════════════════════════════════════════════════════════════

with st.sidebar:
    st.title("📊 BOJ 광고 대시보드")
    page = st.radio(
        "페이지",
        ["📝 코멘트 생성기", "📈 리포트 대시보드"],
        label_visibility="collapsed",
    )
    st.divider()

# ══════════════════════════════════════════════════════════════
# PAGE 1 — 코멘트 생성기
# ══════════════════════════════════════════════════════════════

if page == "📝 코멘트 생성기":

    with st.sidebar:
        st.subheader("⚙️ 설정")
        api_key = st.text_input("OpenAI API Key", type="password",
                                help="없으면 탑라인 멘트만 생성됩니다.")
        st.divider()
        st.subheader("📅 날짜")
        target_date = st.date_input("리포트 날짜", value=datetime.today() - timedelta(days=1))
        st.divider()
        st.subheader("📺 매체")
        selected_media = st.multiselect(
            "코멘트 생성할 매체",
            options=list(COMMENT_MEDIA_MAP.keys()),
            default=["Meta", "Naver BSA", "Naver SSA", "Naver ADVoost", "TikTok"],
        )
        report_type = st.radio("리포트 유형", ["일간", "주간+누적", "일간+주간+누적"], index=0)
        rtype = {"일간": "daily", "주간+누적": "weekly", "일간+주간+누적": "both"}[report_type]

    st.title("📝 코멘트 생성기")
    st.caption("최종 리포트 xlsx(Total_Raw 시트 포함)를 업로드하면 탑라인 멘트 + 특이사항을 자동 생성합니다.")

    uploaded = st.file_uploader("📂 최종 리포트 xlsx 업로드", type=["xlsx"])

    if not uploaded:
        st.info("👆 xlsx 파일을 업로드하세요.")
        with st.expander("💡 사용 방법"):
            st.markdown("""
1. 사이드바에서 날짜·매체 설정
2. 최종 리포트 xlsx 업로드 (Total_Raw 시트 필수)
3. **코멘트 생성** 버튼 클릭
4. 생성된 코멘트 복사 → 리포트 붙여넣기

**생성 내용:** 탑라인 멘트(날짜·광고비·구매·매출·ROAS) + 전일/전주 대비 + AI 특이사항
            """)
        st.stop()

    with st.spinner("데이터 로딩 중..."):
        try:
            df_report = load_report_raw(io.BytesIO(uploaded.read()))
            avail = sorted(df_report["날짜"].dt.date.dropna().unique())
            latest_d, earliest_d = max(avail), min(avail)
            st.success(f"✅ {len(df_report):,}행 로드 | 날짜 범위: {earliest_d} ~ {latest_d}")
        except Exception as e:
            st.error(f"파일 로드 실패: {e}")
            st.stop()

    if target_date > latest_d:
        st.error(
            f"⚠️ 선택한 날짜({target_date})의 데이터가 없습니다.\n\n"
            f"파일 최신 날짜: **{latest_d}** — 날짜를 변경하거나 최신 파일을 업로드하세요."
        )
        st.stop()
    elif target_date < earliest_d:
        st.warning(f"⚠️ 선택 날짜({target_date})가 데이터 시작일({earliest_d})보다 이전입니다.")

    with st.expander("📝 매체별 추가 메모 (AI 힌트용)"):
        notes = {}
        cols2 = st.columns(2)
        for i, m in enumerate(selected_media):
            with cols2[i % 2]:
                notes[m] = st.text_area(m, height=70, key=f"note_{m}",
                                        placeholder="소재명, 이벤트 등 특이사항 힌트")

    if st.button("🚀 코멘트 생성", type="primary", use_container_width=True):
        if not selected_media:
            st.warning("매체를 하나 이상 선택하세요.")
            st.stop()

        target_dt = datetime.combine(target_date, datetime.min.time())
        prev_day_dt = target_dt - timedelta(days=1)
        month_start = target_dt.replace(day=1)
        week_start  = target_dt - timedelta(days=target_dt.weekday())
        all_comments = []
        prog = st.progress(0)

        for idx, mk in enumerate(selected_media):
            st.markdown(f"### {COMMENT_MEDIA_MAP[mk]['label']}")
            df_m = filter_by_media(df_report, mk)

            if len(df_m) == 0:
                st.warning(f"{mk}: 데이터 없음")
                continue

            today_d = daily_agg(df_m, target_dt)
            if today_d["spend"] == 0 and today_d["purchase"] == 0 and today_d["revenue"] == 0:
                m_dates = sorted(df_m["날짜"].dt.date.dropna().unique())
                st.warning(f"⚠️ {mk}: {target_date} 데이터 없음 (최신: {max(m_dates) if m_dates else '없음'})")
                st.divider()
                continue

            prev_d  = daily_agg(df_m, prev_day_dt)
            pw_avg  = prev_week_avg(df_m, target_dt)
            monthly = period_agg(df_m, month_start, target_dt)
            weekly  = period_agg(df_m, week_start, target_dt) if rtype in ["weekly", "both"] else None

            c1, c2, c3, c4 = st.columns(4)
            roas_val = today_d["revenue"] / today_d["spend"] * 100 if today_d["spend"] else 0
            c1.metric("광고비",   fmt_won(today_d["spend"]))
            c2.metric("구매건수", f"{int(today_d['purchase']):,}건")
            c3.metric("매출",     fmt_won(today_d["revenue"]))
            c4.metric("ROAS",     f"{roas_val:.0f}%",
                      delta=f"{roas_val-(pw_avg['roas'] if pw_avg else 0):+.0f}%p vs 전주" if pw_avg else None)

            topline = build_topline(
                COMMENT_MEDIA_MAP[mk]["label"], target_dt, today_d,
                prev_d, pw_avg, monthly, weekly, rtype,
            )
            insight = ""
            if api_key and today_d["spend"] > 0:
                with st.spinner(f"{mk} AI 분석 중..."):
                    insight = gen_ai_insight(
                        api_key, today_d,
                        prev_d if prev_d["spend"] > 0 else {"spend":0,"purchase":0,"revenue":0},
                        pw_avg if pw_avg else {"spend":0,"purchase":0,"revenue":0,"roas":0},
                        COMMENT_MEDIA_MAP[mk]["label"], target_dt, notes.get(mk, ""),
                    )

            full = topline + (insight + "\n" if insight else "")
            st.text_area("생성된 코멘트", value=full, height=200, key=f"comment_{mk}")
            all_comments.append(f"[{COMMENT_MEDIA_MAP[mk]['label']}]\n{full}")
            prog.progress((idx + 1) / len(selected_media))
            st.divider()

        if all_comments:
            st.subheader("📋 전체 통합 (복사용)")
            st.text_area("전체", value="\n\n".join(all_comments), height=400, key="all_comments")
        prog.empty()


# ══════════════════════════════════════════════════════════════
# PAGE 2 — 리포트 대시보드
# ══════════════════════════════════════════════════════════════

elif page == "📈 리포트 대시보드":

    with st.sidebar:
        st.subheader("📂 RAW 데이터 업로드")
        st.caption("매체별 RAW 파일(xlsx/csv)을 업로드하세요.")

        for media in DASHBOARD_MEDIA_LIST:
            with st.expander(media, expanded=False):
                up = st.file_uploader(f"{media} RAW", type=["xlsx","csv"],
                                      key=f"raw_{media}", label_visibility="collapsed")
                if up:
                    try:
                        df_raw = (pd.read_csv(up, encoding="utf-8-sig")
                                  if up.name.endswith(".csv")
                                  else pd.read_excel(up, sheet_name=0))
                        df_norm, miss = normalize_df(df_raw, media)
                        st.session_state.media_data[media]    = df_norm
                        st.session_state.media_warnings[media] = miss
                        if miss:
                            st.warning(f"미매핑: {', '.join(miss)}")
                        else:
                            st.success(f"✅ {len(df_norm):,}행")
                    except Exception as e:
                        st.error(f"오류: {e}")

        st.divider()
        st.caption("**업로드 현황**")
        for media in DASHBOARD_MEDIA_LIST:
            if media in st.session_state.media_data:
                warn = st.session_state.media_warnings.get(media, [])
                icon = "🟡" if warn else "🟢"
                st.caption(f"{icon} {media} — {len(st.session_state.media_data[media]):,}행")
            else:
                st.caption(f"⚪ {media} — 미업로드")
        st.divider()
        if st.button("🗑️ 전체 초기화", use_container_width=True):
            st.session_state.media_data = {}
            st.session_state.media_warnings = {}
            st.rerun()

    st.title("📈 리포트 대시보드")
    st.caption("매체별 RAW 파일을 사이드바에 업로드하면 리포트가 생성됩니다.")

    if not st.session_state.media_data:
        st.info("👈 사이드바에서 매체별 RAW 파일을 업로드하세요.")
        with st.expander("📋 매체별 컬럼 매핑 현황"):
            rows = []
            for m, mapping in MEDIA_COL_MAP.items():
                for sk, sn in STD_COLS.items():
                    cands = [c for c in mapping.get(sk, []) if c]
                    rows.append({"매체": m, "표준 컬럼": sn,
                                 "인식 가능한 RAW 컬럼명": " / ".join(cands) if cands else "❌"})
            st.dataframe(pd.DataFrame(rows), use_container_width=True, height=400, hide_index=True)
        st.stop()

    all_dfs = list(st.session_state.media_data.values())
    df_all  = pd.concat(all_dfs, ignore_index=True)
    loaded_media = list(st.session_state.media_data.keys())

    tab_labels = ["📈 Sales Overview"] + [f"📺 {m}" for m in loaded_media] + ["⚙️ 컬럼 매핑"]
    tabs = st.tabs(tab_labels)

    # ── Sales Overview ────────────────────────────────────────
    with tabs[0]:
        st.subheader("Sales Campaign Overview")
        valid_dates = df_all["날짜"].dropna()

        if valid_dates.empty:
            st.warning("날짜 데이터가 없습니다.")
        else:
            min_d, max_d = valid_dates.min().date(), valid_dates.max().date()
            fc1, fc2, _ = st.columns([2, 2, 4])
            with fc1: sel_start = st.date_input("시작일", min_d, min_value=min_d, max_value=max_d, key="s_s")
            with fc2: sel_end   = st.date_input("종료일", max_d, min_value=min_d, max_value=max_d, key="s_e")
            mask = (df_all["날짜"].dt.date >= sel_start) & (df_all["날짜"].dt.date <= sel_end)
            df_f = df_all[mask].copy()

            section("▣ 전체 KPI")
            kpi_row(calc_kpis(df_f))
            st.divider()

            section("▣ 월별 성과")
            df_f["월"] = df_f["날짜"].dt.to_period("M").astype(str)
            grp = df_f.groupby("월").agg(비용=("비용","sum"), 노출=("노출","sum"),
                                         클릭=("클릭","sum"), 구매=("구매","sum"), 매출액=("매출액","sum")).reset_index()
            show_table(kpi_table(grp))
            st.divider()

            section("▣ 매체별 월별 성과")
            m_rows = []
            for media, df_m in st.session_state.media_data.items():
                mf = df_m[(df_m["날짜"].dt.date >= sel_start) & (df_m["날짜"].dt.date <= sel_end)].copy()
                if mf.empty: continue
                mf["월"] = mf["날짜"].dt.to_period("M").astype(str)
                for month, g in mf.groupby("월"):
                    k = calc_kpis(g)
                    m_rows.append({"매체": media, "월": month,
                                   "비용": int(k["spend"]), "노출": int(k["imp"]), "클릭": int(k["click"]),
                                   "CTR": fmt_pct(k["ctr"]), "CPC": fmt_num(k["cpc"]), "CPM": fmt_num(k["cpm"]),
                                   "구매": int(k["pur"]), "CPA": fmt_num(k["cpa"]), "CVR": fmt_pct(k["cvr"]),
                                   "매출액": int(k["rev"]), "AOV": fmt_num(k["aov"]), "ROAS": fmt_pct(k["roas"])})
            if m_rows: show_table(pd.DataFrame(m_rows))
            st.divider()

            section("▣ 매체별 주차별 성과")
            w_rows = []
            for media, df_m in st.session_state.media_data.items():
                mf = df_m[(df_m["날짜"].dt.date >= sel_start) & (df_m["날짜"].dt.date <= sel_end)].copy()
                if mf.empty: continue
                mf["주차"]  = mf["날짜"].dt.to_period("W").apply(
                    lambda p: f"{p.start_time.strftime('%m/%d')}~{p.end_time.strftime('%m/%d')}")
                mf["_sort"] = mf["날짜"].dt.to_period("W").astype(str)
                for (sort_k, week_l), g in mf.groupby(["_sort","주차"]):
                    k = calc_kpis(g)
                    w_rows.append({"매체": media, "주차": week_l,
                                   "비용": int(k["spend"]), "노출": int(k["imp"]), "클릭": int(k["click"]),
                                   "CTR": fmt_pct(k["ctr"]), "CPC": fmt_num(k["cpc"]), "CPM": fmt_num(k["cpm"]),
                                   "구매": int(k["pur"]), "CPA": fmt_num(k["cpa"]), "CVR": fmt_pct(k["cvr"]),
                                   "매출액": int(k["rev"]), "AOV": fmt_num(k["aov"]), "ROAS": fmt_pct(k["roas"]),
                                   "_sort": sort_k})
            if w_rows:
                show_table(pd.DataFrame(w_rows).sort_values("_sort").drop(columns=["_sort"]))
            st.divider()

            section("▣ 매체별 일자별 성과")
            d_rows = []
            for media, df_m in st.session_state.media_data.items():
                mf = df_m[(df_m["날짜"].dt.date >= sel_start) & (df_m["날짜"].dt.date <= sel_end)].copy()
                if mf.empty: continue
                mf["날짜str"] = mf["날짜"].dt.strftime("%Y-%m-%d")
                for ds, g in mf.groupby("날짜str"):
                    k = calc_kpis(g)
                    d_rows.append({"날짜": ds, "매체": media,
                                   "비용": int(k["spend"]), "노출": int(k["imp"]), "클릭": int(k["click"]),
                                   "CTR": fmt_pct(k["ctr"]), "CPC": fmt_num(k["cpc"]), "CPM": fmt_num(k["cpm"]),
                                   "구매": int(k["pur"]), "CPA": fmt_num(k["cpa"]), "CVR": fmt_pct(k["cvr"]),
                                   "매출액": int(k["rev"]), "AOV": fmt_num(k["aov"]), "ROAS": fmt_pct(k["roas"])})
            if d_rows:
                df_day = pd.DataFrame(d_rows).sort_values(["날짜","매체"])
                media_opts = ["전체"] + sorted(df_day["매체"].unique().tolist())
                sel_m = st.selectbox("매체 선택", media_opts, key="day_m_filter")
                if sel_m != "전체":
                    df_day = df_day[df_day["매체"] == sel_m]
                show_table(df_day, height=400)

    # ── 매체별 탭 ─────────────────────────────────────────────
    for ti, media in enumerate(loaded_media):
        with tabs[ti + 1]:
            df_m  = st.session_state.media_data[media].copy()
            warns = st.session_state.media_warnings.get(media, [])

            st.subheader(f"{media} 성과 리포트")
            if warns:
                st.warning(f"⚠️ 미매핑 컬럼: {', '.join(warns)}")

            vd = df_m["날짜"].dropna()
            if not vd.empty:
                mn, mx = vd.min().date(), vd.max().date()
                mc1, mc2, _ = st.columns([2, 2, 4])
                with mc1: ms = st.date_input("시작일", mn, min_value=mn, max_value=mx, key=f"{media}_s")
                with mc2: me = st.date_input("종료일", mx, min_value=mn, max_value=mx, key=f"{media}_e")
                df_mf = df_m[(df_m["날짜"].dt.date >= ms) & (df_m["날짜"].dt.date <= me)].copy()
            else:
                df_mf = df_m.copy()

            kpi_row(calc_kpis(df_mf))
            st.divider()

            it = st.tabs(["그룹별", "소재별", "주차별", "일자별"])

            with it[0]:
                section(f"▣ {media} — 그룹별 성과")
                gc = "광고그룹명"
                if gc in df_mf.columns and df_mf[gc].notna().any():
                    g = df_mf.groupby(gc).agg(비용=("비용","sum"), 노출=("노출","sum"),
                                              클릭=("클릭","sum"), 구매=("구매","sum"),
                                              매출액=("매출액","sum")).reset_index()
                    show_table(kpi_table(g))
                else:
                    st.info("광고그룹명 컬럼이 매핑되지 않았습니다.")

            with it[1]:
                section(f"▣ {media} — 소재별 성과")
                ac = "광고명/소재명"
                if ac in df_mf.columns and df_mf[ac].notna().any():
                    g = df_mf.groupby(ac).agg(비용=("비용","sum"), 노출=("노출","sum"),
                                              클릭=("클릭","sum"), 구매=("구매","sum"),
                                              매출액=("매출액","sum")).reset_index()
                    show_table(kpi_table(g), height=400)
                else:
                    st.info("소재명 컬럼이 매핑되지 않았습니다.")

            with it[2]:
                section(f"▣ {media} — 주차별 성과")
                if df_mf["날짜"].notna().any():
                    df_mf["주차"]  = df_mf["날짜"].dt.to_period("W").apply(
                        lambda p: f"{p.start_time.strftime('%m/%d')}~{p.end_time.strftime('%m/%d')}")
                    df_mf["_ws"]   = df_mf["날짜"].dt.to_period("W").astype(str)
                    g = df_mf.groupby(["_ws","주차"]).agg(비용=("비용","sum"), 노출=("노출","sum"),
                                                          클릭=("클릭","sum"), 구매=("구매","sum"),
                                                          매출액=("매출액","sum")).reset_index()
                    g = g.sort_values("_ws").drop(columns=["_ws"])
                    show_table(kpi_table(g))
                else:
                    st.info("날짜 데이터가 없습니다.")

            with it[3]:
                section(f"▣ {media} — 일자별 성과")
                if df_mf["날짜"].notna().any():
                    df_mf["날짜str"] = df_mf["날짜"].dt.strftime("%Y-%m-%d")
                    g = df_mf.groupby("날짜str").agg(비용=("비용","sum"), 노출=("노출","sum"),
                                                     클릭=("클릭","sum"), 구매=("구매","sum"),
                                                     매출액=("매출액","sum")).reset_index()
                    g = g.rename(columns={"날짜str": "날짜"})
                    show_table(kpi_table(g), height=400)
                else:
                    st.info("날짜 데이터가 없습니다.")

    # ── 컬럼 매핑 설정 탭 ────────────────────────────────────
    with tabs[-1]:
        st.subheader("⚙️ 컬럼 매핑 확인 및 수동 설정")
        st.markdown("RAW 파일 컬럼명이 자동 인식되지 않을 때 여기서 직접 매핑합니다.")

        if not st.session_state.media_data:
            st.info("먼저 사이드바에서 RAW 파일을 업로드하세요.")
        else:
            tgt = st.selectbox("수정할 매체", loaded_media, key="map_media")
            df_s   = st.session_state.media_data[tgt]
            w_cols = st.session_state.media_warnings.get(tgt, [])
            st.markdown(f"**미매핑 컬럼:** {', '.join(w_cols) if w_cols else '없음 (정상)'}")
            st.dataframe(df_s.head(5), use_container_width=True, hide_index=True)

            st.divider()
            re_up = st.file_uploader(f"{tgt} 재업로드", type=["xlsx","csv"], key=f"remap_{tgt}")
            if re_up:
                df_re = (pd.read_csv(re_up, encoding="utf-8-sig")
                         if re_up.name.endswith(".csv") else pd.read_excel(re_up, sheet_name=0))
                raw_cols = ["(없음)"] + df_re.columns.tolist()
                st.write("RAW 컬럼:", df_re.columns.tolist())

                manual_map = {}
                cp = st.columns(2)
                for i, (sk, sn) in enumerate(STD_COLS.items()):
                    with cp[i % 2]:
                        sel = st.selectbox(sn, raw_cols, key=f"man_{tgt}_{sk}")
                        if sel != "(없음)":
                            manual_map[sk] = sel

                if st.button("✅ 매핑 적용", type="primary"):
                    result, miss2 = {}, []
                    for sk, sn in STD_COLS.items():
                        if sk in manual_map:
                            result[sn] = df_re[manual_map[sk]]
                        else:
                            result[sn] = pd.Series([None] * len(df_re))
                            if sk not in ["cart", "ad"]:
                                miss2.append(sn)
                    df_new = pd.DataFrame(result)
                    df_new["날짜"] = pd.to_datetime(df_new["날짜"], errors="coerce")
                    for col in ["비용","노출","클릭","구매","매출액","장바구니"]:
                        df_new[col] = pd.to_numeric(
                            df_new[col].astype(str).str.replace(",","").str.replace("₩","").str.strip(),
                            errors="coerce").fillna(0)
                    st.session_state.media_data[tgt]    = df_new
                    st.session_state.media_warnings[tgt] = miss2
                    st.success("✅ 적용 완료!")
                    st.rerun()

        st.divider()
        st.markdown("**전체 컬럼 매핑 현황**")
        mr = []
        for m, mapping in MEDIA_COL_MAP.items():
            for sk, sn in STD_COLS.items():
                cands = [c for c in mapping.get(sk, []) if c]
                mr.append({"매체": m, "표준 컬럼": sn,
                           "인식 가능한 컬럼명": " / ".join(cands) if cands else "❌"})
        st.dataframe(pd.DataFrame(mr), use_container_width=True, height=400, hide_index=True)
