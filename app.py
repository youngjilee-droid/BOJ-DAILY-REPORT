import io
import re
from typing import Optional, Dict, List

import pandas as pd
import streamlit as st


# =========================================================
# 1. 기본 설정
# =========================================================
st.set_page_config(page_title="광고 통합 리포트 & 대시보드", layout="wide")

st.title("광고 통합 리포트 & 대시보드")
st.write("매체별 RAW 데이터를 업로드하면 통합 리포트 생성과 대시보드 확인을 동시에 할 수 있습니다.")


# =========================================================
# 2. 최종 표준 컬럼 정의
# =========================================================
FINAL_COLUMNS = [
    "날짜",
    "캠페인명",
    "광고그룹명",
    "광고명",
    "비용",
    "실제 비용",
    "노출",
    "클릭",
    "구매",
    "매출액",
    "장바구니담기수",
    "도달",
    "참여",
    "팔로우",
    "동영상조회",
    "매체",
]

NUMERIC_COLUMNS = [
    "비용",
    "실제 비용",
    "노출",
    "클릭",
    "구매",
    "매출액",
    "장바구니담기수",
    "도달",
    "참여",
    "팔로우",
    "동영상조회",
]


# =========================================================
# 3. 매체별 컬럼 매핑 사전
# =========================================================
NAVER_COLUMN_MAP = {
    "일별": "날짜",
    "일자": "날짜",
    "날짜": "날짜",
    "캠페인": "캠페인명",
    "캠페인명": "캠페인명",
    "광고그룹": "광고그룹명",
    "광고그룹명": "광고그룹명",
    "소재": "광고명",
    "광고": "광고명",
    "광고명": "광고명",
    "총비용(vat포함,원)": "비용",
    "총비용": "비용",
    "광고비": "비용",
    "소진액": "비용",
    "비용": "비용",
    "노출": "노출",
    "노출수": "노출",
    "클릭": "클릭",
    "클릭수": "클릭",
    "총 전환수": "구매",
    "총전환수": "구매",
    "구매수": "구매",
    "전환구매": "구매",
    "총 전환매출액(원)": "매출액",
    "총전환매출액(원)": "매출액",
    "매출액": "매출액",
    "구매액": "매출액",
    "장바구니": "장바구니담기수",
    "장바구니담기": "장바구니담기수",
    "장바구니담기수": "장바구니담기수",
    "도달": "도달",
    "도달수": "도달",
    "참여": "참여",
    "참여수": "참여",
    "팔로우": "팔로우",
    "팔로우수": "팔로우",
    "동영상조회": "동영상조회",
    "동영상 조회": "동영상조회",
    "영상조회": "동영상조회",
    "3초조회": "동영상조회",
}

META_COLUMN_MAP = {
    "일": "날짜",
    "날짜": "날짜",
    "date": "날짜",
    "day": "날짜",
    "캠페인 이름": "캠페인명",
    "campaign name": "캠페인명",
    "campaign": "캠페인명",
    "광고 세트 이름": "광고그룹명",
    "ad set name": "광고그룹명",
    "adset name": "광고그룹명",
    "광고 이름": "광고명",
    "ad name": "광고명",
    "ad": "광고명",
    "지출 금액": "비용",
    "지출 금액 (krw)": "비용",
    "amount spent": "비용",
    "spend": "비용",
    "노출": "노출",
    "impressions": "노출",
    "링크 클릭": "클릭",
    "클릭": "클릭",
    "clicks": "클릭",
    "구매": "구매",
    "purchases": "구매",
    "공유 항목이 포함된 구매": "구매",
    "website purchases": "구매",
    "meta purchases": "구매",
    "구매 전환값": "매출액",
    "공유 항목의 구매 전환값": "매출액",
    "purchase conversion value": "매출액",
    "website purchase conversion value": "매출액",
    "conversion value": "매출액",
    "공유 항목이 포함된 장바구니에 담기": "장바구니담기수",
    "장바구니에 담기": "장바구니담기수",
    "adds to cart": "장바구니담기수",
    "add to cart": "장바구니담기수",
    "도달": "도달",
    "reach": "도달",
    "참여": "참여",
    "post engagement": "참여",
    "engagement": "참여",
    "게시물 참여": "참여",
    "팔로우": "팔로우",
    "follows": "팔로우",
    "instagram profile follows": "팔로우",
    "동영상 3초 이상 재생": "동영상조회",
    "동영상 조회": "동영상조회",
    "video plays": "동영상조회",
    "video views": "동영상조회",
    "thruplays": "동영상조회",
    "3-second video plays": "동영상조회",
}

KAKAO_COLUMN_MAP = {
    "날짜": "날짜",
    "일자": "날짜",
    "일": "날짜",
    "캠페인": "캠페인명",
    "캠페인명": "캠페인명",
    "광고그룹": "광고그룹명",
    "광고그룹명": "광고그룹명",
    "광고": "광고명",
    "광고명": "광고명",
    "비용": "비용",
    "광고비": "비용",
    "소진액": "비용",
    "사용금액": "비용",
    "노출": "노출",
    "노출수": "노출",
    "클릭": "클릭",
    "클릭수": "클릭",
    "구매": "구매",
    "구매 (7일)": "구매",
    "구매수": "구매",
    "전환": "구매",
    "매출": "매출액",
    "구매금액 (7일)": "매출액",
    "매출액": "매출액",
    "구매금액": "매출액",
    "장바구니추가(7일)": "장바구니담기수",
    "장바구니담기": "장바구니담기수",
    "장바구니담기수": "장바구니담기수",
    "장바구니": "장바구니담기수",
    "도달": "도달",
    "도달수": "도달",
    "참여": "참여",
    "참여수": "참여",
    "팔로우": "팔로우",
    "팔로우수": "팔로우",
    "동영상조회": "동영상조회",
    "동영상 조회": "동영상조회",
    "영상조회": "동영상조회",
    "동영상 재생": "동영상조회",
}

TIKTOK_COLUMN_MAP = {
    "date": "날짜",
    "By day": "날짜",
    "stat_time_day": "날짜",
    "날짜": "날짜",
    "Campaign name": "캠페인명",
    "campaign": "캠페인명",
    "Ad group name": "광고그룹명",
    "adgroup name": "광고그룹명",
    "ad group": "광고그룹명",
    "Ad name": "광고명",
    "ad": "광고명",
    "spend": "비용",
    "Cost": "비용",
    "amount spent": "비용",
    "지출 금액": "비용",
    "노출": "노출",
    "Impressions": "노출",
    "Clicks (destination)": "클릭",
    "clicks": "클릭",
    "클릭": "클릭",
    "Total purchase (all-channels)": "구매",
    "purchase": "구매",
    "purchases": "구매",
    "complete payment": "구매",
    "purchase value": "매출액",
    "complete payment value": "매출액",
    "conversion value": "매출액",
    "add to cart": "장바구니담기수",
    "Total add to cart (all-channels)": "장바구니담기수",
    "adds to cart": "장바구니담기수",
    "Reach": "도달",
    "도달": "도달",
    "engagement": "참여",
    "engaged view": "참여",
    "follows": "팔로우",
    "followers": "팔로우",
    "Video views": "동영상조회",
    "video views at 2s": "동영상조회",
    "video views at 6s": "동영상조회",
    "동영상 조회": "동영상조회",
}

PLATFORM_MAPS = {
    "네이버": NAVER_COLUMN_MAP,
    "메타": META_COLUMN_MAP,
    "카카오": KAKAO_COLUMN_MAP,
    "틱톡": TIKTOK_COLUMN_MAP,
}


# =========================================================
# 4. 유틸 함수
# =========================================================
def clean_column_name(col_name: str) -> str:
    col = str(col_name)
    col = col.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    col = col.replace("\xa0", " ").replace("\u200b", " ").replace("\ufeff", " ")
    col = col.strip()
    col = re.sub(r"\s+", " ", col)
    return col


def normalize_for_matching(col_name: str) -> str:
    col = clean_column_name(col_name).lower()
    col = re.sub(r"[^0-9a-zA-Z가-힣]", "", col)
    return col


def build_normalized_map(column_map: Dict[str, str]) -> Dict[str, str]:
    return {
        normalize_for_matching(k): v for k, v in column_map.items()
    }


def load_csv_file(uploaded_file) -> Optional[pd.DataFrame]:
    encodings = ["utf-8-sig", "utf-8", "cp949", "euc-kr", "utf-16"]
    separators = [None, ",", "\t", ";", "|"]
    file_bytes = uploaded_file.getvalue()

    for enc in encodings:
        for sep in separators:
            try:
                buffer = io.BytesIO(file_bytes)
                buffer.seek(0)

                if sep is None:
                    df = pd.read_csv(buffer, encoding=enc, sep=None, engine="python")
                else:
                    df = pd.read_csv(buffer, encoding=enc, sep=sep)

                return df
            except Exception:
                continue
    return None


def load_excel_file(uploaded_file) -> Optional[pd.DataFrame]:
    try:
        uploaded_file.seek(0)
        return pd.read_excel(uploaded_file, engine="openpyxl")
    except Exception:
        return None


def load_naver_excel_file(uploaded_file) -> Optional[pd.DataFrame]:
    try:
        uploaded_file.seek(0)
        df = pd.read_excel(uploaded_file, engine="openpyxl", skiprows=1)
        df.columns = [clean_column_name(c) for c in df.columns]
        df = df.dropna(how="all").reset_index(drop=True)
        return df
    except Exception:
        return None


def load_naver_csv_file(uploaded_file) -> Optional[pd.DataFrame]:
    encodings = ["utf-8-sig", "utf-8", "cp949", "euc-kr", "utf-16"]
    separators = [None, ",", "\t", ";", "|"]
    file_bytes = uploaded_file.getvalue()

    for enc in encodings:
        for sep in separators:
            try:
                buffer = io.BytesIO(file_bytes)
                buffer.seek(0)

                if sep is None:
                    df = pd.read_csv(buffer, encoding=enc, sep=None, engine="python", skiprows=1)
                else:
                    df = pd.read_csv(buffer, encoding=enc, sep=sep, skiprows=1)

                df.columns = [clean_column_name(c) for c in df.columns]
                df = df.dropna(how="all").reset_index(drop=True)
                return df
            except Exception:
                continue
    return None


def load_file(uploaded_file, platform_name):
    if uploaded_file is None:
        return None

    file_name = uploaded_file.name.lower()

    if platform_name == "네이버":
        if file_name.endswith(".csv"):
            df = load_naver_csv_file(uploaded_file)
        else:
            df = load_naver_excel_file(uploaded_file)
    else:
        if file_name.endswith(".csv"):
            df = load_csv_file(uploaded_file)
        else:
            df = load_excel_file(uploaded_file)

    if df is None:
        return None

    df["매체"] = platform_name
    return df


def convert_numeric_columns(df: pd.DataFrame, numeric_columns: List[str]) -> pd.DataFrame:
    for col in numeric_columns:
        if col in df.columns:
            series = df[col].astype(str)
            series = series.replace(
                {"None": "", "nan": "", "NaN": "", "null": "", "NULL": "", "-": ""},
                regex=False,
            )
            series = series.str.replace(",", "", regex=False)
            series = series.str.replace(" ", "", regex=False)
            series = series.str.replace("%", "", regex=False)
            series = series.str.replace(r"[^0-9\.\-]", "", regex=True)
            df[col] = pd.to_numeric(series, errors="coerce")
    return df


def convert_date_column(df: pd.DataFrame) -> pd.DataFrame:
    if "날짜" in df.columns:
        parsed = pd.to_datetime(df["날짜"], errors="coerce")
        df["날짜"] = parsed.dt.strftime("%Y-%m-%d").fillna(df["날짜"].astype(str))
    return df


def add_naver_actual_cost(df: pd.DataFrame) -> pd.DataFrame:
    """
    네이버만 실제 비용 = 비용 / 1.1
    다른 매체는 빈 값 유지
    """
    df = df.copy()

    if "실제 비용" not in df.columns:
        df["실제 비용"] = ""

    if "매체" in df.columns and "비용" in df.columns:
        naver_mask = df["매체"] == "네이버"
        df.loc[naver_mask, "실제 비용"] = df.loc[naver_mask, "비용"] / 1.1

    return df


def standardize_columns(df, platform_name, column_map):
    df = df.copy()
    df.columns = [clean_column_name(c) for c in df.columns]

    norm_map = build_normalized_map(column_map)

    rename_dict = {}
    for col in df.columns:
        norm = normalize_for_matching(col)
        if norm in norm_map:
            rename_dict[col] = norm_map[norm]

    df = df.rename(columns=rename_dict)

    for col in FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df = df[FINAL_COLUMNS]
    df = convert_numeric_columns(df, NUMERIC_COLUMNS)
    df = convert_date_column(df)
    df = add_naver_actual_cost(df)

    return df


def to_csv(df):
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def to_excel(df):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="통합리포트")
    buffer.seek(0)
    return buffer.getvalue()


def format_metric_value(value):
    if pd.isna(value):
        return "0"
    try:
        value = float(value)
        if value.is_integer():
            return f"{int(value):,}"
        return f"{value:,.2f}"
    except Exception:
        return str(value)


def safe_divide(numerator, denominator):
    if denominator in [0, None] or pd.isna(denominator):
        return 0
    return numerator / denominator


def build_dashboard_df(df: pd.DataFrame) -> pd.DataFrame:
    dashboard_df = df.copy()
    dashboard_df = convert_numeric_columns(dashboard_df, NUMERIC_COLUMNS)
    dashboard_df = convert_date_column(dashboard_df)
    dashboard_df["날짜_dt"] = pd.to_datetime(dashboard_df["날짜"], errors="coerce")
    return dashboard_df


def make_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    daily = (
        df.groupby("날짜", dropna=False)[
            ["비용", "실제 비용", "노출", "클릭", "구매", "매출액", "장바구니담기수", "도달", "참여", "팔로우", "동영상조회"]
        ]
        .sum(numeric_only=True)
        .reset_index()
        .sort_values("날짜")
    )
    return daily


def make_media_summary(df: pd.DataFrame) -> pd.DataFrame:
    media = (
        df.groupby("매체", dropna=False)[
            ["비용", "실제 비용", "노출", "클릭", "구매", "매출액", "장바구니담기수", "도달", "참여", "팔로우", "동영상조회"]
        ]
        .sum(numeric_only=True)
        .reset_index()
        .sort_values("비용", ascending=False)
    )

    if not media.empty:
        media["CTR"] = media.apply(lambda x: safe_divide(x["클릭"], x["노출"]) * 100, axis=1)
        media["CPC"] = media.apply(lambda x: safe_divide(x["비용"], x["클릭"]), axis=1)
        media["CPA"] = media.apply(lambda x: safe_divide(x["비용"], x["구매"]), axis=1)
        media["ROAS"] = media.apply(lambda x: safe_divide(x["매출액"], x["비용"]) * 100, axis=1)

    return media


def make_campaign_summary(df: pd.DataFrame) -> pd.DataFrame:
    campaign = (
        df.groupby(["매체", "캠페인명"], dropna=False)[
            ["비용", "실제 비용", "노출", "클릭", "구매", "매출액", "장바구니담기수", "도달", "참여", "팔로우", "동영상조회"]
        ]
        .sum(numeric_only=True)
        .reset_index()
    )

    if not campaign.empty:
        campaign["CTR"] = campaign.apply(lambda x: safe_divide(x["클릭"], x["노출"]) * 100, axis=1)
        campaign["CPC"] = campaign.apply(lambda x: safe_divide(x["비용"], x["클릭"]), axis=1)
        campaign["CPA"] = campaign.apply(lambda x: safe_divide(x["비용"], x["구매"]), axis=1)
        campaign["ROAS"] = campaign.apply(lambda x: safe_divide(x["매출액"], x["비용"]) * 100, axis=1)

    campaign = campaign.sort_values("비용", ascending=False)
    return campaign


def render_dashboard(df: pd.DataFrame):
    dashboard_df = build_dashboard_df(df)

    st.subheader("대시보드")

    valid_dates = dashboard_df["날짜_dt"].dropna()
    min_date = valid_dates.min().date() if not valid_dates.empty else None
    max_date = valid_dates.max().date() if not valid_dates.empty else None

    with st.sidebar:
        st.header("대시보드 필터")

        if min_date and max_date:
            selected_dates = st.date_input(
                "날짜 범위",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
            )
        else:
            selected_dates = None

        media_options = sorted([m for m in dashboard_df["매체"].dropna().unique().tolist() if str(m).strip() != ""])
        selected_media = st.multiselect("매체 선택", media_options, default=media_options)

        campaign_options = sorted([c for c in dashboard_df["캠페인명"].dropna().unique().tolist() if str(c).strip() != ""])
        selected_campaigns = st.multiselect("캠페인 선택", campaign_options, default=campaign_options)

    filtered_df = dashboard_df.copy()

    if selected_dates and isinstance(selected_dates, tuple) and len(selected_dates) == 2:
        start_date, end_date = selected_dates
        filtered_df = filtered_df[
            (filtered_df["날짜_dt"].dt.date >= start_date) &
            (filtered_df["날짜_dt"].dt.date <= end_date)
        ]

    if selected_media:
        filtered_df = filtered_df[filtered_df["매체"].isin(selected_media)]

    if selected_campaigns:
        filtered_df = filtered_df[filtered_df["캠페인명"].isin(selected_campaigns)]

    if filtered_df.empty:
        st.warning("필터 조건에 맞는 데이터가 없습니다.")
        return

    total_cost = filtered_df["비용"].sum()
    total_real_cost = filtered_df["실제 비용"].sum()
    total_impressions = filtered_df["노출"].sum()
    total_clicks = filtered_df["클릭"].sum()
    total_purchases = filtered_df["구매"].sum()
    total_sales = filtered_df["매출액"].sum()

    ctr = safe_divide(total_clicks, total_impressions) * 100
    cpc = safe_divide(total_cost, total_clicks)
    cpa = safe_divide(total_cost, total_purchases)
    roas = safe_divide(total_sales, total_cost) * 100

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("총 비용", format_metric_value(total_cost))
    m2.metric("총 실제 비용", format_metric_value(total_real_cost))
    m3.metric("총 매출액", format_metric_value(total_sales))
    m4.metric("ROAS", f"{roas:,.2f}%")

    m5, m6, m7, m8 = st.columns(4)
    m5.metric("총 노출", format_metric_value(total_impressions))
    m6.metric("총 클릭", format_metric_value(total_clicks))
    m7.metric("CTR", f"{ctr:,.2f}%")
    m8.metric("CPC", f"{cpc:,.2f}")

    m9, m10, m11, m12 = st.columns(4)
    m9.metric("총 구매", format_metric_value(total_purchases))
    m10.metric("CPA", f"{cpa:,.2f}")
    m11.metric("장바구니 담기", format_metric_value(filtered_df["장바구니담기수"].sum()))
    m12.metric("도달", format_metric_value(filtered_df["도달"].sum()))

    st.markdown("---")

    daily_summary = make_daily_summary(filtered_df)
    media_summary = make_media_summary(filtered_df)
    campaign_summary = make_campaign_summary(filtered_df)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### 일자별 비용 추이")
        st.line_chart(daily_summary.set_index("날짜")[["비용", "실제 비용"]])

    with c2:
        st.markdown("### 일자별 매출 추이")
        st.line_chart(daily_summary.set_index("날짜")[["매출액"]])

    c3, c4 = st.columns(2)
    with c3:
        st.markdown("### 일자별 노출 / 클릭")
        st.line_chart(daily_summary.set_index("날짜")[["노출", "클릭"]])

    with c4:
        st.markdown("### 일자별 구매")
        st.line_chart(daily_summary.set_index("날짜")[["구매"]])

    st.markdown("### 매체별 요약")
    st.dataframe(media_summary, use_container_width=True)

    st.markdown("### 캠페인별 요약")
    st.dataframe(campaign_summary, use_container_width=True)

    st.markdown("### 필터 적용 원본 데이터")
    st.dataframe(
        filtered_df.drop(columns=["날짜_dt"], errors="ignore"),
        use_container_width=True
    )


# =========================================================
# 5. UI
# =========================================================
tab1, tab2 = st.tabs(["리포트 생성기", "대시보드"])

with tab1:
    st.markdown("### 매체별 RAW 업로드")

    c1, c2 = st.columns(2)
    with c1:
        naver = st.file_uploader("네이버", type=["csv", "xlsx"], key="naver")
        kakao = st.file_uploader("카카오", type=["csv", "xlsx"], key="kakao")

    with c2:
        meta = st.file_uploader("메타", type=["csv", "xlsx"], key="meta")
        tiktok = st.file_uploader("틱톡", type=["csv", "xlsx"], key="tiktok")

    files = [
        ("네이버", naver),
        ("메타", meta),
        ("카카오", kakao),
        ("틱톡", tiktok),
    ]

    if st.button("리포트 생성", key="build_report"):
        dfs = []

        for name, file in files:
            if file:
                df = load_file(file, name)

                if df is None:
                    continue

                df = standardize_columns(df, name, PLATFORM_MAPS[name])
                dfs.append(df)

        if dfs:
            final = pd.concat(dfs, ignore_index=True)
            final = convert_numeric_columns(final, NUMERIC_COLUMNS)
            final = convert_date_column(final)
            final = add_naver_actual_cost(final)

            st.session_state["final_report_df"] = final

            st.success("리포트 생성이 완료되었습니다.")
            st.markdown("### 통합 리포트")
            st.dataframe(final, use_container_width=True)

            st.download_button(
                "CSV 다운로드",
                to_csv(final),
                "report.csv",
                mime="text/csv",
            )
            st.download_button(
                "엑셀 다운로드",
                to_excel(final),
                "report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            st.error("파일이 없거나 읽을 수 없습니다.")

with tab2:
    if "final_report_df" in st.session_state and not st.session_state["final_report_df"].empty:
        render_dashboard(st.session_state["final_report_df"])
    else:
        st.info("먼저 '리포트 생성기' 탭에서 통합 리포트를 생성해 주세요.")
