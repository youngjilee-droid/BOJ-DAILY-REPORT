import base64
import hashlib
import hmac
import io
import json
import re
import requests
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List

import pandas as pd
import streamlit as st
from meta_api import fetch_meta_data

# =========================================================
# 0. OpenAI 임포트
# =========================================================
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False


# =========================================================
# 1. 기본 설정
# =========================================================
st.set_page_config(
    page_title="광고 통합 대시보드",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main { background-color: #f8f9fa; }

    [data-testid="metric-container"] {
        background-color: white;
        border: 1px solid #e0e0e0;
        border-radius: 12px;
        padding: 16px 20px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    }

    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: #f0f2f6;
        padding: 6px;
        border-radius: 12px;
    }

    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 20px;
        font-weight: 600;
    }

    .filter-card {
        background: white;
        border-radius: 10px;
        padding: 16px;
        margin-bottom: 12px;
        border: 1px solid #e8e8e8;
    }

    .media-badge-connected {
        background: #d4edda;
        color: #155724;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 12px;
        font-weight: 600;
    }

    .media-badge-manual {
        background: #fff3cd;
        color: #856404;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 12px;
        font-weight: 600;
    }

    .ai-comment-box {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 20px;
        border-radius: 12px;
        line-height: 1.8;
    }

    .section-header {
        border-left: 4px solid #4f46e5;
        padding-left: 12px;
        margin: 20px 0 12px 0;
    }

    .status-grid {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 12px;
        margin-bottom: 16px;
    }

    .status-card {
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 14px 16px;
        text-align: center;
        box-shadow: 0 1px 4px rgba(0,0,0,0.04);
    }

    .status-title {
        font-size: 14px;
        font-weight: 700;
        margin-bottom: 8px;
        color: #111827;
    }

    .status-pill-connected {
        display: inline-block;
        background: #dcfce7;
        color: #166534;
        padding: 4px 10px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 700;
    }

    .status-pill-manual {
        display: inline-block;
        background: #fef3c7;
        color: #92400e;
        padding: 4px 10px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 700;
    }

    .action-wrap {
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 16px;
        margin-top: 8px;
        margin-bottom: 12px;
    }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div style='background: linear-gradient(90deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
     padding: 24px 32px; border-radius: 16px; margin-bottom: 24px;'>
    <h1 style='color: white; margin: 0; font-size: 28px;'>📊 광고 통합 대시보드</h1>
    <p style='color: #a8b2d8; margin: 6px 0 0 0; font-size: 14px;'>
        Meta · 네이버 · 카카오 · 틱톡 · 크리테오 · 버즈빌 통합 성과 분석 + AI 코멘트/챗봇
    </p>
</div>
""", unsafe_allow_html=True)

for key in [
    "meta_auto_df", "naver_auto_df", "kakao_auto_df",
    "tiktok_auto_df", "criteo_auto_df", "buzzvil_auto_df",
    "final_report_df", "ai_comment", "ai_chat_history"
]:
    if key not in st.session_state:
        if key == "ai_chat_history":
            st.session_state[key] = []
        elif key == "ai_comment":
            st.session_state[key] = ""
        else:
            st.session_state[key] = pd.DataFrame()


# =========================================================
# 2. 최종 표준 컬럼 정의
# =========================================================
FINAL_COLUMNS = [
    "날짜", "캠페인명", "광고그룹명", "광고명",
    "비용", "실제 비용", "노출", "클릭",
    "구매", "매출액", "장바구니담기수",
    "도달", "참여", "팔로우", "동영상조회", "매체",
]

NUMERIC_COLUMNS = [
    "비용", "실제 비용", "노출", "클릭",
    "구매", "매출액", "장바구니담기수",
    "도달", "참여", "팔로우", "동영상조회",
]


# =========================================================
# 3. 매체별 컬럼 매핑
# =========================================================
NAVER_COLUMN_MAP = {
    "일별": "날짜", "캠페인": "캠페인명", "광고그룹": "광고그룹명",
    "광고그룹명": "광고그룹명", "소재": "광고명", "광고명": "광고명",
    "총비용(vat포함,원)": "비용", "총비용": "비용", "소진액": "비용", "비용": "비용",
    "노출": "노출", "노출수": "노출", "클릭": "클릭", "클릭수": "클릭",
    "총 전환수": "구매", "총전환수": "구매", "전환구매": "구매",
    "총 전환매출액(원)": "매출액", "총전환매출액(원)": "매출액",
    "장바구니": "장바구니담기수", "장바구니담기": "장바구니담기수",
    "장바구니담기수": "장바구니담기수", "도달": "도달", "도달수": "도달",
    "참여": "참여", "참여수": "참여",
}

NAVER_GFA_COLUMN_MAP = {
    "기간": "날짜", "일자": "날짜", "날짜": "날짜",
    "캠페인 이름": "캠페인명", "캠페인명": "캠페인명",
    "광고그룹": "광고그룹명", "광고그룹명": "광고그룹명", "에셋 그룹 이름": "광고그룹명",
    "소재": "광고명", "광고": "광고명", "광고명": "광고명",
    "총비용(vat포함,원)": "비용", "총 비용": "비용",
    "노출": "노출", "클릭": "클릭", "클릭수": "클릭",
    "구매완료수": "구매", "전환구매": "구매",
    "구매완료 전환 매출액": "매출액", "총전환매출액(원)": "매출액",
    "매출액": "매출액", "구매액": "매출액",
    "장바구니": "장바구니담기수", "장바구니담기": "장바구니담기수",
    "장바구니 담기수": "장바구니담기수", "도달": "도달", "도달수": "도달",
    "참여": "참여", "참여수": "참여",
}

META_COLUMN_MAP = {
    "일": "날짜", "날짜": "날짜", "date": "날짜", "day": "날짜",
    "캠페인 이름": "캠페인명", "campaign name": "캠페인명", "campaign": "캠페인명",
    "광고 세트 이름": "광고그룹명", "ad set name": "광고그룹명", "adset name": "광고그룹명",
    "광고 이름": "광고명", "ad name": "광고명", "ad": "광고명",
    "지출 금액": "비용", "지출 금액 (krw)": "비용", "amount spent": "비용", "spend": "비용",
    "노출": "노출", "impressions": "노출",
    "링크 클릭": "클릭", "클릭": "클릭", "clicks": "클릭",
    "구매": "구매", "공유 항목이 포함된 구매": "구매",
    "website purchases": "구매", "meta purchases": "구매",
    "구매 전환값": "매출액", "공유 항목의 구매 전환값": "매출액",
    "purchase conversion value": "매출액", "website purchase conversion value": "매출액",
    "conversion value": "매출액",
    "공유 항목이 포함된 장바구니에 담기": "장바구니담기수",
    "장바구니에 담기": "장바구니담기수", "adds to cart": "장바구니담기수", "add to cart": "장바구니담기수",
    "도달": "도달", "reach": "도달",
    "참여": "참여", "post engagement": "참여", "engagement": "참여", "게시물 참여": "참여",
    "팔로우": "팔로우", "follows": "팔로우", "instagram profile follows": "팔로우",
    "동영상 3초 이상 재생": "동영상조회", "동영상 조회": "동영상조회",
    "video plays": "동영상조회", "video views": "동영상조회",
    "thruplays": "동영상조회", "3-second video plays": "동영상조회",
}

KAKAO_COLUMN_MAP = {
    "날짜": "날짜", "일자": "날짜", "일": "날짜",
    "캠페인": "캠페인명", "캠페인 이름": "캠페인명", "캠페인명": "캠페인명",
    "광고그룹": "광고그룹명", "광고그룹 이름": "광고그룹명", "광고그룹명": "광고그룹명",
    "광고": "광고명", "소재 이름": "광고명", "광고명": "광고명",
    "비용": "비용", "광고비": "비용", "소진액": "비용", "사용금액": "비용",
    "노출": "노출", "노출수": "노출", "클릭": "클릭", "클릭수": "클릭",
    "구매": "구매", "구매 (7일)": "구매", "구매수": "구매", "전환": "구매",
    "매출": "매출액", "구매금액 (7일)": "매출액", "매출액": "매출액", "구매금액": "매출액",
    "장바구니추가(7일)": "장바구니담기수", "장바구니담기": "장바구니담기수",
    "장바구니담기수": "장바구니담기수", "장바구니": "장바구니담기수",
    "도달": "도달", "도달수": "도달", "참여": "참여", "참여수": "참여",
    "팔로우": "팔로우", "팔로우수": "팔로우",
    "동영상조회": "동영상조회", "동영상 조회": "동영상조회",
    "영상조회": "동영상조회", "동영상 재생": "동영상조회",
}

CRITEO_COLUMN_MAP = {
    "date": "날짜", "day": "날짜", "날짜": "날짜", "일자": "날짜",
    "campaign": "캠페인명", "campaign name": "캠페인명", "캠페인": "캠페인명",
    "ad set": "광고그룹명", "ad group": "광고그룹명", "광고그룹": "광고그룹명",
    "creative": "광고명", "ad": "광고명", "광고": "광고명",
    "cost": "비용", "spend": "비용", "광고비": "비용",
    "노출": "노출", "impressions": "노출", "클릭": "클릭", "clicks": "클릭",
    "orders": "구매", "purchase": "구매", "구매": "구매",
    "sales": "매출액", "revenue": "매출액", "매출": "매출액",
    "basket": "장바구니담기수", "add to cart": "장바구니담기수", "장바구니": "장바구니담기수",
    "reach": "도달", "도달": "도달", "engagement": "참여", "참여": "참여",
    "follow": "팔로우", "팔로우": "팔로우", "video views": "동영상조회", "동영상조회": "동영상조회",
}

BUZZVIL_COLUMN_MAP = {
    "date": "날짜", "day": "날짜", "날짜": "날짜", "일자": "날짜",
    "캠페인 이름": "캠페인명", "광고세트 이름": "광고그룹명", "광고그룹": "광고그룹명",
    "소재 이름": "광고명", "cost": "비용", "spend": "비용", "광고비": "비용",
    "노출수": "노출", "impressions": "노출", "클릭수": "클릭", "clicks": "클릭",
    "purchase": "구매", "purchases": "구매", "구매 수": "구매",
    "구매금액(GMV)": "매출액", "sales": "매출액", "매출": "매출액",
    "장바구니": "장바구니담기수", "reach": "도달", "도달": "도달",
    "engagement": "참여", "참여": "참여", "follow": "팔로우", "팔로우": "팔로우",
    "video views": "동영상조회", "동영상조회": "동영상조회",
}

TIKTOK_COLUMN_MAP = {
    "date": "날짜", "By day": "날짜", "stat_time_day": "날짜", "날짜": "날짜",
    "Campaign name": "캠페인명", "campaign": "캠페인명",
    "Ad group name": "광고그룹명", "adgroup name": "광고그룹명", "ad group": "광고그룹명",
    "Ad name": "광고명", "ad": "광고명",
    "spend": "비용", "Cost": "비용", "amount spent": "비용", "지출 금액": "비용",
    "노출": "노출", "Impressions": "노출",
    "Clicks (destination)": "클릭", "clicks": "클릭", "클릭": "클릭",
    "Total purchase (all-channels)": "구매", "purchase": "구매",
    "purchases": "구매", "complete payment": "구매",
    "purchase value": "매출액", "complete payment value": "매출액", "conversion value": "매출액",
    "add to cart": "장바구니담기수", "Total add to cart (all-channels)": "장바구니담기수",
    "adds to cart": "장바구니담기수",
    "Reach": "도달", "도달": "도달", "engagement": "참여", "engaged view": "참여",
    "follows": "팔로우", "followers": "팔로우",
    "Video views": "동영상조회", "video views at 2s": "동영상조회",
    "video views at 6s": "동영상조회", "동영상 조회": "동영상조회",
}

PLATFORM_MAPS = {
    "네이버": NAVER_COLUMN_MAP,
    "네이버 성과형디스플레이": NAVER_GFA_COLUMN_MAP,
    "메타": META_COLUMN_MAP,
    "카카오": KAKAO_COLUMN_MAP,
    "크리테오": CRITEO_COLUMN_MAP,
    "버즈빌": BUZZVIL_COLUMN_MAP,
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
    return {normalize_for_matching(k): v for k, v in column_map.items()}


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
    naver_like_platforms = ["네이버", "네이버 성과형디스플레이"]

    if platform_name in naver_like_platforms:
        df = load_naver_csv_file(uploaded_file) if file_name.endswith(".csv") else load_naver_excel_file(uploaded_file)
    else:
        df = load_csv_file(uploaded_file) if file_name.endswith(".csv") else load_excel_file(uploaded_file)

    if df is None:
        return None

    df["매체"] = platform_name
    return df


def convert_numeric_columns(df: pd.DataFrame, numeric_columns: List[str]) -> pd.DataFrame:
    df = df.copy()
    for col in numeric_columns:
        if col in df.columns:
            series = df[col].astype(str)
            series = series.replace(
                {"None": "", "nan": "", "NaN": "", "null": "", "NULL": "", "-": ""},
                regex=False
            )
            series = series.str.replace(",", "", regex=False)
            series = series.str.replace(" ", "", regex=False)
            series = series.str.replace("%", "", regex=False)
            series = series.str.replace(r"[^0-9\.\-]", "", regex=True)
            df[col] = pd.to_numeric(series, errors="coerce").fillna(0)
    return df


def convert_date_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "날짜" in df.columns:
        parsed = pd.to_datetime(df["날짜"], errors="coerce")
        df["날짜"] = parsed.dt.strftime("%Y-%m-%d").fillna(df["날짜"].astype(str))
    return df


def add_naver_actual_cost(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "실제 비용" not in df.columns:
        df["실제 비용"] = 0

    if "매체" in df.columns and "비용" in df.columns:
        naver_mask = df["매체"] == "네이버"
        df.loc[naver_mask, "실제 비용"] = df.loc[naver_mask, "비용"] / 1.1

    df["실제 비용"] = pd.to_numeric(df["실제 비용"], errors="coerce").fillna(0)
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
            if col in NUMERIC_COLUMNS:
                df[col] = 0
            else:
                df[col] = ""

    df = df[FINAL_COLUMNS]
    df = convert_numeric_columns(df, NUMERIC_COLUMNS)
    df = convert_date_column(df)
    df = add_naver_actual_cost(df)
    return df


def to_csv(df):
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def to_excel_multi_sheet(dfs_dict: dict) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for sheet_name, df in dfs_dict.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    buffer.seek(0)
    return buffer.getvalue()


def to_excel(df, sheet_name="통합리포트"):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buffer.seek(0)
    return buffer.getvalue()


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


def format_summary_for_display(df: pd.DataFrame) -> pd.DataFrame:
    display_df = df.copy()

    for col in NUMERIC_COLUMNS:
        if col in display_df.columns:
            display_df[col] = pd.to_numeric(display_df[col], errors="coerce").fillna(0)

    metric_float_cols = ["CTR", "CPC", "CPA", "ROAS", "CVR"]
    for col in metric_float_cols:
        if col in display_df.columns:
            display_df[col] = pd.to_numeric(display_df[col], errors="coerce").fillna(0).round(2)

    count_cols = [
        "비용", "실제 비용", "노출", "클릭", "구매", "매출액",
        "장바구니담기수", "도달", "참여", "팔로우", "동영상조회"
    ]
    for col in count_cols:
        if col in display_df.columns:
            display_df[col] = pd.to_numeric(display_df[col], errors="coerce").fillna(0)

    return display_df


def combine_all_dataframes(df_list: List[pd.DataFrame]) -> pd.DataFrame:
    valid_dfs = [df for df in df_list if isinstance(df, pd.DataFrame) and not df.empty]
    if not valid_dfs:
        return pd.DataFrame(columns=FINAL_COLUMNS)

    final_df = pd.concat(valid_dfs, ignore_index=True)
    final_df = final_df[FINAL_COLUMNS]
    final_df = convert_numeric_columns(final_df, NUMERIC_COLUMNS)
    final_df = convert_date_column(final_df)
    final_df = add_naver_actual_cost(final_df)
    return final_df


def make_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    daily = (
        df.groupby("날짜", dropna=False)[
            ["비용", "실제 비용", "노출", "클릭", "구매", "매출액", "장바구니담기수", "도달", "참여", "팔로우", "동영상조회"]
        ]
        .sum(numeric_only=True)
        .reset_index()
        .sort_values("날짜")
    )
    if not daily.empty:
        daily["CTR"] = daily.apply(lambda x: safe_divide(x["클릭"], x["노출"]) * 100, axis=1)
        daily["CPC"] = daily.apply(lambda x: safe_divide(x["비용"], x["클릭"]), axis=1)
        daily["CPA"] = daily.apply(lambda x: safe_divide(x["비용"], x["구매"]), axis=1)
        daily["ROAS"] = daily.apply(lambda x: safe_divide(x["매출액"], x["비용"]) * 100, axis=1)
    return format_summary_for_display(daily)


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
        media["CVR"] = media.apply(lambda x: safe_divide(x["구매"], x["클릭"]) * 100, axis=1)

    return format_summary_for_display(media)


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
        campaign["CVR"] = campaign.apply(lambda x: safe_divide(x["구매"], x["클릭"]) * 100, axis=1)

    campaign = campaign.sort_values("비용", ascending=False)
    return format_summary_for_display(campaign)


# =========================================================
# 5. 전일/전전일 요약
# =========================================================
def get_latest_two_dates(df: pd.DataFrame):
    valid_dates = df["날짜_dt"].dropna().dt.normalize().drop_duplicates().sort_values()
    if len(valid_dates) == 0:
        return None, None
    if len(valid_dates) == 1:
        return valid_dates.iloc[-1], None
    return valid_dates.iloc[-1], valid_dates.iloc[-2]


def make_summary_row(df: pd.DataFrame, target_date: pd.Timestamp, label: str) -> pd.DataFrame:
    target_df = df[df["날짜_dt"].dt.normalize() == target_date].copy()
    if target_df.empty:
        return pd.DataFrame()

    cost = target_df["비용"].sum()
    real_cost = target_df["실제 비용"].sum()
    impressions = target_df["노출"].sum()
    clicks = target_df["클릭"].sum()
    purchases = target_df["구매"].sum()
    sales = target_df["매출액"].sum()
    cart = target_df["장바구니담기수"].sum()
    reach = target_df["도달"].sum()
    engagement = target_df["참여"].sum()
    follow = target_df["팔로우"].sum()
    video_views = target_df["동영상조회"].sum()

    summary = pd.DataFrame([{
        "구분": label,
        "날짜": target_date.strftime("%Y-%m-%d"),
        "비용": cost,
        "실제 비용": real_cost,
        "노출": impressions,
        "클릭": clicks,
        "CTR": safe_divide(clicks, impressions) * 100,
        "CPC": safe_divide(cost, clicks),
        "구매": purchases,
        "CPA": safe_divide(cost, purchases),
        "CVR": safe_divide(purchases, clicks) * 100,
        "매출액": sales,
        "ROAS": safe_divide(sales, cost) * 100,
        "장바구니담기수": cart,
        "도달": reach,
        "참여": engagement,
        "팔로우": follow,
        "동영상조회": video_views,
    }])

    return format_summary_for_display(summary)


def make_comparison_summary(df: pd.DataFrame) -> pd.DataFrame:
    latest_date, previous_date = get_latest_two_dates(df)

    if latest_date is None:
        return pd.DataFrame()

    latest_summary = make_summary_row(df, latest_date, "전일")

    if previous_date is None:
        return latest_summary

    previous_summary = make_summary_row(df, previous_date, "전전일")
    return format_summary_for_display(pd.concat([latest_summary, previous_summary], ignore_index=True))


def make_media_comparison_summary(df: pd.DataFrame) -> pd.DataFrame:
    latest_date, previous_date = get_latest_two_dates(df)
    if latest_date is None:
        return pd.DataFrame()

    cols_order = [
        "구분", "날짜", "매체", "비용", "실제 비용", "노출", "클릭", "CTR", "CPC",
        "구매", "CPA", "CVR", "매출액", "ROAS", "장바구니담기수", "도달", "참여", "팔로우", "동영상조회"
    ]

    def _make(df_day, label, date):
        m = df_day.groupby("매체", dropna=False)[
            ["비용", "실제 비용", "노출", "클릭", "구매", "매출액", "장바구니담기수", "도달", "참여", "팔로우", "동영상조회"]
        ].sum(numeric_only=True).reset_index()

        m["구분"] = label
        m["날짜"] = date.strftime("%Y-%m-%d")
        m["CTR"] = m.apply(lambda x: safe_divide(x["클릭"], x["노출"]) * 100, axis=1)
        m["CPC"] = m.apply(lambda x: safe_divide(x["비용"], x["클릭"]), axis=1)
        m["CPA"] = m.apply(lambda x: safe_divide(x["비용"], x["구매"]), axis=1)
        m["CVR"] = m.apply(lambda x: safe_divide(x["구매"], x["클릭"]) * 100, axis=1)
        m["ROAS"] = m.apply(lambda x: safe_divide(x["매출액"], x["비용"]) * 100, axis=1)
        return format_summary_for_display(m)

    latest_df = df[df["날짜_dt"].dt.normalize() == latest_date].copy()
    latest_media = _make(latest_df, "전일", latest_date)

    if previous_date is None:
        return latest_media[cols_order]

    prev_df = df[df["날짜_dt"].dt.normalize() == previous_date].copy()
    prev_media = _make(prev_df, "전전일", previous_date)
    return format_summary_for_display(pd.concat([latest_media, prev_media], ignore_index=True))[cols_order]


def make_campaign_comparison_summary(df: pd.DataFrame) -> pd.DataFrame:
    latest_date, previous_date = get_latest_two_dates(df)
    if latest_date is None:
        return pd.DataFrame()

    cols_order = [
        "구분", "날짜", "매체", "캠페인명", "비용", "실제 비용", "노출", "클릭", "CTR", "CPC",
        "구매", "CPA", "CVR", "매출액", "ROAS", "장바구니담기수", "도달", "참여", "팔로우", "동영상조회"
    ]

    def _make(df_day, label, date):
        c = df_day.groupby(["매체", "캠페인명"], dropna=False)[
            ["비용", "실제 비용", "노출", "클릭", "구매", "매출액", "장바구니담기수", "도달", "참여", "팔로우", "동영상조회"]
        ].sum(numeric_only=True).reset_index()

        c["구분"] = label
        c["날짜"] = date.strftime("%Y-%m-%d")
        c["CTR"] = c.apply(lambda x: safe_divide(x["클릭"], x["노출"]) * 100, axis=1)
        c["CPC"] = c.apply(lambda x: safe_divide(x["비용"], x["클릭"]), axis=1)
        c["CPA"] = c.apply(lambda x: safe_divide(x["비용"], x["구매"]), axis=1)
        c["CVR"] = c.apply(lambda x: safe_divide(x["구매"], x["클릭"]) * 100, axis=1)
        c["ROAS"] = c.apply(lambda x: safe_divide(x["매출액"], x["비용"]) * 100, axis=1)
        c = c.sort_values("비용", ascending=False)
        return format_summary_for_display(c)

    latest_df = df[df["날짜_dt"].dt.normalize() == latest_date].copy()
    latest_c = _make(latest_df, "전일", latest_date)

    if previous_date is None:
        return latest_c[cols_order]

    prev_df = df[df["날짜_dt"].dt.normalize() == previous_date].copy()
    prev_c = _make(prev_df, "전전일", previous_date)
    return format_summary_for_display(pd.concat([latest_c, prev_c], ignore_index=True))[cols_order]


# =========================================================
# 6. OpenAI 공통 함수
# =========================================================
def get_openai_client():
    if not OPENAI_AVAILABLE:
        return None
    try:
        api_key = st.secrets.get("OPENAI_API_KEY", "")
        if not api_key:
            return None
        return OpenAI(api_key=api_key)
    except Exception:
        return None


def get_openai_model():
    try:
        return st.secrets.get("OPENAI_MODEL", "gpt-4o")
    except Exception:
        return "gpt-4o"


def call_openai_text(system_prompt: str, user_prompt: str) -> str:
    client = get_openai_client()
    if client is None:
        return "❌ OpenAI API를 사용할 수 없습니다. OPENAI_API_KEY 또는 openai 라이브러리를 확인해주세요."

    model = get_openai_model()

    try:
        response = client.responses.create(
            model=model,
            instructions=system_prompt,
            input=user_prompt,
        )
        return response.output_text
    except Exception:
        pass

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.4,
            max_tokens=1200
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"❌ OpenAI 호출 실패: {str(e)}"


def build_ai_context(total_df: pd.DataFrame, media_df: pd.DataFrame, campaign_df: pd.DataFrame) -> dict:
    latest_total = total_df[total_df["구분"] == "전일"] if not total_df.empty else pd.DataFrame()
    previous_total = total_df[total_df["구분"] == "전전일"] if not total_df.empty else pd.DataFrame()
    latest_media = media_df[media_df["구분"] == "전일"] if not media_df.empty else pd.DataFrame()
    latest_campaign = campaign_df[campaign_df["구분"] == "전일"] if not campaign_df.empty else pd.DataFrame()

    context = {
        "total_latest": latest_total.to_dict(orient="records"),
        "total_previous": previous_total.to_dict(orient="records"),
        "media_latest_top5": latest_media.head(5).to_dict(orient="records"),
        "campaign_latest_top10": latest_campaign.head(10).to_dict(orient="records"),
    }
    return context


# =========================================================
# 7. AI 코멘트 생성
# =========================================================
def generate_ai_comment(total_df: pd.DataFrame, media_df: pd.DataFrame, campaign_df: pd.DataFrame) -> str:
    if total_df.empty:
        return "❌ 분석할 데이터가 없습니다."

    context = build_ai_context(total_df, media_df, campaign_df)

    system_prompt = """
당신은 퍼포먼스 마케팅 전문 애널리스트입니다.
반드시 한국어로 답변하세요.
과장 없이, 제공된 수치만 근거로 작성하세요.
모르는 것은 모른다고 말하세요.

출력 형식:
1. 전체 성과 요약
2. 전일 대비 변화
3. 매체별 하이라이트
4. 캠페인 인사이트
5. 바로 실행할 액션 3개

문장은 짧고 실무적으로 작성하세요.
"""

    user_prompt = f"""
아래 광고 성과 데이터로 데일리 코멘트를 작성하세요.

{json.dumps(context, ensure_ascii=False, indent=2)}
"""

    return call_openai_text(system_prompt, user_prompt)


# =========================================================
# 8. 룰 기반 코멘트
# =========================================================
def pct_change(curr, prev):
    if prev in [0, None] or pd.isna(prev):
        if curr in [0, None] or pd.isna(curr):
            return 0
        return None
    return ((curr - prev) / prev) * 100


def change_word(value, up_word="증가", down_word="감소", flat_word="유사"):
    if value is None:
        return "비교불가"
    if value > 0.5:
        return up_word
    if value < -0.5:
        return down_word
    return flat_word


def metric_sentence(label, curr, prev, unit="", prefer_rate=True):
    diff = pct_change(curr, prev)
    word = change_word(diff)
    curr_text = f"{curr:,.0f}" if pd.notna(curr) else "0"

    if diff is None:
        return f"{label}은(는) {curr_text}{unit}로 집계되었으며 비교 기준 데이터가 부족합니다."
    if not prefer_rate:
        return f"{label}은(는) 전전일 대비 {word}하여 {curr_text}{unit}입니다."
    return f"{label}은(는) 전전일 대비 {word}하여 {curr_text}{unit}이며 증감률은 {diff:+.1f}%입니다."


def get_top_media_insight(media_df: pd.DataFrame) -> str:
    if media_df.empty:
        return "매체별 비교 데이터가 부족합니다."

    latest = media_df[media_df["구분"] == "전일"].copy()
    prev = media_df[media_df["구분"] == "전전일"].copy()

    if latest.empty:
        return "전일 매체 데이터가 없습니다."

    top_cost_row = latest.sort_values("비용", ascending=False).iloc[0]
    cost_media = top_cost_row["매체"]
    prev_match = prev[prev["매체"] == cost_media]
    prev_roas = prev_match["ROAS"].iloc[0] if not prev_match.empty else None
    curr_roas = top_cost_row["ROAS"]
    roas_diff = pct_change(curr_roas, prev_roas)
    roas_word = change_word(roas_diff, up_word="개선", down_word="하락", flat_word="유사")

    return f"비용 비중이 가장 큰 매체는 {cost_media}이며, 해당 매체의 ROAS는 전전일 대비 {roas_word} 흐름을 보였습니다."


def get_best_roas_media_insight(media_df: pd.DataFrame) -> str:
    if media_df.empty:
        return "매체별 ROAS 비교 데이터가 부족합니다."

    latest = media_df[media_df["구분"] == "전일"].copy()
    latest = latest[latest["비용"] > 0]

    if latest.empty:
        return "ROAS 계산이 가능한 전일 매체 데이터가 없습니다."

    best_row = latest.sort_values("ROAS", ascending=False).iloc[0]
    return f"전일 기준 ROAS가 가장 높은 매체는 {best_row['매체']}이며, ROAS는 {best_row['ROAS']:.1f}%입니다."


def get_campaign_insight(campaign_df: pd.DataFrame) -> str:
    if campaign_df.empty:
        return "캠페인 비교 데이터가 부족합니다."

    latest = campaign_df[campaign_df["구분"] == "전일"].copy()
    latest = latest[latest["비용"] > 0]

    if latest.empty:
        return "전일 캠페인 데이터가 없습니다."

    top_campaign = latest.sort_values("매출액", ascending=False).iloc[0]
    return f"전일 매출액 기준 상위 캠페인은 {top_campaign['매체']}의 {top_campaign['캠페인명']}이며, 매출액은 {top_campaign['매출액']:,.0f}입니다."


def generate_rule_based_comment(total_df: pd.DataFrame, media_df: pd.DataFrame, campaign_df: pd.DataFrame) -> str:
    if total_df.empty:
        return "전일/전전일 비교 데이터가 없어 데일리 코멘트를 생성할 수 없습니다."

    latest = total_df[total_df["구분"] == "전일"]
    previous = total_df[total_df["구분"] == "전전일"]

    if latest.empty:
        return "전일 데이터가 없어 데일리 코멘트를 생성할 수 없습니다."

    latest_row = latest.iloc[0]

    if previous.empty:
        return (
            f"전일 데이터는 {latest_row['날짜']} 기준으로 집계되었습니다. "
            f"총 비용은 {latest_row['비용']:,.0f}, 매출액은 {latest_row['매출액']:,.0f}, "
            f"ROAS는 {latest_row['ROAS']:.1f}%입니다."
        )

    prev_row = previous.iloc[0]
    intro_cost = metric_sentence("총 비용", latest_row["비용"], prev_row["비용"], unit="", prefer_rate=True)
    intro_sales = metric_sentence("매출액", latest_row["매출액"], prev_row["매출액"], unit="", prefer_rate=True)

    roas_diff = pct_change(latest_row["ROAS"], prev_row["ROAS"])
    roas_word = change_word(roas_diff, up_word="개선", down_word="하락", flat_word="유사")
    roas_sentence = f"ROAS는 전전일 대비 {roas_word}되어 {latest_row['ROAS']:.1f}%입니다."

    purchase_diff = pct_change(latest_row["구매"], prev_row["구매"])
    purchase_word = change_word(purchase_diff)
    purchase_sentence = f"구매수는 전전일 대비 {purchase_word}하여 {latest_row['구매']:,.0f}건입니다."

    media_sentence = get_top_media_insight(media_df)
    best_media_sentence = get_best_roas_media_insight(media_df)
    campaign_sentence = get_campaign_insight(campaign_df)

    comments = [
        f"전일({latest_row['날짜']}) 기준 전체 성과를 보면, {intro_cost}",
        f"{intro_sales} {roas_sentence}",
        f"{purchase_sentence} {media_sentence}",
        f"{best_media_sentence} {campaign_sentence}",
    ]
    return "\n\n".join(comments)


# =========================================================
# 9. 챗봇 응답 생성
# =========================================================
def generate_chatbot_answer(user_question: str, total_df: pd.DataFrame, media_df: pd.DataFrame, campaign_df: pd.DataFrame, history: list) -> str:
    if total_df.empty:
        return "현재 비교 데이터가 없어 답변드리기 어렵습니다."

    context = build_ai_context(total_df, media_df, campaign_df)
    recent_history = history[-6:] if history else []

    system_prompt = """
당신은 광고 성과 대시보드 안에서 동작하는 데이터 분석 챗봇입니다.
반드시 한국어로 답변하세요.
반드시 제공된 데이터만 근거로 답변하세요.
추정은 최소화하세요.
데이터에 없는 내용은 '현재 데이터 기준으로는 확인되지 않습니다'라고 답하세요.
가능하면 수치를 포함해 짧고 실무적으로 설명하세요.
"""

    user_prompt = f"""
[대시보드 데이터]
{json.dumps(context, ensure_ascii=False, indent=2)}

[최근 대화]
{json.dumps(recent_history, ensure_ascii=False, indent=2)}

[사용자 질문]
{user_question}
"""

    return call_openai_text(system_prompt, user_prompt)


# =========================================================
# 10. 매체 API 연동 함수들
# =========================================================
def fetch_naver_api_data(start_date: str, end_date: str) -> pd.DataFrame:
    try:
        api_key = st.secrets.get("NAVER_API_KEY", "")
        secret_key = st.secrets.get("NAVER_SECRET_KEY", "")
        customer_id = st.secrets.get("NAVER_CUSTOMER_ID", "")
    except Exception:
        api_key = secret_key = customer_id = ""

    if not all([api_key, secret_key, customer_id]):
        return pd.DataFrame()

    try:
        base_url = "https://api.searchad.naver.com"
        timestamp = str(int(time.time() * 1000))

        def get_signature(ts, method, path, sec_key):
            message = f"{ts}.{method}.{path}"
            secret = bytes(sec_key, "utf-8")
            message_bytes = bytes(message, "utf-8")
            sign = base64.b64encode(
                hmac.new(secret, message_bytes, digestmod=hashlib.sha256).digest()
            ).decode("utf-8")
            return sign

        path = "/stats"
        signature = get_signature(timestamp, "GET", path, secret_key)

        headers = {
            "X-Timestamp": timestamp,
            "X-API-KEY": api_key,
            "X-Customer": str(customer_id),
            "X-Signature": signature,
        }

        params = {
            "fields": '["clkCnt","impCnt","salesAmt","crdiSalesAmt","rvsCnt","cnvPurchaseAmt"]',
            "timeRange": json.dumps({"since": start_date.replace("-", ""), "until": end_date.replace("-", "")}),
            "timeUnit": "DAY",
        }

        response = requests.get(f"{base_url}{path}", headers=headers, params=params, timeout=30)

        if response.status_code == 200:
            data = response.json()
            rows = []

            for item in data.get("data", []):
                rows.append({
                    "날짜": item.get("dt", ""),
                    "캠페인명": item.get("campaignName", ""),
                    "광고그룹명": item.get("adGroupName", ""),
                    "광고명": item.get("adName", ""),
                    "비용": item.get("salesAmt", 0),
                    "노출": item.get("impCnt", 0),
                    "클릭": item.get("clkCnt", 0),
                    "구매": item.get("rvsCnt", 0),
                    "매출액": item.get("crdiSalesAmt", 0),
                    "매체": "네이버",
                })

            if rows:
                df = pd.DataFrame(rows)
                return standardize_columns(df, "네이버", PLATFORM_MAPS["네이버"])
    except Exception as e:
        st.error(f"네이버 API 연결 실패: {str(e)}")

    return pd.DataFrame()


def fetch_kakao_api_data(start_date: str, end_date: str) -> pd.DataFrame:
    try:
        access_token = st.secrets.get("KAKAO_ACCESS_TOKEN", "")
    except Exception:
        access_token = ""

    if not access_token:
        return pd.DataFrame()

    try:
        base_url = "https://apis.moment.kakao.com/openapi/v4"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        account_resp = requests.get(f"{base_url}/adAccounts", headers=headers, timeout=30)

        if account_resp.status_code != 200:
            return pd.DataFrame()

        accounts = account_resp.json().get("content", [])
        all_rows = []

        for account in accounts:
            account_id = account.get("id")

            params = {
                "adAccountId": account_id,
                "startDate": start_date,
                "endDate": end_date,
                "timeUnit": "DAY",
                "metricsGroups": "BASIC,CONVERSION",
                "dimension": "CAMPAIGN",
            }

            report_resp = requests.get(f"{base_url}/stats/adAccounts", headers=headers, params=params, timeout=30)

            if report_resp.status_code == 200:
                for item in report_resp.json().get("rows", []):
                    dims = item.get("dimensions", {})
                    metrics = item.get("metrics", {})
                    all_rows.append({
                        "날짜": dims.get("date", ""),
                        "캠페인명": dims.get("campaignName", ""),
                        "광고그룹명": dims.get("adGroupName", ""),
                        "광고명": dims.get("creativeName", ""),
                        "비용": metrics.get("spend", 0),
                        "노출": metrics.get("impression", 0),
                        "클릭": metrics.get("click", 0),
                        "구매": metrics.get("purchase_7d", 0),
                        "매출액": metrics.get("purchaseAmt_7d", 0),
                        "장바구니담기수": metrics.get("addToCart_7d", 0),
                        "매체": "카카오",
                    })

        if all_rows:
            df = pd.DataFrame(all_rows)
            return standardize_columns(df, "카카오", PLATFORM_MAPS["카카오"])
    except Exception as e:
        st.error(f"카카오 API 연결 실패: {str(e)}")

    return pd.DataFrame()


def fetch_tiktok_api_data(start_date: str, end_date: str) -> pd.DataFrame:
    try:
        access_token = st.secrets.get("TIKTOK_ACCESS_TOKEN", "")
        advertiser_id = st.secrets.get("TIKTOK_ADVERTISER_ID", "")
    except Exception:
        access_token = advertiser_id = ""

    if not all([access_token, advertiser_id]):
        return pd.DataFrame()

    try:
        base_url = "https://business-api.tiktok.com/open_api/v1.3"
        headers = {
            "Access-Token": access_token,
            "Content-Type": "application/json",
        }

        payload = {
            "advertiser_id": advertiser_id,
            "report_type": "BASIC",
            "data_level": "AUCTION_CAMPAIGN",
            "dimensions": ["campaign_id", "stat_time_day"],
            "metrics": [
                "campaign_name", "spend", "impressions", "clicks",
                "purchase", "add_to_cart"
            ],
            "start_date": start_date,
            "end_date": end_date,
            "page_size": 1000,
            "page": 1,
        }

        response = requests.post(
            f"{base_url}/report/integrated/get/",
            headers=headers,
            json=payload,
            timeout=30
        )

        if response.status_code == 200:
            result = response.json()
            if result.get("code") == 0:
                rows = []
                for item in result.get("data", {}).get("list", []):
                    dims = item.get("dimensions", {})
                    metrics = item.get("metrics", {})
                    rows.append({
                        "날짜": dims.get("stat_time_day", ""),
                        "캠페인명": metrics.get("campaign_name", ""),
                        "광고그룹명": "",
                        "광고명": "",
                        "비용": float(metrics.get("spend", 0)),
                        "노출": int(metrics.get("impressions", 0)),
                        "클릭": int(metrics.get("clicks", 0)),
                        "구매": float(metrics.get("purchase", 0)),
                        "매출액": 0,
                        "장바구니담기수": float(metrics.get("add_to_cart", 0)),
                        "매체": "틱톡",
                    })

                if rows:
                    df = pd.DataFrame(rows)
                    return standardize_columns(df, "틱톡", PLATFORM_MAPS["틱톡"])
    except Exception as e:
        st.error(f"틱톡 API 연결 실패: {str(e)}")

    return pd.DataFrame()


def fetch_criteo_api_data(start_date: str, end_date: str) -> pd.DataFrame:
    try:
        client_id = st.secrets.get("CRITEO_CLIENT_ID", "")
        client_secret = st.secrets.get("CRITEO_CLIENT_SECRET", "")
    except Exception:
        client_id = client_secret = ""

    if not all([client_id, client_secret]):
        return pd.DataFrame()

    try:
        token_response = requests.post(
            "https://api.criteo.com/oauth2/token",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=30
        )

        if token_response.status_code != 200:
            return pd.DataFrame()

        access_token = token_response.json().get("access_token")
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        report_payload = {
            "data": {
                "type": "Statistics",
                "attributes": {
                    "reportFormat": "json",
                    "startDate": start_date,
                    "endDate": end_date,
                    "dimensions": ["Day", "Campaign"],
                    "metrics": ["Displays", "Clicks", "Cost", "Sales", "ConversionsValue", "Carts"],
                    "currency": "KRW",
                    "timezone": "Asia/Seoul",
                }
            }
        }

        report_response = requests.post(
            "https://api.criteo.com/2023-10/statistics/report",
            headers=headers,
            json=report_payload,
            timeout=60
        )

        if report_response.status_code == 200:
            rows = []
            for item in report_response.json().get("Rows", []):
                rows.append({
                    "날짜": item.get("Day", ""),
                    "캠페인명": item.get("Campaign", ""),
                    "광고그룹명": "",
                    "광고명": "",
                    "비용": float(item.get("Cost", 0)),
                    "노출": int(item.get("Displays", 0)),
                    "클릭": int(item.get("Clicks", 0)),
                    "구매": float(item.get("Sales", 0)),
                    "매출액": float(item.get("ConversionsValue", 0)),
                    "장바구니담기수": float(item.get("Carts", 0)),
                    "매체": "크리테오",
                })

            if rows:
                df = pd.DataFrame(rows)
                return standardize_columns(df, "크리테오", PLATFORM_MAPS["크리테오"])
    except Exception as e:
        st.error(f"크리테오 API 연결 실패: {str(e)}")

    return pd.DataFrame()


# =========================================================
# 11. 대시보드 렌더링
# =========================================================
def render_dashboard(df: pd.DataFrame):
    dashboard_df = build_dashboard_df(df)

    valid_dates = dashboard_df["날짜_dt"].dropna()
    min_date = valid_dates.min().date() if not valid_dates.empty else None
    max_date = valid_dates.max().date() if not valid_dates.empty else None

    with st.sidebar:
        st.markdown("## 🔍 대시보드 필터")
        st.markdown("---")

        st.markdown("**📅 기간 빠른 선택**")
        qcol1, qcol2, qcol3 = st.columns(3)
        today = max_date or datetime.today().date()

        if qcol1.button("7일", key="q7", use_container_width=True):
            st.session_state["q_start"] = today - timedelta(days=6)
            st.session_state["q_end"] = today
        if qcol2.button("14일", key="q14", use_container_width=True):
            st.session_state["q_start"] = today - timedelta(days=13)
            st.session_state["q_end"] = today
        if qcol3.button("30일", key="q30", use_container_width=True):
            st.session_state["q_start"] = today - timedelta(days=29)
            st.session_state["q_end"] = today

        default_start = st.session_state.get("q_start", min_date)
        default_end = st.session_state.get("q_end", max_date)

        if min_date and max_date:
            selected_dates = st.date_input(
                "날짜 직접 선택",
                value=(default_start or min_date, default_end or max_date),
                min_value=min_date,
                max_value=max_date,
                key="dashboard_date_range",
            )
        else:
            selected_dates = None

        st.markdown("---")
        st.markdown("**📺 매체 선택**")
        media_options = sorted([m for m in dashboard_df["매체"].dropna().unique().tolist() if str(m).strip() != ""])

        select_all_media = st.checkbox("전체 매체 선택", value=True, key="all_media")
        if select_all_media:
            selected_media = media_options
        else:
            selected_media = st.multiselect(
                "매체 선택",
                media_options,
                default=media_options,
                key="dashboard_media",
                label_visibility="collapsed"
            )

        st.markdown("---")
        st.markdown("**🎯 캠페인 선택**")
        campaign_options = sorted([c for c in dashboard_df["캠페인명"].dropna().unique().tolist() if str(c).strip() != ""])

        select_all_campaign = st.checkbox("전체 캠페인 선택", value=True, key="all_campaign")
        if select_all_campaign:
            selected_campaigns = campaign_options
        else:
            selected_campaigns = st.multiselect(
                "캠페인 선택",
                campaign_options,
                default=campaign_options,
                key="dashboard_campaign",
                label_visibility="collapsed"
            )

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
        st.warning("⚠️ 필터 조건에 맞는 데이터가 없습니다.")
        return

    if selected_dates and len(selected_dates) == 2:
        st.markdown(f"**📅 {selected_dates[0]} ~ {selected_dates[1]} | 📺 {', '.join(selected_media) if selected_media else '전체'}**")

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
    cvr = safe_divide(total_purchases, total_clicks) * 100

    st.markdown("### 📊 핵심 성과 지표")

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("💰 총 비용", f"₩{total_cost:,.0f}")
    col2.metric("💵 실제 비용", f"₩{total_real_cost:,.0f}")
    col3.metric("🛒 총 매출액", f"₩{total_sales:,.0f}")
    col4.metric("📈 ROAS", f"{roas:.1f}%")
    col5.metric("🎯 CPA", f"₩{cpa:,.0f}")

    col6, col7, col8, col9, col10 = st.columns(5)
    col6.metric("👁️ 총 노출", f"{total_impressions:,.0f}")
    col7.metric("🖱️ 총 클릭", f"{total_clicks:,.0f}")
    col8.metric("📊 CTR", f"{ctr:.2f}%")
    col9.metric("💲 CPC", f"₩{cpc:,.0f}")
    col10.metric("🛍️ 구매수", f"{total_purchases:,.0f}")

    col11, _, _, _, _ = st.columns(5)
    col11.metric("📌 CVR", f"{cvr:.2f}%")

    st.markdown("---")

    daily_summary = make_daily_summary(filtered_df)
    media_summary = make_media_summary(filtered_df)
    campaign_summary = make_campaign_summary(filtered_df)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### 📉 일자별 비용 추이")
        if not daily_summary.empty:
            st.line_chart(daily_summary.set_index("날짜")[["비용", "실제 비용"]])
        else:
            st.info("표시할 데이터가 없습니다.")

    with c2:
        st.markdown("#### 📈 일자별 매출 추이")
        if not daily_summary.empty:
            st.line_chart(daily_summary.set_index("날짜")[["매출액"]])
        else:
            st.info("표시할 데이터가 없습니다.")

    c3, c4 = st.columns(2)
    with c3:
        st.markdown("#### 👁️ 일자별 노출 / 클릭")
        if not daily_summary.empty:
            st.line_chart(daily_summary.set_index("날짜")[["노출", "클릭"]])
        else:
            st.info("표시할 데이터가 없습니다.")

    with c4:
        st.markdown("#### 🛍️ 일자별 구매")
        if not daily_summary.empty:
            st.line_chart(daily_summary.set_index("날짜")[["구매"]])
        else:
            st.info("표시할 데이터가 없습니다.")

    st.markdown("---")

    sub_tab1, sub_tab2, sub_tab3 = st.tabs(["📺 매체별 요약", "🎯 캠페인별 요약", "📋 원본 데이터"])

    with sub_tab1:
        st.dataframe(media_summary, use_container_width=True)
        st.download_button(
            "📥 매체별 요약 다운로드",
            to_csv(media_summary),
            "media_summary.csv",
            mime="text/csv",
            key="dashboard_media_summary_download"
        )

    with sub_tab2:
        st.dataframe(campaign_summary, use_container_width=True)
        st.download_button(
            "📥 캠페인별 요약 다운로드",
            to_csv(campaign_summary),
            "campaign_summary.csv",
            mime="text/csv",
            key="dashboard_campaign_summary_download"
        )

    with sub_tab3:
        st.dataframe(filtered_df.drop(columns=["날짜_dt"], errors="ignore"), use_container_width=True)


# =========================================================
# 12. 코멘트/챗봇 섹션
# =========================================================
def render_comment_data_section(df: pd.DataFrame):
    comparison_summary = make_comparison_summary(df)
    media_comparison_summary = make_media_comparison_summary(df)
    campaign_comparison_summary = make_campaign_comparison_summary(df)

    if comparison_summary.empty:
        st.warning("⚠️ 전일/전전일 요약 데이터를 만들 수 없습니다.")
        return

    t1, t2, t3 = st.tabs(["전체 요약", "매체별 요약", "캠페인별 요약"])

    with t1:
        st.dataframe(comparison_summary, use_container_width=True)
        st.download_button(
            "📥 전체 요약 다운로드",
            to_csv(comparison_summary),
            "total_summary.csv",
            mime="text/csv",
            key="comparison_total_summary_download"
        )

    with t2:
        st.dataframe(media_comparison_summary, use_container_width=True)
        st.download_button(
            "📥 매체별 요약 다운로드",
            to_csv(media_comparison_summary),
            "media_summary.csv",
            mime="text/csv",
            key="comparison_media_summary_download"
        )

    with t3:
        st.dataframe(campaign_comparison_summary, use_container_width=True)
        st.download_button(
            "📥 캠페인별 요약 다운로드",
            to_csv(campaign_comparison_summary),
            "campaign_summary.csv",
            mime="text/csv",
            key="comparison_campaign_summary_download"
        )


def render_daily_comment_section(df: pd.DataFrame):
    total_summary = make_comparison_summary(df)
    media_summary = make_media_comparison_summary(df)
    campaign_summary = make_campaign_comparison_summary(df)

    if total_summary.empty:
        st.warning("⚠️ AI 코멘트와 챗봇을 위해 비교 데이터가 필요합니다.")
        return

    st.markdown("### 🤖 AI 데일리 코멘트")
    st.markdown("OpenAI API로 전일/전전일 비교 데이터를 분석해 코멘트를 생성합니다.")

    ai_col1, ai_col2 = st.columns([1, 4])
    with ai_col1:
        generate_ai = st.button("✨ AI 코멘트 생성", type="primary", use_container_width=True)

    if generate_ai:
        with st.spinner("🤖 AI가 데이터를 분석하고 있습니다..."):
            ai_comment = generate_ai_comment(total_summary, media_summary, campaign_summary)
            st.session_state["ai_comment"] = ai_comment

    if st.session_state.get("ai_comment", ""):
        st.markdown(f"""
<div style='background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
     color: #e8e8f0; padding: 24px; border-radius: 12px;
     border-left: 4px solid #6c63ff; line-height: 1.9; margin: 12px 0;
     font-size: 15px;'>
{st.session_state["ai_comment"].replace(chr(10), "<br>")}
</div>
""", unsafe_allow_html=True)
        st.download_button(
            "📥 AI 코멘트 다운로드",
            data=st.session_state["ai_comment"].encode("utf-8"),
            file_name="ai_daily_comment.txt",
            mime="text/plain",
            key="ai_comment_download"
        )

    st.markdown("---")
    st.markdown("### 📋 룰 기반 데일리 코멘트")
    rule_comment = generate_rule_based_comment(total_summary, media_summary, campaign_summary)
    st.text_area("룰 기반 코멘트 결과", value=rule_comment, height=220)
    st.download_button(
        "📥 룰 기반 코멘트 다운로드",
        data=rule_comment.encode("utf-8"),
        file_name="rule_daily_comment.txt",
        mime="text/plain",
        key="rule_comment_download"
    )

    st.markdown("---")
    st.markdown("### 💬 데이터 기반 AI 챗봇")
    st.markdown("현재 전일/전전일 비교 데이터와 상위 매체/캠페인 데이터를 바탕으로 답변합니다.")

    if "ai_chat_history" not in st.session_state:
        st.session_state["ai_chat_history"] = []

    for msg in st.session_state["ai_chat_history"]:
        with st.chat_message("user" if msg["role"] == "user" else "assistant"):
            st.markdown(msg["content"])

    user_question = st.chat_input("예: 어떤 매체가 ROAS가 가장 높았어? / 전일 대비 매출이 왜 변했을까?")

    if user_question:
        st.session_state["ai_chat_history"].append({"role": "user", "content": user_question})

        with st.chat_message("user"):
            st.markdown(user_question)

        with st.chat_message("assistant"):
            with st.spinner("답변 생성 중입니다..."):
                answer = generate_chatbot_answer(
                    user_question=user_question,
                    total_df=total_summary,
                    media_df=media_summary,
                    campaign_df=campaign_summary,
                    history=st.session_state["ai_chat_history"],
                )
                st.markdown(answer)

        st.session_state["ai_chat_history"].append({"role": "assistant", "content": answer})

    c1, c2 = st.columns([1, 5])
    with c1:
        if st.button("🧹 챗봇 대화 초기화", use_container_width=True):
            st.session_state["ai_chat_history"] = []
            st.rerun()


# =========================================================
# 13. 데이터 수집 UI
# =========================================================
def render_collection_tab():
    st.markdown("### 📅 수집 기간 설정")
    d1, d2, _ = st.columns([2, 2, 4])

    with d1:
        start_date = st.date_input(
            "시작일",
            value=datetime.today().date() - timedelta(days=7),
            key="global_start"
        )

    with d2:
        end_date = st.date_input(
            "종료일",
            value=datetime.today().date() - timedelta(days=1),
            key="global_end"
        )

    st.markdown("---")
    st.markdown("### ⚡ API 자동 수집 (원클릭)")

    status_items = [
        ("Meta", "meta_auto_df"),
        ("네이버", "naver_auto_df"),
        ("카카오", "kakao_auto_df"),
        ("틱톡", "tiktok_auto_df"),
        ("크리테오", "criteo_auto_df"),
        ("버즈빌", "buzzvil_auto_df"),
    ]

    status_html = "<div class='status-grid'>"
    for label, key in status_items:
        is_connected = not st.session_state[key].empty
        pill_class = "status-pill-connected" if is_connected else "status-pill-manual"
        badge = "연결됨" if is_connected else "미수집"

        status_html += f"""
        <div class="status-card">
            <div class="status-title">{label}</div>
            <div class="{pill_class}">{badge}</div>
        </div>
        """
    status_html += "</div>"

    st.markdown(status_html, unsafe_allow_html=True)

    st.markdown("<div class='action-wrap'>", unsafe_allow_html=True)
    btn1, btn2, _ = st.columns([1.2, 1.2, 3.6])

    auto_collect_clicked = False
    create_report_clicked = False

    with btn1:
        auto_collect_clicked = st.button(
            "🚀 자동 수집 실행",
            type="primary",
            use_container_width=True
        )

    with btn2:
        create_report_clicked = st.button(
            "🧩 통합 리포트 생성",
            use_container_width=True
        )

    st.markdown("</div>", unsafe_allow_html=True)

    if auto_collect_clicked:
        with st.spinner("API 데이터를 수집 중입니다..."):
            st.session_state["meta_auto_df"] = fetch_meta_data(str(start_date), str(end_date))
            st.session_state["naver_auto_df"] = fetch_naver_api_data(str(start_date), str(end_date))
            st.session_state["kakao_auto_df"] = fetch_kakao_api_data(str(start_date), str(end_date))
            st.session_state["tiktok_auto_df"] = fetch_tiktok_api_data(str(start_date), str(end_date))
            st.session_state["criteo_auto_df"] = fetch_criteo_api_data(str(start_date), str(end_date))
            st.success("자동 수집을 완료했습니다.")

    if create_report_clicked:
        manual_dfs = st.session_state.get("manual_upload_dfs", [])
        st.session_state["final_report_df"] = combine_all_dataframes(
            [
                st.session_state["meta_auto_df"],
                st.session_state["naver_auto_df"],
                st.session_state["kakao_auto_df"],
                st.session_state["tiktok_auto_df"],
                st.session_state["criteo_auto_df"],
                st.session_state["buzzvil_auto_df"],
            ] + manual_dfs
        )
        if st.session_state["final_report_df"].empty:
            st.warning("통합할 데이터가 없습니다.")
        else:
            st.success("통합 리포트를 생성했습니다.")

    st.markdown("---")
    st.markdown("### 📂 수동 업로드")

    if "manual_upload_dfs" not in st.session_state:
        st.session_state["manual_upload_dfs"] = []

    upload_tabs = st.tabs(["메타", "네이버", "네이버 성과형디스플레이", "카카오", "틱톡", "크리테오", "버즈빌"])

    upload_configs = [
        ("메타", META_COLUMN_MAP),
        ("네이버", NAVER_COLUMN_MAP),
        ("네이버 성과형디스플레이", NAVER_GFA_COLUMN_MAP),
        ("카카오", KAKAO_COLUMN_MAP),
        ("틱톡", TIKTOK_COLUMN_MAP),
        ("크리테오", CRITEO_COLUMN_MAP),
        ("버즈빌", BUZZVIL_COLUMN_MAP),
    ]

    new_manual_dfs = []

    for tab, (platform_name, column_map) in zip(upload_tabs, upload_configs):
        with tab:
            uploaded_file = st.file_uploader(
                f"{platform_name} 파일 업로드",
                type=["csv", "xlsx"],
                key=f"upload_{platform_name}"
            )

            if uploaded_file is not None:
                raw_df = load_file(uploaded_file, platform_name)
                if raw_df is None:
                    st.error("파일을 읽지 못했습니다.")
                else:
                    std_df = standardize_columns(raw_df, platform_name, column_map)
                    st.success(f"{platform_name} 파일 표준화 완료")
                    st.dataframe(std_df.head(20), use_container_width=True)
                    new_manual_dfs.append(std_df)

    st.session_state["manual_upload_dfs"] = new_manual_dfs

    st.markdown("---")
    st.markdown("### 📦 현재 통합 데이터")

    final_df = st.session_state["final_report_df"]

    if final_df.empty:
        st.info("아직 통합 리포트가 없습니다. 자동 수집 또는 수동 업로드 후 통합 리포트를 생성해주세요.")
    else:
        st.dataframe(final_df, use_container_width=True, height=380)

        dcol1, dcol2 = st.columns(2)
        with dcol1:
            st.download_button(
                "📥 CSV 다운로드",
                to_csv(final_df),
                "final_report.csv",
                mime="text/csv",
                use_container_width=True
            )

        with dcol2:
            st.download_button(
                "📥 Excel 다운로드",
                to_excel(final_df),
                "final_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )


# =========================================================
# 14. 메인 UI
# =========================================================
tab1, tab2, tab3, tab4 = st.tabs([
    "🔗 데이터 수집",
    "📊 대시보드",
    "📋 성과 비교 데이터",
    "💬 데일리 코멘트",
])

with tab1:
    render_collection_tab()

with tab2:
    if st.session_state["final_report_df"].empty:
        st.info("통합 리포트가 없습니다. 먼저 데이터 수집 탭에서 리포트를 생성해주세요.")
    else:
        render_dashboard(st.session_state["final_report_df"])

with tab3:
    if st.session_state["final_report_df"].empty:
        st.info("통합 리포트가 없습니다. 먼저 데이터 수집 탭에서 리포트를 생성해주세요.")
    else:
        render_comment_data_section(build_dashboard_df(st.session_state["final_report_df"]))

with tab4:
    if st.session_state["final_report_df"].empty:
        st.info("통합 리포트가 없습니다. 먼저 데이터 수집 탭에서 리포트를 생성해주세요.")
    else:
        render_daily_comment_section(build_dashboard_df(st.session_state["final_report_df"]))
