import io
import re
from typing import Optional, Dict, List, Tuple

import pandas as pd
import streamlit as st


# =========================================================
# 1. 기본 설정
# =========================================================
st.set_page_config(page_title="광고 통합 리포트 생성기", layout="wide")

st.title("광고 통합 리포트 생성기")
st.write("매체별 RAW 데이터를 업로드하면 필수 지표 기준으로 통합 리포트를 생성합니다.")


# =========================================================
# 2. 최종 표준 컬럼 정의
# =========================================================
FINAL_COLUMNS = [
    "날짜",
    "캠페인명",
    "광고그룹명",
    "광고명",
    "비용",
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
    "date": "날짜",
    "day": "날짜",
    "날짜": "날짜",
    "일": "날짜",
    "campaign name": "캠페인명",
    "캠페인 이름": "캠페인명",
    "ad set name": "광고그룹명",
    "광고 세트 이름": "광고그룹명"
    "광고 이름": "광고명",
    "ad name": "광고명",
    "amount spent": "비용",
    "spend": "비용",
    "지출 금액": "비용",
    "impressions": "노출",
    "clicks": "클릭",
    "링크 클릭": "클릭",
    "purchases": "구매",
    "공유 항목이 포함된 구매": "구매",
    "공유 항목의 구매 전환값": "매출액",
    "purchase conversion value": "매출액",
    "공유 항목이 포함된 장바구니에 담기": "장바구니담기수",
    "adds to cart": "장바구니담기수",
    "reach": "도달",
    "engagement": "참여",
    "follows": "팔로우",
    "동영상 3초 이상 재생": "동영상조회",
    "video views": "동영상조회",
}

KAKAO_COLUMN_MAP = {
    "날짜": "날짜",
    "캠페인": "캠페인명",
    "광고그룹": "광고그룹명",
    "광고": "광고명",
    "비용": "비용",
    "노출": "노출",
    "클릭": "클릭",
    "구매": "구매",
    "매출": "매출액",
    "장바구니": "장바구니담기수",
    "도달": "도달",
    "참여": "참여",
    "팔로우": "팔로우",
    "동영상조회": "동영상조회",
}

TIKTOK_COLUMN_MAP = {
    "date": "날짜",
    "campaign name": "캠페인명",
    "ad group name": "광고그룹명",
    "ad name": "광고명",
    "spend": "비용",
    "impressions": "노출",
    "clicks": "클릭",
    "purchase": "구매",
    "purchase value": "매출액",
    "add to cart": "장바구니담기수",
    "reach": "도달",
    "engagement": "참여",
    "follows": "팔로우",
    "video views": "동영상조회",
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
            except:
                continue
    return None


def load_excel_file(uploaded_file) -> Optional[pd.DataFrame]:
    try:
        uploaded_file.seek(0)
        return pd.read_excel(uploaded_file, engine="openpyxl")
    except:
        return None


# 🔥 핵심: 네이버는 무조건 첫 줄 제거
def load_naver_excel_file(uploaded_file) -> Optional[pd.DataFrame]:
    try:
        uploaded_file.seek(0)
        df = pd.read_excel(uploaded_file, engine="openpyxl", skiprows=1)
        df.columns = [clean_column_name(c) for c in df.columns]
        df = df.dropna(how="all").reset_index(drop=True)
        return df
    except:
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
            except:
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
    return df


def to_csv(df):
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def to_excel(df):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    buffer.seek(0)
    return buffer.getvalue()


# =========================================================
# 5. UI
# =========================================================
naver = st.file_uploader("네이버", type=["csv", "xlsx"])
meta = st.file_uploader("메타", type=["csv", "xlsx"])
kakao = st.file_uploader("카카오", type=["csv", "xlsx"])
tiktok = st.file_uploader("틱톡", type=["csv", "xlsx"])

files = [
    ("네이버", naver),
    ("메타", meta),
    ("카카오", kakao),
    ("틱톡", tiktok),
]


# =========================================================
# 6. 실행
# =========================================================
if st.button("리포트 생성"):
    dfs = []

    for name, file in files:
        if file:
            df = load_file(file, name)

            # 디버깅 (필요시 사용)
            # if name == "네이버":
            #     st.write(df.head())

            df = standardize_columns(df, name, PLATFORM_MAPS[name])
            dfs.append(df)

    if dfs:
        final = pd.concat(dfs, ignore_index=True)

        st.dataframe(final)

        st.download_button("CSV 다운로드", to_csv(final), "report.csv")
        st.download_button("엑셀 다운로드", to_excel(final), "report.xlsx")
    else:
        st.error("파일 없음")
