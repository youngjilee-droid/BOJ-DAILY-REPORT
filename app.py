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
# 사용자가 원하는 순서로 고정
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
# 여기만 수정하면 유지보수가 쉬움
# key: 원본 컬럼명
# value: 표준 컬럼명
# =========================================================
NAVER_COLUMN_MAP = {
    "일별": "날짜",
    "일자": "날짜",
    "캠페인": "캠페인명",
    "캠페인명": "캠페인명",
    "광고그룹": "광고그룹명",
    "광고그룹명": "광고그룹명",
    "소재": "광고명",
    "광고명": "광고명",
    "총비용(VAT포함,원)": "비용",
    "광고비": "비용",
    "소진액": "비용",
    "노출": "노출",
    "노출수": "노출",
    "클릭": "클릭",
    "클릭수": "클릭",
    "총 전환수": "구매",
    "구매수": "구매",
    "전환구매": "구매",
    "총 전환매출액(원)": "매출액",
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
    "클릭": "클릭",
    "clicks": "클릭",
    "구매": "구매",
    "purchases": "구매",
    "website purchases": "구매",
    "meta purchases": "구매",
    "구매 전환값": "매출액",
    "purchase conversion value": "매출액",
    "website purchase conversion value": "매출액",
    "conversion value": "매출액",
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
    "동영상 조회": "동영상조회",
    "video plays": "동영상조회",
    "video views": "동영상조회",
    "thruplays": "동영상조회",
    "동영상 3초 이상 재생": "동영상조회",
    "3-second video plays": "동영상조회",
}

KAKAO_COLUMN_MAP = {
    "날짜": "날짜",
    "일자": "날짜",
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
    "구매수": "구매",
    "전환": "구매",
    "매출": "매출액",
    "매출액": "매출액",
    "구매금액": "매출액",
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
    "day": "날짜",
    "stat_time_day": "날짜",
    "날짜": "날짜",
    "campaign name": "캠페인명",
    "campaign": "캠페인명",
    "ad group name": "광고그룹명",
    "adgroup name": "광고그룹명",
    "ad group": "광고그룹명",
    "ad name": "광고명",
    "ad": "광고명",
    "spend": "비용",
    "amount spent": "비용",
    "지출 금액": "비용",
    "노출": "노출",
    "impressions": "노출",
    "clicks": "클릭",
    "클릭": "클릭",
    "purchase": "구매",
    "purchases": "구매",
    "complete payment": "구매",
    "purchase value": "매출액",
    "complete payment value": "매출액",
    "conversion value": "매출액",
    "add to cart": "장바구니담기수",
    "adds to cart": "장바구니담기수",
    "reach": "도달",
    "도달": "도달",
    "engagement": "참여",
    "engaged view": "참여",
    "follows": "팔로우",
    "followers": "팔로우",
    "video views": "동영상조회",
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
# 4. 공통 유틸 함수
# =========================================================
def clean_column_name(col_name: str) -> str:
    """
    컬럼명 전처리
    - 앞뒤 공백 제거
    - 줄바꿈 제거
    - 보이지 않는 공백 제거
    - 중복 공백 제거
    """
    col = str(col_name)
    col = col.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    col = col.replace("\xa0", " ").replace("\u200b", " ").replace("\ufeff", " ")
    col = col.strip()
    col = re.sub(r"\s+", " ", col)
    return col


def normalize_for_matching(col_name: str) -> str:
    """
    매핑 비교용 정규화
    - 소문자화
    - 특수문자 제거
    - 공백 제거
    """
    col = clean_column_name(col_name).lower()
    col = col.replace("(krw)", "")
    col = col.replace("krw", "")
    col = re.sub(r"[^0-9a-zA-Z가-힣]", "", col)
    return col


def build_normalized_map(column_map: Dict[str, str]) -> Dict[str, str]:
    normalized_map = {}
    for raw_col, standard_col in column_map.items():
        normalized_map[normalize_for_matching(raw_col)] = standard_col
    return normalized_map


def is_unnamed_column(col_name: str) -> bool:
    return clean_column_name(col_name).lower().startswith("unnamed:")


def load_csv_file(uploaded_file) -> Optional[pd.DataFrame]:
    """
    CSV 파일 로드
    - 여러 인코딩 시도
    - 여러 구분자 시도
    """
    encodings = ["utf-8-sig", "utf-8", "cp949", "euc-kr", "utf-16"]
    separators = [None, ",", "\t", ";", "|"]

    file_bytes = uploaded_file.getvalue()
    if not file_bytes:
        return None

    for encoding in encodings:
        for sep in separators:
            try:
                buffer = io.BytesIO(file_bytes)
                buffer.seek(0)

                if sep is None:
                    df = pd.read_csv(buffer, encoding=encoding, sep=None, engine="python")
                else:
                    df = pd.read_csv(buffer, encoding=encoding, sep=sep)

                if df is not None:
                    return df
            except Exception:
                continue

    return None


def load_excel_file(uploaded_file) -> Optional[pd.DataFrame]:
    """
    XLSX 파일 로드
    """
    try:
        uploaded_file.seek(0)
        df = pd.read_excel(uploaded_file, engine="openpyxl")
        return df
    except Exception:
        return None


def load_file(uploaded_file, platform_name: str) -> Optional[pd.DataFrame]:
    """
    업로드 파일 로드 후 매체 컬럼 추가
    """
    if uploaded_file is None:
        return None

    file_name = uploaded_file.name.lower()

    if file_name.endswith(".csv"):
        df = load_csv_file(uploaded_file)
    elif file_name.endswith(".xlsx"):
        df = load_excel_file(uploaded_file)
    else:
        return None

    if df is None:
        return None

    df.columns = [str(col) for col in df.columns]
    df["매체"] = platform_name
    return df


def coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    같은 표준 컬럼으로 rename되어 중복 컬럼이 생긴 경우 병합
    앞 컬럼 값이 비어 있으면 뒤 컬럼 값으로 보완
    """
    result = pd.DataFrame(index=df.index)

    for col in df.columns.unique():
        same_cols = df.loc[:, df.columns == col]

        if same_cols.shape[1] == 1:
            result[col] = same_cols.iloc[:, 0]
        else:
            combined = same_cols.iloc[:, 0].copy()
            for i in range(1, same_cols.shape[1]):
                combined = combined.where(
                    combined.notna() & (combined.astype(str).str.strip() != ""),
                    same_cols.iloc[:, i],
                )
            result[col] = combined

    return result


def convert_numeric_columns(df: pd.DataFrame, numeric_columns: List[str]) -> pd.DataFrame:
    """
    숫자형 컬럼 정리
    """
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
    """
    날짜 컬럼 형식 통일
    """
    if "날짜" in df.columns:
        parsed = pd.to_datetime(df["날짜"], errors="coerce")
        formatted = parsed.dt.strftime("%Y-%m-%d")
        df["날짜"] = formatted.fillna(df["날짜"].astype(str))
    return df


def standardize_columns(
    df: pd.DataFrame,
    platform_name: str,
    platform_map: Dict[str, str],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    컬럼 표준화
    반환값:
    - 표준화된 데이터프레임
    - 매핑 결과 요약 데이터프레임
    - 매핑되지 않은 컬럼 데이터프레임
    """
    work_df = df.copy()

    # 불필요한 unnamed 컬럼 제거
    filtered_cols = [col for col in work_df.columns if not is_unnamed_column(col)]
    work_df = work_df[filtered_cols].copy()

    # 컬럼명 정리
    work_df.columns = [clean_column_name(col) for col in work_df.columns]

    normalized_map = build_normalized_map(platform_map)

    rename_dict = {}
    mapping_rows = []
    unmapped_rows = []

    for col in work_df.columns:
        normalized_col = normalize_for_matching(col)

        if normalized_col in normalized_map:
            standard_col = normalized_map[normalized_col]
            rename_dict[col] = standard_col
            mapping_rows.append({
                "매체": platform_name,
                "원본 컬럼명": col,
                "표준 컬럼명": standard_col,
                "매핑 성공 여부": "성공",
            })
        elif col in FINAL_COLUMNS:
            rename_dict[col] = col
            mapping_rows.append({
                "매체": platform_name,
                "원본 컬럼명": col,
                "표준 컬럼명": col,
                "매핑 성공 여부": "성공",
            })
        else:
            rename_dict[col] = col
            mapping_rows.append({
                "매체": platform_name,
                "원본 컬럼명": col,
                "표준 컬럼명": "",
                "매핑 성공 여부": "실패",
            })
            unmapped_rows.append({
                "매체": platform_name,
                "매핑되지 않은 원본 컬럼명": col,
            })

    # rename
    work_df = work_df.rename(columns=rename_dict)

    # 중복 컬럼 병합
    work_df = coalesce_duplicate_columns(work_df)

    # 필수 컬럼 없으면 빈 값 생성
    for col in FINAL_COLUMNS:
        if col not in work_df.columns:
            work_df[col] = ""

    # 매체 보정
    work_df["매체"] = platform_name

    # 타입 정리
    work_df = convert_numeric_columns(work_df, NUMERIC_COLUMNS)
    work_df = convert_date_column(work_df)

    # 최종 컬럼만 남김
    work_df = work_df[FINAL_COLUMNS]

    mapping_df = pd.DataFrame(mapping_rows)
    unmapped_df = pd.DataFrame(unmapped_rows)

    if mapping_df.empty:
        mapping_df = pd.DataFrame(columns=["매체", "원본 컬럼명", "표준 컬럼명", "매핑 성공 여부"])

    if unmapped_df.empty:
        unmapped_df = pd.DataFrame(columns=["매체", "매핑되지 않은 원본 컬럼명"])

    return work_df, mapping_df, unmapped_df


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def dataframe_to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="통합리포트")
    output.seek(0)
    return output.getvalue()


# =========================================================
# 5. 파일 업로드 UI
# =========================================================
st.markdown("### 매체별 RAW 업로드")

col1, col2 = st.columns(2)
with col1:
    naver_file = st.file_uploader("네이버 파일", type=["csv", "xlsx"], key="naver")
    kakao_file = st.file_uploader("카카오 파일", type=["csv", "xlsx"], key="kakao")

with col2:
    meta_file = st.file_uploader("메타 파일", type=["csv", "xlsx"], key="meta")
    tiktok_file = st.file_uploader("틱톡 파일", type=["csv", "xlsx"], key="tiktok")

uploaded_files = [
    ("네이버", naver_file),
    ("메타", meta_file),
    ("카카오", kakao_file),
    ("틱톡", tiktok_file),
]


# =========================================================
# 6. 실행 로직
# =========================================================
if st.button("통합 리포트 생성", type="primary"):
    standardized_dataframes = []
    mapping_summary_list = []
    unmapped_summary_list = []
    failed_platforms = []

    for platform_name, uploaded_file in uploaded_files:
        if uploaded_file is None:
            continue

        raw_df = load_file(uploaded_file, platform_name)

        if raw_df is None:
            failed_platforms.append(platform_name)
            continue

        standardized_df, mapping_df, unmapped_df = standardize_columns(
            df=raw_df,
            platform_name=platform_name,
            platform_map=PLATFORM_MAPS[platform_name],
        )

        standardized_dataframes.append(standardized_df)
        mapping_summary_list.append(mapping_df)
        unmapped_summary_list.append(unmapped_df)

    # 로드 실패 알림
    if failed_platforms:
        st.warning("파일 읽기에 실패한 매체: " + ", ".join(failed_platforms))

    standardized_dataframes = [df for df in standardized_dataframes if df is not None]

    if not standardized_dataframes:
        st.error("통합할 수 있는 데이터가 없습니다. 파일 형식과 내용을 확인해 주세요.")
    else:
        final_report_df = pd.concat(standardized_dataframes, ignore_index=True)

        if mapping_summary_list:
            mapping_summary_df = pd.concat(mapping_summary_list, ignore_index=True)
        else:
            mapping_summary_df = pd.DataFrame(columns=["매체", "원본 컬럼명", "표준 컬럼명", "매핑 성공 여부"])

        if unmapped_summary_list:
            unmapped_summary_df = pd.concat(unmapped_summary_list, ignore_index=True)
        else:
            unmapped_summary_df = pd.DataFrame(columns=["매체", "매핑되지 않은 원본 컬럼명"])

        csv_bytes = dataframe_to_csv_bytes(final_report_df)
        excel_bytes = dataframe_to_excel_bytes(final_report_df)

        st.success("통합 리포트 생성이 완료되었습니다.")

        st.markdown("## 1) 최종 통합 리포트")
        st.dataframe(final_report_df, use_container_width=True)

        st.markdown("## 2) 매체별 컬럼 매핑 결과")
        st.dataframe(mapping_summary_df, use_container_width=True)

        st.markdown("## 3) 매핑되지 않은 원본 컬럼 목록")
        st.dataframe(unmapped_summary_df, use_container_width=True)

        st.markdown("## 4) 최종 컬럼 순서")
        st.dataframe(pd.DataFrame({"컬럼명": FINAL_COLUMNS}), use_container_width=True)

        st.markdown("## 다운로드")
        dcol1, dcol2 = st.columns(2)

        with dcol1:
            st.download_button(
                label="CSV 다운로드",
                data=csv_bytes,
                file_name="광고_통합리포트.csv",
                mime="text/csv",
            )

        with dcol2:
            st.download_button(
                label="XLSX 다운로드",
                data=excel_bytes,
                file_name="광고_통합리포트.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

st.markdown("---")
st.caption("매체별 컬럼명 수정은 코드 상단의 NAVER_COLUMN_MAP / META_COLUMN_MAP / KAKAO_COLUMN_MAP / TIKTOK_COLUMN_MAP에서 할 수 있습니다.")
