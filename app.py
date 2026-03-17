import io
import os
import re
from typing import Dict, List, Tuple, Optional

import pandas as pd
import streamlit as st
from matplotlib import font_manager, rc

# =========================
# 기본 설정
# =========================
st.set_page_config(page_title="광고 데이터 통합 리포트", layout="wide")

APP_TITLE = "광고 데이터 통합 리포트"
FONT_FILE_NAME = "NanumGothic.ttf"
FONT_PATH = os.path.join(os.path.dirname(__file__), FONT_FILE_NAME)

# 최종 표준 컬럼 순서
STANDARD_COLUMNS = [
    "일자",
    "캠페인명",
    "광고세트명",
    "광고명",
    "소재명",
    "노출수",
    "클릭수",
    "링크클릭수",
    "도달수",
    "전환수",
    "광고비",
    "CPM",
    "CPC",
    "CTR",
    "동영상3초조회",
    "동영상시청완료",
    "공유수",
    "저장수",
    "좋아요수",
    "댓글수",
    "구매수",
    "매출",
    "매체",
]

# 숫자형 변환 시도 대상 컬럼
NUMERIC_COLUMNS = [
    "노출수",
    "클릭수",
    "링크클릭수",
    "도달수",
    "전환수",
    "광고비",
    "CPM",
    "CPC",
    "CTR",
    "동영상3초조회",
    "동영상시청완료",
    "공유수",
    "저장수",
    "좋아요수",
    "댓글수",
    "구매수",
    "매출",
]

# 날짜 컬럼
DATE_COLUMNS = ["일자"]


# =========================
# 컬럼명 정리 함수
# =========================
def clean_column_name(col_name: str) -> str:
    """
    컬럼명 전처리:
    - None 방지
    - 줄바꿈 제거
    - 보이지 않는 공백 제거
    - 앞뒤 공백 제거
    - 중복 공백 제거
    """
    if col_name is None:
        return ""

    col = str(col_name)

    # 줄바꿈 / 탭 처리
    col = col.replace("\n", " ").replace("\r", " ").replace("\t", " ")

    # 보이지 않는 공백 제거
    col = col.replace("\xa0", " ").replace("\u200b", " ").replace("\ufeff", " ")

    # 앞뒤 공백 제거
    col = col.strip()

    # 중복 공백 제거
    col = re.sub(r"\s+", " ", col)

    return col


def normalize_for_matching(col_name: str) -> str:
    """
    매핑 비교용 정규화:
    - clean_column_name 적용
    - 소문자 처리
    - 공백/특수문자 제거
    """
    col = clean_column_name(col_name).lower()
    col = re.sub(r"[^0-9a-zA-Z가-힣]", "", col)
    return col


def is_unnamed_column(col_name: str) -> bool:
    cleaned = clean_column_name(col_name)
    return cleaned.lower().startswith("unnamed:")


# =========================
# 매핑 사전
# 초보자도 여기만 수정하면 유지보수 가능하도록 상단에 정리
# key   : 원본 컬럼명(예시)
# value : 표준 컬럼명
# =========================
NAVER_COLUMN_MAP = {
    "일자": "일자",
    "날짜": "일자",
    "캠페인": "캠페인명",
    "캠페인명": "캠페인명",
    "광고그룹": "광고세트명",
    "광고그룹명": "광고세트명",
    "광고세트": "광고세트명",
    "광고세트명": "광고세트명",
    "광고": "광고명",
    "광고명": "광고명",
    "소재": "소재명",
    "소재명": "소재명",
    "노출": "노출수",
    "노출수": "노출수",
    "클릭": "클릭수",
    "클릭수": "클릭수",
    "링크클릭": "링크클릭수",
    "링크클릭수": "링크클릭수",
    "도달": "도달수",
    "도달수": "도달수",
    "전환": "전환수",
    "전환수": "전환수",
    "총전환": "전환수",
    "소진액": "광고비",
    "광고비": "광고비",
    "비용": "광고비",
    "금액": "광고비",
    "cpm": "CPM",
    "cpc": "CPC",
    "ctr": "CTR",
    "동영상3초조회": "동영상3초조회",
    "3초조회": "동영상3초조회",
    "동영상재생3초": "동영상3초조회",
    "동영상시청완료": "동영상시청완료",
    "동영상완료조회": "동영상시청완료",
    "공유": "공유수",
    "공유수": "공유수",
    "저장": "저장수",
    "저장수": "저장수",
    "좋아요": "좋아요수",
    "좋아요수": "좋아요수",
    "댓글": "댓글수",
    "댓글수": "댓글수",
    "구매": "구매수",
    "구매수": "구매수",
    "매출": "매출",
    "구매액": "매출",
    "전환매출": "매출",
}

META_COLUMN_MAP = {
    "date": "일자",
    "day": "일자",
    "reporting starts": "일자",
    "reporting start": "일자",
    "보고 시작": "일자",
    "날짜": "일자",
    "campaign name": "캠페인명",
    "campaign": "캠페인명",
    "캠페인명": "캠페인명",
    "ad set name": "광고세트명",
    "adset name": "광고세트명",
    "ad set": "광고세트명",
    "광고세트명": "광고세트명",
    "ad name": "광고명",
    "ad": "광고명",
    "광고명": "광고명",
    "creative name": "소재명",
    "creative": "소재명",
    "소재명": "소재명",
    "impressions": "노출수",
    "노출": "노출수",
    "clicks": "클릭수",
    "all clicks": "클릭수",
    "클릭": "클릭수",
    "link clicks": "링크클릭수",
    "outbound clicks": "링크클릭수",
    "링크 클릭": "링크클릭수",
    "reach": "도달수",
    "도달": "도달수",
    "results": "전환수",
    "conversions": "전환수",
    "purchases conversion value count": "전환수",
    "전환": "전환수",
    "amount spent": "광고비",
    "spend": "광고비",
    "지출 금액": "광고비",
    "광고비": "광고비",
    "cpm": "CPM",
    "cpc": "CPC",
    "ctr": "CTR",
    "3-second video plays": "동영상3초조회",
    "video plays at 3 seconds": "동영상3초조회",
    "thruplays": "동영상시청완료",
    "video completions": "동영상시청완료",
    "shares": "공유수",
    "saved": "저장수",
    "saves": "저장수",
    "post saves": "저장수",
    "likes": "좋아요수",
    "post reactions": "좋아요수",
    "comments": "댓글수",
    "purchases": "구매수",
    "website purchases": "구매수",
    "purchase roas": "매출",
    "purchase conversion value": "매출",
    "website purchase conversion value": "매출",
    "conversion value": "매출",
    "value": "매출",
}

KAKAO_COLUMN_MAP = {
    "일자": "일자",
    "날짜": "일자",
    "캠페인": "캠페인명",
    "캠페인명": "캠페인명",
    "광고그룹": "광고세트명",
    "광고그룹명": "광고세트명",
    "소재": "소재명",
    "소재명": "소재명",
    "광고명": "광고명",
    "광고": "광고명",
    "노출": "노출수",
    "노출수": "노출수",
    "클릭": "클릭수",
    "클릭수": "클릭수",
    "링크클릭": "링크클릭수",
    "링크 클릭": "링크클릭수",
    "도달": "도달수",
    "도달수": "도달수",
    "전환": "전환수",
    "전환수": "전환수",
    "구매전환": "전환수",
    "광고비": "광고비",
    "소진액": "광고비",
    "사용금액": "광고비",
    "cpm": "CPM",
    "cpc": "CPC",
    "ctr": "CTR",
    "동영상 재생 3초": "동영상3초조회",
    "동영상3초조회": "동영상3초조회",
    "동영상 재생 완료": "동영상시청완료",
    "동영상시청완료": "동영상시청완료",
    "공유": "공유수",
    "공유수": "공유수",
    "저장": "저장수",
    "저장수": "저장수",
    "좋아요": "좋아요수",
    "좋아요수": "좋아요수",
    "댓글": "댓글수",
    "댓글수": "댓글수",
    "구매": "구매수",
    "구매수": "구매수",
    "매출": "매출",
    "구매금액": "매출",
    "전환매출": "매출",
}

TIKTOK_COLUMN_MAP = {
    "date": "일자",
    "stat_time_day": "일자",
    "day": "일자",
    "날짜": "일자",
    "campaign name": "캠페인명",
    "campaign": "캠페인명",
    "adgroup name": "광고세트명",
    "ad group name": "광고세트명",
    "ad group": "광고세트명",
    "광고그룹명": "광고세트명",
    "ad name": "광고명",
    "ad": "광고명",
    "광고명": "광고명",
    "creative name": "소재명",
    "creative": "소재명",
    "소재명": "소재명",
    "impressions": "노출수",
    "노출": "노출수",
    "clicks": "클릭수",
    "클릭": "클릭수",
    "click-through rate destination": "링크클릭수",
    "destination clicks": "링크클릭수",
    "landing page clicks": "링크클릭수",
    "reach": "도달수",
    "conversions": "전환수",
    "result": "전환수",
    "conversion": "전환수",
    "amount spent": "광고비",
    "spend": "광고비",
    "지출 금액": "광고비",
    "cpm": "CPM",
    "cpc": "CPC",
    "ctr": "CTR",
    "video views at 3s": "동영상3초조회",
    "video views 3s": "동영상3초조회",
    "3s video views": "동영상3초조회",
    "video views at 100%": "동영상시청완료",
    "video completions": "동영상시청완료",
    "shares": "공유수",
    "likes": "좋아요수",
    "comments": "댓글수",
    "complete payment": "구매수",
    "purchase": "구매수",
    "purchases": "구매수",
    "purchase value": "매출",
    "complete payment value": "매출",
    "conversion value": "매출",
}


PLATFORM_MAPS = {
    "네이버": NAVER_COLUMN_MAP,
    "메타": META_COLUMN_MAP,
    "카카오": KAKAO_COLUMN_MAP,
    "틱톡": TIKTOK_COLUMN_MAP,
}


# =========================
# 폰트 처리
# =========================
def apply_streamlit_font_css() -> None:
    """
    Streamlit 화면 전체에 NanumGothic 폰트 적용 시도
    """
    if os.path.exists(FONT_PATH):
        with open(FONT_PATH, "rb") as f:
            font_bytes = f.read()

        import base64
        font_base64 = base64.b64encode(font_bytes).decode()

        css = f"""
        <style>
        @font-face {{
            font-family: 'NanumGothicCustom';
            src: url(data:font/ttf;base64,{font_base64}) format('truetype');
            font-weight: normal;
            font-style: normal;
        }}

        html, body, [class*="css"], .stApp, .stMarkdown, .stText, .stDataFrame, div, p, span, label {{
            font-family: 'NanumGothicCustom', sans-serif !important;
        }}
        </style>
        """
        st.markdown(css, unsafe_allow_html=True)
    else:
        st.warning(f"폰트 파일을 찾지 못했습니다: {FONT_FILE_NAME}")


def register_matplotlib_font() -> None:
    """
    matplotlib 사용 시 NanumGothic 폰트 적용
    """
    if os.path.exists(FONT_PATH):
        try:
            font_manager.fontManager.addfont(FONT_PATH)
            font_prop = font_manager.FontProperties(fname=FONT_PATH)
            rc("font", family=font_prop.get_name())
            rc("axes", unicode_minus=False)
        except Exception:
            pass


# =========================
# 파일 로드 함수
# =========================
def load_csv_file(uploaded_file) -> Optional[pd.DataFrame]:
    """
    CSV 파일 로드
    - 여러 인코딩 순서대로 시도
    - 여러 구분자 자동 시도
    - seek(0) 처리
    """
    encodings = ["utf-8-sig", "utf-8", "cp949", "euc-kr", "utf-16"]
    separators = [None, ",", "\t", ";", "|"]  # None은 pandas 자동 추정(sep=None, engine='python')

    file_bytes = uploaded_file.getvalue()
    if not file_bytes:
        return None

    for enc in encodings:
        for sep in separators:
            try:
                buffer = io.BytesIO(file_bytes)
                buffer.seek(0)

                if sep is None:
                    df = pd.read_csv(buffer, encoding=enc, sep=None, engine="python")
                else:
                    df = pd.read_csv(buffer, encoding=enc, sep=sep)

                if df is not None and not df.empty:
                    return df

                # 비어 있어도 컬럼만 있으면 일단 반환 가능
                if df is not None:
                    return df

            except Exception:
                continue

    return None


def load_excel_file(uploaded_file) -> Optional[pd.DataFrame]:
    """
    XLSX 파일 로드
    - openpyxl 엔진 명시
    - seek(0) 처리
    """
    try:
        uploaded_file.seek(0)
        df = pd.read_excel(uploaded_file, engine="openpyxl")
        return df
    except Exception:
        return None


def load_file(uploaded_file, platform_name: str) -> Optional[pd.DataFrame]:
    """
    업로드 파일 확장자에 따라 적절한 함수 호출
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

    # 컬럼명 먼저 문자열화
    df.columns = [str(col) for col in df.columns]

    # 기존 구조 유지: 매체 컬럼 추가
    df["매체"] = platform_name
    return df


# =========================
# 데이터 정리 함수
# =========================
def build_normalized_mapping(column_map: Dict[str, str]) -> Dict[str, str]:
    """
    사람이 읽기 쉬운 컬럼 매핑 dict를
    비교용 정규화 dict로 변환
    """
    normalized_map = {}
    for raw_col, std_col in column_map.items():
        normalized_key = normalize_for_matching(raw_col)
        normalized_map[normalized_key] = std_col
    return normalized_map


def convert_numeric_columns(df: pd.DataFrame, numeric_columns: List[str]) -> pd.DataFrame:
    """
    숫자형 컬럼 변환 시도
    - 쉼표 제거
    - 공백 제거
    - 숫자가 아닌 문자 최소 정리
    """
    for col in numeric_columns:
        if col in df.columns:
            temp = df[col].astype(str)

            # null 비슷한 문자열 정리
            temp = temp.replace(
                {"None": "", "nan": "", "NaN": "", "null": "", "NULL": "", "-": ""},
                regex=False,
            )

            # 쉼표, 공백 제거
            temp = temp.str.replace(",", "", regex=False)
            temp = temp.str.replace(" ", "", regex=False)

            # %는 일단 제거해서 숫자로 변환
            temp = temp.str.replace("%", "", regex=False)

            # 숫자/부호/소수점 외 문자 제거
            temp = temp.str.replace(r"[^0-9\.\-]", "", regex=True)

            df[col] = pd.to_numeric(temp, errors="coerce")

    return df


def convert_date_columns(df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
    """
    날짜 컬럼 변환 시도 후 YYYY-MM-DD 문자열로 통일
    """
    for col in date_columns:
        if col in df.columns:
            parsed = pd.to_datetime(df[col], errors="coerce")
            df[col] = parsed.dt.strftime("%Y-%m-%d")
            df[col] = df[col].fillna("")
    return df


def standardize_columns(
    df: pd.DataFrame,
    platform_name: str,
    standard_columns: List[str],
    platform_map: Dict[str, str],
) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
    """
    컬럼 표준화 로직
    동작 순서:
    1) 원본 파일 읽기 후 df 입력
    2) 컬럼명 공백/특수문자/줄바꿈 정리
    3) 매체별 컬럼 매핑 dict 적용
    4) 표준 컬럼명으로 rename
    5) 없는 표준 컬럼은 빈 값 생성
    6) 표준 컬럼 순서 정렬
    7) 매핑 요약 / 미매핑 컬럼 목록 반환
    """
    work_df = df.copy()

    # 1. 불필요한 unnamed 컬럼 제거
    original_columns = list(work_df.columns)
    filtered_columns = [col for col in original_columns if not is_unnamed_column(col)]
    work_df = work_df[filtered_columns].copy()

    # 2. 컬럼명 정리
    cleaned_columns = [clean_column_name(col) for col in work_df.columns]
    work_df.columns = cleaned_columns

    normalized_platform_map = build_normalized_mapping(platform_map)

    rename_dict = {}
    mapping_rows = []
    unmapped_columns = []

    # 3. 매핑 적용
    for col in work_df.columns:
        normalized_col = normalize_for_matching(col)

        if normalized_col in normalized_platform_map:
            std_col = normalized_platform_map[normalized_col]
            rename_dict[col] = std_col
            mapping_rows.append(
                {
                    "매체명": platform_name,
                    "원본 컬럼명": col,
                    "치환 후 표준 컬럼명": std_col,
                    "매핑 성공 여부": "성공",
                }
            )
        else:
            # 이미 표준 컬럼이면 그대로 인정
            if col in standard_columns:
                rename_dict[col] = col
                mapping_rows.append(
                    {
                        "매체명": platform_name,
                        "원본 컬럼명": col,
                        "치환 후 표준 컬럼명": col,
                        "매핑 성공 여부": "성공",
                    }
                )
            else:
                rename_dict[col] = col
                unmapped_columns.append(col)
                mapping_rows.append(
                    {
                        "매체명": platform_name,
                        "원본 컬럼명": col,
                        "치환 후 표준 컬럼명": "",
                        "매핑 성공 여부": "실패",
                    }
                )

    # 4. rename 적용
    work_df = work_df.rename(columns=rename_dict)

    # 5. 같은 표준 컬럼으로 합쳐진 경우 중복 컬럼 처리
    # 예: 노출 / 노출수 / Impressions -> 모두 노출수
    # 중복 컬럼명 발생 시 첫 번째 값을 우선, 비어 있으면 뒤 컬럼 값으로 보완
    if work_df.columns.duplicated().any():
        deduped_df = pd.DataFrame(index=work_df.index)
        for col in work_df.columns.unique():
            same_cols = work_df.loc[:, work_df.columns == col]

            if same_cols.shape[1] == 1:
                deduped_df[col] = same_cols.iloc[:, 0]
            else:
                combined = same_cols.iloc[:, 0].copy()
                for idx in range(1, same_cols.shape[1]):
                    combined = combined.where(
                        combined.notna() & (combined.astype(str).str.strip() != ""),
                        same_cols.iloc[:, idx],
                    )
                deduped_df[col] = combined

        work_df = deduped_df

    # 6. 없는 표준 컬럼 생성
    for col in standard_columns:
        if col not in work_df.columns:
            work_df[col] = ""

    # 7. 매체 컬럼 강제 보정
    work_df["매체"] = platform_name

    # 8. 숫자형 / 날짜형 정리
    work_df = convert_numeric_columns(work_df, NUMERIC_COLUMNS)
    work_df = convert_date_columns(work_df, DATE_COLUMNS)

    # 9. 표준 컬럼 + 미매핑 컬럼 순으로 정렬
    extra_columns = [col for col in work_df.columns if col not in standard_columns]
    ordered_columns = standard_columns + extra_columns
    work_df = work_df[ordered_columns]

    mapping_df = pd.DataFrame(mapping_rows)

    # 중복 제거된 미매핑 목록 반환
    unmapped_columns = sorted(list(set(unmapped_columns)))

    return work_df, mapping_df, unmapped_columns


# =========================
# 다운로드 함수
# =========================
def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """
    CSV 다운로드용 bytes
    - utf-8-sig 저장
    """
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def dataframe_to_excel_bytes(df: pd.DataFrame) -> bytes:
    """
    XLSX 다운로드용 bytes
    - openpyxl 기반 저장
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="통합데이터")
    output.seek(0)
    return output.getvalue()


# =========================
# UI 시작
# =========================
apply_streamlit_font_css()
register_matplotlib_font()

st.title(APP_TITLE)
st.write("RAW 광고 데이터를 업로드하면 컬럼명을 표준화한 뒤 하나의 통합 데이터로 정리합니다.")

st.markdown("---")

# 파일 업로드
naver_file = st.file_uploader("네이버 광고 데이터", type=["csv", "xlsx"], key="naver")
meta_file = st.file_uploader("메타 광고 데이터", type=["csv", "xlsx"], key="meta")
kakao_file = st.file_uploader("카카오 광고 데이터", type=["csv", "xlsx"], key="kakao")
tiktok_file = st.file_uploader("틱톡 광고 데이터", type=["csv", "xlsx"], key="tiktok")

uploaded_files = [
    ("네이버", naver_file),
    ("메타", meta_file),
    ("카카오", kakao_file),
    ("틱톡", tiktok_file),
]

if st.button("데이터 통합 실행", type="primary"):
    standardized_dfs = []
    mapping_summary_list = []
    unmapped_summary_rows = []
    load_failures = []

    for platform_name, uploaded_file in uploaded_files:
        if uploaded_file is None:
            continue

        raw_df = load_file(uploaded_file, platform_name)

        if raw_df is None:
            load_failures.append(platform_name)
            continue

        std_df, mapping_df, unmapped_cols = standardize_columns(
            df=raw_df,
            platform_name=platform_name,
            standard_columns=STANDARD_COLUMNS,
            platform_map=PLATFORM_MAPS[platform_name],
        )

        standardized_dfs.append(std_df)
        mapping_summary_list.append(mapping_df)

        for col in unmapped_cols:
            unmapped_summary_rows.append(
                {
                    "매체명": platform_name,
                    "매핑되지 않은 원본 컬럼명": col,
                }
            )

    # 로드 실패 안내
    if load_failures:
        st.warning("다음 매체 파일은 읽기에 실패했습니다: " + ", ".join(load_failures))

    # 병합 전 None 제거 및 빈 리스트 예외 처리
    standardized_dfs = [df for df in standardized_dfs if df is not None]

    if not standardized_dfs:
        st.error("통합할 수 있는 데이터가 없습니다. 파일 형식이나 내용을 다시 확인해 주세요.")
    else:
        # 최종 병합
        final_df = pd.concat(standardized_dfs, ignore_index=True)

        # 매핑 요약 병합
        if mapping_summary_list:
            mapping_summary_df = pd.concat(mapping_summary_list, ignore_index=True)
        else:
            mapping_summary_df = pd.DataFrame(
                columns=["매체명", "원본 컬럼명", "치환 후 표준 컬럼명", "매핑 성공 여부"]
            )

        unmapped_df = pd.DataFrame(unmapped_summary_rows)
        if unmapped_df.empty:
            unmapped_df = pd.DataFrame(columns=["매체명", "매핑되지 않은 원본 컬럼명"])

        # 다운로드용 bytes
        csv_bytes = dataframe_to_csv_bytes(final_df)
        excel_bytes = dataframe_to_excel_bytes(final_df)

        st.success("컬럼 표준화 및 데이터 통합이 완료되었습니다.")

        st.markdown("## 1) 통합 데이터 미리보기")
        st.dataframe(final_df, use_container_width=True)

        st.markdown("## 2) 매체별 컬럼 매핑 결과 요약")
        st.dataframe(mapping_summary_df, use_container_width=True)

        st.markdown("## 3) 최종 표준 컬럼 목록")
        st.dataframe(pd.DataFrame({"표준 컬럼명": STANDARD_COLUMNS}), use_container_width=True)

        st.markdown("## 4) 매핑되지 않은 원본 컬럼 목록")
        st.dataframe(unmapped_df, use_container_width=True)

        st.markdown("## 다운로드")
        col1, col2 = st.columns(2)

        with col1:
            st.download_button(
                label="CSV 다운로드",
                data=csv_bytes,
                file_name="광고_통합데이터.csv",
                mime="text/csv",
            )

        with col2:
            st.download_button(
                label="XLSX 다운로드",
                data=excel_bytes,
                file_name="광고_통합데이터.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

st.markdown("---")
st.caption("매핑 사전(dict)은 코드 상단의 NAVER/META/KAKAO/TIKTOK_COLUMN_MAP에서 수정할 수 있습니다.")
