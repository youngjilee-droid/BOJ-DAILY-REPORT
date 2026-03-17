import base64
import csv
from io import BytesIO, StringIO
from pathlib import Path

import pandas as pd
import streamlit as st

# matplotlib은 현재 필수는 아니지만,
# 추후 차트 추가 시 한글 깨짐 방지를 위해 함께 설정합니다.
try:
    import matplotlib.pyplot as plt
    from matplotlib import font_manager, rc
except Exception:
    plt = None
    font_manager = None
    rc = None


# -----------------------------
# 기본 설정
# -----------------------------
st.set_page_config(page_title="광고 데이터 통합 리포트", layout="wide")

FONT_PATH = Path("NanumGothic.ttf")

st.title("광고 데이터 통합 리포트")
st.write("RAW 광고 데이터를 업로드하면 하나의 통합 데이터로 정리됩니다.")


# -----------------------------
# 폰트 적용
# -----------------------------
def apply_streamlit_font_css(font_path: Path) -> None:
    """
    Streamlit 화면 전반에 NanumGothic 폰트를 적용합니다.
    """
    if not font_path.exists():
        st.warning("NanumGothic.ttf 파일을 찾지 못해 기본 폰트로 표시됩니다.")
        return

    try:
        font_bytes = font_path.read_bytes()
        font_base64 = base64.b64encode(font_bytes).decode("utf-8")

        css = f"""
        <style>
        @font-face {{
            font-family: 'NanumGothicCustom';
            src: url(data:font/ttf;base64,{font_base64}) format('truetype');
            font-weight: normal;
            font-style: normal;
        }}

        html, body, [class*="css"], [data-testid="stAppViewContainer"],
        [data-testid="stHeader"], [data-testid="stSidebar"], .stMarkdown,
        .stText, .stTable, .stDataFrame, div, p, span, label, button, input {{
            font-family: 'NanumGothicCustom', sans-serif !important;
        }}
        </style>
        """
        st.markdown(css, unsafe_allow_html=True)

    except Exception as e:
        st.warning(f"화면 폰트 적용 중 오류가 발생했습니다: {e}")


def register_matplotlib_font(font_path: Path) -> None:
    """
    matplotlib 차트에서 한글이 깨지지 않도록 폰트를 등록합니다.
    """
    if not font_path.exists():
        return

    if font_manager is None or rc is None:
        return

    try:
        font_manager.fontManager.addfont(str(font_path))
        font_name = font_manager.FontProperties(fname=str(font_path)).get_name()
        rc("font", family=font_name)
        rc("axes", unicode_minus=False)
    except Exception as e:
        st.warning(f"matplotlib 폰트 등록 중 오류가 발생했습니다: {e}")


apply_streamlit_font_css(FONT_PATH)
register_matplotlib_font(FONT_PATH)


# -----------------------------
# CSV 로드 관련 함수
# -----------------------------
def is_likely_wrong_delimiter(df: pd.DataFrame) -> bool:
    """
    구분자를 잘못 읽어 한 컬럼에 전부 몰린 경우를 대략적으로 판별합니다.
    """
    if df is None or len(df.columns) != 1:
        return False

    first_col = str(df.columns[0])
    suspicious_tokens = [",", "\t", ";", "|"]

    return any(token in first_col for token in suspicious_tokens)


def try_read_csv_from_text(text: str, sep_option):
    """
    디코딩된 문자열을 DataFrame으로 변환합니다.
    """
    buffer = StringIO(text)

    if sep_option == "auto":
        return pd.read_csv(
            buffer,
            sep=None,
            engine="python",
            skip_blank_lines=True,
        )
    else:
        return pd.read_csv(
            buffer,
            sep=sep_option,
            engine="python",
            skip_blank_lines=True,
        )


def load_csv_file(uploaded_file) -> pd.DataFrame:
    """
    CSV 파일을 여러 인코딩과 여러 구분자로 안전하게 시도하여 읽습니다.
    - 인코딩: utf-8-sig, utf-8, cp949, euc-kr, utf-16
    - 구분자: 자동 추정, 쉼표, 탭, 세미콜론, 파이프
    """
    encodings = ["utf-8-sig", "utf-8", "cp949", "euc-kr", "utf-16"]
    separators = ["auto", ",", "\t", ";", "|"]

    last_error = None

    # 업로드 파일의 원본 바이트를 먼저 고정적으로 확보
    uploaded_file.seek(0)
    raw_bytes = uploaded_file.read()

    # 완전히 빈 파일 방어
    if raw_bytes is None or len(raw_bytes) == 0:
        raise ValueError("업로드된 파일이 비어 있습니다.")

    for enc in encodings:
        try:
            text = raw_bytes.decode(enc)
        except Exception as e:
            last_error = e
            continue

        # BOM, 공백만 있는 경우 방어
        if text is None or not text.strip():
            raise ValueError("파일 내용이 비어 있습니다.")

        for sep in separators:
            try:
                df = try_read_csv_from_text(text, sep)

                # 컬럼 자체가 아예 없으면 실패로 간주
                if df is None or len(df.columns) == 0:
                    continue

                # 구분자 잘못 읽은 흔적이면 다음 후보 시도
                if is_likely_wrong_delimiter(df):
                    continue

                # 컬럼명 공백 정리
                df.columns = [str(col).strip() for col in df.columns]

                # 헤더만 있고 데이터 행이 없는 경우도 허용
                return df

            except Exception as e:
                last_error = e
                continue

    raise ValueError(
        f"CSV 파일을 읽지 못했습니다. "
        f"인코딩 또는 구분자가 일반 형식이 아닐 수 있습니다. "
        f"마지막 오류: {last_error}"
    )


# -----------------------------
# XLSX 로드 함수
# -----------------------------
def load_excel_file(uploaded_file) -> pd.DataFrame:
    """
    XLSX 파일을 openpyxl 엔진으로 읽습니다.
    """
    try:
        uploaded_file.seek(0)
        df = pd.read_excel(uploaded_file, engine="openpyxl")

        if df is None:
            raise ValueError("엑셀 파일을 읽지 못했습니다.")

        df.columns = [str(col).strip() for col in df.columns]
        return df

    except Exception as e:
        raise ValueError(f"XLSX 파일을 읽지 못했습니다: {e}")


# -----------------------------
# 통합 로드 함수
# -----------------------------
def load_file(uploaded_file, platform: str) -> pd.DataFrame | None:
    """
    업로드 파일을 안전하게 읽고 '매체' 컬럼을 추가합니다.
    실패 시 None 반환.
    """
    if uploaded_file is None:
        return None

    file_name = uploaded_file.name.lower()

    try:
        if file_name.endswith(".csv"):
            df = load_csv_file(uploaded_file)
        elif file_name.endswith(".xlsx"):
            df = load_excel_file(uploaded_file)
        else:
            raise ValueError("지원하지 않는 파일 형식입니다. csv 또는 xlsx만 업로드해주세요.")

        # 방어 코드
        if df is None:
            raise ValueError("파일 로드 결과가 비어 있습니다.")

        df = df.copy()
        df["매체"] = platform

        return df

    except Exception as e:
        st.error(f"[{platform}] 파일 로드 실패 - 파일명: {uploaded_file.name} / 오류: {e}")
        return None


# -----------------------------
# 저장 함수
# -----------------------------
def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """
    Excel에서 한글 깨짐 가능성을 줄이기 위해 utf-8-sig로 저장합니다.
    """
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def dataframe_to_excel_bytes(df: pd.DataFrame) -> bytes:
    """
    openpyxl 기반으로 XLSX 다운로드 파일을 만듭니다.
    """
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="통합데이터")
    output.seek(0)
    return output.getvalue()


# -----------------------------
# 파일 업로드 UI
# -----------------------------
naver_file = st.file_uploader("네이버 광고 데이터", type=["csv", "xlsx"])
meta_file = st.file_uploader("메타 광고 데이터", type=["csv", "xlsx"])
kakao_file = st.file_uploader("카카오 광고 데이터", type=["csv", "xlsx"])
tiktok_file = st.file_uploader("틱톡 광고 데이터", type=["csv", "xlsx"])

dataframes = []

# 매체별 파일 로드
loaded_naver = load_file(naver_file, "Naver")
if loaded_naver is not None:
    dataframes.append(loaded_naver)

loaded_meta = load_file(meta_file, "Meta")
if loaded_meta is not None:
    dataframes.append(loaded_meta)

loaded_kakao = load_file(kakao_file, "Kakao")
if loaded_kakao is not None:
    dataframes.append(loaded_kakao)

loaded_tiktok = load_file(tiktok_file, "TikTok")
if loaded_tiktok is not None:
    dataframes.append(loaded_tiktok)


# -----------------------------
# 데이터 통합
# -----------------------------
if dataframes:
    try:
        # None 제거
        valid_dataframes = [df for df in dataframes if df is not None]

        if len(valid_dataframes) == 0:
            st.warning("불러온 데이터가 없습니다.")
        else:
            merged_df = pd.concat(valid_dataframes, ignore_index=True, sort=False)

            st.subheader("통합 데이터 미리보기")
            st.dataframe(merged_df, use_container_width=True)

            st.subheader("파일 다운로드")
            csv_data = dataframe_to_csv_bytes(merged_df)
            excel_data = dataframe_to_excel_bytes(merged_df)

            col1, col2 = st.columns(2)

            with col1:
                st.download_button(
                    label="CSV 다운로드",
                    data=csv_data,
                    file_name="ads_report.csv",
                    mime="text/csv",
                )

            with col2:
                st.download_button(
                    label="XLSX 다운로드",
                    data=excel_data,
                    file_name="ads_report.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

            st.caption(
                "참고: st.dataframe 표 영역은 Streamlit 내부 렌더링 특성상 "
                "커스텀 폰트가 완전히 반영되지 않을 수 있습니다."
            )

    except Exception as e:
        st.error(f"데이터 병합 또는 다운로드 파일 생성 중 오류가 발생했습니다: {e}")

else:
    st.info("광고 데이터를 1개 이상 업로드해주세요.")
