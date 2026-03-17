import base64
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st

# matplotlib은 차트 사용 시 한글 폰트 적용을 위해 등록
# 현재 화면에는 필수가 아니지만, 이후 차트 추가를 대비해 포함
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
st.title("광고 데이터 통합 리포트")
st.write("RAW 광고 데이터를 업로드하면 하나의 통합 데이터로 정리됩니다.")

FONT_PATH = Path("NanumGothic.ttf")


# -----------------------------
# 폰트 적용 함수
# -----------------------------
def apply_streamlit_font_css(font_path: Path) -> None:
    """
    Streamlit 화면 전반에 NanumGothic 폰트를 적용합니다.
    앱과 같은 경로에 NanumGothic.ttf가 있다고 가정합니다.
    """
    if not font_path.exists():
        st.warning("NanumGothic.ttf 파일을 찾지 못해 기본 폰트로 표시됩니다.")
        return

    try:
        font_bytes = font_path.read_bytes()
        font_base64 = base64.b64encode(font_bytes).decode()
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
        .stText, .stDataFrame, .stTable, div, p, span, label, button, input, textarea {{
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
# 파일 로드 함수
# -----------------------------
def load_csv_file(uploaded_file) -> pd.DataFrame:
    """
    CSV 파일을 여러 인코딩 + 여러 구분자로 순차 시도하여 읽습니다.
    TikTok CSV처럼 탭 구분 가능성도 반영합니다.
    """
    encodings = ["utf-8-sig", "utf-8", "cp949", "euc-kr"]
    seps = [",", "\t"]

    for enc in encodings:
        for sep in seps:
            try:
                uploaded_file.seek(0)
                df = pd.read_csv(
                    uploaded_file,
                    encoding=enc,
                    sep=sep,
                    engine="python",
                )

                # 완전히 빈 데이터면 실패로 간주하고 다음 케이스 시도
                if df is None or df.empty:
                    continue

                # 구분자가 잘못 들어가서 컬럼이 1개로 뭉치는 경우가 많아 방어
                # 다만 실제 1컬럼 CSV일 수도 있으므로, 데이터가 있으면 허용
                return df

            except Exception:
                continue

    raise ValueError("CSV 파일을 읽지 못했습니다. 인코딩 또는 구분자를 확인해주세요.")


def load_excel_file(uploaded_file) -> pd.DataFrame:
    """
    XLSX 파일을 openpyxl 엔진으로 읽습니다.
    """
    try:
        uploaded_file.seek(0)
        df = pd.read_excel(uploaded_file, engine="openpyxl")
        if df is None:
            raise ValueError("엑셀 파일이 비어 있습니다.")
        return df
    except Exception as e:
        raise ValueError(f"XLSX 파일을 읽지 못했습니다: {e}")


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

        # 컬럼명/데이터 정리
        df = df.copy()
        df.columns = [str(col).strip() for col in df.columns]
        df["매체"] = platform

        return df

    except Exception as e:
        st.error(f"[{platform}] 파일 로드 실패: {e}")
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
    openpyxl 기반 XLSX 파일로 저장합니다.
    """
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="통합데이터")
    output.seek(0)
    return output.getvalue()


# -----------------------------
# 업로드 UI
# -----------------------------
naver_file = st.file_uploader("네이버 광고 데이터", type=["csv", "xlsx"])
meta_file = st.file_uploader("메타 광고 데이터", type=["csv", "xlsx"])
kakao_file = st.file_uploader("카카오 광고 데이터", type=["csv", "xlsx"])
tiktok_file = st.file_uploader("틱톡 광고 데이터", type=["csv", "xlsx"])

dataframes = []

# 매체별 데이터 로드
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
# 데이터 통합 및 출력
# -----------------------------
if dataframes:
    try:
        # None 제거 후 최종 병합
        valid_dataframes = [df for df in dataframes if df is not None and not df.empty]

        if not valid_dataframes:
            st.warning("불러온 데이터가 없거나 모두 비어 있습니다.")
        else:
            merged_df = pd.concat(valid_dataframes, ignore_index=True)

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
                "참고: st.dataframe 영역은 Streamlit 내부 렌더링 방식 때문에 "
                "커스텀 폰트가 일부만 반영될 수 있습니다."
            )

    except Exception as e:
        st.error(f"데이터 병합 또는 다운로드 파일 생성 중 오류가 발생했습니다: {e}")

else:
    st.info("광고 데이터를 1개 이상 업로드해주세요.")
