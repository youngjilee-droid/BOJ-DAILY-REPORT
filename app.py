import base64
import hashlib
import hmac
import io
import json
import os
import re
import requests
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

import pandas as pd
import streamlit as st
from meta_api import fetch_meta_data

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

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
div.stButton > button {
    border-radius: 10px;
    border: 1px solid #d0d7de;
    background: white;
}
.block-container {
    padding-top: 1.5rem;
    padding-bottom: 2rem;
}
.small-muted {
    color: #6b7280;
    font-size: 0.9rem;
}
</style>
""", unsafe_allow_html=True)

# =========================================================
# 2. 공통 유틸
# =========================================================
INDEX_FILE_PATH = "index_mapping.csv"
INDEX_WORKSHEET_NAME = "index_mapping"
GOOGLE_SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def clean_text(x):
    if pd.isna(x):
        return ""
    s = str(x)
    s = s.replace("\ufeff", "")
    s = s.replace("\u200b", "")
    s = s.replace("\xa0", " ")
    s = s.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def compact_text(x):
    return re.sub(r"\s+", "", clean_text(x))

def normalize_media(x):
    s = compact_text(x).lower()
    media_map = {
        "naver": "네이버",
        "네이버": "네이버",
        "meta": "메타",
        "facebook": "메타",
        "instagram": "메타",
        "ig": "메타",
        "kakao": "카카오",
        "카카오": "카카오",
        "tiktok": "틱톡",
        "틱톡": "틱톡",
        "gfa": "네이버 성과형디스플레이",
        "네이버성과형디스플레이": "네이버 성과형디스플레이",
        "criteo": "크리테오",
        "크리테오": "크리테오",
        "buzzvil": "버즈빌",
        "버즈빌": "버즈빌",
    }
    return media_map.get(s, clean_text(x))

def normalize_colname(col):
    s = compact_text(col).lower()
    mapping = {
        "매체": "매체",
        "media": "매체",
        "platform": "매체",

        "소재id": "소재ID",
        "소재아이디": "소재ID",
        "광고id": "광고ID",
        "광고아이디": "광고ID",
        "creativeid": "광고ID",
        "adid": "광고ID",
        "id": "광고ID",

        "실제소재명": "실제소재명",
        "실제광고명": "실제소재명",
        "소재명": "실제소재명",
        "mappedname": "실제소재명",

        "광고명": "광고명",
        "소재": "광고명",
        "creativename": "광고명",
        "adname": "광고명",

        "일별": "일별",
        "날짜": "일별",
        "일자": "일별",

        "캠페인": "캠페인",
        "캠페인명": "캠페인",

        "광고그룹": "광고그룹",
        "광고그룹명": "광고그룹",

        "총비용(vat포함)": "총비용(VAT포함)",
        "총비용": "총비용(VAT포함)",
        "비용": "총비용(VAT포함)",

        "노출수": "노출수",
        "노출": "노출수",
        "클릭수": "클릭수",
        "클릭": "클릭수",
        "총전환수": "총 전환수",
        "총전환매출액(원)": "총 전환매출액(원)",
        "총전환매출액": "총 전환매출액(원)",
    }
    return mapping.get(s, clean_text(col))

def read_any_file(file):
    if file.name.lower().endswith(".csv"):
        file_bytes = file.getvalue()
        # 네이버 CSV는 첫 줄 제목 행을 건너뛰도록 우선 시도
        for enc in ["utf-8-sig", "utf-8", "cp949", "euc-kr"]:
            try:
                df = pd.read_csv(io.BytesIO(file_bytes), encoding=enc, skiprows=1)
                return df
            except Exception:
                pass
        # 일반 CSV fallback
        for enc in ["utf-8-sig", "utf-8", "cp949", "euc-kr"]:
            try:
                return pd.read_csv(io.BytesIO(file_bytes), encoding=enc)
            except Exception:
                continue
        raise ValueError("CSV 인코딩을 읽지 못했습니다.")
    return pd.read_excel(file)

# =========================================================
# 3. Google Sheets / Local 인덱스 저장
# =========================================================
def get_google_sheet_id():
    try:
        return st.secrets["INDEX_SHEET_ID"]
    except Exception:
        return None

def get_google_service_account_info():
    try:
        # 가장 안정적인 방식: base64로 저장된 JSON
        if "GOOGLE_SERVICE_ACCOUNT_B64" in st.secrets:
            raw_b64 = st.secrets["GOOGLE_SERVICE_ACCOUNT_B64"]
            raw_b64 = str(raw_b64).replace("\n", "").replace("\r", "").replace(" ", "").strip()
            decoded = base64.b64decode(raw_b64).decode("utf-8")
            return json.loads(decoded)

        # TOML 안의 JSON 문자열 방식
        if "GOOGLE_SERVICE_ACCOUNT" in st.secrets:
            raw = st.secrets["GOOGLE_SERVICE_ACCOUNT"]
            if isinstance(raw, str):
                raw = raw.strip()
                return json.loads(raw)
            return dict(raw)

        # 기존 block 방식 fallback
        if "gcp_service_account" in st.secrets:
            return dict(st.secrets["gcp_service_account"])

        return None
    except Exception as e:
        st.error(f"서비스 계정 파싱 실패: {e}")
        return None

def get_storage_mode_and_reason():
    service_info = get_google_service_account_info()
    sheet_id = get_google_sheet_id()

    if not GSPREAD_AVAILABLE:
        return "local", "gspread_not_installed"
    if not sheet_id:
        return "local", "missing_INDEX_SHEET_ID"
    if not service_info:
        return "local", "missing_or_invalid_service_account"
    return "gsheets", "all_requirements_ok"

def get_gspread_client():
    service_info = get_google_service_account_info()
    sheet_id = get_google_sheet_id()

    if not (GSPREAD_AVAILABLE and service_info and sheet_id):
        return None, None

    try:
        creds = Credentials.from_service_account_info(service_info, scopes=GOOGLE_SHEETS_SCOPES)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)
        return client, spreadsheet
    except Exception as e:
        st.error(f"Google Sheets 연결 오류: {e}")
        return None, None

def get_or_create_index_worksheet():
    client, spreadsheet = get_gspread_client()
    if spreadsheet is None:
        return None
    try:
        ws = spreadsheet.worksheet(INDEX_WORKSHEET_NAME)
        return ws
    except Exception:
        try:
            ws = spreadsheet.add_worksheet(title=INDEX_WORKSHEET_NAME, rows=1000, cols=10)
            return ws
        except Exception as e:
            st.error(f"워크시트 생성/접근 실패: {e}")
            return None

def load_index_from_local():
    if os.path.exists(INDEX_FILE_PATH):
        try:
            return pd.read_csv(INDEX_FILE_PATH, encoding="utf-8-sig"), "로컬 CSV"
        except Exception:
            return pd.DataFrame(columns=["매체", "소재ID", "실제소재명"]), "로컬 CSV(읽기실패)"
    return pd.DataFrame(columns=["매체", "소재ID", "실제소재명"]), "비어 있음"

def save_index_to_local(df):
    save_df = df.copy()
    save_df.to_csv(INDEX_FILE_PATH, index=False, encoding="utf-8-sig")

def gs_load_index():
    ws = get_or_create_index_worksheet()
    if ws is None:
        return None, "gsheets_unavailable"
    try:
        rows = ws.get_all_values()
        if not rows:
            return pd.DataFrame(columns=["매체", "소재ID", "실제소재명"]), "gsheets_empty"
        header = rows[0]
        data = rows[1:] if len(rows) > 1 else []
        df = pd.DataFrame(data, columns=header)
        return df, "gsheets"
    except Exception as e:
        st.error(f"Google Sheets 로드 실패: {e}")
        return None, f"gsheets_load_error: {e}"

def gs_save_index(index_df):
    ws = get_or_create_index_worksheet()
    if ws is None:
        return False, "worksheet_unavailable"

    try:
        values = [index_df.columns.tolist()] + index_df.fillna("").astype(str).values.tolist()
        ws.clear()
        ws.update("A1", values)
        return True, None
    except Exception as e:
        st.error(f"Google Sheets 저장 실패: {e}")
        return False, str(e)

# =========================================================
# 4. 인덱스 표준화 / 매핑
# =========================================================
def standardize_index(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [normalize_colname(c) for c in out.columns]

    if "소재ID" not in out.columns and "광고ID" in out.columns:
        out["소재ID"] = out["광고ID"]

    for col in ["매체", "소재ID", "실제소재명"]:
        if col not in out.columns:
            out[col] = ""

    out = out[["매체", "소재ID", "실제소재명"]].copy()
    out["매체"] = out["매체"].apply(normalize_media)
    out["소재ID"] = out["소재ID"].apply(clean_text)
    out["실제소재명"] = out["실제소재명"].apply(clean_text)

    out = out[(out["소재ID"] != "") & (out["실제소재명"] != "")]
    out = out.drop_duplicates(subset=["매체", "소재ID"], keep="last")
    return out

def standardize_raw(df: pd.DataFrame, platform: str) -> pd.DataFrame:
    out = df.copy()
    out.columns = [normalize_colname(c) for c in out.columns]

    if "광고ID" not in out.columns:
        if "소재ID" in out.columns:
            out["광고ID"] = out["소재ID"]
        elif "광고명" in out.columns:
            out["광고ID"] = out["광고명"]
        else:
            out["광고ID"] = ""

    if "광고명" not in out.columns:
        out["광고명"] = ""

    out["광고ID_원본"] = out["광고ID"].astype(str)
    out["광고ID"] = out["광고ID"].apply(clean_text)
    out["광고명"] = out["광고명"].apply(clean_text)
    out["매체"] = platform
    return out

def apply_index_mapping(raw_df: pd.DataFrame, index_df: pd.DataFrame, platform: str):
    raw = standardize_raw(raw_df, platform)
    idx = standardize_index(index_df)
    idx_platform = idx[idx["매체"] == platform].copy()

    merged = raw.merge(
        idx_platform.rename(columns={"소재ID": "매핑소재ID"}),
        how="left",
        left_on="광고ID",
        right_on="매핑소재ID"
    )

    merged["실제소재명"] = merged["실제소재명"].fillna("")
    merged["광고명_원본"] = merged["광고명"]
    merged["광고명"] = merged["실제소재명"].where(merged["실제소재명"] != "", merged["광고명"])
    merged["매칭성공"] = merged["실제소재명"] != ""

    unmatched = merged[merged["실제소재명"] == ""][["광고ID", "광고명_원본"]].drop_duplicates()

    debug = {
        "raw_rows": len(raw),
        "index_rows_platform": len(idx_platform),
        "matched_rows": int(merged["매칭성공"].sum()),
        "unmatched_rows": int((~merged["매칭성공"]).sum()),
        "raw_unique_keys": int(raw["광고ID"].nunique()),
        "index_unique_keys": int(idx_platform["소재ID"].nunique()),
        "common_keys": int(len(set(raw["광고ID"]).intersection(set(idx_platform["소재ID"]))))
    }
    return merged, unmatched, debug

def add_real_cost(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "총비용(VAT포함)" in out.columns:
        out["실제비용"] = pd.to_numeric(out["총비용(VAT포함)"], errors="coerce").fillna(0) / 1.1
        out["실제비용"] = out["실제비용"].round(0).astype(int)
    return out

# =========================================================
# 5. 인덱스 로드
# =========================================================
storage_mode, storage_reason = get_storage_mode_and_reason()

if storage_mode == "gsheets":
    loaded_index_df, load_source = gs_load_index()
    if loaded_index_df is None:
        index_df, current_index_source = load_index_from_local()
        current_index_source = f"로컬 fallback ({current_index_source})"
    else:
        index_df = standardize_index(loaded_index_df)
        current_index_source = "Google Sheets"
else:
    index_df, src = load_index_from_local()
    index_df = standardize_index(index_df)
    current_index_source = f"로컬 CSV ({len(index_df)}행)"

# =========================================================
# 6. 탭 UI
# =========================================================
tabs = st.tabs(["🔗 데이터 수집", "🗂️ 인덱스 관리", "📊 대시보드", "🧮 성과 비교 데이터", "🧠 데일리 코멘트", "🛠️ 매핑 디버그"])

with tabs[0]:
    st.info("데이터 수집 영역입니다. 현재는 인덱스 관리와 네이버 RAW 테스트에 집중합니다.")

# =========================================================
# 7. 인덱스 관리
# =========================================================
with tabs[1]:
    st.header("🗂️ 인덱스 관리")

    if storage_mode == "gsheets":
        st.success("Google Sheets 영구 저장 모드로 연결되었습니다.")
    else:
        st.warning("현재 로컬 CSV 저장 모드입니다. Streamlit Cloud에서는 영구 저장이 보장되지 않습니다.")

    sheet_id = get_google_sheet_id()
    st.caption(f"시트 ID: {sheet_id or '없음'} / 워크시트: {INDEX_WORKSHEET_NAME}")

    with st.expander("연결 상태 확인", expanded=True):
        c1, c2, c3 = st.columns(3)
        service_info = get_google_service_account_info()
        client, spreadsheet = get_gspread_client()

        with c1:
            st.markdown("**서비스 계정**")
            st.subheader("있음" if service_info else "없음")
        with c2:
            st.markdown("**INDEX_SHEET_ID**")
            st.subheader("있음" if sheet_id else "없음")
        with c3:
            st.markdown("**Google Sheets 연결**")
            st.subheader("성공" if spreadsheet is not None else "실패")

        st.write(f"gspread 설치 여부: `{GSPREAD_AVAILABLE}`")
        st.write(f"서비스 계정 이메일: {service_info.get('client_email', '확인 불가') if service_info else '확인 불가'}")
        st.write(f"대상 시트 ID: {sheet_id or '없음'}")
        st.write(f"대상 워크시트: {INDEX_WORKSHEET_NAME}")
        st.write(f"연결된 스프레드시트 제목: {getattr(spreadsheet, 'title', '확인 불가') if spreadsheet else '확인 불가'}")
        st.write(f"저장 모드 판정: `{storage_mode}`")
        st.write(f"저장 모드 사유: `{storage_reason}`")

    st.info(f"현재 인덱스 로드 경로: {current_index_source}")

    left, right = st.columns([1.3, 1.0])

    with left:
        st.subheader("인덱스 파일 업로드")
        idx_file = st.file_uploader("인덱스 업로드", type=["csv", "xlsx"], key="index_upload")

        if idx_file is not None:
            try:
                uploaded_index = read_any_file(idx_file)
                standardized = standardize_index(uploaded_index)

                save_result = {
                    "requested_storage_mode": storage_mode,
                    "actual_saved_mode": None,
                    "saved_rows": int(len(standardized)),
                    "worksheet_name": INDEX_WORKSHEET_NAME,
                    "sheet_id": sheet_id,
                    "error": None
                }

                if storage_mode == "gsheets":
                    ok, err = gs_save_index(standardized)
                    if ok:
                        save_result["actual_saved_mode"] = "gsheets"
                        index_df = standardized.copy()
                        current_index_source = "Google Sheets"
                        st.success("Google Sheets에 인덱스를 저장했습니다.")
                    else:
                        save_index_to_local(standardized)
                        save_result["actual_saved_mode"] = "local_fallback"
                        save_result["error"] = err
                        index_df = standardized.copy()
                        current_index_source = "로컬 fallback"
                        st.warning("Google Sheets 저장 실패로 로컬 CSV에 대신 저장했습니다.")
                else:
                    save_index_to_local(standardized)
                    save_result["actual_saved_mode"] = "local"
                    index_df = standardized.copy()
                    current_index_source = "로컬 CSV"
                    st.warning("Google Sheets 연결이 없어 로컬 CSV로 저장했습니다.")

                st.markdown("### 저장 결과")
                st.json(save_result)

            except Exception as e:
                st.error(f"인덱스 저장 중 오류가 발생했습니다: {e}")

    with right:
        st.subheader("필수 컬럼")
        st.write("매체 / 소재ID / 실제소재명")
        st.subheader("저장 대상")
        st.write("네이버 등 업로드 시 자동 참조")
        st.subheader("확인 방법")
        st.write("업로드 후 저장 결과와 현재 인덱스 로드 경로를 확인하세요")

    st.markdown("---")
    st.subheader("현재 인덱스")
    st.dataframe(index_df, use_container_width=True, height=320)

    if not index_df.empty:
        st.download_button(
            "현재 인덱스 다운로드",
            index_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
            "current_index.csv",
            "text/csv"
        )

# =========================================================
# 8. 대시보드 / 성과비교 / 코멘트 (placeholder)
# =========================================================
with tabs[2]:
    st.info("대시보드 영역입니다.")

with tabs[3]:
    st.info("성과 비교 데이터 영역입니다.")

with tabs[4]:
    st.info("데일리 코멘트 영역입니다.")

# =========================================================
# 9. 매핑 디버그
# =========================================================
with tabs[5]:
    st.header("🛠️ 매핑 디버그")
    st.caption("네이버 RAW와 인덱스 파일의 매칭 실패 원인을 별도 화면에서 확인합니다.")

    raw_file = st.file_uploader("네이버 RAW 업로드", type=["csv", "xlsx"], key="naver_raw_debug")
    if raw_file is not None:
        try:
            raw_df = read_any_file(raw_file)
            mapped_df, unmatched_df, debug = apply_index_mapping(raw_df, index_df, "네이버")
            mapped_df = add_real_cost(mapped_df)

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("RAW 행 수", debug["raw_rows"])
            c2.metric("인덱스 행 수(네이버)", debug["index_rows_platform"])
            c3.metric("매칭 행 수", debug["matched_rows"])
            c4.metric("미매칭 행 수", debug["unmatched_rows"])

            c5, c6, c7 = st.columns(3)
            c5.metric("RAW 고유 키 수", debug["raw_unique_keys"])
            c6.metric("인덱스 고유 키 수", debug["index_unique_keys"])
            c7.metric("공통 키 수", debug["common_keys"])

            st.markdown("### 1) 결과 미리보기")
            preview_cols = [c for c in ["광고ID_원본", "광고ID", "광고명_원본", "광고명", "실제소재명", "매칭성공"] if c in mapped_df.columns]
            st.dataframe(mapped_df[preview_cols + [c for c in mapped_df.columns if c not in preview_cols]], use_container_width=True)

            st.markdown("### 2) 미매칭 목록")
            st.dataframe(unmatched_df, use_container_width=True)

            st.download_button(
                "결과 CSV 다운로드",
                mapped_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                "mapping_debug_result.csv",
                "text/csv"
            )
        except Exception as e:
            st.error(f"RAW 분석 중 오류: {e}")
