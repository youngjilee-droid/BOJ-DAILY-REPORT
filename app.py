# ===============================
# Google Sheets 안정 연결 버전 (줄바꿈 제거 포함)
# ===============================
import base64
import json
import streamlit as st

# =========================================================
# 서비스 계정 읽기 (줄바꿈/공백 제거 포함)
# =========================================================
def get_google_service_account_info():
    try:
        if "GOOGLE_SERVICE_ACCOUNT_B64" in st.secrets:
            raw_b64 = st.secrets["GOOGLE_SERVICE_ACCOUNT_B64"]

            # ✅ 핵심: 줄바꿈/공백 제거
            raw_b64 = raw_b64.replace("\n", "").replace(" ", "").strip()

            decoded = base64.b64decode(raw_b64).decode("utf-8")
            return json.loads(decoded)

        return None

    except Exception as e:
        st.error(f"서비스 계정 파싱 실패: {e}")
        return None


# =========================================================
# 테스트 UI
# =========================================================
st.title("Google Sheets 연결 테스트")

service_info = get_google_service_account_info()

if service_info:
    st.success("서비스 계정 로드 성공")
    st.write("client_email:", service_info.get("client_email"))
else:
    st.error("서비스 계정 로드 실패")


# =========================================================
# 디버그 출력
# =========================================================
st.markdown("### Debug Info")
st.write("Secrets keys:", list(st.secrets.keys()))


