import json
from datetime import datetime
import requests
import pandas as pd
import streamlit as st


API_VERSION = "v25.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"


def _validate_date(date_text: str) -> str:
    """
    YYYY-MM-DD 형식 검증
    """
    try:
        datetime.strptime(date_text, "%Y-%m-%d")
        return date_text
    except ValueError as e:
        raise ValueError(f"날짜 형식이 잘못되었습니다: {date_text} (예: 2026-03-18)") from e


def _normalize_ad_account_id(ad_account_id: str) -> str:
    """
    act_ 접두어가 없으면 자동 보정
    """
    ad_account_id = str(ad_account_id).strip()
    if not ad_account_id.startswith("act_"):
        ad_account_id = f"act_{ad_account_id}"
    return ad_account_id


def _safe_int(value, default=0):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _extract_action_value(action_list, action_type="purchase"):
    """
    actions / action_values 리스트에서 특정 action_type 값 추출
    """
    if not isinstance(action_list, list):
        return 0.0

    total = 0.0
    for item in action_list:
        if item.get("action_type") == action_type:
            total += _safe_float(item.get("value", 0))
    return total


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_meta_data(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Meta Ads Insights 조회
    """
    try:
        start_date = _validate_date(start_date)
        end_date = _validate_date(end_date)
    except ValueError as e:
        st.error(str(e))
        return pd.DataFrame()

    access_token = st.secrets.get("META_ACCESS_TOKEN", "").strip()
    ad_account_id = _normalize_ad_account_id(
        st.secrets.get("META_AD_ACCOUNT_ID", "").strip()
    )

    if not access_token:
        st.error("META_ACCESS_TOKEN 값이 비어 있습니다.")
        return pd.DataFrame()

    if not ad_account_id or ad_account_id == "act_":
        st.error("META_AD_ACCOUNT_ID 값이 비어 있습니다.")
        return pd.DataFrame()

    url = f"{BASE_URL}/{ad_account_id}/insights"

    fields = [
        "date_start",
        "campaign_name",
        "adset_name",
        "ad_name",
        "impressions",
        "clicks",
        "spend",
        "actions",
        "action_values",
    ]

    # 핵심: time_range를 JSON 문자열로 명시 전달
    # Meta가 기대하는 형식: {'since':'YYYY-MM-DD','until':'YYYY-MM-DD'}
    params = {
        "access_token": access_token,
        "fields": ",".join(fields),
        "level": "ad",
        "limit": 500,
        "time_increment": 1,
        "time_range": json.dumps(
            {
                "since": start_date,
                "until": end_date,
            },
            ensure_ascii=False,
        ),
    }

    rows = []
    next_url = url
    next_params = params.copy()

    try:
        while next_url:
            response = requests.get(next_url, params=next_params, timeout=30)

            if response.status_code != 200:
                st.error(f"Meta API 오류: {response.text}")
                return pd.DataFrame()

            result = response.json()

            # API 자체 에러 처리
            if "error" in result:
                st.error(f"Meta API 오류: {result['error']}")
                return pd.DataFrame()

            data = result.get("data", [])

            for item in data:
                purchase = _extract_action_value(item.get("actions", []), "purchase")
                revenue = _extract_action_value(item.get("action_values", []), "purchase")

                rows.append(
                    {
                        "날짜": item.get("date_start", ""),
                        "캠페인명": item.get("campaign_name", ""),
                        "광고그룹명": item.get("adset_name", ""),
                        "광고명": item.get("ad_name", ""),
                        "노출": _safe_int(item.get("impressions", 0)),
                        "클릭": _safe_int(item.get("clicks", 0)),
                        "비용": _safe_float(item.get("spend", 0)),
                        "전환": purchase,
                        "매출": revenue,
                        "매체": "META",
                    }
                )

            paging = result.get("paging", {})
            next_url = paging.get("next")

            # next URL에는 이미 쿼리스트링이 포함되므로 params 제거
            next_params = None

    except requests.exceptions.RequestException as e:
        st.error(f"Meta API 요청 실패: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Meta 데이터 처리 중 오류 발생: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    # 컬럼 순서 정리
    ordered_cols = [
        "날짜",
        "캠페인명",
        "광고그룹명",
        "광고명",
        "노출",
        "클릭",
        "비용",
        "전환",
        "매출",
        "매체",
    ]
    df = df[ordered_cols]

    return df
