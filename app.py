import json
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

API_VERSION = "v25.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"


def _validate_date(date_text):
    try:
        datetime.strptime(date_text, "%Y-%m-%d")
        return date_text
    except Exception:
        raise ValueError(f"날짜 형식이 잘못되었습니다: {date_text}")


def _normalize_ad_account_id(ad_account_id):
    ad_account_id = str(ad_account_id).strip()
    if not ad_account_id.startswith("act_"):
        ad_account_id = f"act_{ad_account_id}"
    return ad_account_id


def _safe_int(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return default


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def _extract_action_total(action_list, target_types):
    if not isinstance(action_list, list):
        return 0.0

    total = 0.0
    for item in action_list:
        action_type = str(item.get("action_type", "")).strip()
        if action_type in target_types:
            total += _safe_float(item.get("value", 0))
    return total


def fetch_meta_data(start_date, end_date):
    try:
        start_date = _validate_date(start_date)
        end_date = _validate_date(end_date)
    except Exception as e:
        st.error(str(e))
        return pd.DataFrame()

    try:
        access_token = str(st.secrets["META_ACCESS_TOKEN"]).strip()
        ad_account_id = _normalize_ad_account_id(st.secrets["META_AD_ACCOUNT_ID"])
    except Exception:
        st.error("Streamlit Secrets의 META_ACCESS_TOKEN 또는 META_AD_ACCOUNT_ID를 확인해주세요.")
        return pd.DataFrame()

    url = f"{BASE_URL}/{ad_account_id}/insights"

    fields = [
        "date_start",
        "date_stop",
        "campaign_name",
        "adset_name",
        "ad_name",
        "impressions",
        "clicks",
        "spend",
        "reach",
        "actions",
    ]

    params = {
        "access_token": access_token,
        "fields": ",".join(fields),
        "level": "ad",
        "limit": 500,
        "time_increment": 1,
        "action_attribution_windows": json.dumps(["7d_click", "1d_view"]),
        "action_report_time": "conversion",
        "time_range": json.dumps(
            {"since": start_date, "until": end_date},
            ensure_ascii=False,
        ),
    }

    add_to_cart_types = {
        "add_to_cart",
        "omni_add_to_cart",
        "offsite_conversion.fb_pixel_add_to_cart",
        "onsite_web_add_to_cart",
        "offsite_conversion.add_to_cart",
        "onsite_conversion.add_to_cart",
    }

    follow_types = {
        "follow",
        "follows",
        "instagram_profile_follows",
        "page_like",
        "like",
    }

    engagement_types = {
        "page_engagement",
        "post_engagement",
        "post_interaction_gross",
        "post",
    }

    video_view_types = {
        "video_view",
    }

    rows = []
    next_url = url
    next_params = params

    try:
        while next_url:
            response = requests.get(next_url, params=next_params, timeout=30)

            if response.status_code != 200:
                st.error(f"Meta API 오류: {response.text}")
                return pd.DataFrame()

            result = response.json()

            if "error" in result:
                st.error(f"Meta API 오류: {result['error']}")
                return pd.DataFrame()

            data = result.get("data", [])

            for item in data:
                actions = item.get("actions", [])

                add_to_cart = _extract_action_total(actions, add_to_cart_types)
                follows = _extract_action_total(actions, follow_types)
                engagement = _extract_action_total(actions, engagement_types)
                video_views = _extract_action_total(actions, video_view_types)

                rows.append(
                    {
                        "날짜": item.get("date_start", ""),
                        "캠페인명": item.get("campaign_name", ""),
                        "광고그룹명": item.get("adset_name", ""),
                        "광고명": item.get("ad_name", ""),
                        "비용": _safe_float(item.get("spend", 0)),
                        "실제 비용": "",
                        "노출": _safe_int(item.get("impressions", 0)),
                        "클릭": _safe_int(item.get("clicks", 0)),
                        "구매": 0,
                        "매출액": 0,
                        "장바구니담기수": add_to_cart,
                        "도달": _safe_int(item.get("reach", 0)),
                        "참여": engagement,
                        "팔로우": follows,
                        "동영상조회": video_views,
                        "매체": "메타",
                    }
                )

            paging = result.get("paging", {})
            next_url = paging.get("next")
            next_params = None

    except Exception as e:
        st.error(f"Meta 데이터 처리 중 오류 발생: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(
            columns=[
                "날짜", "캠페인명", "광고그룹명", "광고명", "비용", "실제 비용",
                "노출", "클릭", "구매", "매출액", "장바구니담기수",
                "도달", "참여", "팔로우", "동영상조회", "매체"
            ]
        )

    ordered_cols = [
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

    return df[ordered_cols]
