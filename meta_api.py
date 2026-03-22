import json
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

API_VERSION = "v25.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"


def _validate_date(date_text: str) -> str:
    try:
        datetime.strptime(date_text, "%Y-%m-%d")
        return date_text
    except ValueError as e:
        raise ValueError(
            f"날짜 형식이 잘못되었습니다: {date_text} (예: 2026-03-18)"
        ) from e


def _normalize_ad_account_id(ad_account_id: str) -> str:
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


def _extract_action_total(action_list, target_types):
    """
    list[{"action_type": "...", "value": "..."}] 에서
    target_types 에 해당하는 value 합산
    """
    if not isinstance(action_list, list):
        return 0.0

    total = 0.0
    for item in action_list:
        action_type = str(item.get("action_type", "")).strip()
        if action_type in target_types:
            total += _safe_float(item.get("value", 0))
    return total


def _extract_action_total_fuzzy(action_list, include_keywords):
    """
    action_type 이름이 계정/캠페인마다 조금 다를 수 있어
    특정 키워드가 포함된 항목을 합산
    """
    if not isinstance(action_list, list):
        return 0.0

    total = 0.0
    for item in action_list:
        action_type = str(item.get("action_type", "")).strip().lower()
        if all(keyword in action_type for keyword in include_keywords):
            total += _safe_float(item.get("value", 0))
    return total


def _extract_video_views(video_action_list):
    if not isinstance(video_action_list, list):
        return 0.0

    total = 0.0
    for item in video_action_list:
        total += _safe_float(item.get("value", 0))
    return total


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_meta_data(start_date: str, end_date: str) -> pd.DataFrame:
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
        "date_stop",
        "campaign_name",
        "adset_name",
        "ad_name",
        "impressions",
        "clicks",
        "spend",
        "reach",
        "actions",
        "action_values",
        "cost_per_action_type",
        "video_play_actions",
        "catalog_segment_actions",
        "catalog_segment_value",
        "purchase_roas",
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
            {
                "since": start_date,
                "until": end_date,
            },
            ensure_ascii=False,
        ),
    }

    # 일반 전환 후보
    purchase_types = {
        "purchase",
        "omni_purchase",
        "offsite_conversion.fb_pixel_purchase",
        "onsite_web_purchase",
        "offsite_conversion.purchase",
        "onsite_conversion.purchase",
    }

    # 협력광고 / shared items / catalog segment 계열 후보
    collaborative_purchase_types = {
        "purchase",
        "omni_purchase",
        "onsite_web_purchase",
        "offsite_conversion.fb_pixel_purchase",
        "catalog_segment_purchase",
        "catalog_segment_omni_purchase",
        "purchase_with_shared_items",
        "omni_purchase_with_shared_items",
        "website_purchase_with_shared_items",
        "purchases_with_shared_items",
        "shared_items_purchase",
    }

    add_to_cart_types = {
        "add_to_cart",
        "omni_add_to_cart",
        "offsite_conversion.fb_pixel_add_to_cart",
        "onsite_web_add_to_cart",
        "offsite_conversion.add_to_cart",
        "onsite_conversion.add_to_cart",
    }

    initiate_checkout_types = {
        "initiate_checkout",
        "omni_initiated_checkout",
        "onsite_conversion.initiate_checkout",
        "onsite_web_initiate_checkout",
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

    rows = []
    next_url = url
    next_params = params.copy()
    debug_logged = False

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

            if not debug_logged:
                st.info(f"📦 Meta API에서 받은 첫 페이지 데이터 행 수: {len(data)}")
                if len(data) > 0:
                    first_item = data[0]
                    st.write("🔑 첫 번째 데이터 키 목록:", list(first_item.keys()))
                    st.write("📋 catalog_segment_actions:", first_item.get("catalog_segment_actions", []))
                    st.write("💵 catalog_segment_value:", first_item.get("catalog_segment_value", []))
                debug_logged = True

            for item in data:
                actions = item.get("actions", [])
                action_values = item.get("action_values", [])
                catalog_actions = item.get("catalog_segment_actions", [])
                catalog_values = item.get("catalog_segment_value", [])
                video_actions = item.get("video_play_actions", [])

                # 1) 구매: collaborative ads 우선
                collaborative_purchase = _extract_action_total(
                    catalog_actions, collaborative_purchase_types
                )

                if collaborative_purchase == 0:
                    collaborative_purchase = _extract_action_total_fuzzy(
                        catalog_actions, ["purchase"]
                    )

                if collaborative_purchase == 0:
                    collaborative_purchase = _extract_action_total(
                        actions, purchase_types
                    )

                # 2) 매출액: collaborative ads 우선
                collaborative_revenue = _extract_action_total(
                    catalog_values, collaborative_purchase_types
                )

                if collaborative_revenue == 0:
                    collaborative_revenue = _extract_action_total_fuzzy(
                        catalog_values, ["purchase"]
                    )

                if collaborative_revenue == 0:
                    collaborative_revenue = _extract_action_total(
                        action_values, purchase_types
                    )

                # 3) 기타 지표
                add_to_cart = _extract_action_total(actions, add_to_cart_types)

                initiate_checkout = _extract_action_total(
                    actions, initiate_checkout_types
                )

                engagement = _extract_action_total(actions, engagement_types)
                follows = _extract_action_total(actions, follow_types)
                video_views = _extract_video_views(video_actions)

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
                        "구매": collaborative_purchase,
                        "매출액": collaborative_revenue,
                        "장바구니담기수": add_to_cart,
                        "결제시작수": initiate_checkout,
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

    except requests.exceptions.RequestException as e:
        st.error(f"Meta API 요청 실패: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Meta 데이터 처리 중 오류 발생: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    if df.empty:
        return df

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
        "결제시작수",
        "도달",
        "참여",
        "팔로우",
        "동영상조회",
        "매체",
    ]

    # 없는 컬럼이 있어도 에러 안 나게 처리
    existing_cols = [col for col in ordered_cols if col in df.columns]
    df = df[existing_cols]

    return df
