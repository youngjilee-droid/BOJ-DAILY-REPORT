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


def _extract_action_first(action_list, priority_types):
    """
    여러 후보 중 첫 번째로 발견되는 값만 사용
    중복 합산 방지용
    """
    if not isinstance(action_list, list):
        return 0.0

    action_map = {}
    for item in action_list:
        action_type = str(item.get("action_type", "")).strip()
        action_map[action_type] = _safe_float(item.get("value", 0))

    for action_type in priority_types:
        if action_type in action_map:
            return action_map[action_type]

    return 0.0


def _extract_action_first_fuzzy(action_list, priority_keywords):
    """
    정확한 이름이 다를 때 키워드 우선순위로 첫 값만 사용
    예: ["shared", "purchase"] 포함 항목 우선
    """
    if not isinstance(action_list, list):
        return 0.0

    for keywords in priority_keywords:
        for item in action_list:
            action_type = str(item.get("action_type", "")).strip().lower()
            if all(keyword.lower() in action_type for keyword in keywords):
                return _safe_float(item.get("value", 0))
    return 0.0


def _request_meta_insights(url, params):
    response = requests.get(url, params=params, timeout=30)

    if response.status_code != 200:
        raise RuntimeError(response.text)

    result = response.json()

    if "error" in result:
        raise RuntimeError(str(result["error"]))

    return result


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

    # 1) 협력광고 필드 우선 시도
    collaborative_fields = [
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
        "catalog_segment_actions",
        "catalog_segment_value",
    ]

    # 2) 실패 시 안전 필드로 자동 폴백
    safe_fields = [
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

    base_params = {
        "access_token": access_token,
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

    link_click_types = {
        "link_click",
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

    initiate_checkout_types = {
        "initiate_checkout",
        "omni_initiated_checkout",
        "onsite_conversion.initiate_checkout",
        "onsite_web_initiate_checkout",
    }

    # 구매/매출 우선순위
    collaborative_purchase_priority = [
        "shared_items_purchase",
        "purchase_with_shared_items",
        "purchases_with_shared_items",
        "website_purchase_with_shared_items",
        "catalog_segment_purchase",
        "catalog_segment_omni_purchase",
        "purchase",
        "omni_purchase",
    ]

    rows = []
    next_url = url
    next_params = dict(base_params)
    use_collaborative_fields = True

    try:
        while next_url:
            if use_collaborative_fields:
                try:
                    collaborative_params = dict(next_params)
                    collaborative_params["fields"] = ",".join(collaborative_fields)
                    result = _request_meta_insights(next_url, collaborative_params)
                except Exception:
                    use_collaborative_fields = False
                    safe_params = dict(next_params)
                    safe_params["fields"] = ",".join(safe_fields)
                    result = _request_meta_insights(next_url, safe_params)
            else:
                safe_params = dict(next_params)
                safe_params["fields"] = ",".join(safe_fields)
                result = _request_meta_insights(next_url, safe_params)

            data = result.get("data", [])

            for item in data:
                actions = item.get("actions", [])
                catalog_actions = item.get("catalog_segment_actions", [])
                catalog_values = item.get("catalog_segment_value", [])

                # 클릭: 전체 clicks 대신 link_click 사용
                link_clicks = _extract_action_total(actions, link_click_types)

                add_to_cart = _extract_action_total(actions, add_to_cart_types)
                follows = _extract_action_total(actions, follow_types)
                engagement = _extract_action_total(actions, engagement_types)
                video_views = _extract_action_total(actions, video_view_types)
                initiate_checkout = _extract_action_total(actions, initiate_checkout_types)

                # 구매: 합산하지 않고 우선순위 1개만 선택
                purchase = _extract_action_first(
                    catalog_actions,
                    collaborative_purchase_priority
                )

                if purchase == 0:
                    purchase = _extract_action_first_fuzzy(
                        catalog_actions,
                        [
                            ["shared", "purchase"],
                            ["purchase", "shared"],
                            ["purchase"],
                        ]
                    )

                # 매출액: 합산하지 않고 우선순위 1개만 선택
                revenue = _extract_action_first(
                    catalog_values,
                    collaborative_purchase_priority
                )

                if revenue == 0:
                    revenue = _extract_action_first_fuzzy(
                        catalog_values,
                        [
                            ["shared", "purchase"],
                            ["purchase", "shared"],
                            ["purchase"],
                        ]
                    )

                rows.append(
                    {
                        "날짜": item.get("date_start", ""),
                        "캠페인명": item.get("campaign_name", ""),
                        "광고그룹명": item.get("adset_name", ""),
                        "광고명": item.get("ad_name", ""),
                        "비용": _safe_float(item.get("spend", 0)),
                        "실제 비용": "",
                        "노출": _safe_int(item.get("impressions", 0)),
                        "클릭": _safe_int(link_clicks),
                        "구매": purchase,
                        "매출액": revenue,
                        "장바구니담기수": add_to_cart,
                        "도달": _safe_int(item.get("reach", 0)),
                        "참여": engagement,
                        "팔로우": follows,
                        "동영상조회": video_views,
                        "매체": "메타",
                        "결제시작수": initiate_checkout,
                    }
                )

            paging = result.get("paging", {})
            next_url = paging.get("next")
            next_params = dict(base_params)

    except Exception as e:
        st.error(f"Meta 데이터 처리 중 오류 발생: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(
            columns=[
                "날짜", "캠페인명", "광고그룹명", "광고명", "비용", "실제 비용",
                "노출", "클릭", "구매", "매출액", "장바구니담기수",
                "도달", "참여", "팔로우", "동영상조회", "매체", "결제시작수"
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
        "결제시작수",
    ]

    return df[ordered_cols]
