import requests
import pandas as pd
import streamlit as st

@st.cache_data(ttl=3600)
def fetch_meta_data(start_date, end_date):
    access_token = st.secrets["META_ACCESS_TOKEN"]
    ad_account_id = st.secrets["META_AD_ACCOUNT_ID"]

    url = f"https://graph.facebook.com/v19.0/{ad_account_id}/insights"

 params = {
    "access_token": access_token,
    "time_range": {
        "since": start_date,
        "until": end_date
    },
    "fields": ",".join([
        "date_start",
        "campaign_name",
        "adset_name",
        "ad_name",
        "impressions",
        "clicks",
        "spend",
        "actions",
        "action_values"
    ]),
    "level": "ad"
}

    response = requests.get(url, params=params)

    if response.status_code != 200:
        st.error(f"Meta API 오류: {response.text}")
        return pd.DataFrame()

    data = response.json().get("data", [])

    rows = []
    for item in data:
        purchase = 0
        revenue = 0

        # 전환 추출
        if "actions" in item:
            for act in item["actions"]:
                if act["action_type"] == "purchase":
                    purchase = float(act["value"])

        # 매출 추출
        if "action_values" in item:
            for val in item["action_values"]:
                if val["action_type"] == "purchase":
                    revenue = float(val["value"])

        rows.append({
            "날짜": item.get("date_start"),
            "캠페인명": item.get("campaign_name"),
            "광고그룹명": item.get("adset_name"),
            "광고명": item.get("ad_name"),
            "노출": int(item.get("impressions", 0)),
            "클릭": int(item.get("clicks", 0)),
            "비용": float(item.get("spend", 0)),
            "전환": purchase,
            "매출": revenue,
            "매체": "META"
        })

    return pd.DataFrame(rows)
