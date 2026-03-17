import streamlit as st
import pandas as pd

st.title("광고 데이터 통합 리포트")

st.write("RAW 광고 데이터를 업로드하면 하나의 통합 데이터로 정리됩니다.")

naver_file = st.file_uploader("네이버 광고 데이터", type=["csv","xlsx"])
meta_file = st.file_uploader("메타 광고 데이터", type=["csv","xlsx"])
kakao_file = st.file_uploader("카카오 광고 데이터", type=["csv","xlsx"])
tiktok_file = st.file_uploader("틱톡 광고 데이터", type=["csv","xlsx"])

dataframes = []

def load_file(file, platform):
    if file:
        if file.name.endswith(".csv"):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)

        df["매체"] = platform
        return df

if naver_file:
    dataframes.append(load_file(naver_file,"Naver"))

if meta_file:
    dataframes.append(load_file(meta_file,"Meta"))

if kakao_file:
    dataframes.append(load_file(kakao_file,"Kakao"))

if tiktok_file:
    dataframes.append(load_file(tiktok_file,"TikTok"))

if dataframes:

    merged_df = pd.concat(dataframes)

    st.subheader("통합 데이터")
    st.dataframe(merged_df)

    csv = merged_df.to_csv(index=False).encode("utf-8")

    st.download_button(
        "통합 데이터 다운로드",
        csv,
        "ads_report.csv",
        "text/csv"
    )
