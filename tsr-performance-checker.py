import streamlit as st
import pandas as pd
import re
import asyncio
import aiohttp
import os
import matplotlib.pyplot as plt
from sec_api import QueryApi, XbrlApi

# SEC API Key
API_KEY = "a1293bb279cf316f31123670887b10c1fad2c098a90ff5bae1e3868ab327cf8f"
queryApi = QueryApi(API_KEY)
xbrlApi = XbrlApi(API_KEY)

# File path for class mapping
CLASS_MAPPING_FILE = "class_series_mapping.csv"

# Helper to extract classid from segment value
def extract_classid(segment_value):
    match = re.findall(r'C\d{9}', str(segment_value))
    return match[0] if match else None

# SEC Filing Search Function
def fetch_filing_metadata(form_types, from_date, to_date, limit):
    st.write("🔍 Searching SEC filings...")
    form_query = " OR ".join([f'formType:"{ft}"' for ft in form_types])
    search_query = f'({form_query}) AND filedAt:[{from_date} TO {to_date}]'

    search_params = {
        "query": search_query,
        "from": "0",
        "size": "50",
        "sort": [{"filedAt": {"order": "desc"}}],
    }

    filing_metadata = []
    try:
        while len(filing_metadata) < limit:
            result = queryApi.get_filings(search_params)
            filings = result.get("filings", [])
            if not filings:
                break

            for f in filings:
                filing_metadata.append({
                    "Filing URL": f.get("linkToFilingDetails"),
                    "Filed At": f.get("filedAt"),
                    "Ticker": f.get("ticker", "N/A")
                })

            if len(filing_metadata) >= limit:
                break
            search_params["from"] = str(int(search_params["from"]) + int(search_params["size"]))

        return filing_metadata[:limit]

    except Exception as e:
        st.error(f"❌ Error during SEC query: {e}")
        return []

# Async performance check with classid extraction
def process_filing_with_details(filing_url):
    try:
        xbrl_data = xbrlApi.xbrl_to_json(htm_url=filing_url)
        perf_present = "AvgAnnlRtrTableTextBlock" in xbrl_data

        expense_amt_data = xbrl_data.get("ExpensesPaidAmt", {})
        if not expense_amt_data:
            return []

        df = pd.json_normalize(expense_amt_data)
        df["classid"] = df["segment.value"].apply(extract_classid)
        df = df.dropna(subset=["classid"])
        df["Filing URL"] = filing_url
        df["Has Performance Data"] = perf_present
        return df[["classid", "Filing URL", "Has Performance Data"]]

    except Exception:
        return pd.DataFrame()

async def check_all_filings(metadata):
    loop = asyncio.get_event_loop()
    results = []
    for idx, entry in enumerate(metadata):
        df = await loop.run_in_executor(None, process_filing_with_details, entry["Filing URL"])
        if isinstance(df, pd.DataFrame) and not df.empty:
            df["Filed At"] = entry["Filed At"]
            df["Ticker"] = entry["Ticker"]
            results.append(df)
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

# Streamlit UI
st.set_page_config(page_title="Performance Disclosure Checker", layout="centered")

st.markdown("""
    <h1 style="text-align:center; color:#2E86C1;">📈 Performance Disclosure Checker (SEC Filings)</h1>
    <hr>
""", unsafe_allow_html=True)

form_types = st.multiselect("📄 Select Form Type(s)", ["N-CSR", "N-CSRS"])
col1, col2 = st.columns(2)
with col1:
    from_date = st.date_input("🗓️ From Date")
with col2:
    to_date = st.date_input("🗓️ To Date")

limit = st.selectbox("🔢 Number of filings to check", options=[5, 20, 50, 100, 200], index=1)

if st.button("🚀 Submit"):
    if form_types and from_date and to_date:
        if not os.path.exists(CLASS_MAPPING_FILE):
            st.error(f"❌ Required file '{CLASS_MAPPING_FILE}' is missing!")
        else:
            df_mapping = pd.read_csv(CLASS_MAPPING_FILE)
            if "classid" not in df_mapping.columns:
                st.error("❌ Mapping file must include 'classid' column")
            else:
                metadata = fetch_filing_metadata(form_types, from_date, to_date, limit)
                if not metadata:
                    st.error("❌ No filings found.")
                else:
                    st.info(f"🔄 Checking {len(metadata)} filings for performance information...")
                    df_results = asyncio.run(check_all_filings(metadata))

                    if df_results.empty:
                        st.error("❌ No valid data extracted.")
                    else:
                        df_results = df_results.merge(df_mapping, on="classid", how="left")

                        perf_count = df_results["Has Performance Data"].sum()
                        st.success(f"✅ {perf_count} out of {len(df_results)} share classes disclose performance information.")

                        st.write("### 📄 Detailed Results")
                        st.dataframe(df_results)

                        csv_data = df_results.to_csv(index=False).encode("utf-8")
                        st.download_button("⬇️ Download Results as CSV", csv_data, "performance_disclosure_results.csv", "text/csv")

                        # 📊 Bar Chart: Entity Name vs Count of Classes Disclosing Performance
                        st.write("### 📊 Performance Disclosure by Entity")
                        if "Entity Name" in df_results.columns:
                            perf_by_entity = df_results[df_results["Has Performance Data"]].groupby("Entity Name")["classid"].count().sort_values(ascending=False)
                            fig, ax = plt.subplots()
                            perf_by_entity.plot(kind='bar', ax=ax)
                            ax.set_ylabel("# of Classes Disclosing Performance")
                            ax.set_xlabel("Entity Name")
                            ax.set_title("Performance Disclosure by Entity")
                            st.pyplot(fig)
                        else:
                            st.info("ℹ️ 'Entity Name' column not found in mapping file.")

    else:
        st.error("❌ Please select form types and a valid date range.")
