import streamlit as st
import pandas as pd
import re
import asyncio
import aiohttp
import os
import matplotlib.pyplot as plt
import seaborn as sns
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

# Async performance check with classid, expenses, and performance extraction
def process_filing_with_details(filing_url):
    try:
        xbrl_data = xbrlApi.xbrl_to_json(htm_url=filing_url)
        perf_present = "AvgAnnlRtrTableTextBlock" in xbrl_data

        expense_amt_data = xbrl_data.get("ExpensesPaidAmt", [])
        performance_data = xbrl_data.get("AvgAnnlRtrPct", [])

        df_exp = pd.json_normalize(expense_amt_data)
        df_perf = pd.json_normalize(performance_data)

        if "segment.value" in df_exp.columns:
            df_exp["classid"] = df_exp["segment.value"].apply(extract_classid)
            df_exp = df_exp.dropna(subset=["classid"])
            df_exp = df_exp.rename(columns={"value": "expense_amt"})

        if "segment.value" in df_perf.columns:
            df_perf["classid"] = df_perf["segment.value"].apply(extract_classid)
            df_perf = df_perf.dropna(subset=["classid"])
            df_perf = df_perf.rename(columns={"value": "performance_pct"})

        if df_exp.empty and not df_perf.empty:
            df_exp = pd.DataFrame({"classid": df_perf["classid"], "expense_amt": [None]*len(df_perf)})
        elif df_perf.empty and not df_exp.empty:
            df_perf = pd.DataFrame({"classid": df_exp["classid"], "performance_pct": [None]*len(df_exp)})
        elif df_exp.empty and df_perf.empty:
            return pd.DataFrame()

        df_combined = pd.merge(df_exp, df_perf, on="classid", how="outer")
        skipped_before = len(df_combined)
        df_combined = df_combined.dropna(subset=["classid"])
        skipped_after = len(df_combined)
        skipped_count = skipped_before - skipped_after
        if skipped_count > 0:
            st.info(f"ℹ️ Skipped {skipped_count} rows with missing classid in filing: {filing_url}")

        df_combined["Filing URL"] = filing_url
        df_combined["Has Performance Data"] = perf_present

        return df_combined[["classid", "Filing URL", "Has Performance Data", "expense_amt", "performance_pct"]]

    except Exception as e:
        st.warning(f"⚠️ Failed to process filing: {filing_url} — {e}")
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
st.set_page_config(page_title="Performance Disclosure Checker", layout="wide")

st.markdown("""
    <div style='background-color:#eaf2f8;padding:2vw;border-radius:10px;width:100%;box-sizing:border-box;max-width:100%;overflow-x:auto;'>
    <h1 style='text-align:center;color:#2E86C1;'>📊 Mutual Fund Performance & Expense Disclosure Analyzer</h1>
    <p style='text-align:center;font-size:16px;'>Analyze iXBRL-tagged SEC filings to understand how funds report performance and expenses.</p>
    </div>
    <br>
""", unsafe_allow_html=True)

with st.sidebar:
    st.header("🔧 Search Filters")
    form_types = st.multiselect("📄 Select Form Type(s)", ["N-CSR", "N-CSRS"])
    from_date = st.date_input(" ▋
