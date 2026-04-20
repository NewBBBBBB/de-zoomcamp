"""Malaysia Internet Speed dashboard — Streamlit + BigQuery."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

# -------------------------------------------------------------------- config

PROJECT = os.environ.get("GCP_PROJECT", "iron-figure-312018")
MARTS = os.environ.get("BQ_MARTS", "speed_marts")
LOCATION = os.environ.get("BQ_LOCATION", "US")

st.set_page_config(
    page_title="Malaysia Internet Speed",
    page_icon=":globe_with_meridians:",
    layout="wide",
)

# -------------------------------------------------------------------- client


@st.cache_resource
def bq_client() -> bigquery.Client:
    key = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if key and Path(key).exists():
        creds = service_account.Credentials.from_service_account_file(key)
        return bigquery.Client(project=PROJECT, location=LOCATION, credentials=creds)
    # Fallback to ADC (e.g. local gcloud auth)
    return bigquery.Client(project=PROJECT, location=LOCATION)


@st.cache_data(ttl=600)
def run_query(sql: str) -> pd.DataFrame:
    return bq_client().query(sql).to_dataframe()


# -------------------------------------------------------------------- queries


def q_national() -> pd.DataFrame:
    return run_query(
        f"""
        select connection_type, quarter_start, year, quarter,
               avg_d_mbps, avg_u_mbps, avg_lat_ms,
               total_tests, total_devices
        from `{PROJECT}.{MARTS}.fct_national_quarterly`
        order by quarter_start, connection_type
        """
    )


def q_state(quarter_start: str, connection_type: str) -> pd.DataFrame:
    return run_query(
        f"""
        select state_code, state_name, region,
               avg_d_mbps, avg_u_mbps, avg_lat_ms,
               total_tests, tile_count
        from `{PROJECT}.{MARTS}.fct_state_quarterly`
        where quarter_start = DATE '{quarter_start}'
          and connection_type = '{connection_type}'
          and state_code != 'UNK'
        order by avg_d_mbps desc
        """
    )


def q_tiles(quarter_start: str, connection_type: str, sample: int = 10000) -> pd.DataFrame:
    return run_query(
        f"""
        select lon, lat, avg_d_mbps, avg_u_mbps, avg_lat_ms, state_name, tests
        from `{PROJECT}.{MARTS}.fct_tile_performance`
        where quarter_start = DATE '{quarter_start}'
          and connection_type = '{connection_type}'
        order by rand()
        limit {sample}
        """
    )


# -------------------------------------------------------------------- sidebar

st.title(":globe_with_meridians: Malaysia Internet Speed")
st.caption(
    "Quarterly Speedtest metrics from Ookla Open Data, aggregated by state "
    "for Malaysia. Pipeline: Airflow → GCS → BigQuery → dbt → Streamlit."
)

with st.sidebar:
    st.header("Filters")
    try:
        nat = q_national()
    except Exception as e:
        st.error(f"BigQuery query failed — has the pipeline run yet?\n\n{e}")
        st.stop()

    if nat.empty:
        st.warning("No data in marts yet — trigger the Airflow DAG first.")
        st.stop()

    quarters = sorted(nat["quarter_start"].astype(str).unique(), reverse=True)
    quarter_pick = st.selectbox("Quarter", quarters, index=0)
    conn_pick = st.radio("Connection", ["fixed", "mobile"], horizontal=True)

# -------------------------------------------------------------------- KPIs

latest = nat[
    (nat["quarter_start"].astype(str) == quarter_pick)
    & (nat["connection_type"] == conn_pick)
]
if latest.empty:
    st.warning("No data for this quarter × connection combo.")
    st.stop()
row = latest.iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Avg download", f"{row.avg_d_mbps:,.1f} Mbps")
c2.metric("Avg upload", f"{row.avg_u_mbps:,.1f} Mbps")
c3.metric("Avg latency", f"{row.avg_lat_ms:,.0f} ms")
c4.metric("Total tests (quarter)", f"{int(row.total_tests):,}")

st.divider()

# -------------------------------------------------------------------- trend

st.subheader("National trend")
fig_trend = px.line(
    nat,
    x="quarter_start",
    y="avg_d_mbps",
    color="connection_type",
    markers=True,
    labels={"quarter_start": "Quarter", "avg_d_mbps": "Avg download (Mbps)"},
)
fig_trend.update_layout(height=360, margin=dict(l=10, r=10, t=30, b=10))
st.plotly_chart(fig_trend, use_container_width=True)

# -------------------------------------------------------------------- state

col_l, col_r = st.columns([3, 2])

with col_l:
    st.subheader(f"Tile heatmap — {conn_pick} / {quarter_pick}")
    tiles = q_tiles(quarter_pick, conn_pick)
    if tiles.empty:
        st.info("No tile data.")
    else:
        fig_map = px.scatter_mapbox(
            tiles,
            lon="lon",
            lat="lat",
            color="avg_d_mbps",
            size="tests",
            size_max=8,
            color_continuous_scale="Viridis",
            range_color=(0, max(200, tiles["avg_d_mbps"].quantile(0.95))),
            hover_data={"state_name": True, "avg_d_mbps": ":.1f", "avg_lat_ms": True},
            zoom=4.8,
            center={"lat": 4.2, "lon": 109.0},
            height=540,
        )
        fig_map.update_layout(mapbox_style="carto-positron", margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig_map, use_container_width=True)

with col_r:
    st.subheader("State ranking")
    states = q_state(quarter_pick, conn_pick)
    if states.empty:
        st.info("No state data.")
    else:
        fig_bar = px.bar(
            states,
            x="avg_d_mbps",
            y="state_name",
            orientation="h",
            color="region",
            labels={"avg_d_mbps": "Avg download (Mbps)", "state_name": ""},
        )
        fig_bar.update_layout(
            height=540,
            yaxis={"categoryorder": "total ascending"},
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig_bar, use_container_width=True)

st.divider()

# -------------------------------------------------------------------- table

with st.expander("Raw state-level numbers"):
    st.dataframe(
        states.style.format(
            {
                "avg_d_mbps": "{:.1f}",
                "avg_u_mbps": "{:.1f}",
                "avg_lat_ms": "{:.0f}",
                "total_tests": "{:,}",
            }
        ),
        use_container_width=True,
    )

st.caption(
    "Data © Ookla 2019-present, licensed CC BY-NC-SA 4.0. "
    "State attribution uses simplified bounding boxes and is approximate."
)
