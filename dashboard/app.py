"""TransitLake dashboard — reads the gold marts straight from the DuckDB lake.

Run:  uv run streamlit run dashboard/app.py
"""
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = Path(__file__).resolve().parent.parent / "lake" / "transitlake.duckdb"

st.set_page_config(page_title="TransitLake", page_icon="🚆", layout="wide")


@st.cache_data(ttl=60)
def q(sql: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        return con.execute(sql).df()
    finally:
        con.close()


st.title("🚆 TransitLake")
st.caption("A Dagster-orchestrated medallion lakehouse over Chicago multi-modal transit data.")

# --- Headline metrics ------------------------------------------------------
fresh = q("select * from main_marts.mart_pipeline_freshness")
fresh_map = {r["source"]: r for _, r in fresh.iterrows()}
c1, c2, c3, c4 = st.columns(4)
c1.metric("Vehicle position rows", f"{int(fresh_map['vehicle_positions']['row_count']):,}")
c2.metric("Congestion rows", f"{int(fresh_map['congestion']['row_count']):,}")
c3.metric("Weather days", f"{int(fresh_map['weather']['row_count']):,}")
c4.metric("Latest position", str(fresh_map["vehicle_positions"]["latest_ts"]))

tab_otp, tab_congestion, tab_weather, tab_fleet = st.tabs(
    ["On-time performance", "Congestion", "Delay ↔ Weather", "Live fleet"]
)

# --- On-time performance ---------------------------------------------------
with tab_otp:
    st.subheader("Worst on-time performance by route")
    otp = q("""
        select route_id, route_name, mode, total_reports, on_time_rate
        from main_marts.mart_otp_by_route
        where total_reports >= 20
        order by on_time_rate asc
        limit 20
    """)
    if otp.empty:
        st.info("Not enough real-time data yet — the poller is still accumulating.")
    else:
        fig = px.bar(otp, x="on_time_rate", y="route_name", color="mode",
                     orientation="h", hover_data=["route_id", "total_reports"],
                     labels={"on_time_rate": "On-time rate", "route_name": "Route"})
        fig.update_layout(yaxis={"categoryorder": "total descending"}, height=600)
        st.plotly_chart(fig, width='stretch')

    st.subheader("On-time rate by hour of day")
    by_hour = q("select mode, report_hour, on_time_rate from main_marts.mart_otp_by_mode_hour order by report_hour")
    if not by_hour.empty:
        st.plotly_chart(
            px.line(by_hour, x="report_hour", y="on_time_rate", color="mode", markers=True,
                    labels={"report_hour": "Hour", "on_time_rate": "On-time rate"}),
            width='stretch',
        )

# --- Congestion ------------------------------------------------------------
with tab_congestion:
    st.subheader("Top congestion hotspots")
    hot = q("""
        select segment_id, street, from_street, to_street, avg_speed_mph,
               worst_hour, observations, start_lat, start_lon, congestion_rank
        from main_marts.mart_congestion_hotspots
        order by congestion_rank limit 30
    """)
    st.dataframe(hot.drop(columns=["start_lat", "start_lon"]), width='stretch', hide_index=True)
    pts = hot.rename(columns={"start_lat": "lat", "start_lon": "lon"}).dropna(subset=["lat", "lon"])
    if not pts.empty:
        st.map(pts[["lat", "lon"]])

    st.subheader("City-wide average road speed by hour")
    cbh = q("select report_hour, avg_speed_mph from main_marts.mart_congestion_by_hour order by report_hour")
    if not cbh.empty:
        st.plotly_chart(
            px.line(cbh, x="report_hour", y="avg_speed_mph", markers=True,
                    labels={"report_hour": "Hour", "avg_speed_mph": "Avg speed (mph)"}),
            width='stretch',
        )

# --- Delay vs weather ------------------------------------------------------
with tab_weather:
    st.subheader("Daily transit delay rate vs weather")
    dw = q("""
        select report_date, mode, delayed_rate, precip_mm, snow_cm, temp_avg_c
        from main_marts.mart_delay_vs_weather order by report_date
    """)
    dw = dw.dropna(subset=["delayed_rate", "precip_mm", "temp_avg_c"])
    if dw.empty:
        st.info("Delay↔weather needs multiple days of overlapping RT + weather data "
                "(the weather archive lags ~5 days behind today).")
    else:
        st.plotly_chart(
            px.scatter(dw, x="precip_mm", y="delayed_rate", color="mode", size="temp_avg_c",
                       hover_data=["report_date"],
                       labels={"precip_mm": "Precipitation (mm)", "delayed_rate": "Delay rate"}),
            width='stretch',
        )

    st.subheader("Transit delay vs road congestion (by hour-of-day)")
    dc = q("""
        select report_hour, delayed_rate, road_avg_speed_mph
        from main_marts.mart_delay_vs_congestion order by report_hour
    """)
    if not dc.empty:
        st.plotly_chart(
            px.scatter(dc, x="road_avg_speed_mph", y="delayed_rate", hover_data=["report_hour"],
                       labels={"road_avg_speed_mph": "Road avg speed (mph)", "delayed_rate": "Transit delay rate"}),
            width='stretch',
        )

# --- Live fleet ------------------------------------------------------------
with tab_fleet:
    st.subheader("Most recent vehicle positions")
    fleet = q("""
        with latest as (select max(report_ts) m from main_marts.fact_vehicle_position)
        select mode, route_id, lat, lon, is_delayed
        from main_marts.fact_vehicle_position, latest
        where report_ts >= latest.m - interval 5 minute
    """)
    if fleet.empty:
        st.info("No recent positions — start the poller (uv run python -m ingestion.poller).")
    else:
        st.write(f"{len(fleet):,} vehicles in the last 5 minutes")
        st.map(fleet[["lat", "lon"]])
