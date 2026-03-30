"""PeregrineOps — Public Safety Executive Intelligence Dashboard."""

import sys
from datetime import datetime
from pathlib import Path

import streamlit as st

# Ensure project root is on the path so anomaly_detection / briefing_engine import cleanly
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dashboard.data_loader import (
    load_anomalies,
    load_daily_volume,
    load_incident_map_data,
    load_kpi_snapshot,
    load_response_time_trend,
)
from dashboard.components import (
    anomaly_timeline,
    briefing_panel,
    hotspot_map,
    kpi_cards,
)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="PeregrineOps",
    page_icon="🦅",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

col_title, col_ts, col_refresh = st.columns([4, 3, 1])
with col_title:
    st.markdown("## 🦅 PeregrineOps — Public Safety Intelligence")
with col_ts:
    st.markdown(
        f"<p style='color:#888; padding-top:14px;'>Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>",
        unsafe_allow_html=True,
    )
with col_refresh:
    if st.button("↺ Refresh"):
        st.cache_data.clear()
        st.session_state.pop("briefing_text", None)
        st.rerun()

st.divider()

# ---------------------------------------------------------------------------
# Load data (cached)
# ---------------------------------------------------------------------------

with st.spinner("Loading operational data…"):
    kpi = load_kpi_snapshot()
    anomalies = load_anomalies()

# ---------------------------------------------------------------------------
# KPI row (always visible)
# ---------------------------------------------------------------------------

kpi_cards.render(kpi)
st.divider()

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_overview, tab_anomalies, tab_map, tab_briefing = st.tabs(
    ["Overview", "Anomalies", "Map", "AI Briefing"]
)

# ---- Overview ----
with tab_overview:
    import plotly.express as px

    st.subheader("Daily Incident Volume")
    vol_df = load_daily_volume()
    if not vol_df.empty:
        fig_vol = px.line(
            vol_df,
            x="report_date",
            y="incident_count",
            color="agency",
            title="Daily Incidents by Agency",
            labels={"incident_count": "Incidents", "report_date": "Date"},
            height=350,
        )
        fig_vol.update_layout(plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="#fafafa")
        st.plotly_chart(fig_vol, use_container_width=True)

    st.subheader("Response Time Trend")
    rt_df = load_response_time_trend()
    if not rt_df.empty:
        fig_rt = px.line(
            rt_df,
            x="report_date",
            y="avg_response_time_s",
            color="agency",
            title="Average Response Time by Agency (seconds)",
            labels={"avg_response_time_s": "Avg RT (s)", "report_date": "Date"},
            height=350,
        )
        fig_rt.update_layout(plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="#fafafa")
        fig_rt.add_hline(y=900, line_dash="dot", line_color="orange",
                         annotation_text="Gap threshold (900s)")
        st.plotly_chart(fig_rt, use_container_width=True)

    st.subheader("Agency Breakdown (24h)")
    breakdown = kpi.get("agency_breakdown", {})
    if breakdown:
        import pandas as pd
        bd_df = pd.DataFrame(
            {"Agency": list(breakdown.keys()), "Incidents": list(breakdown.values())}
        )
        fig_bd = px.bar(bd_df, x="Agency", y="Incidents", color="Agency",
                        title="Incidents by Agency — Last 24h", height=300)
        fig_bd.update_layout(plot_bgcolor="#0e1117", paper_bgcolor="#0e1117", font_color="#fafafa")
        st.plotly_chart(fig_bd, use_container_width=True)

# ---- Anomalies ----
with tab_anomalies:
    anomaly_timeline.render(anomalies)

# ---- Map ----
with tab_map:
    map_df = load_incident_map_data()
    hotspot_map.render(map_df, anomalies)

# ---- AI Briefing ----
with tab_briefing:
    briefing_panel.render(kpi, anomalies)
