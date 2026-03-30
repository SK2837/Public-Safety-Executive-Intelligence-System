"""KPI card row for the dashboard overview."""

import streamlit as st


def render(kpi: dict) -> None:
    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric(
        label="Incidents (24h)",
        value=f"{kpi.get('total_incidents_24h', 0):,}",
        delta=f"7d total: {kpi.get('total_incidents_7d', 0):,}",
    )
    c2.metric(
        label="Avg Response Time",
        value=f"{kpi.get('avg_response_time_24h_s', 0):.0f}s",
    )
    c3.metric(
        label="P90 Response Time",
        value=f"{kpi.get('p90_response_time_24h_s', 0):.0f}s",
    )

    active = kpi.get("active_anomalies", 0)
    c4.metric(label="Active Anomalies", value=active)

    critical = kpi.get("critical_anomalies", 0)
    c5.metric(label="Critical Anomalies", value=critical)
    if critical > 0:
        c5.markdown(
            "<p style='color:red; font-size:12px; margin-top:-12px;'>⚠ Requires attention</p>",
            unsafe_allow_html=True,
        )
