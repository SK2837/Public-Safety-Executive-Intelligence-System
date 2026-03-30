"""Anomaly timeline scatter chart + detail table."""

import pandas as pd
import plotly.express as px
import streamlit as st

from anomaly_detection.models import Anomaly

SEVERITY_COLORS = {
    "CRITICAL": "#d62728",
    "HIGH": "#ff7f0e",
    "MEDIUM": "#ffdd57",
    "LOW": "#2ca02c",
}


def render(anomalies: list[Anomaly]) -> None:
    if not anomalies:
        st.info("No anomalies detected in the current window.")
        return

    rows = [
        {
            "Detected At": a.detected_at,
            "Type": a.anomaly_type.value,
            "Severity": a.severity.value,
            "Z-Score": a.z_score,
            "Agency": a.agency or "—",
            "District": a.district or "—",
            "Description": a.description,
        }
        for a in anomalies
    ]
    df = pd.DataFrame(rows)

    fig = px.scatter(
        df,
        x="Detected At",
        y="Z-Score",
        color="Severity",
        symbol="Type",
        hover_data=["Agency", "District", "Description"],
        color_discrete_map=SEVERITY_COLORS,
        title="Anomaly Timeline — Z-Score by Detection Time",
        height=400,
    )
    fig.update_layout(
        plot_bgcolor="#0e1117",
        paper_bgcolor="#0e1117",
        font_color="#fafafa",
        legend_title_text="Severity / Type",
    )
    fig.add_hline(y=2.5, line_dash="dot", line_color="gray",
                  annotation_text="Threshold (2.5)")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Anomaly Detail")
    st.dataframe(
        df[["Detected At", "Severity", "Type", "Agency", "District", "Z-Score", "Description"]],
        use_container_width=True,
        hide_index=True,
    )
