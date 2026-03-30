"""Incident scatter map with anomaly hotspot overlay."""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from anomaly_detection.models import Anomaly, AnomalyType

AGENCY_COLORS = {"POLICE": "#1f77b4", "FIRE": "#d62728", "EMS": "#2ca02c"}


def render(incidents_df: pd.DataFrame, anomalies: list[Anomaly]) -> None:
    if incidents_df.empty:
        st.info("No incident data available for map.")
        return

    fig = px.scatter_mapbox(
        incidents_df,
        lat="latitude",
        lon="longitude",
        color="agency",
        color_discrete_map=AGENCY_COLORS,
        hover_data=["incident_type", "district", "priority", "reported_at"],
        opacity=0.6,
        zoom=12,
        center={"lat": 37.773, "lon": -122.413},
        mapbox_style="carto-positron",
        title="Incident Map — Last 7 Days",
        height=550,
    )

    # Overlay geographic hotspot anomalies as translucent red circles
    hotspots = [a for a in anomalies if a.anomaly_type == AnomalyType.GEOGRAPHIC_HOTSPOT
                and a.latitude is not None and a.longitude is not None]

    for hs in hotspots:
        fig.add_trace(
            go.Scattermapbox(
                lat=[hs.latitude],
                lon=[hs.longitude],
                mode="markers",
                marker=dict(size=30, color="red", opacity=0.35),
                name=f"Hotspot (z={hs.z_score:.1f})",
                hovertext=hs.description,
            )
        )

    fig.update_layout(
        paper_bgcolor="#0e1117",
        font_color="#fafafa",
        legend_title_text="Agency",
        margin={"r": 0, "t": 40, "l": 0, "b": 0},
    )
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    col1.metric("Incidents on Map", f"{len(incidents_df):,}")
    col2.metric("Hotspot Anomalies", len(hotspots))
