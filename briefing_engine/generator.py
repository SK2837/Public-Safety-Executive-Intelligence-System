"""BriefingGenerator: calls OpenAI GPT-4o to produce executive situation briefings."""

import os
import logging
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser

from anomaly_detection.models import Anomaly, AnomalySeverity
from .prompts import briefing_prompt

load_dotenv()
log = logging.getLogger(__name__)

SEVERITY_EMOJI = {
    AnomalySeverity.CRITICAL: "[CRITICAL]",
    AnomalySeverity.HIGH: "[HIGH]",
    AnomalySeverity.MEDIUM: "[MEDIUM]",
    AnomalySeverity.LOW: "[LOW]",
}

FALLBACK_BRIEFING = (
    "Operational data is currently being processed. "
    "KPI metrics and anomaly signals are available in the Overview and Anomalies tabs. "
    "Please regenerate the briefing once the AI service is available."
)


class BriefingGenerator:
    def __init__(self, api_key: Optional[str] = None) -> None:
        key = api_key or os.getenv("OPENAI_API_KEY")
        if not key:
            raise ValueError("OPENAI_API_KEY not set in environment or .env")

        self.llm = ChatOpenAI(
            model="gpt-4o",
            api_key=key,
            max_tokens=1024,
        )
        self.chain = briefing_prompt | self.llm | StrOutputParser()

    # ------------------------------------------------------------------
    # Formatters
    # ------------------------------------------------------------------

    @staticmethod
    def format_kpi_snapshot(kpi: dict) -> str:
        agency_lines = "\n".join(
            f"  • {agency}: {count} incidents"
            for agency, count in kpi.get("agency_breakdown", {}).items()
        )
        return (
            f"Incidents last 24h:   {kpi.get('total_incidents_24h', 0)}\n"
            f"Incidents last 7d:    {kpi.get('total_incidents_7d', 0)}\n"
            f"Avg response time:    {kpi.get('avg_response_time_24h_s', 0):.0f}s\n"
            f"P90 response time:    {kpi.get('p90_response_time_24h_s', 0):.0f}s\n"
            f"Active anomalies:     {kpi.get('active_anomalies', 0)}\n"
            f"Critical anomalies:   {kpi.get('critical_anomalies', 0)}\n"
            f"Busiest district:     {kpi.get('busiest_district', 'N/A')}\n"
            f"Agency breakdown:\n{agency_lines}"
        )

    @staticmethod
    def format_anomaly_summary(anomalies: list[Anomaly]) -> str:
        top = anomalies[:8]
        if not top:
            return "No significant anomalies detected in the current window."

        lines = []
        for i, a in enumerate(top, 1):
            tag = SEVERITY_EMOJI.get(a.severity, "")
            z_str = f", z={a.z_score:.2f}" if a.z_score is not None else ""
            lines.append(f"{i}. {tag} {a.description}{z_str}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Generate
    # ------------------------------------------------------------------

    def generate(self, kpi: dict, anomalies: list[Anomaly]) -> str:
        kpi_text = self.format_kpi_snapshot(kpi)
        anomaly_text = self.format_anomaly_summary(anomalies)
        current_dt = datetime.now().strftime("%Y-%m-%d %H:%M UTC")

        try:
            result = self.chain.invoke({
                "current_datetime": current_dt,
                "kpi_snapshot": kpi_text,
                "anomaly_summary": anomaly_text,
            })
            return result.strip()
        except Exception as exc:
            log.error("Briefing generation failed: %s", exc)
            return FALLBACK_BRIEFING
