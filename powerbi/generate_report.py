"""
Netflix Streaming Analytics – Local Report Generator

Generates an interactive HTML dashboard from simulated streaming data.
No Power BI, no Azure connection required.

Usage:
    python generate_report.py                      # HTML dashboard (default)
    python generate_report.py --format html        # interactive HTML
    python generate_report.py --format png         # static PNG images
    python generate_report.py --events 5000        # more simulated events
    python generate_report.py --output ./my_report # custom output path

Opens automatically in your default browser (HTML mode).
"""

import argparse
import json
import os
import random
import sys
import webbrowser
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from data_generator.generator import (
    NetflixEventGenerator,
    CONTENT_CATALOG,
    LOCATIONS,
)

# ── Colour Palette (Netflix-inspired) ───────────────────────────────────────

NETFLIX_RED = "#E50914"
NETFLIX_BLACK = "#141414"
NETFLIX_DARK = "#1A1A2E"
NETFLIX_GREY = "#B3B3B3"
NETFLIX_WHITE = "#FFFFFF"

PALETTE = [
    "#E50914", "#46D369", "#FFB703", "#1DB9D4", "#9B59B6",
    "#E74C3C", "#F39C12", "#2ECC71", "#3498DB", "#E91E63",
]

TEMPLATE_LAYOUT = dict(
    paper_bgcolor=NETFLIX_BLACK,
    plot_bgcolor=NETFLIX_DARK,
    font=dict(color=NETFLIX_WHITE, family="Helvetica Neue, Arial, sans-serif"),
    title_font=dict(size=18, color=NETFLIX_WHITE),
    legend=dict(bgcolor="rgba(0,0,0,0.5)", font=dict(size=11)),
    margin=dict(l=60, r=40, t=60, b=50),
)


# ── Data Simulation ─────────────────────────────────────────────────────────

def simulate_data(num_events: int = 3000) -> pd.DataFrame:
    """Generate a DataFrame of simulated streaming events."""
    gen = NetflixEventGenerator(dry_run=True, num_users=2000, events_per_second=100)
    batch = gen.generate_batch(num_events)

    # Spread events across the last 2 hours for realistic time-series
    base_time = datetime.now(timezone.utc) - timedelta(hours=2)
    records = []
    for i, event in enumerate(batch):
        d = event.to_dict()
        offset = timedelta(seconds=(i / num_events) * 7200)
        d["timestamp"] = (base_time + offset).isoformat()
        records.append(d)

    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["minute_bucket"] = df["timestamp"].dt.floor("1min")
    df["5min_bucket"] = df["timestamp"].dt.floor("5min")
    return df


# ── Chart Builders ───────────────────────────────────────────────────────────

def build_kpi_cards(df: pd.DataFrame) -> go.Figure:
    """Top-level KPI summary cards."""
    total_viewers = df["user_id"].nunique()
    total_events = len(df)
    active_titles = df["content_id"].nunique()
    avg_duration = df.loc[
        df["event_type"].isin(["video_stop", "video_complete"]),
        "duration_seconds"
    ].mean()
    avg_duration = avg_duration / 60 if pd.notna(avg_duration) else 0

    buffer_events = len(df[df["event_type"] == "buffer_event"])
    buffer_rate = (buffer_events / total_events * 1000) if total_events else 0

    completions = len(df[df["event_type"] == "video_complete"])
    starts = len(df[df["event_type"] == "video_start"])
    completion_rate = (completions / starts * 100) if starts else 0

    fig = go.Figure()
    kpis = [
        ("Total Viewers", f"{total_viewers:,}", NETFLIX_RED),
        ("Total Events", f"{total_events:,}", "#46D369"),
        ("Active Titles", f"{active_titles}", "#FFB703"),
        ("Avg Watch (min)", f"{avg_duration:.1f}", "#1DB9D4"),
        ("Buffer/1K Events", f"{buffer_rate:.1f}", "#E74C3C"),
        ("Completion Rate", f"{completion_rate:.1f}%", "#9B59B6"),
    ]
    for i, (label, value, colour) in enumerate(kpis):
        fig.add_trace(go.Indicator(
            mode="number",
            value=float(value.replace(",", "").replace("%", "")),
            number=dict(
                font=dict(size=42, color=colour),
                suffix="%" if "%" in value else "",
            ),
            title=dict(text=label, font=dict(size=14, color=NETFLIX_GREY)),
            domain=dict(
                x=[i / len(kpis) + 0.01, (i + 1) / len(kpis) - 0.01],
                y=[0, 1],
            ),
        ))
    kpi_layout = {**TEMPLATE_LAYOUT, "margin": dict(l=20, r=20, t=30, b=10)}
    fig.update_layout(
        **kpi_layout,
        height=140,
        title=None,
    )
    return fig


def build_viewers_over_time(df: pd.DataFrame) -> go.Figure:
    """Line chart: active viewers per minute over time."""
    viewers = (
        df.groupby("minute_bucket")["user_id"]
        .nunique()
        .reset_index()
        .rename(columns={"user_id": "active_viewers"})
    )
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=viewers["minute_bucket"],
        y=viewers["active_viewers"],
        mode="lines",
        fill="tozeroy",
        line=dict(color=NETFLIX_RED, width=2),
        fillcolor="rgba(229,9,20,0.15)",
        name="Active Viewers",
    ))
    fig.update_layout(
        **TEMPLATE_LAYOUT,
        title="Active Viewers Over Time",
        xaxis_title="Time",
        yaxis_title="Unique Viewers",
        height=350,
    )
    return fig


def build_trending_content(df: pd.DataFrame) -> go.Figure:
    """Horizontal bar chart: top 10 trending content by viewer count."""
    starts = df[df["event_type"] == "video_start"]
    trending = (
        starts.groupby(["content_id", "content_title"])["user_id"]
        .nunique()
        .reset_index()
        .rename(columns={"user_id": "viewers"})
        .sort_values("viewers", ascending=True)
        .tail(10)
    )
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=trending["viewers"],
        y=trending["content_title"],
        orientation="h",
        marker=dict(
            color=trending["viewers"],
            colorscale=[[0, "#1A1A2E"], [0.5, "#E50914"], [1, "#FFB703"]],
        ),
        text=trending["viewers"],
        textposition="outside",
        textfont=dict(color=NETFLIX_WHITE),
    ))
    fig.update_layout(
        **TEMPLATE_LAYOUT,
        title="Top 10 Trending Content",
        xaxis_title="Unique Viewers",
        yaxis_title="",
        height=400,
    )
    return fig


def build_device_distribution(df: pd.DataFrame) -> go.Figure:
    """Donut chart: event distribution by device type."""
    device_counts = df["device_type"].value_counts()
    fig = go.Figure(go.Pie(
        labels=device_counts.index,
        values=device_counts.values,
        hole=0.55,
        marker=dict(colors=PALETTE[:len(device_counts)]),
        textinfo="label+percent",
        textfont=dict(size=12),
    ))
    fig.update_layout(
        **TEMPLATE_LAYOUT,
        title="Device Distribution",
        height=400,
        showlegend=True,
    )
    return fig


def build_geo_distribution(df: pd.DataFrame) -> go.Figure:
    """Choropleth map: viewers by country."""
    geo = (
        df.groupby(df["location"].apply(lambda x: x.get("country", "??")))["user_id"]
        .nunique()
        .reset_index()
        .rename(columns={"location": "country", "user_id": "viewers"})
    )
    fig = go.Figure(go.Choropleth(
        locations=geo["country"],
        z=geo["viewers"],
        locationmode="ISO-3",
        colorscale=[[0, NETFLIX_DARK], [0.5, "#E50914"], [1, "#FFB703"]],
        marker_line_color=NETFLIX_GREY,
        marker_line_width=0.5,
        colorbar=dict(title="Viewers", tickfont=dict(color=NETFLIX_WHITE)),
    ))
    fig.update_layout(
        **TEMPLATE_LAYOUT,
        title="Geographic Distribution of Viewers",
        height=420,
        geo=dict(
            bgcolor=NETFLIX_BLACK,
            lakecolor=NETFLIX_DARK,
            landcolor=NETFLIX_DARK,
            showframe=False,
            projection_type="natural earth",
        ),
    )
    return fig


def build_event_type_breakdown(df: pd.DataFrame) -> go.Figure:
    """Stacked area chart: event types over time."""
    events = (
        df.groupby(["5min_bucket", "event_type"])
        .size()
        .reset_index(name="count")
    )
    fig = go.Figure()
    for i, etype in enumerate(df["event_type"].unique()):
        subset = events[events["event_type"] == etype]
        fig.add_trace(go.Scatter(
            x=subset["5min_bucket"],
            y=subset["count"],
            name=etype,
            mode="lines",
            stackgroup="one",
            line=dict(color=PALETTE[i % len(PALETTE)], width=0.5),
        ))
    fig.update_layout(
        **TEMPLATE_LAYOUT,
        title="Event Types Over Time",
        xaxis_title="Time",
        yaxis_title="Event Count",
        height=350,
    )
    return fig


def build_buffer_analysis(df: pd.DataFrame) -> go.Figure:
    """Box plot: buffer duration distribution by device type."""
    buffers = df[df["event_type"] == "buffer_event"].copy()
    if buffers.empty:
        fig = go.Figure()
        fig.add_annotation(text="No buffer events", showarrow=False,
                           font=dict(size=20, color=NETFLIX_GREY))
        fig.update_layout(**TEMPLATE_LAYOUT, title="Buffer Analysis", height=350)
        return fig

    fig = go.Figure()
    for i, device in enumerate(buffers["device_type"].unique()):
        subset = buffers[buffers["device_type"] == device]
        fig.add_trace(go.Box(
            y=subset["buffer_duration_ms"],
            name=device,
            marker_color=PALETTE[i % len(PALETTE)],
            boxmean=True,
        ))
    fig.update_layout(
        **TEMPLATE_LAYOUT,
        title="Buffer Duration by Device (ms)",
        yaxis_title="Buffer Duration (ms)",
        height=350,
    )
    return fig


def build_watch_duration_histogram(df: pd.DataFrame) -> go.Figure:
    """Histogram: watch duration distribution for completed sessions."""
    completed = df[df["event_type"].isin(["video_stop", "video_complete"])]
    durations = completed["duration_seconds"] / 60  # convert to minutes

    fig = go.Figure(go.Histogram(
        x=durations,
        nbinsx=30,
        marker_color=NETFLIX_RED,
        opacity=0.85,
    ))
    fig.update_layout(
        **TEMPLATE_LAYOUT,
        title="Watch Duration Distribution",
        xaxis_title="Duration (minutes)",
        yaxis_title="Session Count",
        height=350,
    )
    return fig


def build_engagement_heatmap(df: pd.DataFrame) -> go.Figure:
    """Heatmap: events by hour-of-day vs content type."""
    df_copy = df.copy()
    df_copy["hour"] = df_copy["timestamp"].dt.hour

    pivot = (
        df_copy.groupby(["hour", "content_type"])
        .size()
        .reset_index(name="count")
        .pivot(index="content_type", columns="hour", values="count")
        .fillna(0)
    )

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=[f"{h:02d}:00" for h in pivot.columns],
        y=pivot.index,
        colorscale=[[0, NETFLIX_DARK], [0.5, "#E50914"], [1, "#FFB703"]],
        colorbar=dict(title="Events"),
    ))
    fig.update_layout(
        **TEMPLATE_LAYOUT,
        title="Engagement Heatmap (Content Type x Hour)",
        xaxis_title="Hour of Day",
        yaxis_title="Content Type",
        height=300,
    )
    return fig


def build_quality_by_tier(df: pd.DataFrame) -> go.Figure:
    """Grouped bar: average bitrate by subscription tier and device."""
    df_copy = df.copy()
    df_copy["bitrate"] = df_copy["quality_settings"].apply(
        lambda x: x.get("bitrate_kbps", 0) if isinstance(x, dict) else 0
    )
    agg = (
        df_copy.groupby(["subscription_tier", "device_type"])["bitrate"]
        .mean()
        .reset_index()
    )
    fig = px.bar(
        agg,
        x="subscription_tier",
        y="bitrate",
        color="device_type",
        barmode="group",
        color_discrete_sequence=PALETTE,
        labels={"bitrate": "Avg Bitrate (kbps)", "subscription_tier": "Tier"},
    )
    fig.update_layout(
        **TEMPLATE_LAYOUT,
        title="Average Bitrate by Tier & Device",
        height=380,
    )
    return fig


def build_content_completion_funnel(df: pd.DataFrame) -> go.Figure:
    """Funnel: start → pause → complete for top 5 content."""
    top5 = (
        df[df["event_type"] == "video_start"]["content_title"]
        .value_counts()
        .head(5)
        .index.tolist()
    )
    stages = ["video_start", "video_pause", "video_stop", "video_complete"]
    stage_labels = ["Started", "Paused", "Stopped", "Completed"]

    data = []
    for title in top5:
        title_df = df[df["content_title"] == title]
        for stage, label in zip(stages, stage_labels):
            data.append({
                "content": title,
                "stage": label,
                "count": len(title_df[title_df["event_type"] == stage]),
            })

    fig = go.Figure()
    for i, title in enumerate(top5):
        subset = [d for d in data if d["content"] == title]
        fig.add_trace(go.Funnel(
            name=title,
            y=[d["stage"] for d in subset],
            x=[d["count"] for d in subset],
            marker=dict(color=PALETTE[i % len(PALETTE)]),
            textinfo="value+percent initial",
        ))
    fig.update_layout(
        **TEMPLATE_LAYOUT,
        title="Content Engagement Funnel (Top 5)",
        height=400,
    )
    return fig


# ── Report Assembly ──────────────────────────────────────────────────────────

def generate_html_report(df: pd.DataFrame, output_path: Path) -> Path:
    """Assemble all charts into a single interactive HTML dashboard."""
    charts = [
        ("kpi", build_kpi_cards(df)),
        ("viewers_time", build_viewers_over_time(df)),
        ("trending", build_trending_content(df)),
        ("events_time", build_event_type_breakdown(df)),
        ("geo", build_geo_distribution(df)),
        ("devices", build_device_distribution(df)),
        ("buffer", build_buffer_analysis(df)),
        ("duration", build_watch_duration_histogram(df)),
        ("heatmap", build_engagement_heatmap(df)),
        ("quality", build_quality_by_tier(df)),
        ("funnel", build_content_completion_funnel(df)),
    ]

    # Build chart HTML divs
    chart_divs = []
    for name, fig in charts:
        div = fig.to_html(full_html=False, include_plotlyjs=False, div_id=f"chart-{name}")
        chart_divs.append(div)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    event_count = len(df)
    user_count = df["user_id"].nunique()

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Netflix Streaming Analytics Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            background-color: {NETFLIX_BLACK};
            color: {NETFLIX_WHITE};
            font-family: 'Helvetica Neue', Arial, sans-serif;
        }}
        .header {{
            background: linear-gradient(135deg, {NETFLIX_BLACK} 0%, #2C0B0E 100%);
            padding: 24px 40px;
            border-bottom: 3px solid {NETFLIX_RED};
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .header h1 {{
            font-size: 28px;
            color: {NETFLIX_RED};
            letter-spacing: 1px;
        }}
        .header .meta {{
            color: {NETFLIX_GREY};
            font-size: 13px;
            text-align: right;
        }}
        .dashboard {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 24px;
        }}
        .row {{
            display: flex;
            gap: 20px;
            margin-bottom: 20px;
            flex-wrap: wrap;
        }}
        .chart-card {{
            background: {NETFLIX_DARK};
            border-radius: 8px;
            padding: 16px;
            flex: 1;
            min-width: 400px;
            border: 1px solid #2A2A3E;
        }}
        .chart-card.full {{
            min-width: 100%;
        }}
        .section-title {{
            color: {NETFLIX_RED};
            font-size: 20px;
            margin: 30px 0 15px;
            padding-bottom: 8px;
            border-bottom: 1px solid #333;
        }}
        .footer {{
            text-align: center;
            padding: 30px;
            color: {NETFLIX_GREY};
            font-size: 12px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>NETFLIX Streaming Analytics</h1>
        <div class="meta">
            Generated: {timestamp}<br>
            {event_count:,} events | {user_count:,} users
        </div>
    </div>
    <div class="dashboard">
        <!-- KPI Cards -->
        <div class="row">
            <div class="chart-card full">{chart_divs[0]}</div>
        </div>

        <h2 class="section-title">Viewership & Trending</h2>
        <div class="row">
            <div class="chart-card">{chart_divs[1]}</div>
            <div class="chart-card">{chart_divs[2]}</div>
        </div>

        <h2 class="section-title">Event Analysis</h2>
        <div class="row">
            <div class="chart-card">{chart_divs[3]}</div>
            <div class="chart-card">{chart_divs[10]}</div>
        </div>

        <h2 class="section-title">Geographic & Device Insights</h2>
        <div class="row">
            <div class="chart-card">{chart_divs[4]}</div>
            <div class="chart-card">{chart_divs[5]}</div>
        </div>

        <h2 class="section-title">Quality & Performance</h2>
        <div class="row">
            <div class="chart-card">{chart_divs[6]}</div>
            <div class="chart-card">{chart_divs[7]}</div>
        </div>

        <h2 class="section-title">Engagement Deep Dive</h2>
        <div class="row">
            <div class="chart-card">{chart_divs[8]}</div>
            <div class="chart-card">{chart_divs[9]}</div>
        </div>
    </div>
    <div class="footer">
        Netflix Streaming Analytics Pipeline &mdash; Generated with Python + Plotly
    </div>
</body>
</html>"""

    output_file = output_path.with_suffix(".html")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(html)
    return output_file


def generate_png_report(df: pd.DataFrame, output_dir: Path) -> list[Path]:
    """Export each chart as a separate PNG image."""
    output_dir.mkdir(parents=True, exist_ok=True)
    charts = {
        "01_viewers_over_time": build_viewers_over_time(df),
        "02_trending_content": build_trending_content(df),
        "03_event_types": build_event_type_breakdown(df),
        "04_geo_distribution": build_geo_distribution(df),
        "05_device_distribution": build_device_distribution(df),
        "06_buffer_analysis": build_buffer_analysis(df),
        "07_watch_duration": build_watch_duration_histogram(df),
        "08_engagement_heatmap": build_engagement_heatmap(df),
        "09_quality_by_tier": build_quality_by_tier(df),
        "10_engagement_funnel": build_content_completion_funnel(df),
    }
    paths = []
    for name, fig in charts.items():
        path = output_dir / f"{name}.png"
        fig.write_image(str(path), width=1200, height=500, scale=2)
        paths.append(path)
        print(f"  Saved: {path}")
    return paths


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate Netflix streaming analytics reports locally",
    )
    parser.add_argument(
        "--format", choices=["html", "png"], default="html",
        help="Output format (default: html)",
    )
    parser.add_argument(
        "--events", type=int, default=3000,
        help="Number of simulated events (default: 3000)",
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Output path (default: ./reports/dashboard)",
    )
    parser.add_argument(
        "--no-open", action="store_true",
        help="Don't auto-open the report in browser",
    )
    args = parser.parse_args()

    reports_dir = Path(args.output) if args.output else PROJECT_ROOT / "reports" / "dashboard"

    print(f"Simulating {args.events:,} streaming events...")
    df = simulate_data(args.events)
    print(f"Generated {len(df):,} events across {df['user_id'].nunique():,} users\n")

    if args.format == "html":
        print("Building interactive HTML dashboard...")
        output_file = generate_html_report(df, reports_dir)
        print(f"\nDashboard saved to: {output_file}")
        if not args.no_open:
            webbrowser.open(f"file://{output_file.resolve()}")
            print("Opened in your default browser.")
    else:
        print("Exporting PNG charts...")
        png_dir = reports_dir if reports_dir.suffix == "" else reports_dir.parent / "png"
        paths = generate_png_report(df, png_dir)
        print(f"\n{len(paths)} charts exported to: {png_dir}")


if __name__ == "__main__":
    main()
