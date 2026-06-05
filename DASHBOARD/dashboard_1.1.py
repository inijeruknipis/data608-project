import streamlit as st
import pandas as pd
import boto3
import plotly.graph_objects as go
from datetime import datetime, timezone
from io import BytesIO

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Pipeline Monitor",
    page_icon="🔧",
    layout="wide",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@300;400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
}
.block-container { padding-top: 1.5rem; padding-bottom: 2rem; }
h1, h2, h3 { font-family: 'IBM Plex Mono', monospace !important; }

.section-header {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 18px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #888;
    margin-bottom: 12px;
    padding-bottom: 6px;
    border-bottom: 1px solid #e5e5e5;
}

.pipe-card { border-radius: 10px; padding: 14px 16px; border: 1px solid; height: 100%; }
.pipe-card-normal   { background: #f0f8e8; border-color: #7cb94e; }
.pipe-card-anomaly  { background: #fff8ed; border-color: #e6a817; }
.pipe-card-critical { background: #fff0f0; border-color: #e05252; }
.pipe-card-nodata   { background: #f5f5f5; border-color: #cccccc; }

.pipe-name { font-family: 'IBM Plex Mono', monospace; font-size: 25px; font-weight: 500; margin-bottom: 2px; }
.pipe-name-normal   { color: #2d6a0a; }
.pipe-name-anomaly  { color: #7a4a00; }
.pipe-name-critical { color: #8b1a1a; }
.pipe-name-nodata   { color: #999999; }

.pipe-status { font-size: 18px; font-weight: 500; letter-spacing: 0.05em; text-transform: uppercase; margin-bottom: 10px; }
.pipe-status-normal   { color: #4a9a1a; }
.pipe-status-anomaly  { color: #c47f00; }
.pipe-status-critical { color: #c03030; }
.pipe-status-nodata   { color: #aaaaaa; }

.pipe-reading { font-family: 'IBM Plex Mono', monospace; font-size: 18px; color: #555; line-height: 1.9; }
.pipe-section-label { font-size: 20px; color: #999; letter-spacing: 0.06em; text-transform: uppercase; margin-bottom: 6px; }

div[data-testid="stMetricValue"] {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 28px;
}
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────
S3_BUCKET      = 'data608-project-sensordata'
RESULTS_PREFIX = 'processed_data'
PIPE_IDS       = ['P-1', 'P-2', 'P-3', 'P-4', 'P-5']
REFRESH_SEC    = 2
WINDOW_MINUTES = 1

PIPE_COLORS = ['#378ADD', '#E6A817', '#4a9a1a', '#E24B4A', '#7F77DD']

# ── S3 data loader ────────────────────────────────────────────────────────────
@st.cache_data(ttl=REFRESH_SEC)
def load_all_data() -> pd.DataFrame:
    """
    Only list S3 file metadata for today's partitions, then download
    only files modified in the last WINDOW_MINUTES. Filter records
    by processed_timestamp as a secondary check.
    """
    s3 = boto3.client('s3', region_name='us-east-1')
    frames = []
    now = datetime.now(timezone.utc)
    cutoff = now - pd.Timedelta(minutes=WINDOW_MINUTES)

    for pipe in PIPE_IDS:
        prefix = (
            f"{RESULTS_PREFIX}/pipe_id={pipe}/"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        )
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get('Contents', []):
                if not obj['Key'].endswith('.parquet'):
                    continue
                # Skip files older than the window
                if obj['LastModified'] < cutoff:
                    continue
                try:
                    buf = BytesIO()
                    s3.download_fileobj(S3_BUCKET, obj['Key'], buf)
                    buf.seek(0)
                    frames.append(pd.read_parquet(buf))
                except Exception as e:
                    st.warning(f"Could not read {obj['Key']}: {e}")

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df['datetime'] = pd.to_datetime(df['datetime'], format='ISO8601')
    df['processed_timestamp'] = pd.to_datetime(df['processed_timestamp'], format='ISO8601')

    # Secondary filter on actual record timestamp
    df = df[df['processed_timestamp'] >= cutoff]
    df = df.sort_values('processed_timestamp', ascending=False)
    return df


def get_latest_per_pipe(df: pd.DataFrame) -> dict:
    """Return the most recent record for each pipe."""
    latest = {}
    for pipe in PIPE_IDS:
        pipe_df = df[df['pipe_id'] == pipe]
        if not pipe_df.empty:
            latest[pipe] = pipe_df.iloc[0].to_dict()
    return latest


# ── Header (static — never re-renders) ───────────────────────────────────────
col_title, col_refresh = st.columns([4, 1])
with col_title:
    st.markdown("## Pipeline Monitor: DATA608 - Group 1 Solution")
    st.markdown("#### Rio Sibuea - Alejandro Alvarado - Ifeanyi Njoku")
    st.caption(
        f"Showing last {WINDOW_MINUTES} min · "
        f"s3://{S3_BUCKET}/{RESULTS_PREFIX}/ · "
        f"auto-refreshes every {REFRESH_SEC}s"
    )
with col_refresh:
    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
    if st.button("⟳ Refresh now"):
        st.cache_data.clear()
        st.rerun()

st.markdown("---")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1 — Pipeline status cards
# ─────────────────────────────────────────────────────────────────────────────
@st.fragment(run_every=REFRESH_SEC)
def show_status_cards():
    st.markdown('<div class="section-header">01 — Pipeline status | Legend: P = Pressure, T = Temperature, Flow = Flow Rate</div>', unsafe_allow_html=True)

    # Initialise latch store
    if 'latched' not in st.session_state:
        st.session_state.latched = {}

    df = load_all_data()
    if df.empty:
        st.warning("No data found for the last minute. Check that Lambda is writing to S3.")
        return

    latest_per_pipe = get_latest_per_pipe(df)
    cols = st.columns(5)

    for i, pipe in enumerate(PIPE_IDS):
        with cols[i]:
            rec    = latest_per_pipe.get(pipe)
            status = rec.get('status', 'normal') if rec else None

            # Latch anomaly/critical — once triggered, stays until cleared
            if status in ('anomaly', 'critical'):
                section = rec.get('section', '—')
                ts      = rec.get('processed_timestamp', '')
                if hasattr(ts, 'strftime'):
                    ts = ts.strftime('%Y-%m-%d %H:%M:%S')
                # Only update latch if not already latched
                if pipe not in st.session_state.latched:
                    st.session_state.latched[pipe] = {
                        'status':  status,
                        'section': section,
                        'ts':      ts,
                        'pressure': rec.get('pressure_MPa', '—'),    # ← add these
                        'temp':     rec.get('temperature_C', '—'),
                        'flow':     rec.get('flow_rate_percent', '—'),
                    }

            # Decide what to display
            if pipe in st.session_state.latched:
                latch   = st.session_state.latched[pipe]
                display_status  = latch['status']
                display_section = latch['section']
                display_ts      = latch['ts']

                st.markdown(
f"""
<div class="pipe-card pipe-card-{display_status}">
<div class="pipe-name pipe-name-{display_status}">{pipe}</div>
<div class="pipe-status pipe-status-{display_status}">{display_status} detected</div>
<div class="pipe-section-label">Section {display_section}</div>
<div class="pipe-reading">
First detected:<br>
{display_ts}<br>
P: {latch['pressure']} MPa<br>
T: {latch['temp']} °C<br>
Flow: {latch['flow']} %
</div>
</div>
""", unsafe_allow_html=True)

                # Clear button per pipe
                if st.button(f"✓ Clear {pipe}", key=f"clear_{pipe}"):
                    del st.session_state.latched[pipe]
                    st.rerun()

            elif rec is None:
                st.markdown(
f"""
<div class="pipe-card pipe-card-nodata">
<div class="pipe-name pipe-name-nodata">{pipe}</div>
<div class="pipe-status pipe-status-nodata">No data</div>
</div>
""", unsafe_allow_html=True)

            else:
                readings = (
f"""<div class="pipe-reading">
P: {rec.get('pressure_MPa', '—')} MPa<br>
T: {rec.get('temperature_C', '—')} °C<br>
Flow: {rec.get('flow_rate_percent', '—')} %
</div>"""
                )
                st.markdown(
f"""
<div class="pipe-card pipe-card-{status}">
<div class="pipe-name pipe-name-{status}">{pipe}</div>
<div class="pipe-status pipe-status-{status}">{status}</div>
<div class="pipe-section-label">Section {rec.get('section', '—')}</div>
{readings}
</div>
""", unsafe_allow_html=True)

    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
    st.caption(f"Last updated: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2 — Latest 20 readings table
# ─────────────────────────────────────────────────────────────────────────────
@st.fragment(run_every=REFRESH_SEC)
def show_latest_readings():
    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">02 — Latest 20 readings</div>', unsafe_allow_html=True)

    df = load_all_data()
    if df.empty:
        st.info("No readings to display yet.")
        return

    latest20 = df.head(20).copy()

    display_cols = ['processed_timestamp', 'pipe_id', 'section',
                    'pressure_MPa', 'temperature_C', 'flow_rate_percent', 'status']
    available = [c for c in display_cols if c in latest20.columns]
    latest20 = latest20[available].copy()

    if 'processed_timestamp' in latest20.columns:
        latest20['processed_timestamp'] = latest20['processed_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

    latest20.columns = [c.replace('_', ' ').title() for c in latest20.columns]
    latest20 = latest20.rename(columns={
        'Processed Timestamp': 'Datetime',
        'Pipe Id': 'Pipe',
        'Pressure Mpa': 'Pressure (MPa)',
        'Temperature C': 'Temp (°C)',
        'Flow Rate Percent': 'Flow (%)',
    })

    def color_row(row):
        status = row.get('Status', 'normal')
        if status == 'critical':
            return ['background-color: #fff0f0; color: #8b1a1a'] * len(row)
        elif status == 'anomaly':
            return ['background-color: #fff8ed; color: #7a4a00'] * len(row)
        return [''] * len(row)

    styled = latest20.style.apply(color_row, axis=1)
    st.dataframe(styled, use_container_width=True, hide_index=True, height=420)


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3 — Time series charts
# ─────────────────────────────────────────────────────────────────────────────
@st.fragment(run_every=REFRESH_SEC)
def show_charts():
    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">03 — Sensor readings over time</div>', unsafe_allow_html=True)

    df = load_all_data()
    if df.empty:
        st.info("No data available for charts yet.")
        return

    selected_pipes = st.multiselect(
        "Filter by pipe",
        options=PIPE_IDS,
        default=PIPE_IDS,
        key="pipe_filter"
    )

    chart_df = df[df['pipe_id'].isin(selected_pipes)].copy()
    chart_df = chart_df.sort_values('processed_timestamp', ascending=True)
    chart_df = chart_df.groupby('pipe_id').tail(50)

    def make_time_series(metric_col: str, y_label: str, title: str) -> go.Figure:
        fig = go.Figure()
        for pipe in selected_pipes:
            pipe_data = chart_df[chart_df['pipe_id'] == pipe]
            if pipe_data.empty or metric_col not in pipe_data.columns:
                continue
            color = PIPE_COLORS[PIPE_IDS.index(pipe) % len(PIPE_COLORS)]
            fig.add_trace(go.Scatter(
                x=pipe_data['processed_timestamp'],
                y=pipe_data[metric_col],
                mode='lines+markers',
                name=pipe,
                line=dict(color=color, width=1.5),
                marker=dict(size=3, color=color),
            ))
        fig.update_layout(
            title=dict(text=title, font=dict(size=13, family='IBM Plex Mono'), x=0),
            xaxis=dict(showgrid=True, gridcolor='#f0f0f0', tickfont=dict(size=10)),
            yaxis=dict(title=y_label, showgrid=True, gridcolor='#f0f0f0', tickfont=dict(size=10)),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1, font=dict(size=10)),
            margin=dict(l=0, r=0, t=40, b=0),
            height=260,
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(family='IBM Plex Sans'),
        )
        return fig

    c1, c2 = st.columns(2)
    with c1:
        if 'pressure_MPa' in chart_df.columns:
            st.plotly_chart(
                make_time_series('pressure_MPa', 'MPa', 'Pressure (MPa)'),
                use_container_width=True,
                config={'displayModeBar': False}
            )
    with c2:
        if 'temperature_C' in chart_df.columns:
            st.plotly_chart(
                make_time_series('temperature_C', '°C', 'Temperature (°C)'),
                use_container_width=True,
                config={'displayModeBar': False}
            )

    if 'flow_rate_percent' in chart_df.columns:
        st.plotly_chart(
            make_time_series('flow_rate_percent', '%', 'Flow rate (%)'),
            use_container_width=True,
            config={'displayModeBar': False}
        )


# ── Render all sections ───────────────────────────────────────────────────────
show_status_cards()
show_latest_readings()
show_charts()

st.markdown("---")
st.caption("Pipeline Monitoring Dashboard · DATA608 Project")
