import os
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, Any
import logging

from modules.data import load_and_process_data, calculate_project_progress, load_sessions_data
from modules.ui import load_css
from config import Config

# Logging config
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
SIN_PROYECTO = "Sin Proyecto"
SIN_ASIGNAR = "Sin Asignar"
ESTADO_COMPLETADO = "Completado"
ESTADO_PENDIENTE = "Pendiente"
MESES_TENDENCIA = 3
CACHE_TTL = 300
WEEKLY_TARGET = 40.0

# Unified date formats
DATE_DISPLAY_FORMAT = '%d/%m/%Y'
DATETIME_DISPLAY_FORMAT = '%d/%m/%Y %H:%M'
EXPORT_FILENAME_TIMESTAMP_FORMAT = '%Y%m%d_%H%M'

# Fixed responsable cohort for hour tracking - matches Monday.com exactly
# Fixed cohort loaded from env var  HOURS_COHORT  (comma-separated names)
# Example: HOURS_COHORT="Alice Smith,Bob Jones,Carol White"
_cohort_env = os.environ.get("HOURS_COHORT", "")
FIXED_COHORT = [n.strip() for n in _cohort_env.split(",") if n.strip()]

COMPLETION_STATUSES = frozenset([
    'completado', 'done', 'finalizado', 'closed', 'terminado', 'listo'
])

DIAS_ORDEN = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
DIAS_ES = {
    'Monday': 'Lun', 'Tuesday': 'Mar', 'Wednesday': 'Mie',
    'Thursday': 'Jue', 'Friday': 'Vie', 'Saturday': 'Sab', 'Sunday': 'Dom'
}

st.set_page_config(
    page_title="Analytics",
    page_icon="[CHART]",
    layout="wide",
    initial_sidebar_state="expanded"
)

load_css()


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def safe_datetime_convert(series: pd.Series, col_name: str = 'column') -> pd.Series:
    """Safely convert series to datetime."""
    try:
        return pd.to_datetime(series, errors='coerce')
    except Exception as e:
        logger.warning("Failed to convert %s to datetime: %s", col_name, str(e))
        return pd.Series([pd.NaT] * len(series))


def get_current_timestamp() -> pd.Timestamp:
    """Get current timestamp normalized to midnight."""
    return pd.Timestamp.now().normalize()


def format_date(value: Any) -> str:
    """Format a date-like value using the unified display format."""
    ts = pd.to_datetime(value, errors='coerce')
    return ts.strftime(DATE_DISPLAY_FORMAT) if pd.notna(ts) else ''


def format_datetime(value: Any) -> str:
    """Format a datetime-like value using the unified display format."""
    ts = pd.to_datetime(value, errors='coerce')
    return ts.strftime(DATETIME_DISPLAY_FORMAT) if pd.notna(ts) else ''


def format_date_range(start: Any, end: Any) -> str:
    """Format a date range using the unified display format."""
    return f"{format_date(start)} - {format_date(end)}"


# =============================================================================
# STATISTICS CALCULATIONS
# =============================================================================

@st.cache_data(ttl=CACHE_TTL)
def calculate_all_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculate all statistics for the dashboard."""
    now = get_current_timestamp()
    df = df.copy()
    df['Fecha Fin'] = safe_datetime_convert(df['Fecha Fin'], 'Fecha Fin')
    
    # Time periods
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)
    two_months_ago = now - timedelta(days=60)
    
    # Base masks
    completed_mask = df['Is_Done'] == True
    pending_mask = df['Is_Done'] == False
    fecha_fin = df['Fecha Fin']
    
    # Velocity metrics
    velocity_week = (completed_mask & (fecha_fin >= week_ago)).sum()
    velocity_month = (completed_mask & (fecha_fin >= month_ago)).sum()
    velocity_prev_month = (
        completed_mask & 
        (fecha_fin >= two_months_ago) & 
        (fecha_fin < month_ago)
    ).sum()
    
    avg_daily = velocity_month / 30 if velocity_month > 0 else 0
    delta_month = (
        (velocity_month - velocity_prev_month) / velocity_prev_month * 100
        if velocity_prev_month > 0 else 0
    )
    
    # Extended statistics
    total_tasks = len(df)
    completed_tasks = completed_mask.sum()
    pending_tasks = pending_mask.sum()
    completion_rate = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0
    
    # Avg days per task
    df_completed_with_dates = df[completed_mask & df['Fecha Fin'].notna()].copy()
    avg_days_per_task = 0
    if 'Fecha Inicio' in df.columns and not df_completed_with_dates.empty:
        df_completed_with_dates['Fecha Inicio'] = safe_datetime_convert(
            df_completed_with_dates['Fecha Inicio'], 'Fecha Inicio'
        )
        valid_dates = df_completed_with_dates.dropna(subset=['Fecha Inicio', 'Fecha Fin'])
        if not valid_dates.empty:
            durations = (valid_dates['Fecha Fin'] - valid_dates['Fecha Inicio']).dt.days
            avg_days_per_task = durations.mean()
    
    # Unassigned tasks
    unassigned_mask = (
        df['Responsable'].isna() | 
        (df['Responsable'].str.strip() == '') |
        (df['Responsable'] == SIN_ASIGNAR)
    )
    unassigned_tasks = (unassigned_mask & pending_mask).sum()
    
    # Most productive responsible
    df_month_completed = df[completed_mask & (fecha_fin >= month_ago)]
    top_responsible = "N/A"
    top_resp_count = 0
    if not df_month_completed.empty:
        resp_counts = df_month_completed['Responsable'].value_counts()
        if not resp_counts.empty:
            top_responsible = resp_counts.index[0]
            top_resp_count = resp_counts.iloc[0]
    
    # Project with most progress
    top_project = "N/A"
    top_project_pct = 0
    project_groups = df.groupby('Proyecto Vinculado').agg(
        total=('Is_Done', 'count'),
        completed=('Is_Done', 'sum')
    ).reset_index()
    project_groups = project_groups[project_groups['total'] >= 3]
    if not project_groups.empty:
        project_groups['pct'] = project_groups['completed'] / project_groups['total'] * 100
        best_project = project_groups.loc[project_groups['pct'].idxmax()]
        top_project = best_project['Proyecto Vinculado']
        top_project_pct = best_project['pct']
    
    # Delayed tasks
    delayed_mask = df.get('Atrasado', pd.Series([False] * len(df)))
    delayed_count = delayed_mask.sum() if isinstance(delayed_mask, pd.Series) else 0
    
    return {
        'velocity_week': int(velocity_week),
        'velocity_month': int(velocity_month),
        'velocity_prev_month': int(velocity_prev_month),
        'daily_avg': avg_daily,
        'projected': avg_daily * 30,
        'delta_month': delta_month,
        'total_tasks': int(total_tasks),
        'completed_tasks': int(completed_tasks),
        'pending_tasks': int(pending_tasks),
        'completion_rate': completion_rate,
        'avg_days_per_task': avg_days_per_task,
        'unassigned_tasks': int(unassigned_tasks),
        'top_responsible': top_responsible,
        'top_resp_count': int(top_resp_count),
        'top_project': top_project,
        'top_project_pct': top_project_pct,
        'delayed_count': int(delayed_count),
    }


# =============================================================================
# CHART 1: WEEKLY TREND (Bar only) - CORRECTED
# =============================================================================

@st.cache_data(ttl=CACHE_TTL)
def create_weekly_trend_chart(df_sessions: pd.DataFrame) -> Optional[go.Figure]:
    """
    Creates a weekly bar chart counting time tracking sessions.
    Window: Last 4 weeks.
    Source: df_sessions (Time Tracking).
    """
    if df_sessions.empty or 'started_at' not in df_sessions.columns:
        return None

    weeks = _build_weeks(4)
    cutoff_start = weeks[-1][1]
    cutoff_end = weeks[0][2]
    week_colors = _get_week_colors(weeks)

    df_work = df_sessions.copy()
    df_work['started_at'] = pd.to_datetime(df_work['started_at'], errors='coerce')
    df_work = df_work.dropna(subset=['started_at'])

    if df_work.empty:
        return None

    df_filtered = df_work[
        (df_work['started_at'] >= cutoff_start) &
        (df_work['started_at'] < cutoff_end)
    ].copy()

    if df_filtered.empty:
        return None

    df_filtered['_week'] = df_filtered['started_at'].apply(lambda d: _assign_week(d, weeks))
    df_filtered = df_filtered[df_filtered['_week'].notna()]

    weekly = pd.DataFrame({
        'Label': [label for label, _, _ in reversed(weeks)],
        'Sessions': [
            int((df_filtered['_week'] == label).sum())
            for label, _, _ in reversed(weeks)
        ]
    })

    if weekly['Sessions'].sum() == 0:
        return None

    weekly['Color'] = weekly['Label'].map(week_colors)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=weekly['Label'],
        y=weekly['Sessions'],
        marker_color=weekly['Color'],
        text=weekly['Sessions'],
        textposition='outside',
        hovertext=weekly.apply(lambda r: f"Week: {r['Label'].replace(chr(10), ' ')}<br>Work Sessions: {r['Sessions']}", axis=1),
        hoverinfo='text'
    ))

    fig.update_layout(
        title="Weekly Work Activity (Last 4 Weeks)",
        xaxis_title="",
        yaxis_title="Number of Sessions",
        height=350,
        plot_bgcolor='white',
        template='plotly_white',
        xaxis=dict(tickangle=-20),
        margin=dict(t=40, b=60, l=40, r=40)
    )

    return fig


# =============================================================================
# CHART 2: PROGRESS DONUT
# =============================================================================

@st.cache_data(ttl=CACHE_TTL)
def create_progress_donut(df: pd.DataFrame) -> Optional[go.Figure]:
    """Create donut chart showing completion status."""
    if df.empty:
        return None
    
    completed = df['Is_Done'].sum()
    delayed = df.get('Atrasado', pd.Series([False] * len(df))).sum()
    pending_ok = len(df) - completed - delayed
    
    if pending_ok < 0:
        pending_ok = 0
    
    labels = ['Completed', 'Pending', 'Delayed']
    values = [completed, pending_ok, delayed]
    colors = [Config.COLORS['success'], Config.COLORS['primary'], Config.COLORS['danger']]
    
    fig = go.Figure(data=[go.Pie(
        labels=labels,
        values=values,
        hole=0.6,
        marker_colors=colors,
        textinfo='percent+value',
        textposition='outside',
        hovertemplate='<b>%{label}</b><br>%{value} tasks<br>%{percent}<extra></extra>'
    )])
    
    total = sum(values)
    pct_done = (completed / total * 100) if total > 0 else 0
    
    fig.update_layout(
        title="Overall Status",
        height=350,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.15, xanchor="center", x=0.5),
        margin=dict(t=40, b=60),
        annotations=[dict(
            text=f'{pct_done:.0f}%<br>Progress',
            x=0.5, y=0.5,
            font_size=18,
            showarrow=False
        )]
    )
    
    return fig


# =============================================================================
# CHART 3: UNIFIED BOTTLENECK & WORKLOAD
# =============================================================================

@st.cache_data(ttl=CACHE_TTL)
def create_bottleneck_and_workload_chart(df: pd.DataFrame) -> Optional[go.Figure]:
    """Create unified chart: pending tasks + delayed items by responsible."""
    if df.empty or 'Responsable' not in df.columns:
        return None
    
    # Pending workload
    pending_df = df[df['Is_Done'] == False].copy()
    workload = pending_df['Responsable'].value_counts().head(5).sort_values()
    
    # Delayed items
    delayed_df = df[df['Atrasado'] == True].copy()
    if delayed_df.empty:
        delayed_count = pd.Series(dtype=int)
    else:
        delayed_count = delayed_df['Responsable'].value_counts().head(5).sort_values()
    
    fig = go.Figure()
    
    # Pending trace
    fig.add_trace(go.Bar(
        y=[f"{r[:15]}" for r in workload.index],
        x=workload.values,
        orientation='h',
        name='Pending',
        marker_color=Config.COLORS['primary'],
        text=workload.values,
        textposition='outside',
        hovertemplate='<b>%{y}</b><br>Pending: %{x}<extra></extra>'
    ))
    
    # Delayed trace
    if not delayed_count.empty:
        fig.add_trace(go.Bar(
            y=[f"{r[:15]}" for r in delayed_count.index],
            x=delayed_count.values,
            orientation='h',
            name='Delayed',
            marker_color=Config.COLORS['danger'],
            text=delayed_count.values,
            textposition='outside',
            hovertemplate='<b>%{y}</b><br>Delayed: %{x}<extra></extra>'
        ))
    
    fig.update_layout(
        title="Task Distribution: Pending & Delayed",
        xaxis_title="Task Count",
        yaxis_title="",
        height=350,
        plot_bgcolor='white',
        template='plotly_white',
        barmode='group',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=120, t=60, b=40)
    )
    
    return fig


# =============================================================================
# DATA VALIDATION
# =============================================================================

def validate_dataframe(df: pd.DataFrame) -> Tuple[bool, str, pd.DataFrame]:
    """Validate and prepare dataframe."""
    required_cols = ['Tarea', 'Responsable', 'Estado', 'Proyecto Vinculado']
    
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        return False, f"Missing columns: {', '.join(missing)}", df
    
    df = df.copy()
    
    # Create derived columns
    if 'Is_Done' not in df.columns:
        df['Is_Done'] = df['Estado'].str.lower().isin(COMPLETION_STATUSES)
    
    if 'Atrasado' not in df.columns and 'Fecha Fin' in df.columns:
        now = pd.Timestamp.now()
        df['Fecha Fin'] = safe_datetime_convert(df['Fecha Fin'], 'Fecha Fin')
        df['Atrasado'] = (~df['Is_Done']) & (df['Fecha Fin'] < now) & df['Fecha Fin'].notna()
    
    # Clean data
    df['Proyecto Vinculado'] = df['Proyecto Vinculado'].fillna(SIN_PROYECTO).replace('', SIN_PROYECTO)
    df['Responsable'] = df['Responsable'].fillna(SIN_ASIGNAR).replace('', SIN_ASIGNAR)
    
    return True, "", df


# =============================================================================
# RENDER FUNCTIONS
# =============================================================================

def render_statistics_section(stats: Dict[str, Any]) -> None:
    """Render statistics in organized rows."""
    
    # Row 1: Velocity metrics
    st.markdown("### Metrics and Statistics")
    cols1 = st.columns(5)
    
    with cols1[0]:
        st.metric(
            "Last Week",
            f"{stats['velocity_week']} tasks",
            help="Tasks completed in last 7 days"
        )
    
    with cols1[1]:
        delta_str = f"{stats['delta_month']:+.1f}%" if stats['delta_month'] != 0 else "0%"
        st.metric(
            "Last Month",
            f"{stats['velocity_month']} tasks",
            delta=delta_str,
            delta_color="normal" if stats['delta_month'] >= 0 else "inverse",
            help="vs previous month"
        )
    
    with cols1[2]:
        st.metric(
            "Daily Average",
            f"{stats['daily_avg']:.1f} tasks/day",
            help="Average last 30 days"
        )
    
    with cols1[3]:
        st.metric(
            "Monthly Projection",
            f"{stats['projected']:.0f} tasks",
            help="Full month estimate"
        )
    
    with cols1[4]:
        month_completion = (
            stats['velocity_month'] / stats['projected'] * 100
            if stats['projected'] > 0 else 0
        )
        st.metric(
            "Month Progress",
            f"{month_completion:.0f}%",
            help="Of projected target"
        )
    
    st.markdown("---")
    
    # Row 2: Extended statistics
    st.markdown("### General Statistics")
    cols2 = st.columns(6)
    
    with cols2[0]:
        st.metric(
            "Total Tasks",
            f"{stats['total_tasks']}",
            help="All tasks in system"
        )
    
    with cols2[1]:
        st.metric(
            "Completion Rate",
            f"{stats['completion_rate']:.1f}%",
            help="Percentage completed"
        )
    
    with cols2[2]:
        avg_days = stats['avg_days_per_task']
        display_days = f"{avg_days:.1f} days" if avg_days > 0 else "N/A"
        st.metric(
            "Avg Days/Task",
            display_days,
            help="Average duration"
        )
    
    with cols2[3]:
        st.metric(
            "Unassigned",
            f"{stats['unassigned_tasks']} tasks",
            delta="pending" if stats['unassigned_tasks'] > 0 else None,
            delta_color="inverse" if stats['unassigned_tasks'] > 0 else "off",
            help="Pending unassigned tasks"
        )
    
    with cols2[4]:
        st.metric(
            "Top Performer",
            stats['top_responsible'][:15],
            delta=f"{stats['top_resp_count']} this month",
            help="Most completed this month"
        )
    
    with cols2[5]:
        proj_name = stats['top_project'][:18] if len(stats['top_project']) > 18 else stats['top_project']
        st.metric(
            "Leading Project",
            proj_name,
            delta=f"{stats['top_project_pct']:.0f}% progress",
            help="Highest completion rate (min 3 tasks)"
        )


def render_sidebar_filters(df: pd.DataFrame) -> Tuple[pd.DataFrame, str, str]:
    """Render sidebar filters."""
    with st.sidebar:
        st.markdown("### Filters")
        
        # Responsible filter
        responsables = ['All'] + sorted(df['Responsable'].unique().tolist())
        filtro_resp = st.selectbox("Responsible", responsables, index=0)
        
        # Project filter
        proyectos = ['All'] + sorted([
            p for p in df['Proyecto Vinculado'].unique() 
            if p != SIN_PROYECTO
        ])
        filtro_proj = st.selectbox("Project", proyectos, index=0)
        
        st.markdown("---")
        
        # Export
        st.markdown("### Export")
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"analytics_{datetime.now().strftime(EXPORT_FILENAME_TIMESTAMP_FORMAT)}.csv",
            mime="text/csv"
        )
    
    return df, filtro_resp, filtro_proj


def _build_weeks(n: int):
    """Return list of (label, w_start, w_end_exclusive) for last n weeks."""
    today = pd.Timestamp.now().normalize()
    current_week_start = today - timedelta(days=today.weekday())
    weeks = []
    for i in range(n):
        w_start = current_week_start - timedelta(weeks=i)
        w_end   = w_start + timedelta(days=7)
        end_display = w_end - timedelta(days=1)
        label = (f"This week\n{format_date(w_start)}" if i == 0
                 else f"W-{i}\n{format_date_range(w_start, end_display)}")
        weeks.append((label, w_start, w_end))
    return weeks


def _get_week_colors(weeks) -> Dict[str, str]:
    """Shared week-to-color mapping, ordered from oldest to newest."""
    palette = [
        Config.COLORS['primary'],
        Config.COLORS['info'],
        Config.COLORS['success'],
        Config.COLORS['warning'],
    ]
    ordered_labels = [label for label, _, _ in reversed(weeks)]
    return {
        label: palette[idx % len(palette)]
        for idx, label in enumerate(ordered_labels)
    }


def _assign_week(dt: pd.Timestamp, weeks) -> str:
    """Return the week label for a given timestamp, or None if outside window."""
    for label, w_start, w_end in weeks:
        if w_start <= dt < w_end:
            return label
    return None


def render_hours_heatmap(df: pd.DataFrame, df_sessions: pd.DataFrame) -> None:
    """
    Heatmap: projects (rows) x last 4 weeks (cols), color = logged hours.
    Canonical activity view; uses sessions when available.
    """
    st.markdown("### Activity by Project - Last 4 Weeks")

    weeks = _build_weeks(4)
    cutoff_start = weeks[-1][1]
    cutoff_end   = weeks[0][2]

    # Source selection
    use_sessions = (
        not df_sessions.empty
        and 'started_at' in df_sessions.columns
        and 'proyecto' in df_sessions.columns
        and 'duration_h' in df_sessions.columns
    )

    if use_sessions:
        mask = (
            df_sessions['started_at'].notna() &
            (df_sessions['started_at'] >= cutoff_start) &
            (df_sessions['started_at'] < cutoff_end) &
            (df_sessions['duration_h'] > 0) &
            (df_sessions['proyecto'].str.strip() != '')
        )
        df_work = df_sessions[mask].copy()
        df_work['_week'] = df_work['started_at'].apply(lambda d: _assign_week(d, weeks))
        df_work = df_work[df_work['_week'].notna()]
        pivot_data = {
            label: df_work[df_work['_week'] == label]
                   .groupby('proyecto')['duration_h'].sum()
            for label, _, _ in weeks
        }
        st.caption("Source: time tracking sessions")
    else:
        if 'Horas Registradas' not in df.columns:
            st.warning("No hours data available.")
            return
        df_work = df.copy()
        df_work['Fecha Fin'] = pd.to_datetime(df_work['Fecha Fin'], errors='coerce')
        df_work['Horas Registradas'] = pd.to_numeric(
            df_work['Horas Registradas'], errors='coerce').fillna(0)
        mask = (
            df_work['Fecha Fin'].notna() &
            (df_work['Fecha Fin'] >= cutoff_start) &
            (df_work['Fecha Fin'] < cutoff_end) &
            (df_work['Horas Registradas'] > 0) &
            (df_work['Proyecto Vinculado'].str.strip() != '')
        )
        df_work = df_work[mask]
        pivot_data = {}
        for label, w_start, w_end in weeks:
            wm = (df_work['Fecha Fin'] >= w_start) & (df_work['Fecha Fin'] < w_end)
            pivot_data[label] = df_work[wm].groupby('Proyecto Vinculado')['Horas Registradas'].sum()
        st.caption("Source: hours logged per task")

    pivot = pd.DataFrame(pivot_data).fillna(0).round(1)
    pivot['_total'] = pivot.sum(axis=1)
    pivot = pivot[pivot['_total'] > 0].sort_values('_total', ascending=False).drop(columns='_total')

    # Reorder columns oldest -> newest
    week_labels = [w[0] for w in reversed(weeks)]
    pivot = pivot[[c for c in week_labels if c in pivot.columns]]

    if pivot.empty:
        st.info("No hours data in last 4 weeks.")
        return

    y_labels = [p[:35] + '...' if len(p) > 35 else p for p in pivot.index]
    text_matrix = [[f"{v:.1f}h" if v > 0 else "" for v in row] for row in pivot.values]

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=y_labels,
        text=text_matrix,
        texttemplate="%{text}",
        textfont={"size": 11, "color": "white"},
        colorscale=[
            [0.0,  "#f5f5f5"],
            [0.01, "#c8e6c9"],
            [0.4,  "#43a047"],
            [1.0,  "#1b5e20"],
        ],
        showscale=True,
        colorbar=dict(title="Hours", ticksuffix="h"),
        hoverongaps=False,
        hovertemplate="<b>%{y}</b><br>%{x}<br><b>%{z:.1f}h</b><extra></extra>",
        xgap=3, ygap=3,
    ))

    row_height  = max(30, min(50, 400 // max(len(pivot), 1)))
    chart_height = max(250, len(pivot) * row_height + 100)

    fig.update_layout(
        height=chart_height,
        margin=dict(l=10, r=80, t=20, b=60),
        plot_bgcolor="white",
        xaxis=dict(side="top", tickangle=0),
        yaxis=dict(autorange="reversed"),
        font=dict(size=12),
    )

    st.plotly_chart(fig, use_container_width=True)

    # Summary metrics
    total_hours  = pivot.values.sum()
    active_proj  = len(pivot)
    busiest_proj = pivot.sum(axis=1).idxmax()
    busiest_h    = pivot.sum(axis=1).max()

    c1, c2, c3 = st.columns(3)
    c1.metric("Total hours (4 weeks)", f"{total_hours:.1f}h")
    c2.metric("Active projects", active_proj)
    c3.metric("Busiest project", busiest_proj[:25], delta=f"{busiest_h:.1f}h")


# =============================================================================
# RESPONSABLE WEEKLY HOURS vs 40h TARGET (FIXED COHORT)
# =============================================================================

def _get_status_color_and_emoji(hours: float, target: float) -> Tuple[str, str]:
    """
    Return (color, emoji) based on % of target reached.
    Uses Streamlit-compatible colored circles.
    """
    pct = hours / target * 100 if target > 0 else 0
    if pct >= 90:
        return "#2ecc71", "🟢"  # Green
    if pct >= 60:
        return "#f39c12", "🟡"  # Orange/Yellow
    return "#e74c3c", "🔴"  # Red


def render_responsable_hours(df: pd.DataFrame, df_sessions: pd.DataFrame) -> None:
    """
    Grouped bar chart + status cards: hours per responsable (fixed cohort only).
    Always shows the fixed cohort defined in HOURS_COHORT env var.
    Target: 40h/week.
    """
    st.markdown("### Hours by Responsible vs 40h Target")
    
    weeks = _build_weeks(4)
    cutoff_start = weeks[-1][1]
    cutoff_end   = weeks[0][2]

    # Source selection
    use_sessions = (
        not df_sessions.empty
        and 'started_at' in df_sessions.columns
        and 'responsable' in df_sessions.columns
        and 'duration_h' in df_sessions.columns
    )

    if use_sessions:
        mask = (
            df_sessions['started_at'].notna() &
            (df_sessions['started_at'] >= cutoff_start) &
            (df_sessions['started_at'] < cutoff_end) &
            (df_sessions['duration_h'] > 0) &
            (df_sessions['responsable'].str.strip() != '') &
            (df_sessions['responsable'] != 'Sin Asignar')
        )
        df_work = df_sessions[mask].copy()
        df_work['_week'] = df_work['started_at'].apply(lambda d: _assign_week(d, weeks))
        df_work = df_work[df_work['_week'].notna()]
        pivot_data = {}
        for label, _, _ in reversed(weeks):
            grp = (df_work[df_work['_week'] == label]
                   .groupby('responsable')['duration_h'].sum())
            pivot_data[label] = grp
        resp_col = 'responsable'
        st.caption("Source: time tracking sessions")
    else:
        if 'Horas Registradas' not in df.columns or 'Responsable' not in df.columns:
            st.warning("No hours data available.")
            return
        df_work = df.copy()
        df_work['Fecha Fin'] = safe_datetime_convert(df_work['Fecha Fin'], 'Fecha Fin')
        df_work['Horas Registradas'] = pd.to_numeric(
            df_work['Horas Registradas'], errors='coerce').fillna(0)
        mask = (
            df_work['Fecha Fin'].notna() &
            (df_work['Fecha Fin'] >= cutoff_start) &
            (df_work['Fecha Fin'] < cutoff_end) &
            (df_work['Horas Registradas'] > 0) &
            (df_work['Responsable'].str.strip() != '') &
            (df_work['Responsable'] != 'Sin Asignar')
        )
        df_work = df_work[mask].copy()
        pivot_data = {}
        for label, w_start, w_end in reversed(weeks):
            wm = (df_work['Fecha Fin'] >= w_start) & (df_work['Fecha Fin'] < w_end)
            pivot_data[label] = df_work[wm].groupby('Responsable')['Horas Registradas'].sum()
        resp_col = 'Responsable'
        st.caption("Source: hours logged per task")

    pivot = pd.DataFrame(pivot_data).fillna(0).round(1)
    
    # Filter to fixed cohort, reindex to ensure all 4 are present (with proper capitalization)
    pivot = pivot.reindex(FIXED_COHORT, fill_value=0.0)
    pivot = pivot[pivot.sum(axis=1) > 0]  # Remove zero rows

    if pivot.empty:
        st.info("No hours recorded for target cohort in last 4 weeks.")
        return

    responsables = pivot.index.tolist()
    week_cols    = pivot.columns.tolist()
    n_weeks      = len(week_cols)

    # Status cards (most recent week) - CORRECTED WITH EMOJIS
    last_week_col = week_cols[-1]
    cols = st.columns(len(responsables))
    for i, resp in enumerate(responsables):
        hrs   = pivot.loc[resp, last_week_col]
        color, emoji = _get_status_color_and_emoji(hrs, WEEKLY_TARGET)
        pct   = min(hrs / WEEKLY_TARGET * 100, 100)
        # Extract first name for display
        first_name = resp.split()[0]
        with cols[i]:
            st.metric(
                label=f"{emoji} {first_name}",
                value=f"{hrs:.1f}h",
                delta=f"{hrs:.1f}h / {WEEKLY_TARGET:.0f}h target",
                delta_color="normal" if hrs >= WEEKLY_TARGET * 0.9 else "inverse",
                help=f"Hours this week: {hrs:.1f}h ({pct:.0f}% of target)"
            )

    st.markdown("")

    # Grouped bar chart
    week_colors = _get_week_colors(weeks)
    fig = go.Figure()
    for week_col in week_cols:
        hours_vals = [pivot.loc[r, week_col] for r in responsables]
        fig.add_trace(go.Bar(
            name=week_col.replace("\n", " "),
            x=[r.split()[0] for r in responsables],  # Use first names on x-axis
            y=hours_vals,
            text=[f"{v:.1f}h" if v > 0 else "" for v in hours_vals],
            textposition='outside',
            textfont=dict(size=10),
            marker_color=week_colors.get(week_col, Config.COLORS['info']),
            hovertemplate="<b>%{x}</b><br>" + week_col.replace("\n", " ") +
                          "<br><b>%{y:.1f}h</b><extra></extra>",
        ))

    fig.add_hline(
        y=WEEKLY_TARGET, line_dash="dash",
        line_color=Config.COLORS['danger'], line_width=1.5,
        annotation_text="Target 40h", annotation_position="top right",
        annotation_font_color=Config.COLORS['danger'],
    )
    fig.update_layout(
        barmode='group', height=400, plot_bgcolor='white',
        template='plotly_white', showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(title=""),
        yaxis=dict(
            title="Hours logged",
            range=[0, max(pivot.values.max() * 1.2, WEEKLY_TARGET * 1.3)],
        ),
        margin=dict(t=60, b=40, l=40, r=40),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Summary table
    summary = pivot.copy()
    summary.index.name = "Responsible"
    summary["Total 4W"] = summary.sum(axis=1).round(1)
    summary["Avg/Week"] = (summary["Total 4W"] / n_weeks).round(1)
    
    # Add badge emoji to vs Target column
    summary["vs Target"] = summary["Avg/Week"].apply(
        lambda h: f"{_get_status_color_and_emoji(h, WEEKLY_TARGET)[1]} {h:.1f}h ({h/WEEKLY_TARGET*100:.0f}%)"
    )
    summary_display = summary.reset_index()
    summary_display.columns = [c.replace("\n", " ") for c in summary_display.columns]
    st.dataframe(summary_display, use_container_width=True, hide_index=True)


def render_charts_section(df: pd.DataFrame, df_sessions: pd.DataFrame) -> None:
    """Render 3-chart grid: trend, progress, and unified bottleneck/workload."""
    
    # Row 1: Trend + Progress
    col1, col2 = st.columns([3, 2])
    
    with col1:
        fig_trend = create_weekly_trend_chart(df_sessions)
        if fig_trend:
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.info("Insufficient session data for trend chart")
    
    with col2:
        fig_donut = create_progress_donut(df)
        if fig_donut:
            st.plotly_chart(fig_donut, use_container_width=True)
        else:
            st.info("No progress data available")
    
    # Row 2: Unified bottleneck/workload
    fig_combined = create_bottleneck_and_workload_chart(df)
    if fig_combined:
        st.plotly_chart(fig_combined, use_container_width=True)
    else:
        st.success("No delays or pending work to display!")


# =============================================================================
# MAIN
# =============================================================================

def main():
    st.title("Analytics Dashboard")
    st.caption("Consolidated metrics, activity, hours tracking, and trend analysis")
    
    try:
        with st.spinner('Loading data...'):
            df = load_and_process_data()
            df_sessions = load_sessions_data()
        
        if df.empty:
            st.warning("No data available.")
            if st.button("Retry"):
                st.rerun()
            return
        
        # Validate data
        is_valid, error_msg, df = validate_dataframe(df)
        if not is_valid:
            st.error(f"Error: {error_msg}")
            return
        
        # Sidebar filters
        df, filtro_resp, filtro_proj = render_sidebar_filters(df)
        
        # Apply filters (affects Metrics + Charts only, not Hours section)
        if filtro_resp != 'All':
            df = df[df['Responsable'] == filtro_resp]
        if filtro_proj != 'All':
            df = df[df['Proyecto Vinculado'] == filtro_proj]
        
        if df.empty:
            st.info("No data with selected filters.")
            return
        
        # Section 1: Metrics
        stats = calculate_all_statistics(df)
        render_statistics_section(stats)
        st.markdown("---")
        
        # Section 2: Project Activity (canonical view)
        render_hours_heatmap(df, df_sessions)
        st.markdown("---")
        
        # Section 3: Responsible Hours (fixed cohort, unaffected by filters)
        render_responsable_hours(df, df_sessions)
        st.markdown("---")
        
        # Section 4: Trends & Bottlenecks
        render_charts_section(df, df_sessions)
        
        # Footer
        sessions_note = f" | Sessions: {len(df_sessions)}" if not df_sessions.empty else ""
        st.markdown("---")
        st.caption(
            f"Updated: {datetime.now().strftime(DATETIME_DISPLAY_FORMAT)} | "
            f"Records: {len(df)}{sessions_note}"
        )
        
    except Exception as e:
        logger.error("Critical error: %s", str(e))
        st.error(f"Critical error: {str(e)}")
        
        with st.expander("Technical details"):
            import traceback
            st.code(traceback.format_exc())


if __name__ == "__main__":
    main()
