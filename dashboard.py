"""
dashboard.py  –  Main Streamlit page: task board with KPIs and project progress.
"""

import os
import streamlit as st
import pandas as pd
from datetime import datetime
import pytz
from io import BytesIO

from config import Config
from modules.data import load_and_process_data, calculate_project_progress
from modules.ui import (
    load_css, render_kpis, render_task_card,
    render_project_progress_card, render_search_box,
)

# Page config
st.set_page_config(page_title="Operations Dashboard", page_icon="", layout="wide")
load_css()

# ---------------------------------------------------------------------------
# DEFAULT FILTER USERS  –  set via env var or fall back to empty list
# Example env var:  DEFAULT_RESPONSABLES="Alice,Bob,Carol"
# ---------------------------------------------------------------------------
_DEFAULT_RESP_ENV = os.environ.get("DEFAULT_RESPONSABLES", "")
DEFAULT_RESPONSABLES = [r.strip() for r in _DEFAULT_RESP_ENV.split(",") if r.strip()]


def init_session_state():
    """Initialize persistent filter state."""
    defaults = {
        "selected_projects":     [],
        "selected_responsables": [],
        "show_completed":        False,
        "sort_by":               "Fecha Fin",
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


init_session_state()


def main():
    # Header
    c1, c2 = st.columns([6, 1])
    c1.title("Operations Dashboard")
    if c2.button("Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    with st.spinner("Loading data..."):
        df = load_and_process_data()

    if df.empty:
        st.warning("No data available.")
        return

    # -----------------------------------------------------------------------
    # SIDEBAR FILTERS
    # -----------------------------------------------------------------------
    st.sidebar.header("Filters")

    # Project filter
    proyectos = sorted([x for x in df["Proyecto Vinculado"].unique() if x])
    sel_proy  = st.sidebar.multiselect(
        "Project",
        proyectos,
        default=st.session_state.selected_projects,
        key="project_filter",
    )
    st.session_state.selected_projects = sel_proy

    # Responsible filter (dynamic based on selected projects)
    df_temp      = df[df["Proyecto Vinculado"].isin(sel_proy)] if sel_proy else df
    responsables = sorted([x for x in df_temp["Responsable"].unique() if x])

    if not st.session_state.selected_responsables:
        sel_resp_default = [r for r in DEFAULT_RESPONSABLES if r in responsables]
    else:
        sel_resp_default = [
            r for r in st.session_state.selected_responsables if r in responsables
        ]

    sel_resp = st.sidebar.multiselect(
        "Responsible",
        responsables,
        default=sel_resp_default,
        key="responsible_filter",
    )
    st.session_state.selected_responsables = sel_resp

    # Completed toggle
    show_completed = st.sidebar.checkbox(
        "Show completed tasks",
        value=st.session_state.show_completed,
        key="show_completed_filter",
    )
    st.session_state.show_completed = show_completed

    # Sort selector
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Sort tasks by:**")
    sort_options = {
        "Due Date":        "Fecha Fin",
        "Priority":        "Prioridad",
        "Days Remaining":  "Dias_Restantes",
        "Project":         "Proyecto Vinculado",
    }

    sort_by = st.sidebar.selectbox(
        "Sort criterion",
        options=list(sort_options.keys()),
        index=list(sort_options.keys()).index(
            next((k for k, v in sort_options.items() if v == st.session_state.sort_by), "Due Date")
        ),
        key="sort_selector",
        label_visibility="collapsed",
    )
    st.session_state.sort_by = sort_options[sort_by]

    # Search box
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Search**")
    search_term = st.sidebar.text_input(
        "Search task",
        placeholder="Type to search...",
        label_visibility="collapsed",
        key="task_search",
    )

    # Clear filters
    if st.sidebar.button("Clear Filters", use_container_width=True):
        st.session_state.selected_projects     = []
        st.session_state.selected_responsables = []
        st.session_state.show_completed        = False
        st.session_state.sort_by               = "Fecha Fin"
        st.rerun()

    # -----------------------------------------------------------------------
    # APPLY FILTERS
    # -----------------------------------------------------------------------
    df_filtered = df.copy()

    if sel_proy:
        df_filtered = df_filtered[df_filtered["Proyecto Vinculado"].isin(sel_proy)]
    if sel_resp:
        df_filtered = df_filtered[df_filtered["Responsable"].isin(sel_resp)]
    if not show_completed:
        df_filtered = df_filtered[~df_filtered["Is_Done"]]
    if search_term:
        q = search_term.lower()
        df_filtered = df_filtered[
            df_filtered["Tarea"].astype(str).str.lower().str.contains(q, na=False)
            | df_filtered["Proyecto Vinculado"].astype(str).str.lower().str.contains(q, na=False)
            | df_filtered["Responsable"].astype(str).str.lower().str.contains(q, na=False)
        ]

    # -----------------------------------------------------------------------
    # KPIs
    # -----------------------------------------------------------------------
    render_kpis(df_filtered)

    # Active filter summary
    if sel_proy or sel_resp or search_term or show_completed:
        parts = []
        if sel_proy:
            parts.append(f"**Projects:** {', '.join(sel_proy[:3])}{'...' if len(sel_proy) > 3 else ''}")
        if sel_resp:
            parts.append(f"**Responsibles:** {', '.join(sel_resp[:3])}{'...' if len(sel_resp) > 3 else ''}")
        if search_term:
            parts.append(f"**Search:** '{search_term}'")
        if show_completed:
            parts.append("**Includes completed**")
        st.info("Active filters: " + " | ".join(parts))

    # -----------------------------------------------------------------------
    # PROJECT PROGRESS
    # -----------------------------------------------------------------------
    st.markdown('<div class="section-header">Project Progress</div>', unsafe_allow_html=True)

    df_for_progress = df.copy()
    if sel_proy:
        df_for_progress = df_for_progress[df_for_progress["Proyecto Vinculado"].isin(sel_proy)]

    progress_df = calculate_project_progress(df_for_progress)

    if not progress_df.empty:
        top_projects       = progress_df.head(6)
        remaining_projects = progress_df.iloc[6:]
        col1, col2         = st.columns(2)

        for idx, row in top_projects.iterrows():
            with col1 if top_projects.index.get_loc(idx) % 2 == 0 else col2:
                render_project_progress_card(
                    project_name=row["Proyecto Vinculado"],
                    total=row["Total"],
                    completadas=row["Completadas"],
                    pendientes=row["Pendientes"],
                    porcentaje=row["Porcentaje"],
                    atrasadas=row["Atrasadas"],
                    horas=row.get("Horas_Totales", 0),
                )

        if not remaining_projects.empty:
            with st.expander(f"View {len(remaining_projects)} additional projects"):
                col3, col4 = st.columns(2)
                for idx, row in remaining_projects.iterrows():
                    with col3 if remaining_projects.index.get_loc(idx) % 2 == 0 else col4:
                        render_project_progress_card(
                            project_name=row["Proyecto Vinculado"],
                            total=row["Total"],
                            completadas=row["Completadas"],
                            pendientes=row["Pendientes"],
                            porcentaje=row["Porcentaje"],
                            atrasadas=row["Atrasadas"],
                            horas=row.get("Horas_Totales", 0),
                        )
    else:
        st.info("No projects with assigned tasks in current filters.")

    st.markdown("---")

    # -----------------------------------------------------------------------
    # PENDING TASKS
    # -----------------------------------------------------------------------
    st.markdown('<div class="section-header">Pending Tasks</div>', unsafe_allow_html=True)

    df_pending = df_filtered[~df_filtered["Is_Done"]].copy()

    # ---------------------------------------------------------------------------
    # PROJECT PRIORITY ORDER
    # Loaded from env var  PROJECT_PRIORITY_ORDER  as a comma-separated list.
    # Example:
    #   PROJECT_PRIORITY_ORDER="Alpha Project,Beta Project,Gamma Project"
    # Projects not in the list are assigned a low sort weight (999).
    # ---------------------------------------------------------------------------
    _proj_order_env = os.environ.get("PROJECT_PRIORITY_ORDER", "")
    _proj_list      = [p.strip() for p in _proj_order_env.split(",") if p.strip()]
    project_priority = {name: idx + 1 for idx, name in enumerate(_proj_list)}

    # Specialty priority order (generic defaults; adjust to your workflow)
    specialty_priority = {
        "Assembly":          1,
        "Wiring":            2,
        "Electrical":        3,
        "Machining":         4,
        "Welding":           5,
        "General Work":      6,
        "Documentation":     7,
        "Final activities":  9,
    }

    df_pending["_project_sort"]   = df_pending["Proyecto Vinculado"].map(
        lambda x: project_priority.get(str(x).strip(), 999)
    )
    df_pending["_specialty_sort"] = df_pending["Especialidad"].map(
        lambda x: specialty_priority.get(str(x).strip(), 8)
    )

    sort_col = st.session_state.sort_by

    if sort_col == "Fecha Fin":
        df_pending = df_pending.sort_values(
            ["Fecha Fin", "_project_sort", "_specialty_sort"], na_position="last"
        )
    elif sort_col == "Dias_Restantes":
        df_pending = df_pending.sort_values(
            ["Dias_Restantes", "_project_sort", "_specialty_sort"], na_position="last"
        )
    elif sort_col == "Prioridad":
        priority_order = {"Alta": 0, "High": 0, "Media": 1, "Medium": 1, "Baja": 2, "Low": 2}
        df_pending["_priority_sort"] = df_pending["Prioridad"].map(
            lambda x: priority_order.get(str(x).strip(), 99)
        )
        df_pending = df_pending.sort_values(
            ["_priority_sort", "_project_sort", "_specialty_sort"]
        ).drop(columns=["_priority_sort"])
    elif sort_col == "Proyecto Vinculado":
        df_pending = df_pending.sort_values(
            ["_project_sort", "_specialty_sort", "Fecha Fin"], na_position="last"
        )
    else:
        df_pending = df_pending.sort_values(sort_col, na_position="last")

    df_pending = df_pending.drop(columns=["_project_sort", "_specialty_sort"])

    if df_pending.empty:
        st.success("All caught up!")
    else:
        st.info(f"Showing {len(df_pending)} pending tasks (sorted by: {sort_by})")
        active_users = sel_resp if sel_resp else df_pending["Responsable"].unique()[:10]
        num_cols     = min(len(active_users), 3)
        cols         = st.columns(num_cols)

        for idx, user in enumerate(active_users):
            user_tasks = df_pending[df_pending["Responsable"] == user].head(25)
            if not user_tasks.empty:
                with cols[idx % num_cols]:
                    st.markdown(
                        f'<div class="responsable-header">{user} ({len(user_tasks)})</div>',
                        unsafe_allow_html=True,
                    )
                    for _, task in user_tasks.iterrows():
                        render_task_card(task, show_badges=True)

    # -----------------------------------------------------------------------
    # COMPLETED TASKS (optional)
    # -----------------------------------------------------------------------
    if show_completed:
        st.markdown("---")
        st.markdown('<div class="section-header">Completed Tasks</div>', unsafe_allow_html=True)
        df_completed = df_filtered[df_filtered["Is_Done"]].sort_values("Fecha Fin", ascending=False)

        if not df_completed.empty:
            st.info(f"{len(df_completed)} completed tasks")
            with st.expander("View completed tasks", expanded=False):
                completed_users  = df_completed["Responsable"].unique()[:5]
                cols_completed   = st.columns(min(len(completed_users), 3))
                for idx, user in enumerate(completed_users):
                    user_done = df_completed[df_completed["Responsable"] == user].head(10)
                    with cols_completed[idx % len(cols_completed)]:
                        st.markdown(f"**{user}** ({len(user_done)})")
                        for _, task in user_done.iterrows():
                            render_task_card(task, show_badges=False)

    # -----------------------------------------------------------------------
    # EXPORT
    # -----------------------------------------------------------------------
    st.markdown("---")
    export_col1, export_col2 = st.columns([3, 1])

    with export_col1:
        st.markdown(f"**Export filtered data** ({len(df_filtered)} tasks)")

    with export_col2:
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df_filtered.to_excel(writer, index=False, sheet_name="Tasks")

        filename = f"tasks_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        st.download_button(
            "Download Excel",
            output.getvalue(),
            filename,
            use_container_width=True,
        )


if __name__ == "__main__":
    main()
