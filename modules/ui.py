# modules/ui.py
import streamlit as st
import html
import pandas as pd
from config import Config

def load_css():
    """Inject global CSS with improved styling and animations"""
    st.markdown(f"""
    <style>
        :root {{ color-scheme: light !important; }}
        
        /* Base styles */
        html, body, .stApp {{ 
            background-color: {Config.COLORS['bg_light']} !important; 
            color: {Config.COLORS['text_primary']} !important; 
        }}
        
        /* Metrics */
        div[data-testid="stMetricValue"] {{ 
            font-size: 2rem; 
            color: {Config.COLORS['primary']}; 
            font-weight: 800; 
        }}
        
        div[data-testid="stMetricDelta"] {{
            font-size: 0.9rem;
        }}
        
        /* Task cards */
        .task-card, .task-card-delayed, .task-card-done {{
            padding: 15px; 
            border-radius: 10px; 
            margin-bottom: 12px;
            background-color: {Config.COLORS['bg_card']};
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            border-left: 5px solid {Config.COLORS['primary']};
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }}
        
        .task-card:hover, .task-card-delayed:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }}
        
        .task-card-delayed {{ 
            border-left: 5px solid {Config.COLORS['danger']}; 
            background-color: #FFEBEE; 
        }}
        
        .task-card-done {{
            opacity: 0.7;
            border-left: 5px solid {Config.COLORS['success']};
            background-color: #F1F8F4;
        }}
        
        .task-title {{ 
            font-weight: 700; 
            font-size: 1rem; 
            margin-bottom: 8px;
            line-height: 1.3;
        }}
        
        .task-meta {{ 
            font-size: 0.85rem; 
            color: {Config.COLORS['text_secondary']};
            line-height: 1.6;
        }}
        
        /* Badges */
        .badge {{
            display: inline-block;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 600;
            margin-right: 5px;
            margin-top: 5px;
        }}
        
        .badge-priority-high {{
            background-color: {Config.COLORS['danger']};
            color: white;
        }}
        
        .badge-priority-medium {{
            background-color: {Config.COLORS['warning']};
            color: white;
        }}
        
        .badge-priority-low {{
            background-color: {Config.COLORS['info']};
            color: white;
        }}
        
        .badge-specialty {{
            background-color: {Config.COLORS['secondary']};
            color: white;
        }}
        
        .badge-module {{
            background-color: #9C27B0;
            color: white;
        }}
        
        /* Progress bar */
        .progress-container {{
            width: 100%;
            height: 6px;
            background-color: #E0E0E0;
            border-radius: 3px;
            margin-top: 8px;
            overflow: hidden;
        }}
        
        .progress-bar {{
            height: 100%;
            background: linear-gradient(90deg, {Config.COLORS['success']} 0%, {Config.COLORS['secondary']} 100%);
            border-radius: 3px;
            transition: width 0.3s ease;
        }}
        
        /* Section headers */
        .section-header {{
            color: {Config.COLORS['text_primary']}; 
            font-weight: 800; 
            font-size: 1.4rem;
            margin-bottom: 20px; 
            padding-bottom: 8px; 
            border-bottom: 3px solid {Config.COLORS['primary']};
        }}
        
        .responsable-header {{
            background: linear-gradient(135deg, {Config.COLORS['primary']} 0%, {Config.COLORS['info']} 100%);
            color: white; 
            padding: 12px; 
            border-radius: 8px; 
            font-weight: 700; 
            margin-bottom: 12px; 
            font-size: 0.95rem;
            box-shadow: 0 2px 6px rgba(0,0,0,0.1);
        }}
        
        /* Project progress cards */
        .project-card {{
            background-color: white;
            border-radius: 10px;
            padding: 15px;
            margin-bottom: 12px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.08);
            border-left: 4px solid {Config.COLORS['primary']};
            transition: all 0.2s ease;
        }}
        
        .project-card:hover {{
            box-shadow: 0 4px 12px rgba(0,0,0,0.12);
            transform: translateX(4px);
        }}
        
        .project-name {{
            font-weight: 700;
            font-size: 1rem;
            color: {Config.COLORS['text_primary']};
            margin-bottom: 8px;
        }}
        
        .project-stats {{
            font-size: 0.85rem;
            color: {Config.COLORS['text_secondary']};
            margin-bottom: 8px;
        }}
        
        /* Search box styling */
        .stTextInput input {{
            border-radius: 8px;
            border: 2px solid {Config.COLORS['border']};
        }}
        
        .stTextInput input:focus {{
            border-color: {Config.COLORS['primary']};
            box-shadow: 0 0 0 2px rgba(21, 101, 192, 0.1);
        }}
        
        /* Expander styling */
        .streamlit-expanderHeader {{
            background-color: {Config.COLORS['bg_card']};
            border-radius: 8px;
            font-weight: 600;
        }}
        
        /* Loading spinner color */
        .stSpinner > div {{
            border-top-color: {Config.COLORS['primary']} !important;
        }}
    </style>
    """, unsafe_allow_html=True)

def render_kpis(df):
    """Render main KPI metrics with tooltips"""
    total = len(df)
    if total == 0: 
        return

    atrasadas = int(df['Atrasado'].sum())
    completadas = int(df['Is_Done'].sum())
    
    k1, k2, k3, k4, k5 = st.columns(5)
    
    k1.metric(
        "Total Tareas", 
        total,
        help="Total de tareas en el sistema"
    )
    
    k2.metric(
        "Operarios", 
        df['Responsable'].nunique(),
        help="Numero de personas con tareas asignadas"
    )
    
    k3.metric(
        "Atrasadas", 
        atrasadas, 
        f"{(atrasadas/total*100):.1f}%", 
        delta_color="inverse",
        help="Tareas que superaron su fecha limite"
    )
    
    k4.metric(
        "Completadas", 
        completadas, 
        f"{(completadas/total*100):.1f}%",
        help="Tareas marcadas como terminadas"
    )
    
    k5.metric(
        "Proyectos", 
        df['Proyecto Vinculado'].nunique(),
        help="Proyectos unicos con tareas activas"
    )
    
    st.markdown("---")

def render_task_card(task, show_badges=True):
    """
    Render individual task card with optional badges
    
    Args:
        task: DataFrame row with task data
        show_badges: Show specialty/module badges
    """
    # Determine card style
    if task['Is_Done']:
        css_class = "task-card-done"
        icon = ""
    elif task['Atrasado']:
        css_class = "task-card-delayed"
        icon = ""
    else:
        css_class = "task-card"
        icon = ""
    
    # Days remaining logic
    d = task['Dias_Restantes']
    days_txt = ""
    if pd.notnull(d):
        if d < 0: 
            days_txt = f"<span style='color:{Config.COLORS['danger']}'><b>{abs(int(d))} dias tarde</b></span>"
        elif d == 0: 
            days_txt = f"<span style='color:{Config.COLORS['warning']}'><b>Vence hoy!</b></span>"
        elif d <= 3:
            days_txt = f"<span style='color:{Config.COLORS['warning']}'><b>{int(d)} dias</b></span>"
        else: 
            days_txt = f"{int(d)} dias"

    proyecto = html.escape(str(task.get('Proyecto Vinculado', '')))
    fecha = task['Fecha Fin'].strftime('%d-%b-%Y') if pd.notnull(task['Fecha Fin']) else 'S/F'
    tarea_nombre = html.escape(str(task['Tarea']))
    
    # Build badges HTML
    badges_html = ""
    if show_badges:
        # Priority badge
        priority = str(task.get('Prioridad', '')).strip()
        if priority:
            priority_class = 'low'
            if 'alta' in priority.lower() or 'high' in priority.lower():
                priority_class = 'high'
            elif 'media' in priority.lower() or 'medium' in priority.lower():
                priority_class = 'medium'
            badges_html += f'<span class="badge badge-priority-{priority_class}">{priority}</span>'
        
        # Specialty badge
        specialty = str(task.get('Especialidad', '')).strip()
        if specialty and specialty != '':
            badges_html += f'<span class="badge badge-specialty">{html.escape(specialty)}</span>'
        
        # Module badge
        module = str(task.get('Modulo', '')).strip()
        if module and module != '':
            badges_html += f'<span class="badge badge-module">{html.escape(module)}</span>'

    st.markdown(f"""
    <div class="{css_class}">
        <div class="task-title">{icon} {tarea_nombre}</div>
        <div class="task-meta">
            <b>{proyecto}</b><br>
            {fecha} | {days_txt}
        </div>
        {badges_html}
    </div>
    """, unsafe_allow_html=True)

def render_project_progress_card(project_name, total, completadas, pendientes, porcentaje, atrasadas, horas=0):
    """Render project progress card with progress bar and logged hours"""
    
    # Ensure clean integer display for counts
    total = int(total)
    completadas = int(completadas)
    pendientes = int(pendientes)
    atrasadas = int(atrasadas)
    porcentaje = round(float(porcentaje), 1)
    horas = round(float(horas), 1)
    
    # Determine border color based on progress
    if porcentaje >= 80:
        border_color = Config.COLORS['success']
    elif porcentaje >= 50:
        border_color = Config.COLORS['warning']
    else:
        border_color = Config.COLORS['danger']
    
    atrasadas_text = ""
    if atrasadas > 0:
        atrasadas_text = f" | <span style='color:{Config.COLORS['danger']}'>{atrasadas} atrasadas</span>"
    
    # Hours badge - only show if there are logged hours
    horas_text = ""
    if horas > 0:
        horas_text = f" | <span style='color:{Config.COLORS['info']};font-weight:500'>&#8987; {horas}h registradas</span>"
    
    st.markdown(f"""
    <div class="project-card" style="border-left-color: {border_color}">
        <div class="project-name">{html.escape(project_name)}</div>
        <div class="project-stats">
            {completadas}/{total} tareas completadas ({porcentaje}%){atrasadas_text}{horas_text}
        </div>
        <div class="progress-container">
            <div class="progress-bar" style="width: {porcentaje}%"></div>
        </div>
    </div>
    """, unsafe_allow_html=True)

def render_search_box():
    """Render search box with icon"""
    return st.text_input(
        "Buscar tarea",
        placeholder="Escribe para buscar...",
        label_visibility="collapsed",
        key="task_search"
    )
