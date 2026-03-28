# config.py
import os
import streamlit as st

class Config:
    # Google Sheets - loaded from Streamlit secrets or env vars
    SHEET_ID = st.secrets.get("google_sheet_id", os.environ.get("GOOGLE_SHEET_ID"))
    WORKSHEET_NAME = st.secrets.get("worksheet_name", "Tabla_1")

    # UI color palette
    COLORS = {
        'primary':        '#1565C0',
        'secondary':      '#2E7D32',
        'warning':        '#EF6C00',
        'danger':         '#C62828',
        'info':           '#00838F',
        'success':        '#388E3C',
        'bg_light':       '#F5F5F5',
        'bg_card':        '#FFFFFF',
        'text_primary':   '#212121',
        'text_secondary': '#757575',
        'border':         '#E0E0E0',
    }
