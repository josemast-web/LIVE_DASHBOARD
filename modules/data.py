# modules/data.py
import os
import streamlit as st
import pandas as pd
import gspread
import pytz
from datetime import datetime
import logging
from config import Config
from google.oauth2 import service_account
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SESSIONS_SHEET_NAME = "Sessions_Log"


def get_colombia_now_naive():
    """Return current Colombia time without timezone info for date comparisons."""
    try:
        co_tz = pytz.timezone("America/Bogota")
        return datetime.now(co_tz).replace(tzinfo=None)
    except Exception as e:
        logger.error("Error getting Colombia time: %s", e)
        return datetime.now().replace(tzinfo=None)


def create_gspread_client(creds_dict, max_retries=3):
    """Create an authenticated gspread client with exponential-backoff retry."""
    for attempt in range(max_retries):
        try:
            # Fix escaped newlines in private key (common when passing JSON via env var)
            if "private_key" in creds_dict:
                creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            credentials = service_account.Credentials.from_service_account_info(
                creds_dict, scopes=scopes
            )
            gc = gspread.authorize(credentials)
            logger.info("gspread client created successfully")
            return gc

        except Exception as e:
            logger.warning("Auth attempt %d/%d failed: %s", attempt + 1, max_retries, str(e))
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise Exception(
                    f"Failed to create gspread client after {max_retries} attempts: {str(e)}"
                )


def fetch_sheet_data(gc, max_retries=3):
    """Fetch all values from the configured Google Sheet worksheet."""
    for attempt in range(max_retries):
        try:
            sh = gc.open_by_key(Config.SHEET_ID)
            try:
                worksheet = sh.worksheet(Config.WORKSHEET_NAME)
            except gspread.WorksheetNotFound:
                logger.warning(
                    "Worksheet '%s' not found - falling back to sheet1", Config.WORKSHEET_NAME
                )
                worksheet = sh.sheet1

            values = worksheet.get_all_values()
            logger.info("Fetched %d rows from sheet", len(values))
            return values

        except gspread.exceptions.APIError as e:
            logger.warning(
                "Sheet fetch attempt %d/%d - API error: %s", attempt + 1, max_retries, str(e)
            )
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise Exception(
                    f"Failed to fetch sheet data after {max_retries} attempts: {str(e)}"
                )

        except Exception as e:
            logger.error("Unexpected error fetching sheet: %s", str(e))
            raise


def _hash_dict(d):
    """Stable hash for dict values – improves Streamlit cache hit rate."""
    return str(sorted(d.items()))


@st.cache_data(ttl=600, show_spinner=False, hash_funcs={dict: _hash_dict})
def load_and_process_data(minimal=False):
    """
    Load task data from Google Sheets and apply business logic.

    Args:
        minimal: Load only essential columns (faster for KPI-only views).

    Returns:
        pd.DataFrame with processed task data, or empty DataFrame on error.
    """
    try:
        if "gcp_service_account" not in st.secrets:
            logger.error("Missing GCP credentials in Streamlit secrets")
            st.error("Missing GCP credentials in secrets")
            return pd.DataFrame()

        creds_dict = dict(st.secrets["gcp_service_account"])
        gc         = create_gspread_client(creds_dict)
        values     = fetch_sheet_data(gc)

        if len(values) < 2:
            logger.warning("Sheet has no data rows")
            return pd.DataFrame()

        headers = [h.strip() for h in values[0]]
        df      = pd.DataFrame(values[1:], columns=headers)
        logger.info("DataFrame created: %d rows, %d columns", len(df), len(df.columns))

        # Required columns based on load mode
        if minimal:
            required_cols = ["Tarea", "Responsable", "Estado", "Fecha Fin", "Proyecto Vinculado"]
        else:
            required_cols = [
                "Tarea", "Responsable", "Proyecto Vinculado", "Estado",
                "Fecha Inicio", "Fecha Fin", "Especialidad", "Modulo",
                "Prioridad", "Duracion Estimada", "Horas Registradas",
            ]

        for col in required_cols:
            if col not in df.columns:
                df[col] = ""
                logger.warning("Added missing column: %s", col)

        # Parse dates
        df["Fecha Inicio"]     = pd.to_datetime(df["Fecha Inicio"], errors="coerce")
        df["Fecha Fin"]        = pd.to_datetime(df["Fecha Fin"],    errors="coerce")
        df["Horas Registradas"] = pd.to_numeric(df["Horas Registradas"], errors="coerce").fillna(0)

        # Completion status from keyword matching
        done_keywords = ["Listo", "Done", "Completado", "Cerrado", "Terminado", "Finished"]
        pattern       = "|".join(done_keywords)
        df["Is_Done"] = df["Estado"].astype(str).str.contains(pattern, case=False, na=False)

        # Delay and days-remaining calculation
        now_naive = get_colombia_now_naive()

        def check_logic(row):
            is_late = (
                pd.notna(row["Fecha Fin"]) and not row["Is_Done"]
                and row["Fecha Fin"] < now_naive
            )
            days = None
            if pd.notna(row["Fecha Fin"]) and not row["Is_Done"]:
                d    = (row["Fecha Fin"] - now_naive).days
                days = max(-365, min(365, d))
            return pd.Series([is_late, days])

        df[["Atrasado", "Dias_Restantes"]] = df.apply(check_logic, axis=1)

        # Normalise text fields
        df["Proyecto Vinculado"] = df["Proyecto Vinculado"].str.strip()
        df["Responsable"]        = df["Responsable"].str.strip()

        logger.info(
            "Data processed: %d rows, %d completed", len(df), int(df["Is_Done"].sum())
        )
        return df

    except Exception as e:
        logger.error("Error in load_and_process_data: %s", str(e), exc_info=True)
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame()


def calculate_project_progress(df):
    """
    Aggregate task counts and hours per project.

    Returns:
        pd.DataFrame sorted by completion percentage (descending).
    """
    if df.empty:
        return pd.DataFrame()

    df_projects = df[df["Proyecto Vinculado"].str.strip() != ""].copy()
    if df_projects.empty:
        return pd.DataFrame()

    progress = df_projects.groupby("Proyecto Vinculado").agg(
        Total       =("Tarea",            "count"),
        Completadas =("Is_Done",          "sum"),
        Atrasadas   =("Atrasado",         "sum"),
        Horas_Totales=("Horas Registradas", "sum"),
    ).reset_index()

    progress["Horas_Totales"] = progress["Horas_Totales"].round(2)
    progress["Porcentaje"]    = (progress["Completadas"] / progress["Total"] * 100).round(1)
    progress["Pendientes"]    = progress["Total"] - progress["Completadas"]
    progress                  = progress.sort_values("Porcentaje", ascending=False)

    logger.info("Calculated progress for %d projects", len(progress))
    return progress


@st.cache_data(ttl=600, show_spinner=False, hash_funcs={dict: _hash_dict})
def load_sessions_data():
    """
    Load the Sessions_Log worksheet into a clean DataFrame.
    Returns an empty DataFrame (not an error) if the sheet does not exist.

    Columns: item_name, grupo, proyecto, responsable,
             session_id, status, started_at, ended_at, duration_h, user_id, manual
    """
    try:
        if "gcp_service_account" not in st.secrets:
            logger.error("Missing GCP credentials")
            return pd.DataFrame()

        creds_dict = dict(st.secrets["gcp_service_account"])
        gc         = create_gspread_client(creds_dict)
        sh         = gc.open_by_key(Config.SHEET_ID)

        try:
            ws = sh.worksheet(SESSIONS_SHEET_NAME)
        except Exception:
            logger.warning("Sessions_Log worksheet not found - ETL may not have run yet")
            return pd.DataFrame()

        values = ws.get_all_values()
        if len(values) < 2:
            logger.warning("Sessions_Log has no data rows")
            return pd.DataFrame()

        headers = [h.strip() for h in values[0]]
        df      = pd.DataFrame(values[1:], columns=headers)

        for col in ("started_at", "ended_at"):
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce").dt.tz_localize(None)

        df["duration_h"]  = pd.to_numeric(df["duration_h"], errors="coerce").fillna(0)
        df["responsable"] = df["responsable"].fillna("").str.strip()
        df["proyecto"]    = df["proyecto"].fillna("").str.strip()

        # Drop incomplete sessions (timer still running)
        df = df[df["duration_h"] > 0].copy()

        logger.info("Sessions loaded: %d rows", len(df))
        return df

    except Exception as e:
        logger.error("Error loading sessions: %s", str(e), exc_info=True)
        return pd.DataFrame()


@st.cache_data(ttl=300)
def get_summary_stats(df):
    """Return quick summary statistics dict for KPI cards."""
    if df.empty:
        return {}
    return {
        "total_tasks":         len(df),
        "completed":           int(df["Is_Done"].sum()),
        "delayed":             int(df["Atrasado"].sum()),
        "unique_projects":     df["Proyecto Vinculado"].nunique(),
        "unique_responsables": df["Responsable"].nunique(),
        "completion_rate":     (df["Is_Done"].sum() / len(df) * 100) if len(df) > 0 else 0,
    }
