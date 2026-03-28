"""
etl.py  –  Monday.com -> Google Sheets sync pipeline

Reads tasks from a Monday.com board via GraphQL, processes them into a
clean DataFrame, and writes two worksheets:
  - Sheet1 (Tabla_1): one row per task
  - Sessions_Log:     one row per time-tracking session

Environment variables required:
  MONDAY_KEY                 Monday.com API token
  GOOGLE_SHEET_ID            Target Google Spreadsheet ID
  GOOGLE_SHEETS_CREDENTIALS  Service-account JSON (string)
  MONDAY_BOARD_ID            Monday.com board to sync (integer as string)
"""

import os
import sys
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
from datetime import datetime
import time

# ---------------------------------------------------------------------------
# CONFIG – all identifiers come from environment variables; no hard-coded IDs
# ---------------------------------------------------------------------------
BOARD_ID = os.environ.get("MONDAY_BOARD_ID", "YOUR_BOARD_ID_HERE")
SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")

# Monday.com column IDs – override via env vars to match your board schema
COL_PROJECTS      = os.environ.get("MONDAY_COL_PROJECTS",      "board_relation_XXXXX")
COL_TIME_TRACKING = os.environ.get("MONDAY_COL_TIME_TRACKING", "duration_XXXXX")
COL_PEOPLE        = os.environ.get("MONDAY_COL_PEOPLE",        "multiple_person_XXXXX")
COL_STATUS        = os.environ.get("MONDAY_COL_STATUS",        "color_XXXXX")
COL_PRIORITY      = os.environ.get("MONDAY_COL_PRIORITY",      "color_YYYYY")
COL_EST_DURATION  = os.environ.get("MONDAY_COL_EST_DURATION",  "numeric_XXXXX")
COL_TIMELINE      = os.environ.get("MONDAY_COL_TIMELINE",      "timerange_XXXXX")
COL_MODULE        = os.environ.get("MONDAY_COL_MODULE",        "dropdown_XXXXX")
COL_SPECIALTY     = os.environ.get("MONDAY_COL_SPECIALTY",     "dropdown_YYYYY")
COL_COMMENTS      = os.environ.get("MONDAY_COL_COMMENTS",      "long_text_XXXXX")

# Human-readable column mapping (Monday column ID -> spreadsheet header)
COLUMN_MAPPING = {
    "name":           "Tarea",
    "group":          "Grupo / Fase",
    COL_STATUS:       "Estado",
    COL_PEOPLE:       "Responsable",
    COL_PRIORITY:     "Prioridad",
    COL_EST_DURATION: "Duracion Estimada",
    COL_TIMELINE:     "Timeline",
    COL_PROJECTS:     "Proyecto Vinculado",
    COL_MODULE:       "Modulo",
    COL_SPECIALTY:    "Especialidad",
    COL_COMMENTS:     "Comentarios",
    COL_TIME_TRACKING:"Horas Registradas",   # seconds -> hours
}

SESSIONS_SHEET_NAME = "Sessions_Log"


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
def log(msg, level="INFO"):
    """Simple structured logger without Unicode or emoji characters."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = {
        "INFO":    "[INFO]",
        "SUCCESS": "[SUCCESS]",
        "WARNING": "[WARNING]",
        "ERROR":   "[ERROR]",
    }.get(level, "[LOG]")
    print(f"{timestamp} {prefix} {msg}")
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# MONDAY.COM DATA EXTRACTION (paginated with cursor)
# ---------------------------------------------------------------------------
def get_monday_data(max_retries=3):
    """Fetch all items from the configured Monday.com board using cursor-based pagination."""
    url     = "https://api.monday.com/v2"
    headers = {
        "Authorization": os.environ["MONDAY_KEY"],
        "API-Version":   "2023-10",
    }

    all_items  = []
    cursor     = None
    page_count = 1

    while True:
        page_args = "limit: 500"
        if cursor:
            page_args = f'limit: 500, cursor: "{cursor}"'

        query = """
        {
          boards(ids: %s) {
            items_page(%s) {
              cursor
              items {
                name
                group { title }
                column_values {
                  id
                  text
                  ... on BoardRelationValue { linked_items { name } }
                  ... on MirrorValue { display_value }
                  ... on TimeTrackingValue {
                    duration
                    history {
                      id
                      status
                      started_at
                      ended_at
                      started_user_id
                      ended_user_id
                      manually_entered_start_date
                      manually_entered_end_date
                    }
                  }
                }
              }
            }
          }
        }
        """ % (BOARD_ID, page_args)

        success = False
        for attempt in range(max_retries):
            try:
                log(f"Fetching page {page_count} (attempt {attempt + 1})...")
                response = requests.post(
                    url, json={"query": query}, headers=headers, timeout=30
                )

                if response.status_code != 200:
                    raise Exception(
                        f"Monday API error: Status {response.status_code}, "
                        f"Response: {response.text}"
                    )

                data = response.json()

                if "errors" in data:
                    raise Exception(f"GraphQL errors: {data['errors']}")

                items_page   = data["data"]["boards"][0]["items_page"]
                current_items = items_page["items"]
                all_items.extend(current_items)
                cursor = items_page.get("cursor")

                log(
                    f"Page {page_count}: {len(current_items)} items | "
                    f"Total: {len(all_items)}",
                    "INFO",
                )
                success = True
                break

            except requests.exceptions.Timeout:
                log(f"Request timeout on attempt {attempt + 1}", "WARNING")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise Exception("Max retries exceeded - Monday API timeout")

            except Exception as e:
                log(f"Error on attempt {attempt + 1}: {str(e)}", "ERROR")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise

        if not success:
            raise Exception(f"Failed to fetch page {page_count}")

        if not cursor:
            break

        page_count += 1

    log(f"Fetched a total of {len(all_items)} items from Monday.com", "SUCCESS")
    return all_items


# ---------------------------------------------------------------------------
# DATA PROCESSING
# ---------------------------------------------------------------------------
def process_data(items):
    """Convert Monday.com item list into a clean DataFrame."""
    clean_rows = []
    log(f"Processing {len(items)} items...")

    for idx, item in enumerate(items):
        try:
            row = {
                "Tarea":       item["name"],
                "Grupo / Fase": item["group"]["title"],
            }

            col_map = {col["id"]: col for col in item["column_values"]}

            for mon_id, sheet_col in COLUMN_MAPPING.items():
                if mon_id in ("name", "group"):
                    continue

                col_data    = col_map.get(mon_id)
                final_value = ""

                if col_data:
                    if mon_id == COL_PROJECTS:
                        # Board-relation column: extract linked item names
                        linked = col_data.get("linked_items") or []
                        final_value = (
                            ", ".join(p["name"] for p in linked)
                            if linked
                            else col_data.get("text", "")
                        )
                    elif mon_id == COL_TIME_TRACKING:
                        # Convert seconds to hours
                        secs = col_data.get("duration")
                        final_value = round(secs / 3600, 2) if secs else 0
                    else:
                        final_value = col_data.get("text", "") or col_data.get("display_value", "")

                row[sheet_col] = final_value if final_value is not None else ""

            # Split timeline string "YYYY-MM-DD - YYYY-MM-DD" into two columns
            timeline_str = row.get("Timeline", "")
            if timeline_str and " - " in timeline_str:
                start, end = timeline_str.split(" - ", 1)
                row["Fecha Inicio"] = start
                row["Fecha Fin"]    = end
            else:
                row["Fecha Inicio"] = ""
                row["Fecha Fin"]    = ""

            clean_rows.append(row)

        except Exception as e:
            log(
                f"Error processing item {idx + 1} "
                f"('{item.get('name', 'Unknown')}'): {str(e)}",
                "WARNING",
            )

    log(f"Processed {len(clean_rows)} rows successfully", "SUCCESS")
    return pd.DataFrame(clean_rows)


# ---------------------------------------------------------------------------
# GOOGLE SHEETS UPLOAD
# ---------------------------------------------------------------------------
def upload_to_sheets(df, client, max_retries=3):
    """Upload main task DataFrame to sheet1."""
    if not SHEET_ID:
        raise ValueError("GOOGLE_SHEET_ID environment variable not set")

    for attempt in range(max_retries):
        try:
            log(f"Uploading main sheet (attempt {attempt + 1}/{max_retries})...")
            sheet = client.open_by_key(SHEET_ID).sheet1
            df    = df.fillna("")
            data  = [df.columns.values.tolist()] + df.values.tolist()
            sheet.clear()
            sheet.update(range_name="A1", values=data)
            log(f"Uploaded {len(df)} rows to sheet1", "SUCCESS")
            return

        except gspread.exceptions.APIError as e:
            log(f"Google Sheets API error (attempt {attempt + 1}): {str(e)}", "WARNING")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise Exception(f"Max retries exceeded - Sheets API: {str(e)}")

        except Exception as e:
            log(f"Upload error (attempt {attempt + 1}): {str(e)}", "ERROR")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise


# ---------------------------------------------------------------------------
# SESSIONS PROCESSING
# ---------------------------------------------------------------------------
def process_sessions(items):
    """
    Flatten TimeTrackingValue history into one row per session.
    Returns a DataFrame with columns:
      item_id, item_name, grupo, proyecto, responsable,
      session_id, status, started_at, ended_at,
      duration_h, user_id, manual
    """
    rows = []
    log(f"Extracting sessions from {len(items)} items...")

    for item in items:
        item_name = item.get("name", "")
        group     = item.get("group", {}).get("title", "")
        col_map   = {col["id"]: col for col in item.get("column_values", [])}

        # Project name from board-relation column
        proj_col = col_map.get(COL_PROJECTS)
        proyecto = ""
        if proj_col:
            linked   = proj_col.get("linked_items") or []
            proyecto = ", ".join(p["name"] for p in linked) if linked else proj_col.get("text", "")

        # Responsible from people column
        people_col   = col_map.get(COL_PEOPLE)
        responsable  = people_col.get("text", "") if people_col else ""

        # Time tracking history
        tt_col  = col_map.get(COL_TIME_TRACKING)
        if not tt_col:
            continue

        history = tt_col.get("history") or []
        if not history:
            continue

        for session in history:
            started_str = session.get("started_at") or ""
            ended_str   = session.get("ended_at")   or ""

            try:
                started_dt = (
                    datetime.fromisoformat(started_str.replace("Z", "+00:00"))
                    if started_str else None
                )
            except ValueError:
                started_dt = None

            try:
                ended_dt = (
                    datetime.fromisoformat(ended_str.replace("Z", "+00:00"))
                    if ended_str else None
                )
            except ValueError:
                ended_dt = None

            duration_h = (
                round((ended_dt - started_dt).total_seconds() / 3600, 4)
                if started_dt and ended_dt else 0.0
            )

            rows.append({
                "item_name":   item_name,
                "grupo":       group,
                "proyecto":    proyecto,
                "responsable": responsable,
                "session_id":  session.get("id", ""),
                "status":      session.get("status", ""),
                "started_at":  started_str,
                "ended_at":    ended_str,
                "duration_h":  duration_h,
                "user_id":     session.get("started_user_id", ""),
                "manual":      session.get("manually_entered_start_date", False),
            })

    df = pd.DataFrame(rows)
    log(f"Extracted {len(df)} sessions from {len(items)} items", "SUCCESS")
    return df


# ---------------------------------------------------------------------------
# SESSIONS UPLOAD
# ---------------------------------------------------------------------------
def upload_sessions_to_sheets(df_sessions, client, max_retries=3):
    """Write sessions DataFrame to the Sessions_Log worksheet (creates it if missing)."""
    if df_sessions.empty:
        log("No sessions to upload - skipping Sessions_Log", "WARNING")
        return

    spreadsheet = client.open_by_key(SHEET_ID)

    try:
        ws = spreadsheet.worksheet(SESSIONS_SHEET_NAME)
        log(f"Worksheet '{SESSIONS_SHEET_NAME}' found - will overwrite")
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=SESSIONS_SHEET_NAME, rows=5000, cols=15)
        log(f"Worksheet '{SESSIONS_SHEET_NAME}' created")

    for attempt in range(max_retries):
        try:
            df_sessions = df_sessions.fillna("")
            data = [df_sessions.columns.values.tolist()] + df_sessions.values.tolist()
            ws.clear()
            ws.update(range_name="A1", values=data)
            log(f"Sessions_Log uploaded: {len(df_sessions)} rows", "SUCCESS")
            return

        except gspread.exceptions.APIError as e:
            log(f"Sheets API error (attempt {attempt + 1}): {str(e)}", "WARNING")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise Exception(f"Max retries exceeded uploading sessions: {str(e)}")

        except Exception as e:
            log(f"Sessions upload error (attempt {attempt + 1}): {str(e)}", "ERROR")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise


# ---------------------------------------------------------------------------
# GOOGLE SHEETS CLIENT
# ---------------------------------------------------------------------------
def get_sheets_client():
    """Build and return an authenticated gspread client from env credentials."""
    creds_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
    if not creds_json:
        raise ValueError("GOOGLE_SHEETS_CREDENTIALS env var not set")

    creds_dict = json.loads(creds_json)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)


# ---------------------------------------------------------------------------
# DATA VALIDATION
# ---------------------------------------------------------------------------
def validate_data(df):
    """Basic sanity check on the processed DataFrame."""
    log("Validating data...")

    if df.empty:
        raise ValueError("DataFrame is empty after processing")

    required_cols = ["Tarea", "Responsable", "Estado", "Proyecto Vinculado"]
    missing_cols  = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        log(f"Warning: missing expected columns: {missing_cols}", "WARNING")

    empty_tasks = df["Tarea"].str.strip().eq("").sum()
    if empty_tasks > 0:
        log(f"Warning: {empty_tasks} rows have empty 'Tarea' field", "WARNING")

    log(f"Validation complete: {len(df)} rows, {len(df.columns)} columns", "SUCCESS")
    log(f"  Unique responsables : {df['Responsable'].nunique()}")
    log(f"  Unique projects     : {df['Proyecto Vinculado'].nunique()}")
    log(f"  Unique states       : {df['Estado'].nunique()}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    start_time = time.time()

    try:
        log("=" * 60)
        log("Starting ETL: Monday.com -> Google Sheets")
        log("=" * 60)

        required_env = ["MONDAY_KEY", "MONDAY_BOARD_ID", "GOOGLE_SHEET_ID", "GOOGLE_SHEETS_CREDENTIALS"]
        missing_env  = [v for v in required_env if not os.environ.get(v)]
        if missing_env:
            raise ValueError(f"Missing required env vars: {', '.join(missing_env)}")

        log("Environment variables validated", "SUCCESS")

        sheets_client = get_sheets_client()
        log("Google Sheets client authenticated", "SUCCESS")

        # Extract – shared across both pipelines
        all_items = get_monday_data()

        # Pipeline 1: main tasks sheet
        df_clean = process_data(all_items)
        validate_data(df_clean)
        upload_to_sheets(df_clean, sheets_client)

        # Pipeline 2: sessions log
        log("Starting sessions extraction pipeline...")
        df_sessions = process_sessions(all_items)
        upload_sessions_to_sheets(df_sessions, sheets_client)

        elapsed = time.time() - start_time
        log("=" * 60)
        log(f"ETL completed successfully in {elapsed:.2f}s", "SUCCESS")
        log("=" * 60)
        sys.exit(0)

    except Exception as e:
        elapsed = time.time() - start_time
        log("=" * 60)
        log(f"ETL failed after {elapsed:.2f}s", "ERROR")
        log(f"Error: {str(e)}", "ERROR")
        log("=" * 60)
        sys.exit(1)
