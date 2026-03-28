# Operations Dashboard — Monday.com + Google Sheets + Streamlit

Disclaimer: This repository is the public version of an original repository that contains private information.

A real-time operations dashboard that syncs task data from **Monday.com** to **Google Sheets** via a scheduled ETL pipeline, then visualises it through a **Streamlit** web application.

Built for production use in a manufacturing/engineering environment to track multi-project task status, team workload, and time-tracking metrics.

---

## Architecture

```
Monday.com Board
      |
      | GraphQL API (paginated cursor)
      v
  etl.py  ──────────────────────────────> Google Sheets
  (GitHub Actions / cron)                  Tabla_1 (tasks)
                                           Sessions_Log (time entries)
                                                |
                                                | gspread
                                                v
                                        Streamlit Dashboard
                                        ├── dashboard.py  (main board)
                                        └── pages/analytics.py (analytics)
```

### ETL Pipeline (`etl.py`)
- Fetches all board items using cursor-based pagination (handles boards with 500+ tasks)
- Maps Monday.com column IDs to human-readable headers via `COLUMN_MAPPING`
- Writes two worksheets: main task sheet and a granular `Sessions_Log` from time-tracking history
- Retry logic with exponential backoff for both Monday.com API and Google Sheets API calls
- Triggered via GitHub Actions `workflow_dispatch` or an external cron service

### Dashboard (`dashboard.py`)
- Live KPI cards: total tasks, team size, delayed tasks, completion rate, active projects
- Project progress cards with visual progress bars and logged hours
- Hierarchical task sorting: by due date, priority, days remaining, or project
- Per-responsible task columns with overdue/at-risk highlighting
- Excel export of filtered data

### Analytics (`pages/analytics.py`)
- Activity heatmap: hours by project × last 4 weeks
- Hours vs 40h/week target per team member (configurable cohort)
- Weekly completion trend chart
- Progress donut by status
- Bottleneck & workload distribution charts

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data source | Monday.com GraphQL API v2023-10 |
| Storage | Google Sheets (gspread) |
| Auth | Google Service Account (google-auth) |
| Frontend | Streamlit >= 1.35 |
| Charts | Plotly >= 5.15 |
| ETL scheduler | GitHub Actions (workflow_dispatch + external cron) |
| Runtime | Python 3.11 |

---

## Setup

### 1. Clone and install dependencies

```bash
git clone https://github.com/your-username/your-repo.git
cd your-repo
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure environment variables

```bash
cp .env.example .env
# Edit .env with your values (see variable reference below)
```

For the Streamlit app, also create the secrets file:

```bash
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Fill in your GCP service account credentials
```

### 3. Find your Monday.com column IDs

Column IDs are board-specific. Retrieve them via the Monday.com API:

```graphql
{
  boards(ids: YOUR_BOARD_ID) {
    columns { id title type }
  }
}
```

Run this query at [monday.com/developers/v2](https://monday.com/developers/v2) and map the IDs to the env vars in `.env`.

### 4. Run the ETL locally

```bash
python etl.py
```

### 5. Run the dashboard

```bash
streamlit run dashboard.py
```

---

## Environment Variables Reference

### Monday.com

| Variable | Description |
|---|---|
| `MONDAY_KEY` | API token (Admin > API) |
| `MONDAY_BOARD_ID` | Board ID from the URL |
| `MONDAY_COL_PROJECTS` | Board-relation column ID (linked projects) |
| `MONDAY_COL_TIME_TRACKING` | Time tracking column ID |
| `MONDAY_COL_PEOPLE` | People/assignee column ID |
| `MONDAY_COL_STATUS` | Status column ID |
| `MONDAY_COL_PRIORITY` | Priority column ID |
| `MONDAY_COL_EST_DURATION` | Estimated duration column ID |
| `MONDAY_COL_TIMELINE` | Timeline date-range column ID |
| `MONDAY_COL_MODULE` | Module dropdown column ID |
| `MONDAY_COL_SPECIALTY` | Specialty dropdown column ID |
| `MONDAY_COL_COMMENTS` | Long-text comments column ID |

### Google Sheets

| Variable | Description |
|---|---|
| `GOOGLE_SHEET_ID` | Spreadsheet ID from the URL |
| `GOOGLE_SHEETS_CREDENTIALS` | Service account JSON as a single-line string (for ETL) |

### Dashboard display

| Variable | Description | Example |
|---|---|---|
| `DEFAULT_RESPONSABLES` | Pre-selected team members in sidebar | `Alice,Bob,Carol` |
| `PROJECT_PRIORITY_ORDER` | Comma-separated project names, highest priority first | `Project A,Project B` |
| `HOURS_COHORT` | Team members tracked in the hours-vs-target section | `Alice,Bob,Carol,Dan` |

---

## Deployment

### Streamlit Cloud
1. Push to GitHub
2. Connect repo at [share.streamlit.io](https://share.streamlit.io)
3. Add `gcp_service_account`, `google_sheet_id`, and `worksheet_name` under **Secrets**

### GitHub Actions (ETL scheduler)
Add all variables from the **Monday.com** and **Google Sheets** sections as **repository secrets** (`Settings > Secrets and variables > Actions`).

The workflow is configured for manual dispatch (`workflow_dispatch`). To schedule it automatically, uncomment the `schedule` block in `.github/workflows/sync.yml`.

---

## Project Structure

```
.
├── dashboard.py              # Main Streamlit page (task board)
├── etl.py                    # ETL pipeline: Monday.com -> Google Sheets
├── config.py                 # Shared config and color palette
├── requirements.txt
├── .env.example              # Environment variable reference
├── modules/
│   ├── data.py               # Data loading, caching, business logic
│   └── ui.py                 # Reusable Streamlit UI components
├── pages/
│   └── analytics.py          # Analytics page (charts, heatmaps, trends)
├── .streamlit/
│   ├── config.toml           # Streamlit theme config
│   └── secrets.toml.example  # Secrets template for local dev
├── .github/
│   └── workflows/
│       └── sync.yml          # GitHub Actions ETL workflow
└── .devcontainer/
    └── devcontainer.json     # GitHub Codespaces config
```

---

## Key Design Decisions

- **Cursor-based pagination** — The Monday.com API limits results to 500 items per request. The ETL uses a cursor loop to reliably fetch boards of any size.
- **Two-worksheet output** — Separating tasks (`Tabla_1`) from sessions (`Sessions_Log`) enables richer time analytics without bloating the main sheet.
- **Shared gspread client** — Both upload functions reuse a single authenticated client to avoid redundant OAuth roundtrips.
- **Env-var driven configuration** — All board-specific IDs, team names, and project lists are injected at runtime, making the codebase fully portable across boards and teams.
- **Streamlit cache with custom hash** — `@st.cache_data` with a `dict` hash function prevents stale cache on credential rotation.
