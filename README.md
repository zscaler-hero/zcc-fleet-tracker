# ZCC Fleet Tracker

Zscaler Client Connector fleet health dashboard generator. Processes ZCC Portal CSV exports and produces an interactive HTML dashboard with optional PDF export.

Built for incident response and fleet migration tracking — identifies which ZCC versions, policies, and configurations are causing service health issues.

## Key Features

- **Version Health Matrix** — service health (ZIA/ZPA/ZDX) broken down by ZCC version
- **Multi-snapshot comparison** — track fleet metrics across multiple export dates
- **Security posture** — bypass risk, blind spots, ghost machines, quarantined devices
- **Policy analysis** — health breakdown by assigned policy
- **Device staleness** — age distribution of last-seen timestamps
- **Zero dependencies** — Python 3.8+ standard library only
- **Interactive HTML** — D3.js charts with tooltips, responsive layout
- **PDF export** — via headless Chrome (optional)

## Quick Start

### 1. Export CSVs from ZCC Portal

From the Zscaler admin portal, export:
- **Service Status Export** (`service_status_export_*.csv`)
- **Device Export** (`device_export_*.csv`)

### 2. Generate Dashboard

```bash
# Single snapshot (pair of CSVs)
python3 generate_dashboard.py service_status_export.csv device_export.csv

# Auto-detect all snapshot pairs in a directory
python3 generate_dashboard.py /path/to/csv/directory/

# Multiple snapshots for temporal comparison
python3 generate_dashboard.py s1_svc.csv s1_dev.csv s2_svc.csv s2_dev.csv

# Custom output path
python3 generate_dashboard.py *.csv --out ~/Desktop/fleet_report.html

# Skip PDF / skip browser open
python3 generate_dashboard.py *.csv --no-pdf --no-open
```

### 3. View

The dashboard opens automatically in your browser. Share the HTML file directly or export as PDF.

## Requirements

- **Python 3.8+** (standard library only — no pip install needed)
- **Google Chrome or Chromium** (optional, for PDF export)
- **Internet connection** (for loading D3.js and fonts from CDN; works offline after first load)

## Dashboard Sections

| Section | What it shows |
|---------|--------------|
| **KPI Cards** | Total fleet, ZIA/ZPA/ZDX active %, versions in play, ghost machines |
| **Security Alerts** | Bypass risk, live blind spots, ghost machines, quarantined |
| **Version Health Matrix** | Health % by ZCC version — the key analysis for upgrade/revert issues |
| **Fleet Composition** | Version distribution bar chart |
| **Service Health** | ZIA/ZPA/ZDX donut charts |
| **Policy Health** | Health by policy (ZIA + ZPA) |
| **Device Staleness** | Last-seen age distribution |
| **Tunnel Protocol** | Tunnel 1.0 vs 2.0 split |
| **Registration State** | Registered / Quarantined / Remove Pending |
| **Revert Status** | ZCC revert tracking |
| **Temporal Comparison** | Health + version trends across snapshots (multi-snapshot only) |

## Multi-Snapshot Workflow

For tracking an upgrade/revert cycle:

1. Export CSVs before the change
2. Export CSVs after the upgrade
3. Export CSVs after the revert
4. Put all exports in one directory
5. Run: `python3 generate_dashboard.py /path/to/exports/`

The temporal section will show how fleet composition and health changed over time.

## Part of ZHERO

This tool is part of the [ZHERO](https://github.com/zhero-tools) toolkit — open-source tools for Zscaler operations and security.
