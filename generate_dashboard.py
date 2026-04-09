#!/usr/bin/env python3
"""
ZCC Fleet Tracker — Zscaler Client Connector Fleet Health Dashboard
Usage:
  python3 generate_dashboard.py /path/to/csv/dir/          # auto-detect all snapshot pairs
  python3 generate_dashboard.py service.csv device.csv      # single snapshot (explicit)
  python3 generate_dashboard.py s1.csv d1.csv s2.csv d2.csv # multi-snapshot comparison
"""

import csv, sys, os, json, glob, argparse, subprocess, webbrowser, shutil, re
from collections import defaultdict, Counter
from datetime import datetime, timedelta

# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Generate ZCC Fleet Health dashboard from Zscaler portal exports",
        epilog="Accepts service_status_export + device_export CSV pairs from the ZCC Portal."
    )
    p.add_argument("paths", nargs="+", help="CSV files (paired) or directory containing exports")
    p.add_argument("--out", "-o", default=None, help="Output HTML path (default: auto-named)")
    p.add_argument("--no-open", action="store_true", help="Don't open report in browser")
    p.add_argument("--no-pdf", action="store_true", help="Skip PDF export")
    return p.parse_args()

# ── Snapshot discovery ────────────────────────────────────────────────────────

def discover_snapshots(paths):
    """Return list of (service_csv, device_csv, label) tuples."""
    if len(paths) == 1 and os.path.isdir(paths[0]):
        return _discover_from_dir(paths[0])
    if len(paths) % 2 != 0:
        sys.exit("ERROR: Provide CSV files in pairs (service_status, device_export) or a directory.")
    pairs = []
    for i in range(0, len(paths), 2):
        svc, dev = paths[i], paths[i + 1]
        if "service" in svc.lower() and "device" in dev.lower():
            pass
        elif "device" in svc.lower() and "service" in dev.lower():
            svc, dev = dev, svc
        else:
            sys.exit(f"ERROR: Cannot determine which file is service/device: {svc}, {dev}")
        pairs.append((svc, dev, _label_from_path(svc)))
    return sorted(pairs, key=lambda x: x[2])


def _discover_from_dir(d):
    svcs = sorted(glob.glob(os.path.join(d, "**", "service_status_export_*.csv"), recursive=True))
    devs = sorted(glob.glob(os.path.join(d, "**", "device_export_*.csv"), recursive=True))
    if not svcs or not devs:
        sys.exit(f"ERROR: No service_status_export / device_export CSVs found in {d}")

    def _ts(f):
        m = re.search(r"(\d{4}_\d{2}_\d{2})", os.path.basename(f))
        return m.group(1) if m else os.path.basename(f)

    svc_map = {_ts(f): f for f in svcs}
    dev_map = {_ts(f): f for f in devs}
    common = sorted(set(svc_map) & set(dev_map))
    if not common:
        if len(svcs) == 1 and len(devs) == 1:
            return [(svcs[0], devs[0], _label_from_path(svcs[0]))]
        sys.exit("ERROR: Cannot match service/device CSV pairs by timestamp.")
    return [(svc_map[k], dev_map[k], k.replace("_", "-")) for k in common]


def _label_from_path(p):
    m = re.search(r"(\d{4}_\d{2}_\d{2})", os.path.basename(p))
    if m:
        return m.group(1).replace("_", "-")
    m = re.search(r"(\d{4}-\d{2}-\d{2})", os.path.basename(p))
    return m.group(1) if m else os.path.basename(p)[:20]

# ── Data loading ──────────────────────────────────────────────────────────────

def load_csv(path):
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def parse_ts(s, fmt="%Y-%m-%d %H:%M:%S GMT"):
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, fmt)
    except ValueError:
        return None

# ── Single-snapshot analysis ──────────────────────────────────────────────────

def analyze_snapshot(svc_rows, dev_rows, label):
    seen_times = [parse_ts(r.get("Last Seen Connected to ZIA", "")) for r in svc_rows]
    seen_times = [t for t in seen_times if t]
    snapshot = max(seen_times) if seen_times else datetime.utcnow()

    d = {
        "label": label,
        "snapshot": snapshot.strftime("%Y-%m-%d %H:%M UTC"),
        "snapshot_short": snapshot.strftime("%d %b %Y"),
        "total_devices": len(dev_rows),
        "total_service": len(svc_rows),
    }

    # ── Device type breakdown ─────────────────────────────────────────────
    dtype_ctr = Counter(r.get("Device type", "Unknown").upper() for r in dev_rows)
    d["device_types"] = dtype_ctr.most_common()

    # ── OS breakdown ──────────────────────────────────────────────────────
    os_ctr = Counter()
    for r in dev_rows:
        osv = r.get("OS Version", "") or ""
        if "windows 11" in osv.lower():
            os_ctr["Windows 11"] += 1
        elif "windows 10" in osv.lower():
            os_ctr["Windows 10"] += 1
        elif "mac" in osv.lower() or "darwin" in osv.lower():
            os_ctr["macOS"] += 1
        elif "linux" in osv.lower():
            os_ctr["Linux"] += 1
        elif osv:
            os_ctr["Other"] += 1
        else:
            os_ctr["Unknown"] += 1
    d["os_breakdown"] = os_ctr.most_common()

    # ── ZCC Version distribution ──────────────────────────────────────────
    ver_ctr = Counter()
    for r in dev_rows:
        v = r.get("Zscaler Client Connector Version", "").strip()
        v = re.sub(r"\s*\(.*\)", "", v)  # strip "(64-bit)" etc.
        ver_ctr[v or "Unknown"] += 1
    d["version_distribution"] = ver_ctr.most_common()

    # ── Version from service status (for health join) ─────────────────────
    svc_ver_ctr = Counter()
    for r in svc_rows:
        v = r.get("Zscaler Client Connector Version", "").strip()
        v = re.sub(r"\s*\(.*\)", "", v)
        svc_ver_ctr[v or "Unknown"] += 1

    # ── Service health overall ────────────────────────────────────────────
    for svc in ("ZIA", "ZPA", "ZDX"):
        enabled = [r for r in svc_rows if r.get(f"{svc} Enabled") == "true"]
        active = sum(1 for r in enabled if r.get(f"{svc} Health") == "Active")
        inactive = len(enabled) - active
        d[f"{svc.lower()}_active"] = active
        d[f"{svc.lower()}_inactive"] = inactive
        d[f"{svc.lower()}_total"] = len(enabled)

    # ── Health by version (key analysis) ──────────────────────────────────
    ver_health = defaultdict(lambda: {"count": 0, "zia_a": 0, "zia_i": 0, "zpa_a": 0, "zpa_i": 0, "zdx_a": 0, "zdx_i": 0})
    for r in svc_rows:
        v = re.sub(r"\s*\(.*\)", "", r.get("Zscaler Client Connector Version", "").strip()) or "Unknown"
        vh = ver_health[v]
        vh["count"] += 1
        for svc in ("ZIA", "ZPA", "ZDX"):
            if r.get(f"{svc} Enabled") == "true":
                if r.get(f"{svc} Health") == "Active":
                    vh[f"{svc.lower()}_a"] += 1
                else:
                    vh[f"{svc.lower()}_i"] += 1

    # Sort by count desc, take top versions
    sorted_vh = sorted(ver_health.items(), key=lambda x: -x[1]["count"])
    d["version_health"] = []
    for v, vh in sorted_vh[:15]:
        total = vh["count"]
        zia_t = vh["zia_a"] + vh["zia_i"]
        zpa_t = vh["zpa_a"] + vh["zpa_i"]
        zdx_t = vh["zdx_a"] + vh["zdx_i"]
        d["version_health"].append({
            "version": v,
            "count": total,
            "zia_pct": round(vh["zia_a"] / zia_t * 100, 1) if zia_t else 0,
            "zpa_pct": round(vh["zpa_a"] / zpa_t * 100, 1) if zpa_t else 0,
            "zdx_pct": round(vh["zdx_a"] / zdx_t * 100, 1) if zdx_t else 0,
            "zia_inactive": vh["zia_i"],
            "zpa_inactive": vh["zpa_i"],
            "zdx_inactive": vh["zdx_i"],
        })

    # ── Policy breakdown ──────────────────────────────────────────────────
    pol_ctr = Counter(r.get("Policy Name", "") or "Unknown" for r in dev_rows)
    d["policy_distribution"] = pol_ctr.most_common(15)

    # ── Health by policy ──────────────────────────────────────────────────
    # Join device + service on UDID to get policy per service row
    udid_policy = {}
    for r in dev_rows:
        udid = r.get("UDID", "")
        if udid:
            udid_policy[udid] = r.get("Policy Name", "") or "Unknown"

    pol_health = defaultdict(lambda: {"count": 0, "zia_a": 0, "zia_i": 0, "zpa_a": 0, "zpa_i": 0})
    for r in svc_rows:
        policy = udid_policy.get(r.get("UDID", ""), "Unknown")
        ph = pol_health[policy]
        ph["count"] += 1
        for svc in ("ZIA", "ZPA"):
            if r.get(f"{svc} Enabled") == "true":
                if r.get(f"{svc} Health") == "Active":
                    ph[f"{svc.lower()}_a"] += 1
                else:
                    ph[f"{svc.lower()}_i"] += 1

    sorted_ph = sorted(pol_health.items(), key=lambda x: -x[1]["count"])
    d["policy_health"] = []
    for p, ph in sorted_ph[:12]:
        zia_t = ph["zia_a"] + ph["zia_i"]
        zpa_t = ph["zpa_a"] + ph["zpa_i"]
        d["policy_health"].append({
            "policy": p,
            "count": ph["count"],
            "zia_pct": round(ph["zia_a"] / zia_t * 100, 1) if zia_t else 0,
            "zpa_pct": round(ph["zpa_a"] / zpa_t * 100, 1) if zpa_t else 0,
        })

    # ── Tunnel version ────────────────────────────────────────────────────
    tun_ctr = Counter()
    for r in dev_rows:
        tv = r.get("Tunnel Version", "") or "Unknown"
        if "2.0" in tv:
            tun_ctr["Tunnel 2.0"] += 1
        elif "1.0" in tv:
            tun_ctr["Tunnel 1.0"] += 1
        else:
            tun_ctr["Unknown/None"] += 1
    d["tunnel_versions"] = tun_ctr.most_common()

    # ── Registration state ────────────────────────────────────────────────
    reg_ctr = Counter(r.get("Registration State", "Unknown") for r in svc_rows)
    d["registration_states"] = reg_ctr.most_common()

    # ── Revert status ─────────────────────────────────────────────────────
    rev_ctr = Counter()
    for r in dev_rows:
        rs = r.get("ZCC Revert Status", "") or "N/A"
        rev_ctr[rs] += 1
    d["revert_status"] = rev_ctr.most_common()

    # ── Device Trust Level ────────────────────────────────────────────────
    trust_ctr = Counter(r.get("Device Trust Level", "") or "Unknown" for r in dev_rows)
    d["trust_levels"] = trust_ctr.most_common()

    # ── Stale device analysis ─────────────────────────────────────────────
    buckets = {"0-1d": 0, "2-7d": 0, "8-30d": 0, "31-90d": 0, "91-180d": 0, "180d+": 0, "Never": 0}
    for r in svc_rows:
        ts = parse_ts(r.get("Last Seen Connected to ZIA", ""))
        if not ts:
            buckets["Never"] += 1
            continue
        age = (snapshot - ts).total_seconds() / 86400
        if age <= 1:
            buckets["0-1d"] += 1
        elif age <= 7:
            buckets["2-7d"] += 1
        elif age <= 30:
            buckets["8-30d"] += 1
        elif age <= 90:
            buckets["31-90d"] += 1
        elif age <= 180:
            buckets["91-180d"] += 1
        else:
            buckets["180d+"] += 1
    d["stale_buckets"] = list(buckets.items())

    # ── Security posture ──────────────────────────────────────────────────
    cutoff_24h = snapshot - timedelta(hours=24)
    cutoff_31d = snapshot - timedelta(days=31)

    bypass_risk = 0
    live_blindspots = 0
    ghost_machines = 0
    quarantined = 0

    for r in svc_rows:
        zia_health = r.get("ZIA Health", "")
        zpa_health = r.get("ZPA Health", "")
        reg_state = r.get("Registration State", "")
        ts = parse_ts(r.get("Last Seen Connected to ZIA", ""))

        if zia_health == "Inactive" and zpa_health == "Active":
            bypass_risk += 1
        if zia_health == "Inactive" and ts and ts >= cutoff_24h:
            live_blindspots += 1
        if reg_state == "Registered" and ts and ts < cutoff_31d:
            ghost_machines += 1
        if reg_state == "Quarantined":
            quarantined += 1

    d["bypass_risk"] = bypass_risk
    d["live_blindspots"] = live_blindspots
    d["ghost_machines"] = ghost_machines
    d["quarantined"] = quarantined

    # ── Installation type ─────────────────────────────────────────────────
    inst_ctr = Counter(r.get("Installation Type", "") or "Unknown" for r in dev_rows)
    d["installation_types"] = inst_ctr.most_common()

    return d

# ── Temporal diff ─────────────────────────────────────────────────────────────

def compute_temporal(snapshots_data):
    """Compare metrics across snapshots for temporal analysis."""
    if len(snapshots_data) < 2:
        return None
    temporal = {"labels": [], "total_devices": [], "zia_pct": [], "zpa_pct": [], "zdx_pct": []}
    version_tracking = {}

    for sd in snapshots_data:
        temporal["labels"].append(sd["snapshot_short"])
        temporal["total_devices"].append(sd["total_devices"])
        temporal["zia_pct"].append(round(sd["zia_active"] / sd["zia_total"] * 100, 1) if sd["zia_total"] else 0)
        temporal["zpa_pct"].append(round(sd["zpa_active"] / sd["zpa_total"] * 100, 1) if sd["zpa_total"] else 0)
        temporal["zdx_pct"].append(round(sd["zdx_active"] / sd["zdx_total"] * 100, 1) if sd["zdx_total"] else 0)

        for v, count in sd["version_distribution"]:
            if v not in version_tracking:
                version_tracking[v] = []
            version_tracking[v].append({"label": sd["snapshot_short"], "count": count})

    # Track top versions across time
    all_ver_counts = Counter()
    for sd in snapshots_data:
        for v, c in sd["version_distribution"]:
            all_ver_counts[v] += c
    top_versions = [v for v, _ in all_ver_counts.most_common(8)]

    temporal["version_series"] = []
    for v in top_versions:
        series = []
        vt = {e["label"]: e["count"] for e in version_tracking.get(v, [])}
        for lbl in temporal["labels"]:
            series.append(vt.get(lbl, 0))
        temporal["version_series"].append({"version": v, "data": series})

    return temporal

# ── HTML Template ─────────────────────────────────────────────────────────────

def generate_html(snapshots_data, temporal_data):
    latest = snapshots_data[-1]
    multi = len(snapshots_data) > 1
    data_json = json.dumps({
        "snapshots": snapshots_data,
        "latest": latest,
        "temporal": temporal_data,
        "multi": multi,
    }, default=str)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ZCC Fleet Tracker — {latest['snapshot_short']}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Chakra+Petch:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600;700&family=DM+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<script src="https://d3js.org/d3.v7.min.js"></script>
<style>
{CSS_TEMPLATE}
</style>
</head>
<body>
<div class="noise"></div>
<header>
  <div class="header-inner">
    <div class="logo">
      <div class="logo-icon">
        <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/>
        </svg>
      </div>
      <div>
        <h1>ZCC Fleet Tracker</h1>
        <div class="subtitle">Zscaler Client Connector Health Dashboard</div>
      </div>
    </div>
    <div class="meta">
      <div class="meta-item">
        <span class="meta-label">Snapshot</span>
        <span class="meta-value">{latest['snapshot']}</span>
      </div>
      <div class="meta-item">
        <span class="meta-label">Devices</span>
        <span class="meta-value">{latest['total_devices']:,}</span>
      </div>
      {"<div class='meta-item'><span class='meta-label'>Snapshots</span><span class='meta-value'>" + str(len(snapshots_data)) + "</span></div>" if multi else ""}
    </div>
  </div>
</header>

<main>
  <!-- KPI Cards -->
  <section class="kpi-row">
    {_kpi_card("Total Fleet", f"{latest['total_devices']:,}", "devices", "neutral")}
    {_kpi_card("ZIA Active", f"{round(latest['zia_active']/latest['zia_total']*100, 1) if latest['zia_total'] else 0}%", f"{latest['zia_active']:,} / {latest['zia_total']:,}", "good" if latest['zia_total'] and latest['zia_active']/latest['zia_total'] > 0.95 else "warn" if latest['zia_total'] and latest['zia_active']/latest['zia_total'] > 0.85 else "bad")}
    {_kpi_card("ZPA Active", f"{round(latest['zpa_active']/latest['zpa_total']*100, 1) if latest['zpa_total'] else 0}%", f"{latest['zpa_active']:,} / {latest['zpa_total']:,}", "good" if latest['zpa_total'] and latest['zpa_active']/latest['zpa_total'] > 0.95 else "warn" if latest['zpa_total'] and latest['zpa_active']/latest['zpa_total'] > 0.85 else "bad")}
    {_kpi_card("ZDX Active", f"{round(latest['zdx_active']/latest['zdx_total']*100, 1) if latest['zdx_total'] else 0}%", f"{latest['zdx_active']:,} / {latest['zdx_total']:,}", "good" if latest['zdx_total'] and latest['zdx_active']/latest['zdx_total'] > 0.95 else "warn" if latest['zdx_total'] and latest['zdx_active']/latest['zdx_total'] > 0.85 else "bad")}
    {_kpi_card("Versions Active", str(len([v for v, c in latest['version_distribution'] if c > 10])), "with 10+ devices", "neutral")}
    {_kpi_card("Ghost Machines", f"{latest['ghost_machines']:,}", "31d+ unseen", "bad" if latest['ghost_machines'] > 500 else "warn" if latest['ghost_machines'] > 100 else "good")}
  </section>

  <!-- Security Posture Alert -->
  <section class="alert-row">
    {_alert_tile("Bypass Risk", latest['bypass_risk'], "ZIA off + ZPA on", "red")}
    {_alert_tile("Live Blind Spots", latest['live_blindspots'], "ZIA off, seen <24h", "amber")}
    {_alert_tile("Ghost Machines", latest['ghost_machines'], "Registered, 31d+ stale", "amber")}
    {_alert_tile("Quarantined", latest['quarantined'], "Compliance failure", "red" if latest['quarantined'] > 50 else "amber")}
  </section>

  <!-- Version Health Matrix — THE key analysis -->
  <section class="card full-width">
    <div class="card-header">
      <h2>Version Health Matrix</h2>
      <p class="card-desc">Service health breakdown by ZCC version — identifies which versions are causing problems</p>
    </div>
    <div id="version-health-chart"></div>
    <div class="table-wrap">
      <table class="data-table" id="version-health-table">
        <thead>
          <tr>
            <th>Version</th><th class="r">Devices</th><th class="r">% Fleet</th>
            <th class="r">ZIA Active</th><th class="r">ZPA Active</th><th class="r">ZDX Active</th>
            <th class="r">ZIA Inactive</th><th class="r">ZPA Inactive</th><th class="r">ZDX Inactive</th>
          </tr>
        </thead>
        <tbody>
          {"".join(_version_health_row(vh, latest['total_service']) for vh in latest['version_health'])}
        </tbody>
      </table>
    </div>
  </section>

  <div class="grid-2">
    <!-- Version Distribution -->
    <section class="card">
      <div class="card-header"><h2>Fleet Composition</h2></div>
      <div id="version-dist-chart"></div>
    </section>

    <!-- Service Health Donuts -->
    <section class="card">
      <div class="card-header"><h2>Service Health</h2></div>
      <div class="donut-row" id="service-donuts"></div>
    </section>
  </div>

  <div class="grid-2">
    <!-- Policy Health -->
    <section class="card">
      <div class="card-header"><h2>Policy Health</h2></div>
      <div id="policy-health-chart"></div>
    </section>

    <!-- Stale Devices -->
    <section class="card">
      <div class="card-header"><h2>Device Staleness</h2></div>
      <div id="stale-chart"></div>
    </section>
  </div>

  <div class="grid-3">
    <!-- Tunnel Versions -->
    <section class="card">
      <div class="card-header"><h2>Tunnel Protocol</h2></div>
      <div id="tunnel-chart"></div>
    </section>

    <!-- Registration State -->
    <section class="card">
      <div class="card-header"><h2>Registration State</h2></div>
      <div id="reg-chart"></div>
    </section>

    <!-- Revert Status -->
    <section class="card">
      <div class="card-header"><h2>Revert Status</h2></div>
      <div id="revert-chart"></div>
    </section>
  </div>

  <div class="grid-2">
    <!-- Trust Levels -->
    <section class="card">
      <div class="card-header"><h2>Device Trust Levels</h2></div>
      <div id="trust-chart"></div>
    </section>

    <!-- OS Breakdown -->
    <section class="card">
      <div class="card-header"><h2>OS Distribution</h2></div>
      <div id="os-chart"></div>
    </section>
  </div>

  <!-- Temporal section (only if multi-snapshot) -->
  <section class="card full-width temporal-section" id="temporal-section" style="display:{'block' if multi else 'none'}">
    <div class="card-header">
      <h2>Temporal Comparison</h2>
      <p class="card-desc">Fleet metrics across {len(snapshots_data)} snapshots</p>
    </div>
    <div class="grid-2">
      <div id="temporal-health-chart"></div>
      <div id="temporal-versions-chart"></div>
    </div>
  </section>

  <!-- Policy breakdown table -->
  <section class="card full-width">
    <div class="card-header"><h2>Policy Breakdown</h2></div>
    <div class="table-wrap">
      <table class="data-table">
        <thead><tr><th>Policy</th><th class="r">Devices</th><th class="r">ZIA Active</th><th class="r">ZPA Active</th></tr></thead>
        <tbody>
          {"".join(_policy_row(ph) for ph in latest['policy_health'])}
        </tbody>
      </table>
    </div>
  </section>
</main>

<footer>
  <div class="footer-inner">
    <span>ZCC Fleet Tracker</span>
    <span class="dot">&#183;</span>
    <span>ZHERO Toolkit</span>
    <span class="dot">&#183;</span>
    <span>Generated {datetime.now().strftime("%Y-%m-%d %H:%M UTC")}</span>
  </div>
</footer>

<script>
const DATA = {data_json};
{JS_TEMPLATE}
</script>
</body>
</html>"""


def _kpi_card(title, value, sub, status):
    cls = {"good": "kpi-good", "warn": "kpi-warn", "bad": "kpi-bad", "neutral": "kpi-neutral"}[status]
    return f"""<div class="kpi-card {cls}">
      <div class="kpi-title">{title}</div>
      <div class="kpi-value">{value}</div>
      <div class="kpi-sub">{sub}</div>
    </div>"""


def _alert_tile(title, count, desc, color):
    cls = f"alert-{color}"
    return f"""<div class="alert-tile {cls}">
      <div class="alert-count">{count:,}</div>
      <div class="alert-title">{title}</div>
      <div class="alert-desc">{desc}</div>
    </div>"""


def _version_health_row(vh, total):
    pct_fleet = round(vh["count"] / total * 100, 1) if total else 0
    return f"""<tr>
      <td class="mono">{vh['version']}</td>
      <td class="r mono">{vh['count']:,}</td>
      <td class="r mono">{pct_fleet}%</td>
      <td class="r {_health_cls(vh['zia_pct'])}">{vh['zia_pct']}%</td>
      <td class="r {_health_cls(vh['zpa_pct'])}">{vh['zpa_pct']}%</td>
      <td class="r {_health_cls(vh['zdx_pct'])}">{vh['zdx_pct']}%</td>
      <td class="r mono dim">{vh['zia_inactive']:,}</td>
      <td class="r mono dim">{vh['zpa_inactive']:,}</td>
      <td class="r mono dim">{vh['zdx_inactive']:,}</td>
    </tr>"""


def _health_cls(pct):
    if pct >= 95:
        return "health-good"
    elif pct >= 85:
        return "health-warn"
    elif pct > 0:
        return "health-bad"
    return "health-na"


def _policy_row(ph):
    return f"""<tr>
      <td>{ph['policy']}</td>
      <td class="r mono">{ph['count']:,}</td>
      <td class="r {_health_cls(ph['zia_pct'])}">{ph['zia_pct']}%</td>
      <td class="r {_health_cls(ph['zpa_pct'])}">{ph['zpa_pct']}%</td>
    </tr>"""

# ── CSS ───────────────────────────────────────────────────────────────────────

CSS_TEMPLATE = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}

:root {
  --bg-deep: #060a10;
  --bg-surface: #0c1018;
  --bg-card: #111827;
  --bg-card-hover: #162032;
  --border: #1e2d3d;
  --border-bright: #2a4060;
  --text-primary: #e2e8f0;
  --text-secondary: #8899aa;
  --text-muted: #4a5a6a;
  --accent-cyan: #00c8f0;
  --accent-green: #00e68a;
  --accent-amber: #ffa800;
  --accent-red: #ff3060;
  --accent-purple: #a78bfa;
  --font-display: 'Chakra Petch', sans-serif;
  --font-body: 'DM Sans', sans-serif;
  --font-mono: 'JetBrains Mono', monospace;
}

html { font-size: 14px; }
body {
  background: var(--bg-deep);
  color: var(--text-primary);
  font-family: var(--font-body);
  line-height: 1.5;
  min-height: 100vh;
  -webkit-font-smoothing: antialiased;
}

.noise {
  position: fixed; top: 0; left: 0; width: 100%; height: 100%;
  background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='0.03'/%3E%3C/svg%3E");
  pointer-events: none; z-index: 0;
}

header {
  position: relative; z-index: 1;
  background: linear-gradient(180deg, #0d1420 0%, var(--bg-deep) 100%);
  border-bottom: 1px solid var(--border);
  padding: 1.5rem 2rem;
}
.header-inner {
  max-width: 1440px; margin: 0 auto;
  display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 1rem;
}
.logo { display: flex; align-items: center; gap: 0.75rem; }
.logo-icon { color: var(--accent-cyan); }
h1 {
  font-family: var(--font-display);
  font-size: 1.6rem; font-weight: 700; letter-spacing: 0.05em;
  text-transform: uppercase; color: var(--text-primary);
}
.subtitle { font-size: 0.75rem; color: var(--text-muted); letter-spacing: 0.1em; text-transform: uppercase; }
.meta { display: flex; gap: 1.5rem; }
.meta-item { display: flex; flex-direction: column; align-items: flex-end; }
.meta-label { font-size: 0.65rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.1em; }
.meta-value { font-family: var(--font-mono); font-size: 0.85rem; color: var(--accent-cyan); font-weight: 600; }

main {
  position: relative; z-index: 1;
  max-width: 1440px; margin: 0 auto; padding: 1.5rem 2rem 3rem;
}

/* KPI Cards */
.kpi-row {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 0.75rem; margin-bottom: 1.25rem;
}
.kpi-card {
  background: var(--bg-card); border: 1px solid var(--border);
  padding: 1rem 1.25rem; position: relative; overflow: hidden;
}
.kpi-card::before {
  content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
}
.kpi-good::before { background: var(--accent-green); }
.kpi-warn::before { background: var(--accent-amber); }
.kpi-bad::before { background: var(--accent-red); }
.kpi-neutral::before { background: var(--accent-cyan); }
.kpi-title { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--text-muted); margin-bottom: 0.25rem; }
.kpi-value { font-family: var(--font-mono); font-size: 1.75rem; font-weight: 700; line-height: 1.1; }
.kpi-good .kpi-value { color: var(--accent-green); }
.kpi-warn .kpi-value { color: var(--accent-amber); }
.kpi-bad .kpi-value { color: var(--accent-red); }
.kpi-neutral .kpi-value { color: var(--text-primary); }
.kpi-sub { font-family: var(--font-mono); font-size: 0.7rem; color: var(--text-muted); margin-top: 0.25rem; }

/* Alert tiles */
.alert-row {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 0.75rem; margin-bottom: 1.25rem;
}
.alert-tile {
  padding: 0.75rem 1rem; border: 1px solid; display: flex; flex-direction: column; gap: 0.1rem;
}
.alert-red { border-color: rgba(255,48,96,0.3); background: rgba(255,48,96,0.05); }
.alert-amber { border-color: rgba(255,168,0,0.3); background: rgba(255,168,0,0.05); }
.alert-count { font-family: var(--font-mono); font-size: 1.4rem; font-weight: 700; }
.alert-red .alert-count { color: var(--accent-red); }
.alert-amber .alert-count { color: var(--accent-amber); }
.alert-title { font-family: var(--font-display); font-size: 0.8rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-primary); }
.alert-desc { font-size: 0.65rem; color: var(--text-muted); }

/* Cards */
.card {
  background: var(--bg-card); border: 1px solid var(--border);
  padding: 1.25rem; margin-bottom: 1.25rem;
}
.card-header { margin-bottom: 1rem; }
.card-header h2 {
  font-family: var(--font-display); font-size: 1rem; font-weight: 600;
  text-transform: uppercase; letter-spacing: 0.06em; color: var(--accent-cyan);
}
.card-desc { font-size: 0.75rem; color: var(--text-muted); margin-top: 0.25rem; }
.full-width { grid-column: 1 / -1; }

/* Grids */
.grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 1.25rem; margin-bottom: 0; }
.grid-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 1.25rem; margin-bottom: 0; }
@media (max-width: 900px) { .grid-2, .grid-3 { grid-template-columns: 1fr; } }

/* Tables */
.table-wrap { overflow-x: auto; }
.data-table {
  width: 100%; border-collapse: collapse; font-size: 0.8rem;
}
.data-table th {
  font-family: var(--font-display); font-size: 0.7rem; font-weight: 600;
  text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-muted);
  padding: 0.5rem 0.75rem; border-bottom: 1px solid var(--border-bright);
  text-align: left; white-space: nowrap;
}
.data-table td {
  padding: 0.4rem 0.75rem; border-bottom: 1px solid var(--border);
  white-space: nowrap;
}
.data-table tbody tr:hover { background: var(--bg-card-hover); }
.r { text-align: right !important; }
.mono { font-family: var(--font-mono); }
.dim { color: var(--text-muted); }
.health-good { font-family: var(--font-mono); font-weight: 600; color: var(--accent-green); }
.health-warn { font-family: var(--font-mono); font-weight: 600; color: var(--accent-amber); }
.health-bad { font-family: var(--font-mono); font-weight: 600; color: var(--accent-red); }
.health-na { font-family: var(--font-mono); color: var(--text-muted); }

/* Donut row */
.donut-row { display: flex; justify-content: space-around; flex-wrap: wrap; gap: 1rem; }
.donut-item { text-align: center; flex: 1; min-width: 140px; }
.donut-label { font-family: var(--font-display); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-muted); margin-top: 0.5rem; }

/* Chart containers */
svg text { fill: var(--text-secondary); font-family: var(--font-mono); font-size: 11px; }
svg .axis-label { font-family: var(--font-display); font-size: 10px; text-transform: uppercase; letter-spacing: 0.05em; fill: var(--text-muted); }
svg .grid-line { stroke: var(--border); stroke-dasharray: 2,3; }

/* Tooltip */
.tooltip {
  position: absolute; pointer-events: none; z-index: 100;
  background: #1a2440; border: 1px solid var(--border-bright);
  padding: 0.5rem 0.75rem; font-family: var(--font-mono); font-size: 0.75rem;
  color: var(--text-primary); max-width: 300px;
  box-shadow: 0 4px 20px rgba(0,0,0,0.5);
}
.tooltip strong { color: var(--accent-cyan); }

/* Footer */
footer {
  position: relative; z-index: 1;
  border-top: 1px solid var(--border); padding: 1rem 2rem;
}
.footer-inner {
  max-width: 1440px; margin: 0 auto;
  display: flex; align-items: center; gap: 0.5rem;
  font-size: 0.7rem; color: var(--text-muted); font-family: var(--font-mono);
}
.dot { color: var(--border-bright); }

/* Print */
@media print {
  body { background: #fff; color: #1a1a1a; }
  .noise { display: none; }
  .card { border-color: #ddd; background: #fff; break-inside: avoid; }
  header { background: #f8f8f8; }
  h1, .card-header h2 { color: #1a1a1a; }
  .kpi-value, .alert-count { color: #1a1a1a !important; }
  .health-good { color: #0a8a50; }
  .health-warn { color: #b07800; }
  .health-bad { color: #c02040; }
  .data-table th, .data-table td { border-color: #ddd; }
  .data-table tbody tr:hover { background: transparent; }
  svg text { fill: #333; }
}
"""

# ── D3 JavaScript ─────────────────────────────────────────────────────────────

JS_TEMPLATE = """
(function() {
  const L = DATA.latest;
  const COLORS = {
    cyan: '#00c8f0', green: '#00e68a', amber: '#ffa800', red: '#ff3060',
    purple: '#a78bfa', blue: '#4e8cff', pink: '#f472b6', teal: '#2dd4bf',
    muted: '#4a5a6a', border: '#1e2d3d', surface: '#111827',
  };
  const PAL = [COLORS.cyan, COLORS.amber, COLORS.green, COLORS.red, COLORS.purple, COLORS.blue, COLORS.pink, COLORS.teal, '#e879f9', '#fb923c', '#6ee7b7', '#93c5fd'];

  const tooltip = d3.select('body').append('div').attr('class', 'tooltip').style('opacity', 0);
  function showTip(event, html) {
    tooltip.html(html).style('opacity', 1)
      .style('left', (event.pageX + 12) + 'px').style('top', (event.pageY - 10) + 'px');
  }
  function hideTip() { tooltip.style('opacity', 0); }

  function healthColor(pct) {
    if (pct >= 95) return COLORS.green;
    if (pct >= 85) return COLORS.amber;
    if (pct > 0) return COLORS.red;
    return COLORS.muted;
  }

  // ── Version Health Chart (grouped bar) ────────────────────────────────
  (function() {
    const data = L.version_health.filter(d => d.count > 20).slice(0, 12);
    if (!data.length) return;

    const container = d3.select('#version-health-chart');
    const W = Math.min(container.node().getBoundingClientRect().width, 1400);
    const H = Math.max(data.length * 42 + 60, 200);
    const M = {top: 30, right: 30, bottom: 10, left: 160};

    const svg = container.append('svg').attr('width', W).attr('height', H);

    const y = d3.scaleBand().domain(data.map(d => d.version)).range([M.top, H - M.bottom]).padding(0.25);
    const x = d3.scaleLinear().domain([0, 100]).range([M.left, W - M.right]);

    // Grid lines
    svg.selectAll('.grid-line').data(x.ticks(5)).join('line')
      .attr('class', 'grid-line').attr('x1', d => x(d)).attr('x2', d => x(d))
      .attr('y1', M.top).attr('y2', H - M.bottom);

    // Labels
    svg.append('g').selectAll('text').data(data).join('text')
      .attr('x', M.left - 8).attr('y', d => y(d.version) + y.bandwidth() / 2)
      .attr('dy', '0.35em').attr('text-anchor', 'end').attr('font-size', '11px')
      .text(d => d.version);

    const services = [
      {key: 'zia_pct', label: 'ZIA', color: COLORS.cyan},
      {key: 'zpa_pct', label: 'ZPA', color: COLORS.green},
      {key: 'zdx_pct', label: 'ZDX', color: COLORS.purple},
    ];

    const barH = y.bandwidth() / 3.5;

    services.forEach((svc, i) => {
      svg.selectAll('.bar-' + svc.key).data(data).join('rect')
        .attr('x', M.left).attr('y', d => y(d.version) + i * (barH + 1))
        .attr('width', d => Math.max(0, x(d[svc.key]) - M.left))
        .attr('height', barH)
        .attr('fill', d => healthColor(d[svc.key]))
        .attr('opacity', 0.85)
        .on('mouseover', function(event, d) {
          d3.select(this).attr('opacity', 1);
          showTip(event, '<strong>' + d.version + '</strong><br>' + svc.label + ': ' + d[svc.key] + '% active (' + d[svc.key.replace('_pct','_inactive')] + ' inactive)');
        })
        .on('mousemove', function(event) {
          tooltip.style('left', (event.pageX + 12) + 'px').style('top', (event.pageY - 10) + 'px');
        })
        .on('mouseout', function() { d3.select(this).attr('opacity', 0.85); hideTip(); });

      // Percentage labels at end of bars
      svg.selectAll('.lbl-' + svc.key).data(data).join('text')
        .attr('x', d => x(d[svc.key]) + 4)
        .attr('y', d => y(d.version) + i * (barH + 1) + barH / 2)
        .attr('dy', '0.35em').attr('font-size', '9px')
        .attr('fill', d => healthColor(d[svc.key]))
        .text(d => d[svc.key] + '%');
    });

    // Legend
    const leg = svg.append('g').attr('transform', 'translate(' + M.left + ', 8)');
    services.forEach((svc, i) => {
      leg.append('rect').attr('x', i * 80).attr('y', 0).attr('width', 10).attr('height', 10).attr('fill', svc.color);
      leg.append('text').attr('x', i * 80 + 14).attr('y', 9).attr('font-size', '10px').text(svc.label);
    });
  })();

  // ── Version Distribution (horizontal bar) ─────────────────────────────
  (function() {
    const raw = L.version_distribution.slice(0, 12);
    if (!raw.length) return;
    const data = raw.map(d => ({label: d[0], value: d[1]}));

    const container = d3.select('#version-dist-chart');
    const W = container.node().getBoundingClientRect().width;
    const H = data.length * 30 + 20;
    const M = {top: 10, right: 60, bottom: 10, left: 140};

    const svg = container.append('svg').attr('width', W).attr('height', H);
    const y = d3.scaleBand().domain(data.map(d => d.label)).range([M.top, H - M.bottom]).padding(0.2);
    const x = d3.scaleLinear().domain([0, d3.max(data, d => d.value)]).range([M.left, W - M.right]);

    svg.selectAll('rect').data(data).join('rect')
      .attr('x', M.left).attr('y', d => y(d.label))
      .attr('width', d => x(d.value) - M.left).attr('height', y.bandwidth())
      .attr('fill', (d, i) => PAL[i % PAL.length]).attr('opacity', 0.8)
      .on('mouseover', function(event, d) { d3.select(this).attr('opacity', 1); showTip(event, '<strong>' + d.label + '</strong><br>' + d.value.toLocaleString() + ' devices'); })
      .on('mousemove', function(event) { tooltip.style('left', (event.pageX+12)+'px').style('top', (event.pageY-10)+'px'); })
      .on('mouseout', function() { d3.select(this).attr('opacity', 0.8); hideTip(); });

    svg.selectAll('.vlabel').data(data).join('text')
      .attr('x', M.left - 6).attr('y', d => y(d.label) + y.bandwidth()/2)
      .attr('dy', '0.35em').attr('text-anchor', 'end').attr('font-size', '10px')
      .text(d => d.label.length > 18 ? d.label.slice(0,18)+'…' : d.label);

    svg.selectAll('.vcount').data(data).join('text')
      .attr('x', d => x(d.value) + 4).attr('y', d => y(d.label) + y.bandwidth()/2)
      .attr('dy', '0.35em').attr('font-size', '10px').attr('fill', COLORS.cyan)
      .text(d => d.value.toLocaleString());
  })();

  // ── Service Health Donuts ──────────────────────────────────────────────
  (function() {
    const services = [
      {key: 'zia', label: 'ZIA', active: L.zia_active, inactive: L.zia_inactive},
      {key: 'zpa', label: 'ZPA', active: L.zpa_active, inactive: L.zpa_inactive},
      {key: 'zdx', label: 'ZDX', active: L.zdx_active, inactive: L.zdx_inactive},
    ];
    const container = d3.select('#service-donuts');

    services.forEach(svc => {
      const total = svc.active + svc.inactive;
      const pct = total ? Math.round(svc.active / total * 100) : 0;
      const item = container.append('div').attr('class', 'donut-item');
      const size = 130;
      const svg = item.append('svg').attr('width', size).attr('height', size);
      const g = svg.append('g').attr('transform', 'translate(' + size/2 + ',' + size/2 + ')');

      const arc = d3.arc().innerRadius(40).outerRadius(58);
      const pie = d3.pie().sort(null).value(d => d);

      g.selectAll('path').data(pie([svc.active, svc.inactive])).join('path')
        .attr('d', arc)
        .attr('fill', (d, i) => i === 0 ? healthColor(pct) : '#1a2235');

      g.append('text').attr('text-anchor', 'middle').attr('dy', '-0.1em')
        .attr('font-size', '18px').attr('font-weight', '700')
        .attr('fill', healthColor(pct)).text(pct + '%');
      g.append('text').attr('text-anchor', 'middle').attr('dy', '1.2em')
        .attr('font-size', '9px').attr('fill', '#8899aa')
        .text(svc.active.toLocaleString() + ' active');

      item.append('div').attr('class', 'donut-label').text(svc.label);
    });
  })();

  // ── Policy Health Chart ────────────────────────────────────────────────
  (function() {
    const data = L.policy_health.filter(d => d.count > 20).slice(0, 10);
    if (!data.length) return;

    const container = d3.select('#policy-health-chart');
    const W = container.node().getBoundingClientRect().width;
    const H = data.length * 34 + 40;
    const M = {top: 25, right: 30, bottom: 10, left: 200};

    const svg = container.append('svg').attr('width', W).attr('height', H);
    const y = d3.scaleBand().domain(data.map(d => d.policy)).range([M.top, H - M.bottom]).padding(0.3);
    const x = d3.scaleLinear().domain([0, 100]).range([M.left, W - M.right]);

    svg.selectAll('.grid-line').data(x.ticks(5)).join('line')
      .attr('class', 'grid-line').attr('x1', d => x(d)).attr('x2', d => x(d))
      .attr('y1', M.top).attr('y2', H - M.bottom);

    svg.selectAll('.plabel').data(data).join('text')
      .attr('x', M.left - 6).attr('y', d => y(d.policy) + y.bandwidth()/2)
      .attr('dy', '0.35em').attr('text-anchor', 'end').attr('font-size', '10px')
      .text(d => d.policy.length > 26 ? d.policy.slice(0,26)+'…' : d.policy);

    const barH = y.bandwidth() / 2.5;
    [{key:'zia_pct', label:'ZIA', color:COLORS.cyan}, {key:'zpa_pct', label:'ZPA', color:COLORS.green}].forEach((svc, i) => {
      svg.selectAll('.pb-'+svc.key).data(data).join('rect')
        .attr('x', M.left).attr('y', d => y(d.policy) + i * (barH + 1))
        .attr('width', d => Math.max(0, x(d[svc.key]) - M.left)).attr('height', barH)
        .attr('fill', d => healthColor(d[svc.key])).attr('opacity', 0.8);
      svg.selectAll('.pl-'+svc.key).data(data).join('text')
        .attr('x', d => x(d[svc.key]) + 3).attr('y', d => y(d.policy) + i * (barH + 1) + barH/2)
        .attr('dy', '0.35em').attr('font-size', '9px').attr('fill', d => healthColor(d[svc.key]))
        .text(d => d[svc.key] + '%');
    });

    const leg = svg.append('g').attr('transform', 'translate(' + M.left + ', 6)');
    [{l:'ZIA',c:COLORS.cyan},{l:'ZPA',c:COLORS.green}].forEach((s,i) => {
      leg.append('rect').attr('x', i*60).attr('y',0).attr('width',10).attr('height',10).attr('fill',s.c);
      leg.append('text').attr('x', i*60+14).attr('y',9).attr('font-size','10px').text(s.l);
    });
  })();

  // ── Stale Devices Chart ────────────────────────────────────────────────
  (function() {
    const data = L.stale_buckets.map(d => ({label: d[0], value: d[1]}));
    if (!data.length) return;

    const container = d3.select('#stale-chart');
    const W = container.node().getBoundingClientRect().width;
    const H = 240;
    const M = {top: 20, right: 20, bottom: 40, left: 55};

    const svg = container.append('svg').attr('width', W).attr('height', H);
    const x = d3.scaleBand().domain(data.map(d => d.label)).range([M.left, W - M.right]).padding(0.2);
    const y = d3.scaleLinear().domain([0, d3.max(data, d => d.value) * 1.1]).range([H - M.bottom, M.top]);

    svg.selectAll('.grid-line').data(y.ticks(4)).join('line')
      .attr('class', 'grid-line').attr('x1', M.left).attr('x2', W - M.right)
      .attr('y1', d => y(d)).attr('y2', d => y(d));

    const staleColors = [COLORS.green, COLORS.cyan, COLORS.amber, COLORS.red, '#ff1744', '#b71c1c', COLORS.muted];
    svg.selectAll('rect').data(data).join('rect')
      .attr('x', d => x(d.label)).attr('y', d => y(d.value))
      .attr('width', x.bandwidth()).attr('height', d => H - M.bottom - y(d.value))
      .attr('fill', (d, i) => staleColors[i] || COLORS.muted).attr('opacity', 0.8)
      .on('mouseover', function(event, d) { d3.select(this).attr('opacity',1); showTip(event, '<strong>'+d.label+'</strong><br>'+d.value.toLocaleString()+' devices'); })
      .on('mousemove', function(event) { tooltip.style('left',(event.pageX+12)+'px').style('top',(event.pageY-10)+'px'); })
      .on('mouseout', function() { d3.select(this).attr('opacity',0.8); hideTip(); });

    svg.selectAll('.slabel').data(data).join('text')
      .attr('x', d => x(d.label) + x.bandwidth()/2).attr('y', d => y(d.value) - 4)
      .attr('text-anchor', 'middle').attr('font-size', '10px').attr('fill', COLORS.cyan)
      .text(d => d.value > 0 ? d.value.toLocaleString() : '');

    svg.append('g').attr('transform', 'translate(0,' + (H - M.bottom) + ')')
      .call(d3.axisBottom(x).tickSize(0)).selectAll('text').attr('font-size', '9px').attr('fill', '#8899aa');
    svg.select('.domain').attr('stroke', 'var(--border)');
  })();

  // ── Simple horizontal bar helper ───────────────────────────────────────
  function simpleHBar(selector, items, maxItems) {
    const data = items.slice(0, maxItems || 8).map(d => ({label: d[0], value: d[1]}));
    if (!data.length) return;
    const container = d3.select(selector);
    const W = container.node().getBoundingClientRect().width;
    const H = data.length * 28 + 10;
    const M = {top: 5, right: 55, bottom: 5, left: 120};

    const svg = container.append('svg').attr('width', W).attr('height', H);
    const y = d3.scaleBand().domain(data.map(d => d.label)).range([M.top, H - M.bottom]).padding(0.2);
    const x = d3.scaleLinear().domain([0, d3.max(data, d => d.value)]).range([M.left, W - M.right]);

    svg.selectAll('rect').data(data).join('rect')
      .attr('x', M.left).attr('y', d => y(d.label))
      .attr('width', d => Math.max(0, x(d.value) - M.left)).attr('height', y.bandwidth())
      .attr('fill', (d, i) => PAL[i % PAL.length]).attr('opacity', 0.7);

    svg.selectAll('.hlabel').data(data).join('text')
      .attr('x', M.left - 6).attr('y', d => y(d.label) + y.bandwidth()/2)
      .attr('dy', '0.35em').attr('text-anchor', 'end').attr('font-size', '10px')
      .text(d => d.label.length > 16 ? d.label.slice(0,16)+'…' : d.label);

    svg.selectAll('.hcount').data(data).join('text')
      .attr('x', d => x(d.value) + 4).attr('y', d => y(d.label) + y.bandwidth()/2)
      .attr('dy', '0.35em').attr('font-size', '10px').attr('fill', COLORS.cyan)
      .text(d => d.value.toLocaleString());
  }

  simpleHBar('#tunnel-chart', L.tunnel_versions, 5);
  simpleHBar('#reg-chart', L.registration_states, 6);
  simpleHBar('#revert-chart', L.revert_status, 6);
  simpleHBar('#trust-chart', L.trust_levels, 6);
  simpleHBar('#os-chart', L.os_breakdown, 6);

  // ── Temporal Charts ────────────────────────────────────────────────────
  if (DATA.multi && DATA.temporal) {
    const T = DATA.temporal;

    // Health over time
    (function() {
      const container = d3.select('#temporal-health-chart');
      const W = container.node().getBoundingClientRect().width;
      const H = 260;
      const M = {top: 30, right: 30, bottom: 40, left: 50};

      const svg = container.append('svg').attr('width', W).attr('height', H);
      const x = d3.scalePoint().domain(T.labels).range([M.left, W - M.right]).padding(0.3);
      const y = d3.scaleLinear().domain([
        Math.min(d3.min(T.zia_pct), d3.min(T.zpa_pct), d3.min(T.zdx_pct)) - 2,
        100
      ]).range([H - M.bottom, M.top]);

      svg.selectAll('.grid-line').data(y.ticks(5)).join('line')
        .attr('class', 'grid-line').attr('x1', M.left).attr('x2', W - M.right)
        .attr('y1', d => y(d)).attr('y2', d => y(d));

      const line = d3.line().x((d,i) => x(T.labels[i])).y(d => y(d)).curve(d3.curveMonotoneX);

      [{data: T.zia_pct, label: 'ZIA', color: COLORS.cyan},
       {data: T.zpa_pct, label: 'ZPA', color: COLORS.green},
       {data: T.zdx_pct, label: 'ZDX', color: COLORS.purple}].forEach(svc => {
        svg.append('path').datum(svc.data).attr('fill', 'none')
          .attr('stroke', svc.color).attr('stroke-width', 2).attr('d', line);
        svg.selectAll('.dot-'+svc.label).data(svc.data).join('circle')
          .attr('cx', (d,i) => x(T.labels[i])).attr('cy', d => y(d)).attr('r', 4)
          .attr('fill', svc.color);
      });

      svg.append('g').attr('transform', 'translate(0,'+(H-M.bottom)+')').call(d3.axisBottom(x).tickSize(0))
        .selectAll('text').attr('font-size','10px').attr('fill','#8899aa');
      svg.append('g').attr('transform', 'translate('+M.left+',0)').call(d3.axisLeft(y).ticks(5).tickFormat(d=>d+'%'))
        .selectAll('text').attr('font-size','10px').attr('fill','#8899aa');
      svg.selectAll('.domain').attr('stroke', 'var(--border)');

      // Legend
      const leg = svg.append('g').attr('transform', 'translate('+(M.left+10)+',10)');
      [{l:'ZIA',c:COLORS.cyan},{l:'ZPA',c:COLORS.green},{l:'ZDX',c:COLORS.purple}].forEach((s,i) => {
        leg.append('line').attr('x1',i*60).attr('x2',i*60+15).attr('y1',0).attr('y2',0).attr('stroke',s.c).attr('stroke-width',2);
        leg.append('text').attr('x',i*60+20).attr('y',4).attr('font-size','10px').text(s.l);
      });

      svg.append('text').attr('class','axis-label').attr('x', W/2).attr('y', H - 4).attr('text-anchor','middle').text('Service Health Over Time');
    })();

    // Version counts over time
    (function() {
      const container = d3.select('#temporal-versions-chart');
      const W = container.node().getBoundingClientRect().width;
      const H = 260;
      const M = {top: 30, right: 30, bottom: 40, left: 55};

      const svg = container.append('svg').attr('width', W).attr('height', H);
      const x = d3.scalePoint().domain(T.labels).range([M.left, W - M.right]).padding(0.3);
      const allVals = T.version_series.flatMap(s => s.data);
      const y = d3.scaleLinear().domain([0, d3.max(allVals) * 1.1]).range([H - M.bottom, M.top]);

      svg.selectAll('.grid-line').data(y.ticks(5)).join('line')
        .attr('class', 'grid-line').attr('x1', M.left).attr('x2', W - M.right)
        .attr('y1', d => y(d)).attr('y2', d => y(d));

      const line = d3.line().x((d,i) => x(T.labels[i])).y(d => y(d)).curve(d3.curveMonotoneX);

      T.version_series.forEach((vs, idx) => {
        svg.append('path').datum(vs.data).attr('fill', 'none')
          .attr('stroke', PAL[idx % PAL.length]).attr('stroke-width', 1.5).attr('d', line);
        svg.selectAll('.vdot-'+idx).data(vs.data).join('circle')
          .attr('cx', (d,i) => x(T.labels[i])).attr('cy', d => y(d)).attr('r', 3)
          .attr('fill', PAL[idx % PAL.length])
          .on('mouseover', function(event) { showTip(event, '<strong>'+vs.version+'</strong><br>'+d3.select(this).datum().toLocaleString()+' devices'); })
          .on('mousemove', function(event) { tooltip.style('left',(event.pageX+12)+'px').style('top',(event.pageY-10)+'px'); })
          .on('mouseout', hideTip);
      });

      svg.append('g').attr('transform', 'translate(0,'+(H-M.bottom)+')').call(d3.axisBottom(x).tickSize(0))
        .selectAll('text').attr('font-size','10px').attr('fill','#8899aa');
      svg.append('g').attr('transform', 'translate('+M.left+',0)').call(d3.axisLeft(y).ticks(5).tickFormat(d3.format(',')))
        .selectAll('text').attr('font-size','10px').attr('fill','#8899aa');
      svg.selectAll('.domain').attr('stroke', 'var(--border)');

      // Legend
      const leg = svg.append('g').attr('transform', 'translate('+(M.left+5)+',6)');
      T.version_series.slice(0, 6).forEach((vs, i) => {
        const col = i < 3 ? 0 : 1;
        const row = i % 3;
        leg.append('rect').attr('x', col*180).attr('y', row*14).attr('width',8).attr('height',8).attr('fill', PAL[i]);
        leg.append('text').attr('x', col*180+12).attr('y', row*14+8).attr('font-size','9px').text(vs.version.slice(0,20));
      });

      svg.append('text').attr('class','axis-label').attr('x', W/2).attr('y', H - 4).attr('text-anchor','middle').text('Version Distribution Over Time');
    })();
  }
})();
"""

# ── PDF export ────────────────────────────────────────────────────────────────

def export_pdf(html_path):
    pdf_path = html_path.replace(".html", ".pdf")
    for browser in [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        shutil.which("google-chrome"),
        shutil.which("chromium"),
        shutil.which("chromium-browser"),
    ]:
        if browser and os.path.exists(browser):
            try:
                subprocess.run([
                    browser, "--headless", "--disable-gpu",
                    f"--print-to-pdf={pdf_path}",
                    "--no-margins",
                    "--virtual-time-budget=5000",
                    f"file://{os.path.abspath(html_path)}"
                ], capture_output=True, timeout=30)
                if os.path.exists(pdf_path):
                    print(f"PDF exported  : {pdf_path}")
                    return pdf_path
            except Exception:
                pass
    print("PDF export    : skipped (Chrome/Chromium not found)")
    return None

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    pairs = discover_snapshots(args.paths)

    print(f"\n  ZCC Fleet Tracker")
    print(f"  {'=' * 40}")
    print(f"  Snapshots found: {len(pairs)}\n")

    snapshots_data = []
    for svc_path, dev_path, label in pairs:
        print(f"  Loading: {label}")
        print(f"    Service : {os.path.basename(svc_path)}")
        print(f"    Device  : {os.path.basename(dev_path)}")
        svc_rows = load_csv(svc_path)
        dev_rows = load_csv(dev_path)
        print(f"    Rows    : {len(svc_rows):,} service, {len(dev_rows):,} device")
        sd = analyze_snapshot(svc_rows, dev_rows, label)
        snapshots_data.append(sd)
        print(f"    Snapshot: {sd['snapshot']}")
        print()

    temporal = compute_temporal(snapshots_data) if len(snapshots_data) > 1 else None

    html = generate_html(snapshots_data, temporal)

    if args.out:
        out_path = args.out
    else:
        latest = snapshots_data[-1]
        ts = latest['snapshot'].replace(" ", "_").replace(":", "-")
        out_path = os.path.join(os.path.dirname(pairs[-1][0]), f"zcc_fleet_tracker_{ts}.html")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  Dashboard   : {out_path}")

    if not args.no_pdf:
        export_pdf(out_path)

    if not args.no_open:
        webbrowser.open(f"file://{os.path.abspath(out_path)}")

    print(f"\n  Done.\n")


if __name__ == "__main__":
    main()
