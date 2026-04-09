#!/usr/bin/env python3
"""
ZCC Fleet Tracker — Zscaler Client Connector Fleet Health Dashboard
Usage:
  python3 generate_dashboard.py                             # auto-detect from ./snapshots/
  python3 generate_dashboard.py /path/to/csv/dir/           # auto-detect all snapshot pairs
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
    p.add_argument("paths", nargs="*", default=None, help="CSV files (paired) or directory containing exports")
    p.add_argument("--out", "-o", default=None, help="Output HTML path (default: auto-named)")
    p.add_argument("--no-open", action="store_true", help="Don't open report in browser")
    p.add_argument("--no-pdf", action="store_true", help="Skip PDF export")
    return p.parse_args()

# ── Snapshot discovery ────────────────────────────────────────────────────────

def discover_snapshots(paths):
    if not paths:
        default_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "snapshots")
        if os.path.isdir(default_dir):
            paths = [default_dir]
        else:
            sys.exit("ERROR: No paths provided and ./snapshots/ directory not found.\n"
                     "Usage: python3 generate_dashboard.py /path/to/csv/dir/")
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

    def _parse_file_ts(f):
        """Parse datetime from filename for proximity matching."""
        m = re.search(r"(\d{4})_(\d{2})_(\d{2})-(\d{2})-(\d{2})-(\d{2})", os.path.basename(f))
        if m:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)),
                            int(m.group(4)), int(m.group(5)), int(m.group(6)))
        m = re.search(r"(\d{4})_(\d{2})_(\d{2})", os.path.basename(f))
        if m:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        return None

    # Match each service file to closest device file (within 12 hours)
    pairs = []
    used_devs = set()
    svc_with_ts = [(f, _parse_file_ts(f)) for f in svcs]
    dev_with_ts = [(f, _parse_file_ts(f)) for f in devs]

    for svc_f, svc_t in sorted(svc_with_ts, key=lambda x: x[1] or datetime.min):
        if svc_t is None:
            continue
        best_dev, best_delta = None, timedelta(hours=12)
        for dev_f, dev_t in dev_with_ts:
            if dev_f in used_devs or dev_t is None:
                continue
            delta = abs(svc_t - dev_t)
            if delta < best_delta:
                best_delta = delta
                best_dev = dev_f
        if best_dev:
            used_devs.add(best_dev)
            label = _label_from_ts_key(svc_t.strftime("%Y_%m_%d-%H-%M"))
            pairs.append((svc_f, best_dev, label))

    if not pairs:
        if len(svcs) == 1 and len(devs) == 1:
            return [(svcs[0], devs[0], _label_from_path(svcs[0]))]
        sys.exit("ERROR: Cannot match service/device CSV pairs by timestamp.")
    return sorted(pairs, key=lambda x: x[2])


def _label_from_ts_key(k):
    m = re.match(r"(\d{4})_(\d{2})_(\d{2})-(\d{2})-(\d{2})", k)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}T{m.group(4)}:{m.group(5)}"
    m = re.match(r"(\d{4})_(\d{2})_(\d{2})", k)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return k.replace("_", "-")


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
    snapshot = max(seen_times) if seen_times else datetime.now()

    d = {
        "label": label,
        "snapshot": snapshot.strftime("%Y-%m-%d %H:%M UTC"),
        "snapshot_short": snapshot.strftime("%d %b %Y"),
        "total_devices": len(dev_rows),
        "total_service": len(svc_rows),
    }

    dtype_ctr = Counter(r.get("Device type", "Unknown").upper() for r in dev_rows)
    d["device_types"] = dtype_ctr.most_common()

    os_ctr = Counter()
    for r in dev_rows:
        osv = r.get("OS Version", "") or ""
        if "windows 11" in osv.lower(): os_ctr["Windows 11"] += 1
        elif "windows 10" in osv.lower(): os_ctr["Windows 10"] += 1
        elif "mac" in osv.lower() or "darwin" in osv.lower(): os_ctr["macOS"] += 1
        elif "linux" in osv.lower(): os_ctr["Linux"] += 1
        elif osv: os_ctr["Other"] += 1
        else: os_ctr["Unknown"] += 1
    d["os_breakdown"] = os_ctr.most_common()

    ver_ctr = Counter()
    for r in dev_rows:
        v = re.sub(r"\s*\(.*\)", "", r.get("Zscaler Client Connector Version", "").strip())
        ver_ctr[v or "Unknown"] += 1
    d["version_distribution"] = ver_ctr.most_common()

    for svc in ("ZIA", "ZPA", "ZDX"):
        enabled = [r for r in svc_rows if r.get(f"{svc} Enabled") == "true"]
        active = sum(1 for r in enabled if r.get(f"{svc} Health") == "Active")
        inactive = len(enabled) - active
        d[f"{svc.lower()}_active"] = active
        d[f"{svc.lower()}_inactive"] = inactive
        d[f"{svc.lower()}_total"] = len(enabled)

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

    sorted_vh = sorted(ver_health.items(), key=lambda x: -x[1]["count"])
    d["version_health"] = []
    for v, vh in sorted_vh[:15]:
        zia_t = vh["zia_a"] + vh["zia_i"]
        zpa_t = vh["zpa_a"] + vh["zpa_i"]
        zdx_t = vh["zdx_a"] + vh["zdx_i"]
        d["version_health"].append({
            "version": v, "count": vh["count"],
            "zia_pct": round(vh["zia_a"] / zia_t * 100, 1) if zia_t else 0,
            "zpa_pct": round(vh["zpa_a"] / zpa_t * 100, 1) if zpa_t else 0,
            "zdx_pct": round(vh["zdx_a"] / zdx_t * 100, 1) if zdx_t else 0,
            "zia_inactive": vh["zia_i"], "zpa_inactive": vh["zpa_i"], "zdx_inactive": vh["zdx_i"],
        })

    pol_ctr = Counter(r.get("Policy Name", "") or "Unknown" for r in dev_rows)
    d["policy_distribution"] = pol_ctr.most_common(15)

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
                if r.get(f"{svc} Health") == "Active": ph[f"{svc.lower()}_a"] += 1
                else: ph[f"{svc.lower()}_i"] += 1

    sorted_ph = sorted(pol_health.items(), key=lambda x: -x[1]["count"])
    d["policy_health"] = []
    for p, ph in sorted_ph[:12]:
        zia_t = ph["zia_a"] + ph["zia_i"]
        zpa_t = ph["zpa_a"] + ph["zpa_i"]
        d["policy_health"].append({
            "policy": p, "count": ph["count"],
            "zia_pct": round(ph["zia_a"] / zia_t * 100, 1) if zia_t else 0,
            "zpa_pct": round(ph["zpa_a"] / zpa_t * 100, 1) if zpa_t else 0,
        })

    tun_ctr = Counter()
    for r in dev_rows:
        tv = r.get("Tunnel Version", "") or "Unknown"
        if "2.0" in tv: tun_ctr["Tunnel 2.0"] += 1
        elif "1.0" in tv: tun_ctr["Tunnel 1.0"] += 1
        else: tun_ctr["Unknown/None"] += 1
    d["tunnel_versions"] = tun_ctr.most_common()

    d["registration_states"] = Counter(r.get("Registration State", "Unknown") for r in svc_rows).most_common()

    rev_ctr = Counter()
    for r in dev_rows:
        rev_ctr[r.get("ZCC Revert Status", "") or "N/A"] += 1
    d["revert_status"] = rev_ctr.most_common()

    d["trust_levels"] = Counter(r.get("Device Trust Level", "") or "Unknown" for r in dev_rows).most_common()

    buckets = {"0-1d": 0, "2-7d": 0, "8-30d": 0, "31-90d": 0, "91-180d": 0, "180d+": 0, "Never": 0}
    for r in svc_rows:
        ts = parse_ts(r.get("Last Seen Connected to ZIA", ""))
        if not ts: buckets["Never"] += 1; continue
        age = (snapshot - ts).total_seconds() / 86400
        if age <= 1: buckets["0-1d"] += 1
        elif age <= 7: buckets["2-7d"] += 1
        elif age <= 30: buckets["8-30d"] += 1
        elif age <= 90: buckets["31-90d"] += 1
        elif age <= 180: buckets["91-180d"] += 1
        else: buckets["180d+"] += 1
    d["stale_buckets"] = list(buckets.items())

    cutoff_24h = snapshot - timedelta(hours=24)
    cutoff_31d = snapshot - timedelta(days=31)
    bypass_risk = live_blindspots = ghost_machines = quarantined = 0
    for r in svc_rows:
        zia_h = r.get("ZIA Health", "")
        zpa_h = r.get("ZPA Health", "")
        reg = r.get("Registration State", "")
        ts = parse_ts(r.get("Last Seen Connected to ZIA", ""))
        if zia_h == "Inactive" and zpa_h == "Active": bypass_risk += 1
        if zia_h == "Inactive" and ts and ts >= cutoff_24h: live_blindspots += 1
        if reg == "Registered" and ts and ts < cutoff_31d: ghost_machines += 1
        if reg == "Quarantined": quarantined += 1
    d["bypass_risk"] = bypass_risk
    d["live_blindspots"] = live_blindspots
    d["ghost_machines"] = ghost_machines
    d["quarantined"] = quarantined

    return d

# ── Country extraction & per-country analysis ────────────────────────────────

def _extract_country(hostname):
    if not hostname or len(hostname) < 2:
        return "XX"
    prefix = hostname[:2].upper()
    return prefix if prefix.isalpha() else "XX"


def analyze_with_countries(svc_rows, dev_rows, label):
    full = analyze_snapshot(svc_rows, dev_rows, label)

    # Build UDID -> country map from both exports
    udid_country = {}
    for r in dev_rows:
        udid = r.get("UDID", "")
        if udid:
            udid_country[udid] = _extract_country(r.get("Hostname", ""))
    for r in svc_rows:
        udid = r.get("UDID", "")
        if udid and udid not in udid_country:
            udid_country[udid] = _extract_country(r.get("Hostname", ""))

    country_counts = Counter(udid_country.values())
    countries = sorted([c for c, n in country_counts.items() if n >= 20 and c != "XX"],
                       key=lambda c: -country_counts[c])

    full["countries"] = [(c, country_counts[c]) for c in countries]
    full["by_country"] = {}

    for country in countries:
        country_udids = {u for u, c in udid_country.items() if c == country}
        c_dev = [r for r in dev_rows if r.get("UDID", "") in country_udids]
        c_svc = [r for r in svc_rows if r.get("UDID", "") in country_udids]
        if len(c_dev) >= 10 and len(c_svc) >= 10:
            full["by_country"][country] = analyze_snapshot(c_svc, c_dev, f"{label}_{country}")

    return full

# ── Temporal diff ─────────────────────────────────────────────────────────────

def compute_temporal(snapshots_data):
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
            if v not in version_tracking: version_tracking[v] = []
            version_tracking[v].append({"label": sd["snapshot_short"], "count": count})

    all_ver_counts = Counter()
    for sd in snapshots_data:
        for v, c in sd["version_distribution"]: all_ver_counts[v] += c
    top_versions = [v for v, _ in all_ver_counts.most_common(8)]

    temporal["version_series"] = []
    for v in top_versions:
        vt = {e["label"]: e["count"] for e in version_tracking.get(v, [])}
        temporal["version_series"].append({"version": v, "data": [vt.get(lbl, 0) for lbl in temporal["labels"]]})
    return temporal

# ── HTML generation ───────────────────────────────────────────────────────────

def generate_html(snapshots_data, temporal_data):
    data_json = json.dumps({"snapshots": snapshots_data, "temporal": temporal_data, "multi": len(snapshots_data) > 1}, default=str)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ZCC Fleet Tracker</title>
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
      <a href="https://zhero.ai" target="_blank" class="logo-icon" title="ZHERO srl"><svg width="32" height="32" viewBox="0 0 1795 1795" xmlns="http://www.w3.org/2000/svg"><rect width="1795" height="1795" rx="200" fill="#246bf5"/><polygon fill="#fff" points="481.08 1378.8 481.08 1325.41 350.28 1325.41 350.28 1360.44 445.07 1360.44 345.21 1433.19 345.21 1486.59 484.67 1486.59 484.67 1451.56 381.22 1451.56 481.08 1378.8"/><polygon fill="#fff" points="679.15 1383.84 597.74 1383.84 597.74 1325.41 556.37 1325.41 556.37 1486.59 597.74 1486.59 597.74 1418.87 679.15 1418.87 679.15 1486.59 720.51 1486.59 720.51 1325.41 679.15 1325.41 679.15 1383.84"/><polygon fill="#fff" points="843.07 1418.45 927.45 1418.45 927.45 1384.69 843.07 1384.69 843.07 1360.23 931.25 1360.23 931.25 1325.41 801.71 1325.41 801.71 1486.59 932.52 1486.59 932.52 1451.77 843.07 1451.77 843.07 1418.45"/><path fill="#fff" d="M1145.79,1377.72c-.2-33.24-24.34-52.31-66.25-52.31h-71.1v161.18h41.36v-56.75h29l34.6,56.75h46.48l-41.99-64.48c13.14-6.38,27.9-19.17,27.9-44.39ZM1049.8,1359.81h29.32c11.16,0,24.46,3.11,24.46,17.92,0,12.31-8.09,18.56-24.04,18.56h-29.74v-36.48Z"/><path fill="#fff" d="M1313.7,1321.61c-56.28,0-94.09,33.91-94.09,84.39s37.81,84.39,94.09,84.39,94.3-33.91,94.3-84.39-37.02-84.39-94.3-84.39ZM1313.7,1453.25c-30.91,0-51.68-18.99-51.68-47.25s20.77-47.25,51.68-47.25,51.89,18.99,51.89,47.25-20.85,47.25-51.89,47.25Z"/><path fill="#fff" d="M1354.74,904.37c12.72-43.23,19.14-88.34,19.14-134.08,0-34.86-3.74-69.43-11.17-102.99l-609.73,317.89,149.73,62.26,299.7-145.65v-.3s90.49-44.07,90.49-44.07l1.28,99.75-387.52,188.68-257.41-106.24-111.19-45.05,107.74-56.5,688.57-357.99c-9.79-22.48-21.53-44.47-34.94-65.51-88.02-137.93-238.13-220.25-401.51-220.25-228.66,0-425.34,162.8-467.65,387.11l-.66,3.51c-5.09,27.95-7.66,56.65-7.66,85.34,0,33.65,3.54,67.18,10.51,99.85l622.91-312.85-149.33-62.12-388.91,188.71-2.52-98.41,387.49-188.68,257.44,106.18,102.24,42.57-571.16,293.14-230.82,117.87c10.28,24.21,22.65,47.67,36.84,69.8,88.05,137.7,238.07,219.91,401.28,219.91,104.97,0,204.48-33.48,287.81-96.83,80.68-61.34,140.7-148.38,169.02-245.07Z"/></svg></a>
      <div>
        <h1>ZCC Fleet Tracker</h1>
        <div class="subtitle">Zscaler Client Connector Health Dashboard &mdash; <a href="https://zhero.ai" target="_blank" class="zhero-link">zhero.ai</a></div>
      </div>
    </div>
    <div class="header-right">
      <div class="meta" id="header-meta"></div>
    </div>
  </div>
  <div class="filter-bar">
    <div class="snapshot-bar" id="snapshot-bar"></div>
    <div class="country-bar" id="country-bar"></div>
  </div>
</header>
<main>
  <section class="kpi-row" id="kpi-row"></section>
  <section class="alert-row" id="alert-row"></section>
  <section class="card full-width" id="vh-section">
    <div class="card-header"><h2>Version Health Matrix <span class="info-btn" data-key="version_health"></span></h2>
    <p class="card-desc">Service health by ZCC version — identifies which versions cause problems</p></div>
    <div id="vh-chart"></div>
    <div class="table-wrap" id="vh-table"></div>
  </section>
  <div class="grid-2">
    <section class="card"><div class="card-header"><h2>Fleet Composition <span class="info-btn" data-key="fleet_composition"></span></h2></div><div id="ver-dist"></div></section>
    <section class="card"><div class="card-header"><h2>Service Health <span class="info-btn" data-key="service_health"></span></h2></div><div class="donut-row" id="svc-donuts"></div></section>
  </div>
  <div class="grid-2">
    <section class="card"><div class="card-header"><h2>Policy Health <span class="info-btn" data-key="policy_health"></span></h2></div><div id="pol-chart"></div></section>
    <section class="card"><div class="card-header"><h2>Device Staleness <span class="info-btn" data-key="device_staleness"></span></h2></div><div id="stale-chart"></div></section>
  </div>
  <div class="grid-3">
    <section class="card"><div class="card-header"><h2>Tunnel Protocol <span class="info-btn" data-key="tunnel_protocol"></span></h2></div><div id="tunnel-chart"></div></section>
    <section class="card"><div class="card-header"><h2>Registration State <span class="info-btn" data-key="registration_state"></span></h2></div><div id="reg-chart"></div></section>
    <section class="card"><div class="card-header"><h2>Revert Status <span class="info-btn" data-key="revert_status"></span></h2></div><div id="revert-chart"></div></section>
  </div>
  <div class="grid-2">
    <section class="card"><div class="card-header"><h2>Device Trust Levels <span class="info-btn" data-key="trust_levels"></span></h2></div><div id="trust-chart"></div></section>
    <section class="card"><div class="card-header"><h2>OS Distribution <span class="info-btn" data-key="os_distribution"></span></h2></div><div id="os-chart"></div></section>
  </div>
  <section class="card full-width" id="temporal-section" style="display:none">
    <div class="card-header"><h2>Temporal Comparison <span class="info-btn" data-key="temporal_health"></span></h2>
    <p class="card-desc">Fleet metrics across snapshots</p></div>
    <div class="grid-2"><div id="temp-health"></div><div id="temp-versions"></div></div>
  </section>
  <section class="card full-width">
    <div class="card-header"><h2>Policy Breakdown</h2></div>
    <div class="table-wrap" id="pol-table"></div>
  </section>
</main>
<footer><div class="footer-inner"><a href="https://zhero.ai" target="_blank" class="footer-logo"><svg width="16" height="16" viewBox="0 0 1795 1795" xmlns="http://www.w3.org/2000/svg"><rect width="1795" height="1795" rx="200" fill="currentColor"/><polygon fill="#0c1018" points="481.08 1378.8 481.08 1325.41 350.28 1325.41 350.28 1360.44 445.07 1360.44 345.21 1433.19 345.21 1486.59 484.67 1486.59 484.67 1451.56 381.22 1451.56"/><polygon fill="#0c1018" points="679.15 1383.84 597.74 1383.84 597.74 1325.41 556.37 1325.41 556.37 1486.59 597.74 1486.59 597.74 1418.87 679.15 1418.87 679.15 1486.59 720.51 1486.59 720.51 1325.41 679.15 1325.41"/><polygon fill="#0c1018" points="843.07 1418.45 927.45 1418.45 927.45 1384.69 843.07 1384.69 843.07 1360.23 931.25 1360.23 931.25 1325.41 801.71 1325.41 801.71 1486.59 932.52 1486.59 932.52 1451.77 843.07 1451.77"/><path fill="#0c1018" d="M1354.74,904.37c12.72-43.23,19.14-88.34,19.14-134.08,0-34.86-3.74-69.43-11.17-102.99l-609.73,317.89,149.73,62.26,299.7-145.65v-.3s90.49-44.07,90.49-44.07l1.28,99.75-387.52,188.68-257.41-106.24-111.19-45.05,107.74-56.5,688.57-357.99c-9.79-22.48-21.53-44.47-34.94-65.51-88.02-137.93-238.13-220.25-401.51-220.25-228.66,0-425.34,162.8-467.65,387.11l-.66,3.51c-5.09,27.95-7.66,56.65-7.66,85.34,0,33.65,3.54,67.18,10.51,99.85l622.91-312.85-149.33-62.12-388.91,188.71-2.52-98.41,387.49-188.68,257.44,106.18,102.24,42.57-571.16,293.14-230.82,117.87c10.28,24.21,22.65,47.67,36.84,69.8,88.05,137.7,238.07,219.91,401.28,219.91,104.97,0,204.48-33.48,287.81-96.83,80.68-61.34,140.7-148.38,169.02-245.07Z"/></svg></a><span>ZCC Fleet Tracker</span><span class="dot">&#183;</span><a href="https://zhero.ai" target="_blank" class="zhero-link">ZHERO srl</a><span class="dot">&#183;</span><span>Generated {datetime.now().strftime("%Y-%m-%d %H:%M")}</span></div></footer>
<div class="info-popover" id="info-popover"></div>
<div class="temporal-overlay" id="temp-overlay"></div>
<script>
const DATA = {data_json};
{JS_TEMPLATE}
</script>
</body>
</html>"""

# ── CSS ───────────────────────────────────────────────────────────────────────

CSS_TEMPLATE = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{--bg-deep:#060a10;--bg-surface:#0c1018;--bg-card:#111827;--bg-hover:#162032;--border:#1e2d3d;--border-b:#2a4060;--text-1:#e2e8f0;--text-2:#8899aa;--text-m:#4a5a6a;--cyan:#00c8f0;--green:#00e68a;--amber:#ffa800;--red:#ff3060;--purple:#a78bfa;--font-d:'Chakra Petch',sans-serif;--font-b:'DM Sans',sans-serif;--font-m:'JetBrains Mono',monospace}
html{font-size:14px}
body{background:var(--bg-deep);color:var(--text-1);font-family:var(--font-b);line-height:1.5;min-height:100vh;-webkit-font-smoothing:antialiased}
.noise{position:fixed;top:0;left:0;width:100%;height:100%;background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='0.03'/%3E%3C/svg%3E");pointer-events:none;z-index:0}
header{position:relative;z-index:1;background:linear-gradient(180deg,#0d1420,var(--bg-deep));border-bottom:1px solid var(--border);padding:1.2rem 2rem 0}
.header-inner{max-width:1440px;margin:0 auto;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:1rem}
.logo{display:flex;align-items:center;gap:.75rem}
.logo-icon{color:var(--cyan);text-decoration:none;transition:color .15s}
.logo-icon:hover{color:var(--green)}
.zhero-link{color:var(--text-m);text-decoration:none;transition:color .15s}
.zhero-link:hover{color:var(--cyan)}
.footer-logo{color:var(--text-m);text-decoration:none;display:flex;transition:color .15s}
.footer-logo:hover{color:var(--cyan)}
h1{font-family:var(--font-d);font-size:1.5rem;font-weight:700;letter-spacing:.05em;text-transform:uppercase}
.subtitle{font-size:.7rem;color:var(--text-m);letter-spacing:.1em;text-transform:uppercase}
.meta{display:flex;gap:1.5rem}
.meta-item{display:flex;flex-direction:column;align-items:flex-end}
.meta-label{font-size:.6rem;color:var(--text-m);text-transform:uppercase;letter-spacing:.1em}
.meta-value{font-family:var(--font-m);font-size:.8rem;color:var(--cyan);font-weight:600}

/* Filter bars */
.filter-bar{max-width:1440px;margin:0 auto;padding:.5rem 0 .25rem;display:flex;flex-direction:column;gap:.35rem}
.snapshot-bar,.country-bar{display:flex;align-items:center;gap:.5rem;flex-wrap:wrap}
.snap-label{font-size:.65rem;color:var(--text-m);text-transform:uppercase;letter-spacing:.08em;margin-right:.25rem}
.snap-pill{font-family:var(--font-m);font-size:.75rem;padding:.3rem .8rem;border:1px solid var(--border);background:transparent;color:var(--text-2);cursor:pointer;transition:all .15s}
.snap-pill:hover{border-color:var(--cyan);color:var(--text-1)}
.snap-pill.active{background:rgba(0,200,240,.12);border-color:var(--cyan);color:var(--cyan);font-weight:600}

main{position:relative;z-index:1;max-width:1440px;margin:0 auto;padding:1.25rem 2rem 3rem}

/* KPI */
.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(175px,1fr));gap:.7rem;margin-bottom:1rem}
.kpi-card{background:var(--bg-card);border:1px solid var(--border);padding:.85rem 1rem;position:relative;overflow:hidden;cursor:default}
.kpi-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px}
.kpi-good::before{background:var(--green)} .kpi-warn::before{background:var(--amber)} .kpi-bad::before{background:var(--red)} .kpi-neutral::before{background:var(--cyan)}
.kpi-title{font-size:.65rem;text-transform:uppercase;letter-spacing:.08em;color:var(--text-m);display:flex;align-items:center;gap:.35rem}
.kpi-value{font-family:var(--font-m);font-size:1.6rem;font-weight:700;line-height:1.1;margin:.15rem 0}
.kpi-good .kpi-value{color:var(--green)} .kpi-warn .kpi-value{color:var(--amber)} .kpi-bad .kpi-value{color:var(--red)} .kpi-neutral .kpi-value{color:var(--text-1)}
.kpi-sub{font-family:var(--font-m);font-size:.65rem;color:var(--text-m)}
.kpi-delta{font-family:var(--font-m);font-size:.65rem;margin-left:.35rem}
.kpi-delta.up{color:var(--green)} .kpi-delta.down{color:var(--red)} .kpi-delta.flat{color:var(--text-m)}

/* Alert tiles */
.alert-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:.7rem;margin-bottom:1rem}
.alert-tile{padding:.65rem .85rem;border:1px solid;display:flex;flex-direction:column;gap:.05rem;cursor:default}
.alert-red{border-color:rgba(255,48,96,.3);background:rgba(255,48,96,.05)}
.alert-amber{border-color:rgba(255,168,0,.3);background:rgba(255,168,0,.05)}
.alert-count{font-family:var(--font-m);font-size:1.3rem;font-weight:700}
.alert-red .alert-count{color:var(--red)} .alert-amber .alert-count{color:var(--amber)}
.alert-title{font-family:var(--font-d);font-size:.75rem;font-weight:600;text-transform:uppercase;letter-spacing:.05em}
.alert-desc{font-size:.6rem;color:var(--text-m)}

/* Cards */
.card{background:var(--bg-card);border:1px solid var(--border);padding:1.15rem;margin-bottom:1.15rem}
.card-header{margin-bottom:.85rem}
.card-header h2{font-family:var(--font-d);font-size:.95rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:var(--cyan);display:flex;align-items:center;gap:.4rem}
.card-desc{font-size:.7rem;color:var(--text-m);margin-top:.2rem}
.full-width{grid-column:1/-1}
.grid-2{display:grid;grid-template-columns:1fr 1fr;gap:1.15rem;margin-bottom:0}
.grid-3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:1.15rem;margin-bottom:0}
@media(max-width:900px){.grid-2,.grid-3{grid-template-columns:1fr}}

/* Info button */
.info-btn{display:inline-flex;align-items:center;justify-content:center;width:16px;height:16px;border-radius:50%;border:1px solid var(--border-b);font-family:var(--font-m);font-size:9px;color:var(--text-m);cursor:help;transition:all .15s;flex-shrink:0}
.info-btn:hover{border-color:var(--cyan);color:var(--cyan);background:rgba(0,200,240,.08)}
.info-btn::after{content:'i'}

/* Info popover */
.info-popover{position:absolute;z-index:200;background:#1a2440;border:1px solid var(--border-b);padding:.7rem .9rem;font-size:.75rem;color:var(--text-1);line-height:1.5;max-width:360px;pointer-events:none;opacity:0;transition:opacity .15s;box-shadow:0 4px 20px rgba(0,0,0,.5)}

/* Temporal overlay (hover on data points) */
.temporal-overlay{position:absolute;z-index:150;background:#131d30;border:1px solid var(--border-b);padding:.6rem .8rem;font-family:var(--font-m);font-size:.7rem;color:var(--text-1);pointer-events:none;opacity:0;transition:opacity .12s;box-shadow:0 4px 16px rgba(0,0,0,.5);min-width:180px}
.to-title{font-family:var(--font-d);font-size:.7rem;color:var(--cyan);text-transform:uppercase;letter-spacing:.04em;margin-bottom:.3rem;border-bottom:1px solid var(--border);padding-bottom:.25rem}
.to-row{display:flex;justify-content:space-between;gap:.75rem;padding:.15rem 0}
.to-row.current{color:var(--cyan);font-weight:600}
.to-date{color:var(--text-2)}
.to-delta{margin-left:.4rem}
.to-delta.up{color:var(--green)} .to-delta.down{color:var(--red)}

/* Tables */
.table-wrap{overflow-x:auto}
.data-table{width:100%;border-collapse:collapse;font-size:.78rem}
.data-table th{font-family:var(--font-d);font-size:.65rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:var(--text-m);padding:.45rem .7rem;border-bottom:1px solid var(--border-b);text-align:left;white-space:nowrap}
.data-table td{padding:.35rem .7rem;border-bottom:1px solid var(--border);white-space:nowrap}
.data-table tbody tr:hover{background:var(--bg-hover)}
.r{text-align:right!important}
.mono{font-family:var(--font-m)}
.dim{color:var(--text-m)}
.h-good{font-family:var(--font-m);font-weight:600;color:var(--green)}
.h-warn{font-family:var(--font-m);font-weight:600;color:var(--amber)}
.h-bad{font-family:var(--font-m);font-weight:600;color:var(--red)}
.h-na{font-family:var(--font-m);color:var(--text-m)}

/* Donuts */
.donut-row{display:flex;justify-content:space-around;flex-wrap:wrap;gap:.75rem}
.donut-item{text-align:center;flex:1;min-width:130px}
.donut-label{font-family:var(--font-d);font-size:.7rem;text-transform:uppercase;letter-spacing:.06em;color:var(--text-m);margin-top:.35rem}

/* SVG */
svg text{fill:var(--text-2);font-family:var(--font-m);font-size:11px}
svg .grid-line{stroke:var(--border);stroke-dasharray:2,3}

/* Tooltip (standard) */
.tooltip{position:absolute;pointer-events:none;z-index:100;background:#1a2440;border:1px solid var(--border-b);padding:.45rem .65rem;font-family:var(--font-m);font-size:.72rem;color:var(--text-1);max-width:300px;opacity:0;box-shadow:0 4px 20px rgba(0,0,0,.5)}
.tooltip strong{color:var(--cyan)}

footer{position:relative;z-index:1;border-top:1px solid var(--border);padding:.85rem 2rem}
.footer-inner{max-width:1440px;margin:0 auto;display:flex;align-items:center;gap:.5rem;font-size:.65rem;color:var(--text-m);font-family:var(--font-m)}
.dot{color:var(--border-b)}

@media print{
  body{background:#fff;color:#1a1a1a} .noise{display:none} .card{border-color:#ddd;background:#fff;break-inside:avoid}
  header{background:#f8f8f8} h1,.card-header h2{color:#1a1a1a} .kpi-value,.alert-count{color:#1a1a1a!important}
  .h-good{color:#0a8a50} .h-warn{color:#b07800} .h-bad{color:#c02040}
  .data-table th,.data-table td{border-color:#ddd} svg text{fill:#333}
  .info-btn,.snap-pill{display:none} .temporal-overlay,.info-popover{display:none!important}
}
"""

# ── JavaScript ────────────────────────────────────────────────────────────────

JS_TEMPLATE = r"""
(function(){
const SS = DATA.snapshots;
const MULTI = DATA.multi;
let ci = SS.length - 1; // current snapshot index
let cc = 'ALL'; // current country filter

const C = {cyan:'#00c8f0',green:'#00e68a',amber:'#ffa800',red:'#ff3060',purple:'#a78bfa',blue:'#4e8cff',pink:'#f472b6',teal:'#2dd4bf',muted:'#4a5a6a',border:'#1e2d3d'};
const PAL = [C.cyan,C.amber,C.green,C.red,C.purple,C.blue,C.pink,C.teal,'#e879f9','#fb923c','#6ee7b7','#93c5fd'];

// ── Info descriptions ────────────────────────────────────────────────────
const INFO = {
  total_fleet: 'Total unique devices in the ZCC Portal device export at snapshot time.',
  zia_active: 'Percentage of ZIA-enabled devices reporting Active health. Inactive = ZIA enabled but the tunnel is not functioning.',
  zpa_active: 'Percentage of ZPA-enabled devices with Active health status.',
  zdx_active: 'Percentage of ZDX-enabled devices with Active health status.',
  versions_active: 'Distinct ZCC client versions with >10 devices. Fewer = more consistent fleet.',
  ghost_machines: 'Devices still Registered but not seen on ZIA for 31+ days. May be decommissioned hardware still consuming licenses.',
  bypass_risk: 'Devices where ZIA is Inactive but ZPA is Active. These can access private apps but internet traffic is NOT inspected.',
  live_blindspots: 'Devices with ZIA Inactive seen alive in the last 24h. Real-time uninspected traffic.',
  quarantined: 'Devices in Quarantined state, typically from compliance/posture check failures.',
  version_health: 'Service health (Active %) by ZCC version. Bars: ZIA (cyan), ZPA (green), ZDX (purple). Identifies which versions correlate with problems.',
  fleet_composition: 'ZCC client version distribution across the fleet, sorted by device count.',
  service_health: 'Overall ZIA, ZPA, ZDX active vs inactive percentages.',
  policy_health: 'Service health by assigned ZCC policy. Reveals policy-specific issues.',
  device_staleness: 'Distribution of device last-seen-on-ZIA age. Green=recent, Red=stale. "Never"=no ZIA timestamp.',
  tunnel_protocol: 'Tunnel 1.0 (L7, web only) vs Tunnel 2.0 (L3/L4, all traffic). 2.0 is the modern target.',
  registration_state: 'Device lifecycle: Registered, Remove Pending, Unregistered, Quarantined.',
  revert_status: 'ZCC Revert Status — tracks devices reverted from a newer version.',
  trust_levels: 'Strict enforcement vs General Deployment. Strict = users cannot disable ZCC.',
  os_distribution: 'Operating system breakdown across the fleet.',
  temporal_health: 'Service health and version trends across all loaded snapshots.',
};

const tip = d3.select('body').append('div').attr('class','tooltip');
const tempOv = d3.select('#temp-overlay');
const infoPop = d3.select('#info-popover');

function showTip(ev,html){tip.html(html).style('opacity',1).style('left',(ev.pageX+12)+'px').style('top',(ev.pageY-10)+'px')}
function hideTip(){tip.style('opacity',0)}
function hc(p){return p>=95?C.green:p>=85?C.amber:p>0?C.red:C.muted}
function hcls(p){return p>=95?'h-good':p>=85?'h-warn':p>0?'h-bad':'h-na'}
function fmt(n){return n.toLocaleString()}
function pct(a,t){return t?Math.round(a/t*100*10)/10:0}

// ── Temporal hover ───────────────────────────────────────────────────────
function getSFor(i) {
  const raw = SS[i];
  if (cc === 'ALL' || !raw.by_country || !raw.by_country[cc]) return raw;
  return raw.by_country[cc];
}
function showTemporal(ev, label, metricFn) {
  if (!MULTI) return;
  let h = '<div class="to-title">' + label + (cc!=='ALL'?' ('+cc+')':'') + '</div>';
  SS.forEach((s,i) => {
    const v = metricFn(getSFor(i));
    const prev = i > 0 ? metricFn(getSFor(i-1)) : null;
    let delta = '';
    if (prev !== null && typeof v === 'number' && typeof prev === 'number') {
      const d = v - prev;
      if (Math.abs(d) > 0.05) {
        const sign = d > 0 ? '+' : '';
        const cls = d > 0 ? 'up' : 'down';
        delta = '<span class="to-delta ' + cls + '">' + sign + (Number.isInteger(d) ? fmt(d) : d.toFixed(1)) + '</span>';
      }
    }
    const cls = i === ci ? ' current' : '';
    const vStr = typeof v === 'number' ? (v % 1 ? v.toFixed(1) + '%' : fmt(v)) : v;
    h += '<div class="to-row' + cls + '"><span class="to-date">' + s.snapshot_short + '</span><span>' + vStr + delta + '</span></div>';
  });
  tempOv.html(h).style('opacity',1).style('left',(ev.pageX+15)+'px').style('top',(ev.pageY-10)+'px');
}
function hideTemporal(){tempOv.style('opacity',0)}

// ── Info popovers ────────────────────────────────────────────────────────
document.addEventListener('click', function(e) {
  const btn = e.target.closest('.info-btn');
  if (!btn) { infoPop.style('opacity',0); return; }
  const key = btn.dataset.key;
  const text = INFO[key] || '';
  if (!text) return;
  const r = btn.getBoundingClientRect();
  infoPop.text(text).style('opacity',1).style('left',(r.left)+'px').style('top',(r.bottom+6+window.scrollY)+'px');
  e.stopPropagation();
});

// ── Snapshot selector ────────────────────────────────────────────────────
function renderSelector() {
  const bar = d3.select('#snapshot-bar');
  bar.html('<span class="snap-label">Snapshot:</span>');
  SS.forEach((s,i) => {
    bar.append('button').attr('class','snap-pill' + (i===ci?' active':'')).text(s.snapshot_short + ' (' + fmt(s.total_devices) + ')')
      .on('click', function(){ selectSnapshot(i); });
  });
  renderCountrySelector();
}
function selectSnapshot(i) {
  ci = i;
  d3.selectAll('#snapshot-bar .snap-pill').classed('active', (d,j) => j===i);
  renderCountrySelector();
  renderAll();
}

// ── Country selector ─────────────────────────────────────────────────────
function renderCountrySelector() {
  const bar = d3.select('#country-bar');
  bar.html('<span class="snap-label">Country:</span>');
  const S = SS[ci];
  const countries = S.countries || [];
  bar.append('button').attr('class','snap-pill' + (cc==='ALL'?' active':'')).text('All (' + fmt(S.total_devices) + ')')
    .on('click', function(){ selectCountry('ALL'); });
  countries.forEach(([code, count]) => {
    bar.append('button').attr('class','snap-pill' + (cc===code?' active':''))
      .text(code + ' (' + fmt(count) + ')')
      .on('click', function(){ selectCountry(code); });
  });
}
function selectCountry(code) {
  cc = code;
  d3.selectAll('#country-bar .snap-pill').classed('active', false);
  d3.selectAll('#country-bar .snap-pill').each(function() {
    const t = d3.select(this).text();
    if ((code==='ALL' && t.startsWith('All')) || t.startsWith(code+' ')) d3.select(this).classed('active', true);
  });
  renderAll();
  renderTemporal();
}

// ── Header meta ──────────────────────────────────────────────────────────
function renderMeta(S) {
  const m = d3.select('#header-meta');
  m.html('');
  m.append('div').attr('class','meta-item').html('<span class="meta-label">Snapshot</span><span class="meta-value">'+S.snapshot+'</span>');
  m.append('div').attr('class','meta-item').html('<span class="meta-label">Devices</span><span class="meta-value">'+fmt(S.total_devices)+'</span>');
  if(MULTI) m.append('div').attr('class','meta-item').html('<span class="meta-label">Snapshots</span><span class="meta-value">'+SS.length+'</span>');
}

// ── KPI Cards ────────────────────────────────────────────────────────────
function renderKPIs(S) {
  const el = d3.select('#kpi-row');
  el.html('');
  const prev = ci > 0 ? getSFor(ci-1) : null;
  function deltaHtml(cur, old, suffix, invert) {
    if (!old && old !== 0) return '';
    const d = cur - old;
    if (Math.abs(d) < 0.05) return '<span class="kpi-delta flat">&rarr;</span>';
    const cls = (invert ? d < 0 : d > 0) ? 'up' : 'down';
    const sign = d > 0 ? '+' : '';
    return '<span class="kpi-delta '+cls+'">' + sign + (Number.isInteger(d)?fmt(d):d.toFixed(1)) + (suffix||'') + '</span>';
  }

  const items = [
    {t:'Total Fleet',v:fmt(S.total_devices),s:'devices',st:'neutral',k:'total_fleet',
     delta: prev ? deltaHtml(S.total_devices,prev.total_devices,'') : '',
     tfn: s=>s.total_devices},
    {t:'ZIA Active',v:pct(S.zia_active,S.zia_total)+'%',s:fmt(S.zia_active)+' / '+fmt(S.zia_total),
     st:pct(S.zia_active,S.zia_total)>=95?'good':pct(S.zia_active,S.zia_total)>=85?'warn':'bad',k:'zia_active',
     delta:prev?deltaHtml(pct(S.zia_active,S.zia_total),pct(prev.zia_active,prev.zia_total),'%'):'',
     tfn:s=>pct(s.zia_active,s.zia_total)},
    {t:'ZPA Active',v:pct(S.zpa_active,S.zpa_total)+'%',s:fmt(S.zpa_active)+' / '+fmt(S.zpa_total),
     st:pct(S.zpa_active,S.zpa_total)>=95?'good':pct(S.zpa_active,S.zpa_total)>=85?'warn':'bad',k:'zpa_active',
     delta:prev?deltaHtml(pct(S.zpa_active,S.zpa_total),pct(prev.zpa_active,prev.zpa_total),'%'):'',
     tfn:s=>pct(s.zpa_active,s.zpa_total)},
    {t:'ZDX Active',v:pct(S.zdx_active,S.zdx_total)+'%',s:fmt(S.zdx_active)+' / '+fmt(S.zdx_total),
     st:pct(S.zdx_active,S.zdx_total)>=95?'good':pct(S.zdx_active,S.zdx_total)>=85?'warn':'bad',k:'zdx_active',
     delta:prev?deltaHtml(pct(S.zdx_active,S.zdx_total),pct(prev.zdx_active,prev.zdx_total),'%'):'',
     tfn:s=>pct(s.zdx_active,s.zdx_total)},
    {t:'Versions Active',v:S.version_distribution.filter(d=>d[1]>10).length,s:'with 10+ devices',st:'neutral',k:'versions_active',
     tfn:s=>s.version_distribution.filter(d=>d[1]>10).length},
    {t:'Ghost Machines',v:fmt(S.ghost_machines),s:'31d+ unseen',
     st:S.ghost_machines>500?'bad':S.ghost_machines>100?'warn':'good',k:'ghost_machines',
     delta:prev?deltaHtml(S.ghost_machines,prev.ghost_machines,'',true):'',
     tfn:s=>s.ghost_machines},
  ];

  items.forEach(it => {
    const card = el.append('div').attr('class','kpi-card kpi-'+it.st);
    card.append('div').attr('class','kpi-title').html(it.t + ' <span class="info-btn" data-key="'+it.k+'"></span>');
    card.append('div').attr('class','kpi-value').html(it.v + (it.delta||''));
    card.append('div').attr('class','kpi-sub').text(it.s);
    if (it.tfn) {
      card.on('mouseenter', function(ev){ showTemporal(ev, it.t, it.tfn); })
          .on('mousemove', function(ev){ tempOv.style('left',(ev.pageX+15)+'px').style('top',(ev.pageY-10)+'px'); })
          .on('mouseleave', hideTemporal);
    }
  });
}

// ── Alert tiles ──────────────────────────────────────────────────────────
function renderAlerts(S) {
  const el = d3.select('#alert-row');
  el.html('');
  [{t:'Bypass Risk',v:S.bypass_risk,d:'ZIA off + ZPA on',c:'red',k:'bypass_risk',fn:s=>s.bypass_risk},
   {t:'Live Blind Spots',v:S.live_blindspots,d:'ZIA off, seen <24h',c:'amber',k:'live_blindspots',fn:s=>s.live_blindspots},
   {t:'Ghost Machines',v:S.ghost_machines,d:'Registered, 31d+ stale',c:'amber',k:'ghost_machines',fn:s=>s.ghost_machines},
   {t:'Quarantined',v:S.quarantined,d:'Compliance failure',c:S.quarantined>50?'red':'amber',k:'quarantined',fn:s=>s.quarantined}
  ].forEach(a => {
    const tile = el.append('div').attr('class','alert-tile alert-'+a.c);
    tile.append('div').attr('class','alert-count').text(fmt(a.v));
    tile.append('div').attr('class','alert-title').html(a.t+' <span class="info-btn" data-key="'+a.k+'"></span>');
    tile.append('div').attr('class','alert-desc').text(a.d);
    tile.on('mouseenter',function(ev){showTemporal(ev,a.t,a.fn)})
        .on('mousemove',function(ev){tempOv.style('left',(ev.pageX+15)+'px').style('top',(ev.pageY-10)+'px')})
        .on('mouseleave',hideTemporal);
  });
}

// ── Version Health Chart ─────────────────────────────────────────────────
function renderVH(S) {
  const ct = d3.select('#vh-chart'); ct.html('');
  const data = S.version_health.filter(d=>d.count>20).slice(0,12);
  if(!data.length) return;
  const W = Math.min(ct.node().getBoundingClientRect().width,1400);
  const H = Math.max(data.length*42+60,200);
  const M = {t:30,r:30,b:10,l:160};
  const svg = ct.append('svg').attr('width',W).attr('height',H);
  const y = d3.scaleBand().domain(data.map(d=>d.version)).range([M.t,H-M.b]).padding(.25);
  const x = d3.scaleLinear().domain([0,100]).range([M.l,W-M.r]);

  svg.selectAll('.gl').data(x.ticks(5)).join('line').attr('class','grid-line')
    .attr('x1',d=>x(d)).attr('x2',d=>x(d)).attr('y1',M.t).attr('y2',H-M.b);

  svg.append('g').selectAll('text').data(data).join('text')
    .attr('x',M.l-8).attr('y',d=>y(d.version)+y.bandwidth()/2).attr('dy','.35em').attr('text-anchor','end').attr('font-size','11px')
    .text(d=>d.version);

  const svcs = [{k:'zia_pct',l:'ZIA',c:C.cyan},{k:'zpa_pct',l:'ZPA',c:C.green},{k:'zdx_pct',l:'ZDX',c:C.purple}];
  const bH = y.bandwidth()/3.5;

  svcs.forEach((svc,i) => {
    svg.selectAll('.b-'+svc.k).data(data).join('rect')
      .attr('x',M.l).attr('y',d=>y(d.version)+i*(bH+1))
      .attr('width',d=>Math.max(0,x(d[svc.k])-M.l)).attr('height',bH)
      .attr('fill',svc.c).attr('opacity',.8)
      .on('mouseover',function(ev,d){
        d3.select(this).attr('opacity',1);
        showTip(ev,'<strong>'+d.version+'</strong><br>'+svc.l+': '+d[svc.k]+'% active ('+d[svc.k.replace('_pct','_inactive')]+' inactive)');
        if(MULTI) showTemporal(ev,d.version+' — '+svc.l,s=>{const vh=s.version_health.find(v=>v.version===d.version);return vh?vh[svc.k]:0});
      })
      .on('mousemove',function(ev){tip.style('left',(ev.pageX+12)+'px').style('top',(ev.pageY-10)+'px');tempOv.style('left',(ev.pageX+15)+'px').style('top',(ev.pageY+30)+'px')})
      .on('mouseout',function(){d3.select(this).attr('opacity',.8);hideTip();hideTemporal()});

    svg.selectAll('.l-'+svc.k).data(data).join('text')
      .attr('x',d=>x(d[svc.k])+4).attr('y',d=>y(d.version)+i*(bH+1)+bH/2)
      .attr('dy','.35em').attr('font-size','9px').attr('fill',svc.c)
      .text(d=>d[svc.k]+'%');
  });

  const leg = svg.append('g').attr('transform','translate('+M.l+',8)');
  svcs.forEach((s,i)=>{leg.append('rect').attr('x',i*80).attr('y',0).attr('width',10).attr('height',10).attr('fill',s.c);leg.append('text').attr('x',i*80+14).attr('y',9).attr('font-size','10px').text(s.l)});
}

// ── Version Health Table ─────────────────────────────────────────────────
function renderVHTable(S) {
  const el = d3.select('#vh-table');
  el.html('');
  let h = '<table class="data-table"><thead><tr><th>Version</th><th class="r">Devices</th><th class="r">% Fleet</th><th class="r">ZIA Active</th><th class="r">ZPA Active</th><th class="r">ZDX Active</th><th class="r dim">ZIA Inact</th><th class="r dim">ZPA Inact</th><th class="r dim">ZDX Inact</th></tr></thead><tbody>';
  S.version_health.forEach(v => {
    const pf = S.total_service ? (v.count/S.total_service*100).toFixed(1) : 0;
    h += '<tr><td class="mono">'+v.version+'</td><td class="r mono">'+fmt(v.count)+'</td><td class="r mono">'+pf+'%</td>';
    h += '<td class="r '+hcls(v.zia_pct)+'">'+v.zia_pct+'%</td>';
    h += '<td class="r '+hcls(v.zpa_pct)+'">'+v.zpa_pct+'%</td>';
    h += '<td class="r '+hcls(v.zdx_pct)+'">'+v.zdx_pct+'%</td>';
    h += '<td class="r mono dim">'+fmt(v.zia_inactive)+'</td><td class="r mono dim">'+fmt(v.zpa_inactive)+'</td><td class="r mono dim">'+fmt(v.zdx_inactive)+'</td></tr>';
  });
  h += '</tbody></table>';
  el.html(h);
}

// ── Version Distribution ─────────────────────────────────────────────────
function renderVerDist(S) {
  const ct = d3.select('#ver-dist'); ct.html('');
  const data = S.version_distribution.slice(0,12).map(d=>({label:d[0],value:d[1]}));
  if(!data.length) return;
  const W = ct.node().getBoundingClientRect().width;
  const H = data.length*30+20;
  const M = {t:10,r:60,b:10,l:140};
  const svg = ct.append('svg').attr('width',W).attr('height',H);
  const y = d3.scaleBand().domain(data.map(d=>d.label)).range([M.t,H-M.b]).padding(.2);
  const x = d3.scaleLinear().domain([0,d3.max(data,d=>d.value)]).range([M.l,W-M.r]);

  svg.selectAll('rect').data(data).join('rect')
    .attr('x',M.l).attr('y',d=>y(d.label)).attr('width',d=>x(d.value)-M.l).attr('height',y.bandwidth())
    .attr('fill',(d,i)=>PAL[i%PAL.length]).attr('opacity',.8)
    .on('mouseover',function(ev,d){d3.select(this).attr('opacity',1);showTip(ev,'<strong>'+d.label+'</strong><br>'+fmt(d.value)+' devices');
      if(MULTI) showTemporal(ev,d.label,s=>{const f=s.version_distribution.find(v=>v[0]===d.label);return f?f[1]:0})})
    .on('mousemove',function(ev){tip.style('left',(ev.pageX+12)+'px').style('top',(ev.pageY-10)+'px');tempOv.style('left',(ev.pageX+15)+'px').style('top',(ev.pageY+30)+'px')})
    .on('mouseout',function(){d3.select(this).attr('opacity',.8);hideTip();hideTemporal()});

  svg.selectAll('.vl').data(data).join('text').attr('x',M.l-6).attr('y',d=>y(d.label)+y.bandwidth()/2)
    .attr('dy','.35em').attr('text-anchor','end').attr('font-size','10px')
    .text(d=>d.label.length>18?d.label.slice(0,18)+'…':d.label);
  svg.selectAll('.vc').data(data).join('text').attr('x',d=>x(d.value)+4).attr('y',d=>y(d.label)+y.bandwidth()/2)
    .attr('dy','.35em').attr('font-size','10px').attr('fill',C.cyan).text(d=>fmt(d.value));
}

// ── Service Donuts ───────────────────────────────────────────────────────
function renderDonuts(S) {
  const ct = d3.select('#svc-donuts'); ct.html('');
  [{k:'zia',l:'ZIA',c:C.cyan},{k:'zpa',l:'ZPA',c:C.green},{k:'zdx',l:'ZDX',c:C.purple}].forEach(svc=>{
    const a=S[svc.k+'_active'],in_=S[svc.k+'_inactive'],t=a+in_;
    const p=t?Math.round(a/t*100):0;
    const item=ct.append('div').attr('class','donut-item');
    const sz=130; const svg=item.append('svg').attr('width',sz).attr('height',sz);
    const g=svg.append('g').attr('transform','translate('+sz/2+','+sz/2+')');
    const arc=d3.arc().innerRadius(40).outerRadius(58);
    g.selectAll('path').data(d3.pie().sort(null).value(d=>d)([a,in_])).join('path').attr('d',arc).attr('fill',(d,i)=>i===0?svc.c:'#1a2235');
    g.append('text').attr('text-anchor','middle').attr('dy','-.1em').attr('font-size','18px').attr('font-weight','700').attr('fill',svc.c).text(p+'%');
    g.append('text').attr('text-anchor','middle').attr('dy','1.2em').attr('font-size','9px').attr('fill','#8899aa').text(fmt(a)+' active');
    item.append('div').attr('class','donut-label').text(svc.l);
    item.on('mouseenter',function(ev){showTemporal(ev,svc.l+' Active %',s=>pct(s[svc.k+'_active'],s[svc.k+'_total']))})
        .on('mousemove',function(ev){tempOv.style('left',(ev.pageX+15)+'px').style('top',(ev.pageY-10)+'px')})
        .on('mouseleave',hideTemporal);
  });
}

// ── Policy Health Chart ──────────────────────────────────────────────────
function renderPolChart(S) {
  const ct = d3.select('#pol-chart'); ct.html('');
  const data = S.policy_health.filter(d=>d.count>20).slice(0,10);
  if(!data.length) return;
  const W = ct.node().getBoundingClientRect().width;
  const H = data.length*34+40;
  const M = {t:25,r:30,b:10,l:200};
  const svg = ct.append('svg').attr('width',W).attr('height',H);
  const y = d3.scaleBand().domain(data.map(d=>d.policy)).range([M.t,H-M.b]).padding(.3);
  const x = d3.scaleLinear().domain([0,100]).range([M.l,W-M.r]);
  svg.selectAll('.gl').data(x.ticks(5)).join('line').attr('class','grid-line').attr('x1',d=>x(d)).attr('x2',d=>x(d)).attr('y1',M.t).attr('y2',H-M.b);
  svg.selectAll('.pl').data(data).join('text').attr('x',M.l-6).attr('y',d=>y(d.policy)+y.bandwidth()/2).attr('dy','.35em').attr('text-anchor','end').attr('font-size','10px')
    .text(d=>d.policy.length>26?d.policy.slice(0,26)+'…':d.policy);
  const bH=y.bandwidth()/2.5;
  [{k:'zia_pct',l:'ZIA',c:C.cyan},{k:'zpa_pct',l:'ZPA',c:C.green}].forEach((svc,i)=>{
    svg.selectAll('.pb-'+svc.k).data(data).join('rect').attr('x',M.l).attr('y',d=>y(d.policy)+i*(bH+1))
      .attr('width',d=>Math.max(0,x(d[svc.k])-M.l)).attr('height',bH).attr('fill',svc.c).attr('opacity',.8);
    svg.selectAll('.plb-'+svc.k).data(data).join('text').attr('x',d=>x(d[svc.k])+3).attr('y',d=>y(d.policy)+i*(bH+1)+bH/2)
      .attr('dy','.35em').attr('font-size','9px').attr('fill',svc.c).text(d=>d[svc.k]+'%');
  });
  const leg=svg.append('g').attr('transform','translate('+M.l+',6)');
  [{l:'ZIA',c:C.cyan},{l:'ZPA',c:C.green}].forEach((s,i)=>{leg.append('rect').attr('x',i*60).attr('y',0).attr('width',10).attr('height',10).attr('fill',s.c);leg.append('text').attr('x',i*60+14).attr('y',9).attr('font-size','10px').text(s.l)});
}

// ── Stale Devices ────────────────────────────────────────────────────────
function renderStale(S) {
  const ct = d3.select('#stale-chart'); ct.html('');
  const data = S.stale_buckets.map(d=>({label:d[0],value:d[1]}));
  if(!data.length) return;
  const W = ct.node().getBoundingClientRect().width;
  const H = 240;
  const M = {t:20,r:20,b:40,l:55};
  const svg = ct.append('svg').attr('width',W).attr('height',H);
  const xS = d3.scaleBand().domain(data.map(d=>d.label)).range([M.l,W-M.r]).padding(.2);
  const yS = d3.scaleLinear().domain([0,d3.max(data,d=>d.value)*1.1]).range([H-M.b,M.t]);
  svg.selectAll('.gl').data(yS.ticks(4)).join('line').attr('class','grid-line').attr('x1',M.l).attr('x2',W-M.r).attr('y1',d=>yS(d)).attr('y2',d=>yS(d));
  const sc = [C.green,C.cyan,C.amber,C.red,'#ff1744','#b71c1c',C.muted];
  svg.selectAll('rect').data(data).join('rect')
    .attr('x',d=>xS(d.label)).attr('y',d=>yS(d.value)).attr('width',xS.bandwidth()).attr('height',d=>H-M.b-yS(d.value))
    .attr('fill',(d,i)=>sc[i]||C.muted).attr('opacity',.8)
    .on('mouseover',function(ev,d){d3.select(this).attr('opacity',1);showTip(ev,'<strong>'+d.label+'</strong><br>'+fmt(d.value)+' devices');
      if(MULTI) showTemporal(ev,'Stale: '+d.label,s=>{const b=s.stale_buckets.find(x=>x[0]===d.label);return b?b[1]:0})})
    .on('mousemove',function(ev){tip.style('left',(ev.pageX+12)+'px').style('top',(ev.pageY-10)+'px');tempOv.style('left',(ev.pageX+15)+'px').style('top',(ev.pageY+30)+'px')})
    .on('mouseout',function(){d3.select(this).attr('opacity',.8);hideTip();hideTemporal()});
  svg.selectAll('.sl').data(data).join('text').attr('x',d=>xS(d.label)+xS.bandwidth()/2).attr('y',d=>yS(d.value)-4).attr('text-anchor','middle').attr('font-size','10px').attr('fill',C.cyan).text(d=>d.value>0?fmt(d.value):'');
  svg.append('g').attr('transform','translate(0,'+(H-M.b)+')').call(d3.axisBottom(xS).tickSize(0)).selectAll('text').attr('font-size','9px').attr('fill','#8899aa');
  svg.select('.domain').attr('stroke','var(--border)');
}

// ── Simple horizontal bar helper ─────────────────────────────────────────
function simpleHBar(sel,items,max){
  const ct = d3.select(sel); ct.html('');
  const data = items.slice(0,max||8).map(d=>({label:d[0],value:d[1]}));
  if(!data.length) return;
  const W = ct.node().getBoundingClientRect().width;
  const H = data.length*28+10;
  const M = {t:5,r:55,b:5,l:120};
  const svg = ct.append('svg').attr('width',W).attr('height',H);
  const y = d3.scaleBand().domain(data.map(d=>d.label)).range([M.t,H-M.b]).padding(.2);
  const x = d3.scaleLinear().domain([0,d3.max(data,d=>d.value)]).range([M.l,W-M.r]);
  svg.selectAll('rect').data(data).join('rect').attr('x',M.l).attr('y',d=>y(d.label)).attr('width',d=>Math.max(0,x(d.value)-M.l)).attr('height',y.bandwidth()).attr('fill',(d,i)=>PAL[i%PAL.length]).attr('opacity',.7);
  svg.selectAll('.hl').data(data).join('text').attr('x',M.l-6).attr('y',d=>y(d.label)+y.bandwidth()/2).attr('dy','.35em').attr('text-anchor','end').attr('font-size','10px')
    .text(d=>d.label.length>16?d.label.slice(0,16)+'…':d.label);
  svg.selectAll('.hc').data(data).join('text').attr('x',d=>x(d.value)+4).attr('y',d=>y(d.label)+y.bandwidth()/2).attr('dy','.35em').attr('font-size','10px').attr('fill',C.cyan).text(d=>fmt(d.value));
}

// ── Policy Table ─────────────────────────────────────────────────────────
function renderPolTable(S) {
  const el = d3.select('#pol-table'); el.html('');
  let h = '<table class="data-table"><thead><tr><th>Policy</th><th class="r">Devices</th><th class="r">ZIA Active</th><th class="r">ZPA Active</th></tr></thead><tbody>';
  S.policy_health.forEach(p => {
    h += '<tr><td>'+p.policy+'</td><td class="r mono">'+fmt(p.count)+'</td><td class="r '+hcls(p.zia_pct)+'">'+p.zia_pct+'%</td><td class="r '+hcls(p.zpa_pct)+'">'+p.zpa_pct+'%</td></tr>';
  });
  el.html(h+'</tbody></table>');
}

// ── Temporal charts ──────────────────────────────────────────────────────
function renderTemporal() {
  if(!MULTI) return;
  d3.select('#temporal-section').style('display','block');
  // Compute temporal from current country filter
  const T = {labels:[], zia_pct:[], zpa_pct:[], zdx_pct:[], total_devices:[], version_series:[]};
  const verTrack = {};
  SS.forEach((raw,i) => {
    const s = getSFor(i);
    // Use time-aware label to avoid duplicates on same day
    const ts = raw.snapshot || '';
    const m = ts.match(/(\d{2})-(\d{2}) (\d{2}):(\d{2})/);
    const lbl = m ? m[1]+'/'+m[2]+' '+m[3]+':'+m[4] : s.snapshot_short;
    T.labels.push(lbl);
    T.total_devices.push(s.total_devices);
    T.zia_pct.push(pct(s.zia_active,s.zia_total));
    T.zpa_pct.push(pct(s.zpa_active,s.zpa_total));
    T.zdx_pct.push(pct(s.zdx_active,s.zdx_total));
    (s.version_distribution||[]).forEach(([v,c])=>{if(!verTrack[v])verTrack[v]={};verTrack[v][lbl]=c});
  });
  // Top versions
  const allVC={};Object.entries(verTrack).forEach(([v,m])=>{allVC[v]=Object.values(m).reduce((a,b)=>a+b,0)});
  const topV=Object.entries(allVC).sort((a,b)=>b[1]-a[1]).slice(0,8).map(d=>d[0]);
  topV.forEach(v=>{T.version_series.push({version:v,data:T.labels.map(l=>verTrack[v][l]||0)})});

  // Health over time
  const ct1 = d3.select('#temp-health'); ct1.html('');
  const W = ct1.node().getBoundingClientRect().width;
  const H = 260; const M = {t:30,r:30,b:40,l:50};
  const svg1 = ct1.append('svg').attr('width',W).attr('height',H);
  const x = d3.scalePoint().domain(T.labels).range([M.l,W-M.r]).padding(.3);
  const y = d3.scaleLinear().domain([Math.min(d3.min(T.zia_pct),d3.min(T.zpa_pct),d3.min(T.zdx_pct))-2,100]).range([H-M.b,M.t]);
  svg1.selectAll('.gl').data(y.ticks(5)).join('line').attr('class','grid-line').attr('x1',M.l).attr('x2',W-M.r).attr('y1',d=>y(d)).attr('y2',d=>y(d));
  const line = d3.line().x((d,i)=>x(T.labels[i])).y(d=>y(d)).curve(d3.curveMonotoneX);
  [{d:T.zia_pct,l:'ZIA',c:C.cyan},{d:T.zpa_pct,l:'ZPA',c:C.green},{d:T.zdx_pct,l:'ZDX',c:C.purple}].forEach(s=>{
    svg1.append('path').datum(s.d).attr('fill','none').attr('stroke',s.c).attr('stroke-width',2).attr('d',line);
    svg1.selectAll('.dot-'+s.l).data(s.d).join('circle').attr('cx',(d,i)=>x(T.labels[i])).attr('cy',d=>y(d)).attr('r',4).attr('fill',s.c);
  });
  svg1.append('g').attr('transform','translate(0,'+(H-M.b)+')').call(d3.axisBottom(x).tickSize(0)).selectAll('text').attr('font-size','10px').attr('fill','#8899aa');
  svg1.append('g').attr('transform','translate('+M.l+',0)').call(d3.axisLeft(y).ticks(5).tickFormat(d=>d+'%')).selectAll('text').attr('font-size','10px').attr('fill','#8899aa');
  svg1.selectAll('.domain').attr('stroke','var(--border)');
  const leg1=svg1.append('g').attr('transform','translate('+(M.l+10)+',10)');
  [{l:'ZIA',c:C.cyan},{l:'ZPA',c:C.green},{l:'ZDX',c:C.purple}].forEach((s,i)=>{leg1.append('line').attr('x1',i*60).attr('x2',i*60+15).attr('y1',0).attr('y2',0).attr('stroke',s.c).attr('stroke-width',2);leg1.append('text').attr('x',i*60+20).attr('y',4).attr('font-size','10px').text(s.l)});

  // Versions over time
  const ct2 = d3.select('#temp-versions'); ct2.html('');
  const W2 = ct2.node().getBoundingClientRect().width;
  const svg2 = ct2.append('svg').attr('width',W2).attr('height',H);
  const x2 = d3.scalePoint().domain(T.labels).range([M.l+5,W2-M.r]).padding(.3);
  const allV = T.version_series.flatMap(s=>s.data);
  const y2 = d3.scaleLinear().domain([0,d3.max(allV)*1.1]).range([H-M.b,M.t]);
  svg2.selectAll('.gl').data(y2.ticks(5)).join('line').attr('class','grid-line').attr('x1',M.l+5).attr('x2',W2-M.r).attr('y1',d=>y2(d)).attr('y2',d=>y2(d));
  const line2 = d3.line().x((d,i)=>x2(T.labels[i])).y(d=>y2(d)).curve(d3.curveMonotoneX);
  T.version_series.forEach((vs,idx)=>{
    svg2.append('path').datum(vs.data).attr('fill','none').attr('stroke',PAL[idx%PAL.length]).attr('stroke-width',1.5).attr('d',line2);
    svg2.selectAll('.vd-'+idx).data(vs.data).join('circle').attr('cx',(d,i)=>x2(T.labels[i])).attr('cy',d=>y2(d)).attr('r',3).attr('fill',PAL[idx%PAL.length])
      .on('mouseover',function(ev){showTip(ev,'<strong>'+vs.version+'</strong><br>'+fmt(d3.select(this).datum())+' devices')})
      .on('mousemove',function(ev){tip.style('left',(ev.pageX+12)+'px').style('top',(ev.pageY-10)+'px')})
      .on('mouseout',hideTip);
  });
  svg2.append('g').attr('transform','translate(0,'+(H-M.b)+')').call(d3.axisBottom(x2).tickSize(0)).selectAll('text').attr('font-size','10px').attr('fill','#8899aa');
  svg2.append('g').attr('transform','translate('+(M.l+5)+',0)').call(d3.axisLeft(y2).ticks(5).tickFormat(d3.format(','))).selectAll('text').attr('font-size','10px').attr('fill','#8899aa');
  svg2.selectAll('.domain').attr('stroke','var(--border)');
  const leg2=svg2.append('g').attr('transform','translate('+(M.l+10)+',6)');
  T.version_series.slice(0,6).forEach((vs,i)=>{const c=Math.floor(i/3),r=i%3;leg2.append('rect').attr('x',c*180).attr('y',r*14).attr('width',8).attr('height',8).attr('fill',PAL[i]);leg2.append('text').attr('x',c*180+12).attr('y',r*14+8).attr('font-size','9px').text(vs.version.slice(0,22))});
}

// ── Get current data (respecting country filter) ─────────────────────────
function getS() {
  const raw = SS[ci];
  if (cc === 'ALL' || !raw.by_country || !raw.by_country[cc]) return raw;
  return raw.by_country[cc];
}

// ── Render All ───────────────────────────────────────────────────────────
function renderAll() {
  const S = getS();
  renderMeta(S); renderKPIs(S); renderAlerts(S); renderVH(S); renderVHTable(S);
  renderVerDist(S); renderDonuts(S); renderPolChart(S); renderStale(S);
  simpleHBar('#tunnel-chart',S.tunnel_versions,5);
  simpleHBar('#reg-chart',S.registration_states,6);
  simpleHBar('#revert-chart',S.revert_status,6);
  simpleHBar('#trust-chart',S.trust_levels,6);
  simpleHBar('#os-chart',S.os_breakdown,6);
  renderPolTable(S);
}

// ── Init ─────────────────────────────────────────────────────────────────
renderSelector();
renderAll();
renderTemporal();
})();
"""

# ── PDF export ────────────────────────────────────────────────────────────────

def export_pdf(html_path):
    pdf_path = html_path.replace(".html", ".pdf")
    for browser in [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        shutil.which("google-chrome") or "",
        shutil.which("chromium") or "",
        shutil.which("chromium-browser") or "",
    ]:
        if browser and os.path.exists(browser):
            try:
                subprocess.run([
                    browser, "--headless", "--disable-gpu",
                    f"--print-to-pdf={pdf_path}", "--no-margins",
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
        sd = analyze_with_countries(svc_rows, dev_rows, label)
        snapshots_data.append(sd)
        print(f"    Snapshot: {sd['snapshot']}  |  Countries: {len(sd['countries'])}\n")

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
