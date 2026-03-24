# streamlit_app.py — SREAgent

import streamlit as st
import time
import re
import json
import math
from datetime import datetime
from databricks.connect import DatabricksSession

CATALOG       = "olympus_hub"
SCHEMA        = "opsdata"
WORKSPACE_URL = "https://adb-XXXXXXX.azuredatabricks.net"  # ← update
USER_NAME     = "Astha"
USER_INITIALS = "AS"

st.set_page_config(
    page_title="SREAgent",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

:root {
  --bg:      #0A0A0A;
  --surface: #141414;
  --card:    #1A1A1A;
  --card2:   #202020;
  --bdr:     rgba(255,255,255,.08);
  --bdr2:    rgba(255,255,255,.05);
  --tx1: #FFFFFF; --tx2: #A1A1AA; --tx3: #71717A; --tx4: #3F3F46;
  --gold:  #F59E0B; --gold-l: #FCD34D; --gold-d: #D97706;
  --red:   #EF4444; --red-bg:  rgba(239,68,68,.12);  --red-b:  rgba(239,68,68,.25);
  --amber: #F59E0B; --amb-bg:  rgba(245,158,11,.12); --amb-b:  rgba(245,158,11,.25);
  --green: #22C55E; --grn-bg:  rgba(34,197,94,.12);  --grn-b:  rgba(34,197,94,.25);
  --blue:  #3B82F6; --blu-bg:  rgba(59,130,246,.12); --blu-b:  rgba(59,130,246,.25);
  --purple:#A855F7; --pur-bg:  rgba(168,85,247,.12); --pur-b:  rgba(168,85,247,.25);
  --radius: 8px; --pad: 28px;
  --font: 'Inter', sans-serif; --mono: 'JetBrains Mono', monospace;
}
html,body,[class*="css"]{font-family:var(--font);background:var(--bg);color:var(--tx1);font-size:14px}
.stApp{background:var(--bg)}
.block-container{padding:0 !important;max-width:100% !important}
#MainMenu,footer,header{visibility:hidden}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}

/* ════ SIDEBAR ════ */
[data-testid="stSidebar"]{background:var(--surface) !important;border-right:1px solid var(--bdr) !important;min-width:200px !important;max-width:200px !important}
[data-testid="stSidebar"]>div{padding:0 !important}
[data-testid="stSidebar"] .stButton>button{
  background:transparent !important;border:none !important;border-radius:var(--radius) !important;
  color:var(--tx2) !important;font-family:var(--font) !important;font-size:13px !important;
  font-weight:400 !important;text-transform:none !important;text-align:left !important;
  padding:8px 12px !important;width:100% !important;box-shadow:none !important;
  letter-spacing:.1px !important;transition:all .1s !important;
  justify-content:flex-start !important;margin:1px 0 !important;
}
[data-testid="stSidebar"] .stButton>button:hover{color:var(--tx1) !important;background:var(--bdr2) !important}
[data-testid="stSidebar"] .nav-on .stButton>button{background:var(--card2) !important;color:var(--tx1) !important;font-weight:500 !important}

/* ════ HEADER ════ */
.top-header{background:var(--bg);border-bottom:1px solid var(--bdr);padding:20px var(--pad) 18px;display:flex;align-items:center;justify-content:space-between}
.th-title{font-size:24px;font-weight:700;color:var(--tx1);line-height:1}
.th-sub{font-size:13px;color:var(--tx2);margin-top:3px;font-weight:400}
.live-pill{display:inline-flex;align-items:center;gap:6px;padding:4px 10px;background:var(--grn-bg);border:1px solid var(--grn-b);border-radius:20px;font-size:10px;font-weight:600;color:var(--green);letter-spacing:.5px;text-transform:uppercase}

/* ════ PAGE WRAPPER ════ */
.pg{padding:24px var(--pad) var(--pad)}

/* ════ KPI CARDS ════ */
.kpi-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:28px}
.kpi-card{background:var(--card);border:1px solid var(--bdr);border-radius:12px;padding:20px 20px 18px;display:flex;flex-direction:column;gap:4px}
.kpi-top{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:8px}
.kpi-icon{width:36px;height:36px;border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:16px;flex-shrink:0}
.kpi-icon.red{background:var(--red-bg)}.kpi-icon.amber{background:var(--amb-bg)}.kpi-icon.green{background:var(--grn-bg)}.kpi-icon.blue{background:var(--blu-bg)}.kpi-icon.purple{background:var(--pur-bg)}
.kpi-lbl{font-size:12px;font-weight:400;color:var(--tx2);margin-bottom:6px}
.kpi-num{font-size:36px;font-weight:700;color:var(--tx1);line-height:1}
.kpi-num .unit{font-size:18px;font-weight:400;color:var(--tx2)}
.kpi-delta{font-size:12px;font-weight:500;margin-top:4px}
.kpi-delta.up{color:var(--red)}.kpi-delta.dn{color:var(--green)}.kpi-delta.neu{color:var(--tx3)}

/* ════ SECTION HEADING ════ */
.sec-hd{display:flex;align-items:baseline;justify-content:space-between;margin-bottom:12px}
.sec-title{font-size:15px;font-weight:600;color:var(--tx1)}
.sec-sub{font-size:12px;color:var(--tx3)}

/* ════ DARK CARD ════ */
.dcard{background:var(--card);border:1px solid var(--bdr);border-radius:12px;padding:20px 22px;margin-bottom:16px}

/* ════ TABLE ════ */
.t-hdr{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:1.2px;color:var(--tx3);padding:8px 0;border-bottom:1px solid var(--bdr)}
.t-job{font-size:13px;font-weight:500;color:var(--tx1);padding:12px 0;font-family:var(--mono)}
.t-cell{font-size:12px;color:var(--tx2);padding:12px 0;line-height:1.4}
.t-muted{font-size:11px;color:var(--tx3);padding:12px 0}

/* ════ BADGES ════ */
.badge{display:inline-flex;align-items:center;gap:4px;font-size:10px;font-weight:700;padding:2px 8px;letter-spacing:.5px;text-transform:uppercase;border-radius:4px;white-space:nowrap}
.dot{width:5px;height:5px;border-radius:50%;display:inline-block;flex-shrink:0}
.b-red{background:var(--red-bg);color:var(--red);border:1px solid var(--red-b)}
.b-amb{background:var(--amb-bg);color:var(--amber);border:1px solid var(--amb-b)}
.b-grn{background:var(--grn-bg);color:var(--green);border:1px solid var(--grn-b)}
.b-blu{background:var(--blu-bg);color:var(--blue);border:1px solid var(--blu-b)}
.b-pur{background:var(--pur-bg);color:var(--purple);border:1px solid var(--pur-b)}
.b-gld{background:rgba(245,158,11,.12);color:var(--gold);border:1px solid rgba(245,158,11,.3)}
.b-gray{background:rgba(255,255,255,.06);color:var(--tx3);border:1px solid var(--bdr)}

/* ════ INVESTIGATE / METRIC DETAIL ════ */
.inv-title{font-size:24px;font-weight:700;color:var(--tx1);margin-bottom:10px;font-family:var(--mono)}
.inv-meta{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:20px}
.stat-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));background:var(--card);border:1px solid var(--bdr);border-radius:var(--radius);overflow:hidden;margin-bottom:22px}
.stat-cell{padding:14px 16px;border-right:1px solid var(--bdr)}
.stat-cell:last-child{border-right:none}
.stat-lbl{font-size:10px;font-weight:500;text-transform:uppercase;letter-spacing:1px;color:var(--tx3);margin-bottom:4px}
.stat-val{font-size:15px;font-weight:600;color:var(--tx1)}

.d-lbl{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:1px;color:var(--tx3);padding-bottom:6px;border-bottom:1px solid var(--bdr);margin-bottom:10px}
.d-box{background:var(--card);border:1px solid var(--bdr);border-radius:var(--radius);padding:14px 16px;margin-bottom:12px}
.d-box.gold{background:rgba(245,158,11,.08);border-color:rgba(245,158,11,.2)}
.d-box.sage{background:var(--grn-bg);border-color:var(--grn-b)}
.d-box.blue{background:var(--blu-bg);border-color:var(--blu-b)}
.d-box.purple{background:var(--pur-bg);border-color:var(--pur-b)}
.d-inner-lbl{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.8px;margin-bottom:7px}
.d-inner-lbl.gold{color:var(--gold)}.d-inner-lbl.sage{color:var(--green)}.d-inner-lbl.gray{color:var(--tx3)}.d-inner-lbl.blue{color:var(--blue)}.d-inner-lbl.purple{color:var(--purple)}
.d-body{font-size:13px;color:var(--tx2);line-height:1.7}
.d-body.dark{color:var(--tx1)}

/* ════ IMPACT BLOCK ════ */
.impact{padding:14px 16px;background:var(--card);border:1px solid var(--bdr);border-radius:var(--radius);border-left:3px solid var(--bdr);margin-bottom:12px}
.impact.high{border-left-color:var(--red)}.impact.medium{border-left-color:var(--amber)}.impact.low{border-left-color:var(--green)}
.impact-lbl{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;margin-bottom:8px;display:flex;align-items:center;gap:8px}
.impact-lbl.high{color:var(--red)}.impact-lbl.medium{color:var(--amber)}.impact-lbl.low{color:var(--green)}
.impact-pill{display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:700;letter-spacing:.5px}
.impact-pill.sla{background:rgba(239,68,68,.15);color:var(--red);border:1px solid rgba(239,68,68,.3)}
.impact-pill.sev-high{background:rgba(239,68,68,.15);color:var(--red);border:1px solid rgba(239,68,68,.3)}
.impact-pill.sev-medium{background:rgba(245,158,11,.15);color:var(--amber);border:1px solid rgba(245,158,11,.3)}
.impact-pill.sev-low{background:rgba(34,197,94,.15);color:var(--green);border:1px solid rgba(34,197,94,.3)}
.impact-desc{font-size:13px;color:var(--tx1);line-height:1.6;margin-top:8px}
.impact-meta{display:flex;gap:6px;flex-wrap:wrap;margin-top:8px}

/* Workspace link */
.ws-link{display:flex;align-items:center;justify-content:center;gap:8px;padding:11px 18px;background:rgba(59,130,246,.15);border:1px solid rgba(59,130,246,.35);border-radius:var(--radius);font-size:13px;font-weight:600;color:#60A5FA;text-decoration:none;transition:all .15s;margin-bottom:6px;letter-spacing:.2px}
.ws-link:hover{background:rgba(59,130,246,.25);border-color:rgba(59,130,246,.5);color:#93C5FD}
.ws-link-hint{font-size:11px;color:var(--tx3);text-align:center;margin-bottom:10px}

/* Metric value display */
.metric-val-block{background:var(--card2);border:1px solid var(--bdr);border-radius:var(--radius);padding:16px 20px;text-align:center;margin-bottom:12px}
.metric-val-big{font-size:42px;font-weight:700;color:var(--tx1);line-height:1;font-family:var(--mono)}
.metric-val-unit{font-size:14px;color:var(--tx3);margin-top:4px}

.conf-track{background:var(--bdr2);height:3px;border-radius:2px;margin-top:6px}
.conf-fill{height:3px;border-radius:2px;background:var(--gold)}

/* ════ TIMELINE ════ */
.tl{display:flex;flex-direction:column}
.tl-row{display:flex;gap:12px;padding-bottom:14px;position:relative}
.tl-row::before{content:'';position:absolute;left:5px;top:13px;bottom:0;width:1px;background:var(--bdr)}
.tl-row:last-child::before{display:none}
.tl-dot{width:11px;height:11px;border-radius:50%;flex-shrink:0;margin-top:2px}
.tl-dot.gray{background:var(--tx4);box-shadow:0 0 0 1px var(--bdr)}
.tl-dot.red{background:var(--red);box-shadow:0 0 0 1px var(--red-b)}
.tl-dot.amber{background:var(--amber);box-shadow:0 0 0 1px var(--amb-b)}
.tl-dot.green{background:var(--green);box-shadow:0 0 0 1px var(--grn-b)}
.tl-dot.blue{background:var(--blue);box-shadow:0 0 0 1px var(--blu-b)}
.tl-evt{font-size:12px;color:var(--tx2);line-height:1.4}
.tl-time{font-size:10px;color:var(--tx3);margin-top:2px}

/* ════ CHART CARDS ════ */
.chart-card{background:var(--card);border:1px solid var(--bdr);border-radius:12px;padding:20px 22px}
.chart-title{font-size:13px;font-weight:600;color:var(--tx1);margin-bottom:16px}
.bar-row{display:flex;align-items:center;gap:10px;margin-bottom:8px}
.bar-lbl{font-size:11px;color:var(--tx2);width:110px;flex-shrink:0;text-align:right}
.bar-track{flex:1;background:rgba(255,255,255,.06);height:5px;border-radius:3px}
.bar-fill{height:5px;border-radius:3px}
.bar-val{font-size:11px;color:var(--tx3);width:36px;text-align:right;flex-shrink:0}
.t-row{display:flex;align-items:center;justify-content:space-between;padding:7px 0;border-bottom:1px solid var(--bdr)}
.t-row:last-child{border-bottom:none}
.t-name{font-size:12px;color:var(--tx2)}
.t-val{font-size:15px;font-weight:600;color:var(--tx1)}

/* ════ NATIVE TABS ════ */
div[data-testid="stTabs"] [data-baseweb="tab-list"]{background:transparent !important;gap:0 !important;margin-bottom:18px !important;border-bottom:1px solid var(--bdr) !important}
div[data-testid="stTabs"] [data-baseweb="tab"]{background:transparent !important;border:none !important;border-bottom:2px solid transparent !important;border-radius:0 !important;color:var(--tx3) !important;font-family:var(--font) !important;font-size:12px !important;font-weight:500 !important;letter-spacing:.3px !important;padding:8px 16px !important;text-transform:uppercase !important}
div[data-testid="stTabs"] [data-baseweb="tab"]:hover{color:var(--tx1) !important;background:transparent !important}
div[data-testid="stTabs"] [aria-selected="true"]{color:var(--tx1) !important;border-bottom:2px solid var(--gold) !important;background:transparent !important}
div[data-testid="stTabs"] [data-baseweb="tab-panel"]{padding:0 !important;background:transparent !important}

/* ════ MAIN BUTTONS ════ */
div[data-testid="stMainBlockContainer"] .stButton>button{background:var(--card2);border:1px solid var(--bdr);color:var(--tx2);font-family:var(--font);font-size:11px;font-weight:500;letter-spacing:.3px;text-transform:uppercase;border-radius:var(--radius);padding:7px 16px;transition:all .12s}
div[data-testid="stMainBlockContainer"] .stButton>button:hover{background:var(--card);color:var(--tx1);border-color:rgba(255,255,255,.15)}
div[data-testid="stMainBlockContainer"] .stButton>button[kind="primary"]{background:var(--tx1);border-color:var(--tx1);color:#000;font-weight:600}
div[data-testid="stMainBlockContainer"] .stButton>button[kind="primary"]:hover{background:#E4E4E7;border-color:#E4E4E7}
div[data-testid="stMainBlockContainer"] .stButton>button:disabled{opacity:.4 !important;cursor:not-allowed !important}
label{font-family:var(--font) !important;font-size:11px !important;font-weight:500 !important;color:var(--tx3) !important;text-transform:uppercase;letter-spacing:.5px}
div[data-testid="stExpander"]{border:1px solid var(--bdr) !important;border-radius:var(--radius) !important;background:var(--card) !important}
hr{border-color:var(--bdr) !important;margin:2px 0 !important}
div[data-testid="stSelectbox"]>div>div{background:var(--card) !important;border-color:var(--bdr) !important;border-radius:var(--radius) !important;font-family:var(--font) !important;font-size:12px !important;color:var(--tx1) !important}
</style>
""", unsafe_allow_html=True)

# ── Connection ────────────────────────────────────────────
@st.cache_resource
def get_spark():
    return DatabricksSession.builder.serverless(True).getOrCreate()

spark = get_spark()

# ── Session state ─────────────────────────────────────────
for k, v in {
    "page":            "Overview",
    "selected_inc":    None,
    "selected_metric": None,
    "iv_dr":           "Last 7 days",
    "iv_sf":           "Open only",
    "iv_ps":           20,
    "iv_pg_o":         0,
    "iv_pg_r":         0,
    "mx_dr":           "Last 7 days",
    "mx_sev":          "All",
    "mx_cat":          "All",
    "mx_ps":           20,
    "mx_pg":           0,
}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Constants ─────────────────────────────────────────────
DELAYED_CATS = {"TIMEOUT","NETWORK","DEPENDENCY"}
BAR_COLOR = {
    "OOM":"#EF4444","CLUSTER_FAILURE":"#EF4444",
    "TIMEOUT":"#F59E0B","PERMISSION":"#F59E0B","DEPENDENCY":"#F59E0B",
    "DATA_QUALITY":"#3B82F6","NETWORK":"#3B82F6","UNKNOWN":"#71717A"
}
CAT_CLS = {
    "OOM":"b-red","CLUSTER_FAILURE":"b-red",
    "TIMEOUT":"b-amb","PERMISSION":"b-amb","DEPENDENCY":"b-amb",
    "DATA_QUALITY":"b-blu","NETWORK":"b-blu","UNKNOWN":"b-gray"
}
IMP_CLS  = {"high":"b-red","medium":"b-amb","low":"b-grn"}
SEV_CLS  = {"HIGH":"b-red","MEDIUM":"b-amb","LOW":"b-grn","CRITICAL":"b-red"}
SEV_BAR  = {"HIGH":"#EF4444","MEDIUM":"#F59E0B","LOW":"#22C55E","CRITICAL":"#EF4444"}

INV_OPTS = ["Last 24 hours","Last 7 days","Last 30 days","Last 90 days","All time"]
INV_DAYS = {"Last 24 hours":1,"Last 7 days":7,"Last 30 days":30,"Last 90 days":90,"All time":3650}

# ── Helpers ───────────────────────────────────────────────
def is_resolved(row):
    v = str(row.get("resolved_at",""))
    return bool(v and v not in ("","None","nan","NaT","<NA>"))

def derive_status(cat, resolved):
    if resolved: return "RESOLVED"
    return "DELAYED" if str(cat).upper() in DELAYED_CATS else "FAILED"

def clean(raw):
    if not raw or str(raw).strip() in ("","None","nan","NaT","<NA>"): return "—"
    s = str(raw).strip()
    s = re.sub(r'^\{[^}]*\}\s*','',s); s = re.sub(r'\s*\{[^}]*\}$','',s)
    s = re.sub(r'\{[^}]{0,200}\}','',s); s = re.sub(r'\*+','',s)
    return ' '.join(s.split()) or "—"

def make_impact(level_raw, impact_raw, fix, cause):
    """
    Parses business_impact — handles both plain text and JSON like
    {"SLA_AT_RISK": true, "SEVERITY": "HIGH", "MESSAGE": "..."}
    Returns (level_str, html_string)
    """
    lvl = str(level_raw).lower().strip() if level_raw else "medium"

    raw = str(impact_raw).strip() if impact_raw else ""
    parsed = None

    # Try JSON parse
    if raw.startswith("{"):
        try:
            parsed = json.loads(raw)
        except Exception:
            m = re.search(r'\{[^}]+\}', raw)
            if m:
                try: parsed = json.loads(m.group())
                except: pass

    if parsed:
        sla      = parsed.get("SLA_AT_RISK") or parsed.get("sla_at_risk")
        sev      = str(parsed.get("SEVERITY") or parsed.get("severity") or "").upper()
        msg      = parsed.get("MESSAGE") or parsed.get("message") or \
                   parsed.get("DESCRIPTION") or parsed.get("description","")
        affected = parsed.get("AFFECTED_PIPELINES") or parsed.get("affected_pipelines","")
        region   = parsed.get("REGION") or parsed.get("region","")
        team     = parsed.get("TEAM") or parsed.get("team","")

        # Derive level from JSON severity if top-level impact is vague
        if lvl in ("nan","none","","low") and sev:
            sev_map = {"CRITICAL":"high","HIGH":"high","MEDIUM":"medium","LOW":"low"}
            lvl = sev_map.get(sev, lvl)

        # Build pill row
        pills = []
        if sla:
            pills.append('<span class="impact-pill sla">⚠ SLA AT RISK</span>')
        if sev:
            sev_cls = "sev-high" if sev in ("HIGH","CRITICAL") else f"sev-{sev.lower()}"
            pills.append(f'<span class="impact-pill {sev_cls}">SEVERITY: {sev}</span>')
        pills_html = '<div class="impact-meta">' + "".join(pills) + '</div>' if pills else ""

        # Build description
        desc_parts = []
        if msg:    desc_parts.append(str(msg))
        if affected: desc_parts.append(f"Affected pipelines: {affected}.")
        if region: desc_parts.append(f"Region: {region}.")
        if team:   desc_parts.append(f"Owning team: {team}.")
        if sla and not msg:
            desc_parts.append("Pipeline is breaching agreed service levels.")

        desc = " ".join(desc_parts) if desc_parts else (
            clean(fix) if clean(fix) != "—" and len(clean(fix)) > 20
            else clean(cause) if clean(cause) != "—"
            else "Review the root cause and apply the suggested fix."
        )

        return lvl, pills_html, desc

    # Plain text fallback
    fc, cc = clean(fix), clean(cause)
    if raw and raw not in ("—","None","nan") and not raw.startswith("{"):
        return lvl, "", raw
    if fc and fc != "—" and len(fc) > 20: return lvl, "", fc
    if cc and cc != "—" and len(cc) > 15: return lvl, "", cc
    return lvl, "", {
        "high":   "Critical failure — downstream consumers blocked, data not updating.",
        "medium": "Pipeline delayed — some reports may show stale data.",
        "low":    "Minor issue — limited impact, monitoring recommended.",
    }.get(lvl,"Pipeline failure detected.")

def conf_pct(row):
    try:
        v = row.get("confidence","")
        return int(float(v)*100) if v and str(v) not in ("","None","nan") else 0
    except: return 0

def b(txt, cls): return f'<span class="badge {cls}">{txt}</span>'
def cat_badge(cat): return b(str(cat).upper(), CAT_CLS.get(str(cat).upper(),"b-gray"))
def imp_badge(imp): return b(f"{str(imp).lower()} impact", IMP_CLS.get(str(imp).lower(),"b-gray"))
def sev_badge(sev):
    s = str(sev).upper()
    return b(s, SEV_CLS.get(s,"b-gray"))

def status_badge(cat, resolved):
    s = derive_status(cat, resolved)
    if s == "RESOLVED": return '<span class="badge b-grn"><span class="dot" style="background:#22C55E"></span>RESOLVED</span>'
    if s == "DELAYED":  return '<span class="badge b-amb"><span class="dot" style="background:#F59E0B"></span>DELAYED</span>'
    return '<span class="badge b-red"><span class="dot" style="background:#EF4444"></span>FAILED</span>'


# ── Cached queries — incidents table ─────────────────────
@st.cache_data(ttl=30, show_spinner=False)
def get_stats(days):
    try:
        df = spark.sql(f"""
            SELECT
              SUM(CASE WHEN resolved_at IS NULL THEN 1 ELSE 0 END)                        AS active_issues,
              SUM(CASE WHEN cause_category IN ('OOM','CLUSTER_FAILURE')
                       AND resolved_at IS NULL THEN 1 ELSE 0 END)                          AS pipelines_failing,
              SUM(CASE WHEN cause_category='CLUSTER_FAILURE'
                       AND resolved_at IS NULL THEN 1 ELSE 0 END)                          AS clusters_stress,
              ROUND(AVG(CASE WHEN mttr_minutes IS NOT NULL AND mttr_minutes > 0
                  THEN mttr_minutes END), 0)                                               AS mttr_min,
              COUNT(*) AS total,
              SUM(CASE WHEN auto_resolved=true THEN 1 ELSE 0 END)                         AS auto_resolved
            FROM {CATALOG}.{SCHEMA}.incidents
            WHERE detected_at >= current_date - {days}
        """).toPandas().fillna(0)
        if df.empty: return {}
        return {k: int(float(v)) if str(v).replace('.','',1).isdigit() else 0
                for k,v in df.to_dict(orient="records")[0].items()}
    except Exception as e: return {"_error": str(e)}

@st.cache_data(ttl=30, show_spinner=False)
def get_recent_incidents(days, limit=5):
    try:
        return spark.sql(f"""
            SELECT incident_id, workspace_id, team_name, job_name,
                   root_cause, cause_category, confidence, suggested_fix,
                   business_impact, auto_resolved, mttr_minutes,
                   detected_at, detected_date, resolved_at, created_at
            FROM {CATALOG}.{SCHEMA}.incidents
            WHERE detected_at >= current_date - {days}
              AND resolved_at IS NULL
            ORDER BY detected_at DESC LIMIT {limit}
        """).toPandas().fillna("").to_dict(orient="records")
    except: return []

@st.cache_data(ttl=30, show_spinner=False)
def get_incidents_paged(days, status_filter, page_size, offset):
    w = f"WHERE detected_at >= current_date - {days}"
    if status_filter == "Open only":     w += " AND resolved_at IS NULL"
    if status_filter == "Resolved only": w += " AND resolved_at IS NOT NULL"
    try:
        return spark.sql(f"""
            SELECT incident_id, workspace_id, team_name, job_name,
                   root_cause, cause_category, confidence, suggested_fix,
                   business_impact, auto_resolved, mttr_minutes,
                   detected_at, detected_date, resolved_at, created_at
            FROM {CATALOG}.{SCHEMA}.incidents {w}
            ORDER BY detected_at DESC
            LIMIT {page_size} OFFSET {offset}
        """).toPandas().fillna("").to_dict(orient="records")
    except: return []

@st.cache_data(ttl=30, show_spinner=False)
def get_incidents_count(days, status_filter):
    w = f"WHERE detected_at >= current_date - {days}"
    if status_filter == "Open only":     w += " AND resolved_at IS NULL"
    if status_filter == "Resolved only": w += " AND resolved_at IS NOT NULL"
    try:
        df = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.incidents {w}").toPandas().fillna(0)
        return int(df.to_dict(orient="records")[0].get("cnt",0)) if not df.empty else 0
    except: return 0

@st.cache_data(ttl=30, show_spinner=False)
def get_insights_data(days):
    try:
        cat = spark.sql(f"""SELECT cause_category, COUNT(*) AS cnt
            FROM {CATALOG}.{SCHEMA}.incidents
            WHERE detected_at >= current_date - {days}
            GROUP BY cause_category ORDER BY cnt DESC LIMIT 8
        """).toPandas().fillna("").to_dict(orient="records")
        daily = spark.sql(f"""SELECT detected_date, COUNT(*) AS cnt
            FROM {CATALOG}.{SCHEMA}.incidents
            WHERE detected_at >= current_date - 14
            GROUP BY detected_date ORDER BY detected_date
        """).toPandas().fillna("").to_dict(orient="records")
        mttr = spark.sql(f"""
            SELECT team_name,
              ROUND(AVG(mttr_minutes) / 60, 1) AS hrs
            FROM {CATALOG}.{SCHEMA}.incidents
            WHERE mttr_minutes IS NOT NULL AND mttr_minutes > 0
              AND detected_at >= current_date - {days}
            GROUP BY team_name ORDER BY hrs DESC LIMIT 5
        """).toPandas().fillna("").to_dict(orient="records")
        ws = spark.sql(f"""SELECT workspace_id,
            SUM(CASE WHEN resolved_at IS NULL     THEN 1 ELSE 0 END) AS open,
            SUM(CASE WHEN resolved_at IS NOT NULL THEN 1 ELSE 0 END) AS resolved,
            SUM(CASE WHEN business_impact='high'
                     AND resolved_at IS NULL      THEN 1 ELSE 0 END) AS crit
            FROM {CATALOG}.{SCHEMA}.incidents
            WHERE detected_at >= current_date - {days}
            GROUP BY workspace_id ORDER BY open DESC
        """).toPandas().fillna("").to_dict(orient="records")
        return cat, daily, mttr, ws
    except: return [], [], [], []

# ── Cached queries — metric_insights table ───────────────
@st.cache_data(ttl=30, show_spinner=False)
def get_metric_kpis(days):
    try:
        df = spark.sql(f"""
            SELECT
              COUNT(*)                                                             AS total,
              SUM(CASE WHEN UPPER(severity)='HIGH'     THEN 1 ELSE 0 END)         AS high_sev,
              SUM(CASE WHEN UPPER(severity)='CRITICAL' THEN 1 ELSE 0 END)         AS critical_sev,
              SUM(CASE WHEN UPPER(severity)='MEDIUM'   THEN 1 ELSE 0 END)         AS medium_sev,
              COUNT(DISTINCT workspace_id)                                         AS workspaces,
              COUNT(DISTINCT metric_category)                                      AS categories
            FROM {CATALOG}.{SCHEMA}.metric_insights
            WHERE timestamp >= current_date - {days}
        """).toPandas().fillna(0)
        if df.empty: return {}
        return {k: int(float(v)) for k,v in df.to_dict(orient="records")[0].items()}
    except Exception as e: return {"_error": str(e)}

@st.cache_data(ttl=30, show_spinner=False)
def get_metric_paged(days, severity_filter, category_filter, page_size, offset):
    conds = [f"timestamp >= current_date - {days}"]
    if severity_filter != "All":
        conds.append(f"UPPER(severity) = '{severity_filter.upper()}'")
    if category_filter != "All":
        conds.append(f"metric_category = '{category_filter}'")
    w = "WHERE " + " AND ".join(conds)
    try:
        return spark.sql(f"""
            SELECT insight_id, workspace_id, cluster_name, metric_category,
                   metric_name, metric_value, metric_unit, severity, anomaly_type,
                   suggested_fix, business_impact, cause_category, confidence,
                   timestamp
            FROM {CATALOG}.{SCHEMA}.metric_insights
            {w}
            ORDER BY timestamp DESC
            LIMIT {page_size} OFFSET {offset}
        """).toPandas().fillna("").to_dict(orient="records")
    except: return []

@st.cache_data(ttl=30, show_spinner=False)
def get_metric_count(days, severity_filter, category_filter):
    conds = [f"timestamp >= current_date - {days}"]
    if severity_filter != "All":
        conds.append(f"UPPER(severity) = '{severity_filter.upper()}'")
    if category_filter != "All":
        conds.append(f"metric_category = '{category_filter}'")
    w = "WHERE " + " AND ".join(conds)
    try:
        df = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.metric_insights {w}").toPandas().fillna(0)
        return int(df.to_dict(orient="records")[0].get("cnt",0)) if not df.empty else 0
    except: return 0

@st.cache_data(ttl=30, show_spinner=False)
def get_metric_categories(days):
    try:
        rows = spark.sql(f"""
            SELECT DISTINCT metric_category
            FROM {CATALOG}.{SCHEMA}.metric_insights
            WHERE timestamp >= current_date - {days}
            ORDER BY metric_category
        """).toPandas().fillna("").to_dict(orient="records")
        return ["All"] + [r["metric_category"] for r in rows if r.get("metric_category")]
    except: return ["All"]

@st.cache_data(ttl=30, show_spinner=False)
def get_metric_by_category(days):
    try:
        return spark.sql(f"""
            SELECT metric_category, COUNT(*) AS cnt
            FROM {CATALOG}.{SCHEMA}.metric_insights
            WHERE timestamp >= current_date - {days}
            GROUP BY metric_category ORDER BY cnt DESC LIMIT 10
        """).toPandas().fillna("").to_dict(orient="records")
    except: return []

@st.cache_data(ttl=30, show_spinner=False)
def get_metric_by_severity(days):
    try:
        return spark.sql(f"""
            SELECT UPPER(severity) AS severity, COUNT(*) AS cnt
            FROM {CATALOG}.{SCHEMA}.metric_insights
            WHERE timestamp >= current_date - {days}
            GROUP BY UPPER(severity) ORDER BY cnt DESC
        """).toPandas().fillna("").to_dict(orient="records")
    except: return []

@st.cache_data(ttl=30, show_spinner=False)
def get_metric_by_workspace(days):
    try:
        return spark.sql(f"""
            SELECT workspace_id, COUNT(*) AS cnt,
                   SUM(CASE WHEN UPPER(severity) IN ('HIGH','CRITICAL')
                            THEN 1 ELSE 0 END) AS high_cnt
            FROM {CATALOG}.{SCHEMA}.metric_insights
            WHERE timestamp >= current_date - {days}
            GROUP BY workspace_id ORDER BY cnt DESC LIMIT 8
        """).toPandas().fillna("").to_dict(orient="records")
    except: return []


# ══════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════
def render_sidebar():
    now = datetime.now().strftime("%H:%M")
    if st.session_state.selected_inc:
        pg = "Investigate"
    elif st.session_state.selected_metric:
        pg = "Metrics"
    else:
        pg = st.session_state.page

    with st.sidebar:
        st.markdown(f"""
        <div style="padding:24px 16px 16px">
          <div style="font-size:13px;font-weight:700;letter-spacing:2px;
                      color:#fff;text-transform:uppercase">SREAgent</div>
          <div style="font-size:9px;letter-spacing:1.5px;text-transform:uppercase;
                      color:rgba(255,255,255,.3);margin-top:2px">Reliability Platform</div>
          <div style="width:14px;height:1px;background:rgba(255,255,255,.15);margin-top:10px"></div>
        </div>
        <div style="font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:1.5px;
                    color:rgba(255,255,255,.2);padding:0 16px 6px">Pages</div>
        """, unsafe_allow_html=True)

        for label, icon in [
            ("Overview",    "▦"),
            ("Investigate", "◎"),
            ("Metrics",     "◈"),
            ("Insights",    "∿"),
        ]:
            is_on = pg == label
            if is_on: st.markdown('<div class="nav-on">', unsafe_allow_html=True)
            if st.button(f"{icon}  {label}", key=f"nav_{label}", use_container_width=True):
                st.session_state.page            = label
                st.session_state.selected_inc    = None
                st.session_state.selected_metric = None
                st.rerun()
            if is_on: st.markdown('</div>', unsafe_allow_html=True)

        st.markdown("""<hr style='border:none;border-top:1px solid rgba(255,255,255,.06);margin:14px 16px'>""",
                    unsafe_allow_html=True)
        st.markdown(f"""
        <div style="padding:8px 16px">
          <div style="display:flex;align-items:center;gap:6px">
            <span style="width:6px;height:6px;background:#22C55E;border-radius:50%;
                         display:inline-block;animation:pulse 2s ease-in-out infinite"></span>
            <span style="font-size:9px;font-weight:600;color:#22C55E;
                         letter-spacing:1px;text-transform:uppercase">Live</span>
            <span style="font-size:10px;color:rgba(255,255,255,.25);margin-left:auto">{now}</span>
          </div>
        </div>
        <hr style='border:none;border-top:1px solid rgba(255,255,255,.06);margin:8px 16px 0'>
        """, unsafe_allow_html=True)
        st.markdown(f"""
        <div style="padding:10px 14px 16px">
          <div style="display:flex;align-items:center;gap:8px;padding:8px 10px;
                      background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.06);
                      border-radius:6px">
            <div style="width:26px;height:26px;border-radius:50%;background:rgba(255,255,255,.1);
                        display:flex;align-items:center;justify-content:center;
                        font-size:10px;font-weight:600;color:#fff;flex-shrink:0">{USER_INITIALS}</div>
            <div>
              <div style="font-size:12px;font-weight:500;color:#fff;line-height:1.2">{USER_NAME}</div>
              <div style="font-size:9px;color:rgba(255,255,255,.3);
                          text-transform:uppercase;letter-spacing:.8px">SRE Engineer</div>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════
# SHARED
# ══════════════════════════════════════════════════════════
def render_header(title, sub=""):
    now = datetime.now().strftime("%H:%M")
    s   = f'<div class="th-sub">{sub}</div>' if sub else ''
    st.markdown(f"""
    <div class="top-header">
      <div><div class="th-title">{title}</div>{s}</div>
      <div class="live-pill">
        <span style="width:6px;height:6px;background:var(--green);border-radius:50%;
                     display:inline-block;animation:pulse 2s ease-in-out infinite"></span>
        LIVE · {now}
      </div>
    </div>""", unsafe_allow_html=True)

def slbl(txt):
    st.markdown(f"<div class='d-lbl'>{txt}</div>", unsafe_allow_html=True)

def _paginator(total, page_size, pg_key, label):
    pages = max(1, math.ceil(total / page_size))
    pg    = min(int(st.session_state.get(pg_key, 0)), pages - 1)
    if pages > 1:
        pc1, pc2, pc3 = st.columns([1, 3, 1])
        with pc1:
            if st.button("← Prev", key=f"{pg_key}_prev"):
                if pg > 0: st.session_state[pg_key] = pg - 1; st.rerun()
        with pc2:
            lo = pg * page_size + 1; hi = min((pg+1)*page_size, total)
            st.markdown(
                f"<div style='text-align:center;font-size:11px;color:var(--tx3);padding:8px 0'>"
                f"Showing {lo}–{hi} of {total} {label}</div>",
                unsafe_allow_html=True
            )
        with pc3:
            if st.button("Next →", key=f"{pg_key}_next"):
                if pg + 1 < pages: st.session_state[pg_key] = pg + 1; st.rerun()
    return pg

def render_incident_table(rows, prefix):
    if not rows:
        st.markdown('<div style="text-align:center;padding:48px 0;font-size:13px;color:var(--tx3)">No failures in this period</div>',
                    unsafe_allow_html=True)
        return

    h1,h2,h3,h4,h5 = st.columns([2.2,1,2.4,1.3,1.2])
    for col,lbl in zip([h1,h2,h3,h4,h5], ["Pipeline","Status","Root Cause","Detected","Action"]):
        col.markdown(f"<div class='t-hdr'>{lbl}</div>", unsafe_allow_html=True)

    for row in rows:
        resolved = is_resolved(row)
        cat      = str(row.get("cause_category","UNKNOWN")).upper()
        cause    = clean(row.get("root_cause","—"))[:55]
        detected = str(row.get("detected_at","—"))[:16]
        uid      = f"{prefix}__{row['incident_id']}"

        st.markdown("<hr style='border:none;border-top:1px solid rgba(255,255,255,.05);margin:0'>",
                    unsafe_allow_html=True)
        c1,c2,c3,c4,c5 = st.columns([2.2,1,2.4,1.3,1.2])
        with c1: st.markdown(f"<div class='t-job'>{row.get('job_name','—')}</div>", unsafe_allow_html=True)
        with c2: st.markdown(f"<div style='padding:12px 0'>{status_badge(cat,resolved)}</div>", unsafe_allow_html=True)
        with c3: st.markdown(f"<div class='t-cell'>{cause}</div>", unsafe_allow_html=True)
        with c4: st.markdown(f"<div class='t-muted'>{detected}</div>", unsafe_allow_html=True)
        with c5:
            btn_lbl  = "View →" if resolved else "Investigate →"
            btn_type = "secondary" if resolved else "primary"
            if st.button(btn_lbl, key=uid, type=btn_type, use_container_width=True):
                st.session_state.selected_inc = row
                st.session_state.page = "Investigate"
                st.rerun()


# ══════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ══════════════════════════════════════════════════════════
def render_overview():
    render_header("Platform Health", f"Hello, {USER_NAME} — your platform health today")
    st.markdown('<div class="pg">', unsafe_allow_html=True)

    tab_today, tab_week, tab_month = st.tabs(["Today", "Week", "Month"])

    def _body(days, tab_prefix):
        stats = get_stats(days)
        if "_error" in stats:
            st.error(f"Permission error — run GRANT commands.\n{stats['_error']}")
            return

        ai_n = int(stats.get("active_issues",0))
        pf_n = int(stats.get("pipelines_failing",0))
        cs_n = int(stats.get("clusters_stress",0))
        mt_n = int(stats.get("mttr_min",0)) if stats.get("mttr_min") else 0

        st.markdown(f"""
        <div class="kpi-grid">
          <div class="kpi-card">
            <div class="kpi-top">
              <div class="kpi-lbl">Active Issues</div>
              <div class="kpi-icon {'red' if ai_n>0 else 'green'}">{'🔺' if ai_n>0 else '✓'}</div>
            </div>
            <div class="kpi-num">{ai_n}</div>
            <div class="kpi-delta {'up' if ai_n>0 else 'dn'}">{'↑ needs attention' if ai_n>0 else '↓ all clear'}</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-top"><div class="kpi-lbl">Pipelines Failing</div><div class="kpi-icon amber">⚡</div></div>
            <div class="kpi-num">{pf_n}</div>
            <div class="kpi-delta {'up' if pf_n>0 else 'dn'}">{'↑ '+str(pf_n)+' failing' if pf_n>0 else '↓ all healthy'}</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-top"><div class="kpi-lbl">Clusters Under Stress</div><div class="kpi-icon blue">≡</div></div>
            <div class="kpi-num">{cs_n}</div>
            <div class="kpi-delta {'up' if cs_n>0 else 'dn'}">{'↑ '+str(cs_n)+' stressed' if cs_n>0 else '↓ stable'}</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-top"><div class="kpi-lbl">MTTR (Today)</div><div class="kpi-icon green">◷</div></div>
            <div class="kpi-num">{mt_n}<span class="unit"> min</span></div>
            <div class="kpi-delta {'dn' if mt_n>0 else 'neu'}">{'↓ improving' if mt_n>0 else '— no data yet'}</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        recent = get_recent_incidents(days, limit=5)
        st.markdown(f"""
        <div class="dcard">
          <div class="sec-hd" style="margin-bottom:8px">
            <div class="sec-title">Recent Failures</div>
            <div class="sec-sub">{len(recent)} open · click Investigate → to analyse</div>
          </div>""", unsafe_allow_html=True)
        render_incident_table(recent, f"ov_{tab_prefix}")
        st.markdown('</div>', unsafe_allow_html=True)

    with tab_today: _body(1,  "today")
    with tab_week:  _body(7,  "week")
    with tab_month: _body(30, "month")

    st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════
# PAGE: INVESTIGATE — list
# ══════════════════════════════════════════════════════════
def render_investigate_list():
    render_header("Investigate", "All pipeline failures · filter by date and status")
    st.markdown('<div class="pg">', unsafe_allow_html=True)

    f1,f2,f3 = st.columns([2,1.8,1.2])
    with f1:
        dr = st.selectbox("Date range", INV_OPTS,
                          index=INV_OPTS.index(st.session_state.iv_dr), key="iv_dr_sel")
        if dr != st.session_state.iv_dr:
            st.session_state.iv_dr=dr; st.session_state.iv_pg_o=0; st.session_state.iv_pg_r=0; st.rerun()
    with f2:
        sf = st.selectbox("Status", ["Open only","All","Resolved only"],
                          index=["Open only","All","Resolved only"].index(st.session_state.iv_sf),
                          key="iv_sf_sel")
        if sf != st.session_state.iv_sf:
            st.session_state.iv_sf=sf; st.session_state.iv_pg_o=0; st.session_state.iv_pg_r=0; st.rerun()
    with f3:
        ps_opts=[10,20,50]
        ps = st.selectbox("Per page", ps_opts,
                          index=ps_opts.index(int(st.session_state.iv_ps)) if int(st.session_state.iv_ps) in ps_opts else 1,
                          key="iv_ps_sel")
        if ps != st.session_state.iv_ps:
            st.session_state.iv_ps=ps; st.session_state.iv_pg_o=0; st.session_state.iv_pg_r=0; st.rerun()

    days=INV_DAYS[st.session_state.iv_dr]; page_size=int(st.session_state.iv_ps); status=st.session_state.iv_sf

    if status in ("Open only","All"):
        total_o = get_incidents_count(days,"Open only")
        pg_o    = _paginator(total_o, page_size, "iv_pg_o", "failures")
        rows_o  = get_incidents_paged(days,"Open only", page_size, pg_o*page_size)
        st.markdown(f'<div class="sec-hd" style="margin-top:8px"><div class="sec-title">Open Failures</div><div class="sec-sub">{total_o} found</div></div>',
                    unsafe_allow_html=True)
        render_incident_table(rows_o, f"iv_o_{dr}")

    if status in ("All","Resolved only"):
        total_r = get_incidents_count(days,"Resolved only")
        pg_r    = _paginator(total_r, page_size, "iv_pg_r", "resolved")
        rows_r  = get_incidents_paged(days,"Resolved only", page_size, pg_r*page_size)
        mt = "margin-top:32px" if status=="All" else "margin-top:8px"
        st.markdown(f'<div class="sec-hd" style="{mt}"><div class="sec-title">Resolved</div><div class="sec-sub">{total_r} this period</div></div>',
                    unsafe_allow_html=True)
        render_incident_table(rows_r, f"iv_r_{dr}")

    st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════
# PAGE: INVESTIGATE — detail
# ══════════════════════════════════════════════════════════
def render_investigate_detail(inc):
    resolved = is_resolved(inc)
    imp_raw  = str(inc.get("business_impact","low")).lower().strip()
    cat      = str(inc.get("cause_category","UNKNOWN")).upper()
    cp       = conf_pct(inc)
    auto_res = str(inc.get("auto_resolved","")).lower() in ("true","1","yes")
    job      = inc.get("job_name","—")
    ws_id    = str(inc.get("workspace_id","—"))

    render_header(f"Investigating: {job}")
    st.markdown('<div class="pg">', unsafe_allow_html=True)

    bc,_ = st.columns([1,9])
    with bc:
        if st.button("← Back", key="det_back"):
            st.session_state.selected_inc=None; st.rerun()

    st.markdown(f"""
    <div class="inv-title">{job}</div>
    <div class="inv-meta">
      {status_badge(cat,resolved)} {imp_badge(imp_raw)} {cat_badge(cat)}
      {b(ws_id,'b-gld') if ws_id and ws_id!='—' else ''}
      {b('Auto-resolved','b-grn') if auto_res else ''}
    </div>""", unsafe_allow_html=True)

    # MTTR from column
    mttr_val = inc.get("mttr_minutes","")
    mttr_str = f"{int(float(mttr_val))} min" if mttr_val and str(mttr_val) not in ("","None","nan") else "—"

    st.markdown(f"""
    <div class="stat-grid">
      <div class="stat-cell"><div class="stat-lbl">Detected at</div><div class="stat-val">{str(inc.get('detected_at','—'))[:16]}</div></div>
      <div class="stat-cell"><div class="stat-lbl">Failure type</div><div class="stat-val">{cat}</div></div>
      <div class="stat-cell"><div class="stat-lbl">AI confidence</div><div class="stat-val">{cp}%</div></div>
      <div class="stat-cell"><div class="stat-lbl">MTTR</div><div class="stat-val">{mttr_str}</div></div>
    </div>""", unsafe_allow_html=True)

    left, right = st.columns([3,2], gap="large")

    with left:
        slbl("Root Cause")
        st.markdown(f'<div class="d-box gold"><div class="d-inner-lbl gold">Identified by AI diagnosis job</div><div class="d-body">{clean(inc.get("root_cause","—"))}</div></div>',
                    unsafe_allow_html=True)

        slbl("Suggested Fix")
        st.markdown(f'<div class="d-box"><div class="d-inner-lbl gray">Recommended action</div><div class="d-body dark">{clean(inc.get("suggested_fix","—"))}</div></div>',
                    unsafe_allow_html=True)

        # Business Impact — parses JSON or plain text
        slbl("Business Impact")
        lvl, pills_html, desc = make_impact(
            imp_raw,
            inc.get("business_impact",""),
            inc.get("suggested_fix",""),
            inc.get("root_cause","")
        )
        st.markdown(f"""
        <div class="impact {lvl}">
          <div class="impact-lbl {lvl}">{lvl.upper()} IMPACT</div>
          {pills_html}
          <div class="impact-desc">{desc}</div>
        </div>""", unsafe_allow_html=True)

        slbl("AI Confidence")
        st.markdown(f'<div class="d-box"><div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px"><span class="d-inner-lbl gray" style="margin-bottom:0">Diagnosis confidence</span><span style="font-size:18px;font-weight:700;color:var(--tx1)">{cp}%</span></div><div class="conf-track"><div class="conf-fill" style="width:{cp}%"></div></div></div>',
                    unsafe_allow_html=True)

        if str(inc.get("team_name","—")) not in ("","—","None"):
            slbl("Context")
            st.markdown(f'<div class="d-box"><div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid var(--bdr)"><span style="font-size:10px;text-transform:uppercase;letter-spacing:.8px;color:var(--tx3);font-weight:600">Team</span><span style="font-size:12px;color:var(--tx1)">{inc.get("team_name","—")}</span></div><div style="display:flex;justify-content:space-between;padding:5px 0"><span style="font-size:10px;text-transform:uppercase;letter-spacing:.8px;color:var(--tx3);font-weight:600">Workspace</span><span style="font-size:12px;color:var(--tx1)">{ws_id}</span></div></div>',
                        unsafe_allow_html=True)

        # ── Actions ──
        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
        slbl("Actions")

        if not resolved:
            # Open workspace
            st.markdown(f"""
            <a href="{WORKSPACE_URL}/#job/list" target="_blank" class="ws-link">
              ↗ &nbsp; Open Jobs in Databricks Workspace
            </a>
            <div class="ws-link-hint">
              Navigate to the workspace to restart or fix this pipeline.
            </div>""", unsafe_allow_html=True)

            # Ask Agent — disabled placeholder
            st.button("🤖  Ask Agent", key="ask_agent_btn",
                      use_container_width=True, disabled=True)
            st.markdown("""
            <div style="font-size:11px;color:var(--tx3);text-align:center;margin-top:4px;margin-bottom:2px">
              Agent integration coming soon
            </div>""", unsafe_allow_html=True)
        else:
            r_ts = str(inc.get("resolved_at",""))[:16]
            st.markdown(f'<div class="d-box sage"><div class="d-inner-lbl sage">{"Auto-resolved" if auto_res else "Resolved"}</div><div class="d-body">Closed at {r_ts}</div></div>',
                        unsafe_allow_html=True)

    with right:
        slbl("Failure Timeline")
        detected = str(inc.get("detected_at",""))[:16]
        created  = str(inc.get("created_at",""))[:16]

        tl = [("gray",detected,"Pipeline run triggered on schedule")]
        if cat=="OOM": tl+=[("amber",detected,"Memory pressure rising — GC overhead building"),("red",detected,"GC limit exceeded — driver OOM · 0 rows written")]
        elif cat=="CLUSTER_FAILURE": tl+=[("amber",detected,"Cluster health check failed"),("red",detected,"Cluster terminated unexpectedly")]
        elif cat=="TIMEOUT": tl+=[("amber",detected,"Upstream latency threshold crossed"),("amber",detected,"Pipeline delayed beyond SLA")]
        elif cat=="DATA_QUALITY": tl+=[("amber",detected,"Schema validation triggered"),("red",detected,"AnalysisException — schema drift detected")]
        elif cat=="PERMISSION": tl.append(("red",detected,"Access denied — credentials expired"))
        elif cat=="DEPENDENCY": tl+=[("amber",detected,"Upstream dependency unavailable"),("red",detected,"Pipeline blocked — dependency unresolved")]
        elif cat=="NETWORK": tl+=[("amber",detected,"Network connectivity degraded"),("red",detected,"Connection timeout — pipeline aborted")]
        else: tl.append(("red",detected,"Pipeline failed — see root cause"))
        tl.append(("amber",created,"AI diagnosis job analysed and logged failure"))
        if resolved:
            r_ts=str(inc.get("resolved_at",""))[:16]
            tl.append(("green",r_ts,"Auto-resolved ✓" if auto_res else "Manually resolved by SRE ✓"))
        else: tl.append(("red","Ongoing","Awaiting SRE resolution"))

        html='<div class="tl">'
        for dc,ts,txt in tl:
            html+=f'<div class="tl-row"><div class="tl-dot {dc}"></div><div><div class="tl-evt">{txt}</div><div class="tl-time">{ts}</div></div></div>'
        st.markdown(html+"</div>", unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════
# PAGE: METRICS — list
# ══════════════════════════════════════════════════════════
def render_metrics_list():
    render_header("Metrics", "Cluster & pipeline metric anomalies · metric_insights")
    st.markdown('<div class="pg">', unsafe_allow_html=True)

    # Filters
    f1,f2,f3,f4 = st.columns([2,1.6,1.6,1.2])
    with f1:
        dr = st.selectbox("Date range", INV_OPTS,
                          index=INV_OPTS.index(st.session_state.mx_dr), key="mx_dr_sel")
        if dr != st.session_state.mx_dr:
            st.session_state.mx_dr=dr; st.session_state.mx_pg=0; st.rerun()

    days = INV_DAYS[st.session_state.mx_dr]
    cats = get_metric_categories(days)

    with f2:
        sev_filter = st.selectbox("Severity",["All","Critical","High","Medium","Low"], key="mx_sev_sel")
    with f3:
        cat_filter = st.selectbox("Category", cats, key="mx_cat_sel")
    with f4:
        ps_opts=[10,20,50]
        ps = st.selectbox("Per page", ps_opts,
                          index=ps_opts.index(int(st.session_state.mx_ps)) if int(st.session_state.mx_ps) in ps_opts else 1,
                          key="mx_ps_sel")
        if ps != st.session_state.mx_ps:
            st.session_state.mx_ps=ps; st.session_state.mx_pg=0; st.rerun()

    page_size = int(st.session_state.mx_ps)

    # KPI cards
    kpis = get_metric_kpis(days)
    if "_error" not in kpis:
        k1,k2,k3,k4 = st.columns(4)
        total_m   = int(kpis.get("total",0))
        crit_high = int(kpis.get("critical_sev",0)) + int(kpis.get("high_sev",0))
        ws_count  = int(kpis.get("workspaces",0))
        cat_count = int(kpis.get("categories",0))

        k1.markdown(f'<div class="kpi-card"><div class="kpi-top"><div class="kpi-lbl">Total Anomalies</div><div class="kpi-icon purple">◈</div></div><div class="kpi-num">{total_m}</div><div class="kpi-delta neu">in selected period</div></div>', unsafe_allow_html=True)
        k2.markdown(f'<div class="kpi-card"><div class="kpi-top"><div class="kpi-lbl">Critical / High</div><div class="kpi-icon red">⚠</div></div><div class="kpi-num">{crit_high}</div><div class="kpi-delta {"up" if crit_high>0 else "neu"}">{"↑ requires attention" if crit_high>0 else "— none"}</div></div>', unsafe_allow_html=True)
        k3.markdown(f'<div class="kpi-card"><div class="kpi-top"><div class="kpi-lbl">Workspaces affected</div><div class="kpi-icon blue">⬡</div></div><div class="kpi-num">{ws_count}</div><div class="kpi-delta neu">unique workspaces</div></div>', unsafe_allow_html=True)
        k4.markdown(f'<div class="kpi-card"><div class="kpi-top"><div class="kpi-lbl">Categories</div><div class="kpi-icon amber">≋</div></div><div class="kpi-num">{cat_count}</div><div class="kpi-delta neu">metric types</div></div>', unsafe_allow_html=True)
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # Charts
    ch1, ch2 = st.columns(2, gap="large")
    with ch1:
        by_cat = get_metric_by_category(days)
        st.markdown('<div class="chart-card"><div class="chart-title">Anomalies by category</div>', unsafe_allow_html=True)
        if by_cat:
            mx = max(int(r.get("cnt",0)) for r in by_cat) or 1
            for row in by_cat:
                ck=str(row.get("metric_category","—")); cnt=int(row.get("cnt",0))
                st.markdown(f'<div class="bar-row"><span class="bar-lbl" style="width:120px">{ck[:18]}</span><div class="bar-track"><div class="bar-fill" style="width:{int(cnt/mx*100)}%;background:var(--purple)"></div></div><span class="bar-val">{cnt}</span></div>',
                            unsafe_allow_html=True)
        else: st.markdown("<div style='color:var(--tx3);padding:12px 0'>No data</div>", unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with ch2:
        by_sev = get_metric_by_severity(days)
        st.markdown('<div class="chart-card"><div class="chart-title">Anomalies by severity</div>', unsafe_allow_html=True)
        if by_sev:
            mx = max(int(r.get("cnt",0)) for r in by_sev) or 1
            for row in by_sev:
                sev=str(row.get("severity","—")).upper(); cnt=int(row.get("cnt",0))
                color=SEV_BAR.get(sev,"#71717A")
                st.markdown(f'<div class="bar-row"><span class="bar-lbl">{sev.title()}</span><div class="bar-track"><div class="bar-fill" style="width:{int(cnt/mx*100)}%;background:{color}"></div></div><span class="bar-val">{cnt}</span></div>',
                            unsafe_allow_html=True)
        else: st.markdown("<div style='color:var(--tx3);padding:12px 0'>No data</div>", unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # Table
    total = get_metric_count(days, sev_filter, cat_filter)
    pg    = _paginator(total, page_size, "mx_pg", "anomalies")
    rows  = get_metric_paged(days, sev_filter, cat_filter, page_size, pg*page_size)

    st.markdown(f'<div class="sec-hd"><div class="sec-title">Metric Anomalies</div><div class="sec-sub">{total} found · metric_insights table</div></div>',
                unsafe_allow_html=True)

    if rows:
        h1,h2,h3,h4,h5,h6 = st.columns([1.8,1.4,1,1.2,1.8,0.9])
        for col,lbl in zip([h1,h2,h3,h4,h5,h6],
                           ["Metric","Category","Severity","Value","Workspace","Action"]):
            col.markdown(f"<div class='t-hdr'>{lbl}</div>", unsafe_allow_html=True)

        for row in rows:
            sev    = str(row.get("severity","—")).upper()
            metric = str(row.get("metric_name","—"))
            cat    = str(row.get("metric_category","—"))
            val    = f"{row.get('metric_value','')} {row.get('metric_unit','')}".strip() or "—"
            ws     = str(row.get("workspace_id","—"))
            ts     = str(row.get("timestamp","—"))[:16]
            uid    = f"mx__{row.get('insight_id','')}__{ts}"

            st.markdown("<hr style='border:none;border-top:1px solid rgba(255,255,255,.05);margin:0'>",
                        unsafe_allow_html=True)
            c1,c2,c3,c4,c5,c6 = st.columns([1.8,1.4,1,1.2,1.8,0.9])
            with c1: st.markdown(f"<div class='t-job' style='font-size:12px'>{metric}</div>", unsafe_allow_html=True)
            with c2: st.markdown(f"<div class='t-cell'>{cat}</div>", unsafe_allow_html=True)
            with c3: st.markdown(f"<div style='padding:12px 0'>{sev_badge(sev)}</div>", unsafe_allow_html=True)
            with c4: st.markdown(f"<div class='t-cell' style='font-family:var(--mono);font-size:11px'>{val}</div>", unsafe_allow_html=True)
            with c5: st.markdown(f"<div class='t-muted'>{ws}<br><span style='font-size:10px;color:var(--tx4)'>{ts}</span></div>", unsafe_allow_html=True)
            with c6:
                if st.button("View →", key=uid, use_container_width=True):
                    st.session_state.selected_metric=row; st.session_state.page="Metrics"; st.rerun()
    else:
        st.markdown('<div style="text-align:center;padding:48px 0;font-size:13px;color:var(--tx3)">No anomalies found for these filters</div>',
                    unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════
# PAGE: METRICS — detail
# ══════════════════════════════════════════════════════════
def render_metric_detail(row):
    metric_name = str(row.get("metric_name","—"))
    metric_val  = str(row.get("metric_value","—"))
    metric_unit = str(row.get("metric_unit",""))
    sev         = str(row.get("severity","—")).upper()
    anomaly     = str(row.get("anomaly_type","—"))
    cat         = str(row.get("metric_category","—"))
    cat_cause   = str(row.get("cause_category","—")).upper()
    cp          = conf_pct(row)
    ws_id       = str(row.get("workspace_id","—"))
    cluster     = str(row.get("cluster_name","—"))
    ts          = str(row.get("timestamp","—"))[:16]

    render_header(f"Metric: {metric_name}")
    st.markdown('<div class="pg">', unsafe_allow_html=True)

    bc,_ = st.columns([1,9])
    with bc:
        if st.button("← Back", key="mx_back"):
            st.session_state.selected_metric=None; st.rerun()

    st.markdown(f"""
    <div class="inv-title">{metric_name}</div>
    <div class="inv-meta">
      {sev_badge(sev)}
      {b(anomaly,'b-pur') if anomaly not in ('—','') else ''}
      {b(cat,'b-gld') if cat not in ('—','') else ''}
      {cat_badge(cat_cause) if cat_cause not in ('—','UNKNOWN','') else ''}
    </div>""", unsafe_allow_html=True)

    st.markdown(f"""
    <div class="stat-grid">
      <div class="stat-cell"><div class="stat-lbl">Metric value</div><div class="stat-val" style="font-family:var(--mono)">{metric_val} {metric_unit}</div></div>
      <div class="stat-cell"><div class="stat-lbl">Severity</div><div class="stat-val">{sev}</div></div>
      <div class="stat-cell"><div class="stat-lbl">AI confidence</div><div class="stat-val">{cp}%</div></div>
      <div class="stat-cell"><div class="stat-lbl">Detected at</div><div class="stat-val" style="font-size:13px">{ts}</div></div>
    </div>""", unsafe_allow_html=True)

    left, right = st.columns([3,2], gap="large")

    with left:
        st.markdown(f"""
        <div class="metric-val-block">
          <div class="metric-val-big">{metric_val}</div>
          <div class="metric-val-unit">{metric_unit or 'no unit'} · {cat}</div>
        </div>""", unsafe_allow_html=True)

        slbl("Anomaly Analysis")
        st.markdown(f'<div class="d-box purple"><div class="d-inner-lbl purple">Identified anomaly type</div><div class="d-body">{anomaly}</div></div>',
                    unsafe_allow_html=True)

        slbl("Suggested Fix")
        st.markdown(f'<div class="d-box gold"><div class="d-inner-lbl gold">Recommended action</div><div class="d-body dark">{clean(row.get("suggested_fix","—"))}</div></div>',
                    unsafe_allow_html=True)

        slbl("Business Impact")
        impact_text = clean(row.get("business_impact","—"))
        sev_map = {"CRITICAL":"high","HIGH":"high","MEDIUM":"medium","LOW":"low"}
        impact_cls = sev_map.get(sev,"medium")
        # Try JSON parse for metric impact too
        _, pills_html, desc = make_impact(impact_cls, row.get("business_impact",""), row.get("suggested_fix",""), "")
        st.markdown(f"""
        <div class="impact {impact_cls}">
          <div class="impact-lbl {impact_cls}">{sev} SEVERITY</div>
          {pills_html}
          <div class="impact-desc">{desc if desc != '—' else 'Review metric anomaly and apply suggested fix.'}</div>
        </div>""", unsafe_allow_html=True)

        slbl("AI Confidence")
        st.markdown(f'<div class="d-box"><div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px"><span class="d-inner-lbl gray" style="margin-bottom:0">Diagnosis confidence</span><span style="font-size:18px;font-weight:700;color:var(--tx1)">{cp}%</span></div><div class="conf-track"><div class="conf-fill" style="width:{cp}%"></div></div></div>',
                    unsafe_allow_html=True)

        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
        slbl("Actions")
        st.markdown(f"""
        <a href="{WORKSPACE_URL}/#setting/clusters" target="_blank" class="ws-link">
          ↗ &nbsp; Open Clusters in Databricks Workspace
        </a>
        <div class="ws-link-hint">Review the cluster metrics and apply the suggested fix.</div>""",
                    unsafe_allow_html=True)
        st.button("🤖  Ask Agent", key="mx_ask_agent", use_container_width=True, disabled=True)
        st.markdown("""<div style="font-size:11px;color:var(--tx3);text-align:center;margin-top:4px">Agent integration coming soon</div>""",
                    unsafe_allow_html=True)

    with right:
        slbl("Context")
        st.markdown(f"""
        <div class="d-box">
          <div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid var(--bdr)">
            <span style="font-size:10px;text-transform:uppercase;letter-spacing:.8px;color:var(--tx3);font-weight:600">Workspace</span>
            <span style="font-size:12px;color:var(--tx1)">{ws_id}</span>
          </div>
          <div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid var(--bdr)">
            <span style="font-size:10px;text-transform:uppercase;letter-spacing:.8px;color:var(--tx3);font-weight:600">Cluster</span>
            <span style="font-size:12px;color:var(--tx1)">{cluster}</span>
          </div>
          <div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid var(--bdr)">
            <span style="font-size:10px;text-transform:uppercase;letter-spacing:.8px;color:var(--tx3);font-weight:600">Category</span>
            <span style="font-size:12px;color:var(--tx1)">{cat}</span>
          </div>
          <div style="display:flex;justify-content:space-between;padding:5px 0">
            <span style="font-size:10px;text-transform:uppercase;letter-spacing:.8px;color:var(--tx3);font-weight:600">Detected</span>
            <span style="font-size:12px;color:var(--tx1)">{ts}</span>
          </div>
        </div>""", unsafe_allow_html=True)

        slbl("Metric Timeline")
        tl_events = [("gray", ts, f"Metric recorded: {metric_val} {metric_unit}")]
        if sev in ("CRITICAL","HIGH"):
            tl_events.append(("red",   ts, f"Anomaly detected — {anomaly}"))
            tl_events.append(("amber", ts, "AI diagnosis job analysed metric"))
            tl_events.append(("red",   ts, f"{sev} severity alert raised"))
        elif sev == "MEDIUM":
            tl_events.append(("amber", ts, f"Anomaly detected — {anomaly}"))
            tl_events.append(("amber", ts, "AI diagnosis job analysed metric"))
        else:
            tl_events.append(("blue",  ts, f"Metric flagged — {anomaly}"))

        html = '<div class="tl">'
        for dc,t,txt in tl_events:
            html+=f'<div class="tl-row"><div class="tl-dot {dc}"></div><div><div class="tl-evt">{txt}</div><div class="tl-time">{t}</div></div></div>'
        st.markdown(html+"</div>", unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════
# PAGE: INSIGHTS
# ══════════════════════════════════════════════════════════
def render_insights():
    render_header("Insights","Trends, patterns and reliability metrics")
    st.markdown('<div class="pg">', unsafe_allow_html=True)

    f1,_ = st.columns([1.8,7])
    with f1: dr = st.selectbox("Date range", INV_OPTS, index=1, key="ins_dr")
    days = INV_DAYS[dr]

    stats = get_stats(days)
    cat_r, daily_r, mttr_r, ws_r = get_insights_data(days)
    m_kpis = get_metric_kpis(days)

    total  = int(stats.get("total",0)); auto = int(stats.get("auto_resolved",0))
    mt     = int(stats.get("mttr_min",0)) if stats.get("mttr_min") else 0
    rate   = round(auto/total*100,1) if total else 0
    score  = max(0,min(100,100-int(stats.get("active_issues",0))*5))
    sc     = "#22C55E" if score>=80 else "#F59E0B" if score>=50 else "#EF4444"
    m_tot  = int(m_kpis.get("total",0))
    m_high = int(m_kpis.get("high_sev",0))+int(m_kpis.get("critical_sev",0))

    k1,k2,k3,k4,k5 = st.columns(5)
    for col,lbl,val,sub,dc in [
        (k1,"Total failures",   total,      f"{total} logged",      "var(--tx2)"),
        (k2,"Avg MTTR",         f"{mt} min","avg to resolve",       "var(--tx2)"),
        (k3,"Auto-resolved",    auto,       f"{rate}% of total",    "var(--green)"),
        (k4,"Metric anomalies", m_tot,      f"{m_high} critical/high","var(--amber)" if m_high else "var(--tx2)"),
        (k5,"Reliability score",score,      "based on open issues",  sc),
    ]:
        col.markdown(f'<div class="chart-card" style="padding:16px 18px"><div style="font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:1px;color:var(--tx3);margin-bottom:8px">{lbl}</div><div style="font-size:28px;font-weight:700;color:{sc if lbl=="Reliability score" else "var(--tx1)"}">{val}</div><div style="font-size:11px;color:{dc};margin-top:4px">{sub}</div></div>',
                     unsafe_allow_html=True)

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

    col_a,col_b = st.columns(2, gap="large")
    with col_a:
        st.markdown('<div class="chart-card"><div class="chart-title">Failures by category</div>', unsafe_allow_html=True)
        if cat_r:
            mx = max(int(r.get("cnt",0)) for r in cat_r) or 1
            for row in cat_r:
                ck=str(row.get("cause_category","UNKNOWN")).upper(); cnt=int(row.get("cnt",0))
                st.markdown(f'<div class="bar-row"><span class="bar-lbl">{ck.replace("_"," ").title()}</span><div class="bar-track"><div class="bar-fill" style="width:{int(cnt/mx*100)}%;background:{BAR_COLOR.get(ck,"#71717A")}"></div></div><span class="bar-val">{cnt}</span></div>',
                            unsafe_allow_html=True)
        else: st.markdown("<div style='color:var(--tx3);padding:12px 0'>No data</div>", unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col_b:
        st.markdown('<div class="chart-card"><div class="chart-title">Daily pipeline failures (14 days)</div>', unsafe_allow_html=True)
        if daily_r:
            counts=[int(r.get("cnt",0)) for r in daily_r]; max_c=max(counts) or 1; last_n=counts[-14:]
            dl=(["M","T","W","T","F","S","S"]*3)[:len(last_n)]
            bars='<div style="display:flex;align-items:flex-end;gap:3px;height:60px;margin-bottom:6px">'
            lbrow='<div style="display:flex;justify-content:space-between;margin-bottom:12px">'
            for i,cnt in enumerate(last_n):
                h=max(5,int(cnt/max_c*100)); bg="background:var(--gold)" if i==len(last_n)-1 else "background:rgba(245,158,11,.2)"
                bars+=f'<div style="flex:1;{bg};border-radius:2px 2px 0 0;height:{h}%"></div>'
                lbrow+=(f'<span style="font-size:9px;color:var(--tx3)">{dl[i]}</span>' if i%2==0 else '<span></span>')
            st.markdown(bars+"</div>"+lbrow+"</div>", unsafe_allow_html=True)
            tc=counts[-1] if counts else 0; avg=round(sum(counts)/len(counts),1) if counts else 0
            st.markdown(f'<div class="t-row"><span class="t-name">Today</span><div style="display:flex;align-items:center;gap:8px"><span class="t-val">{tc}</span><span style="font-size:10px;font-weight:600;color:{"var(--red)" if tc>avg else "var(--green)"}">{"↑ above avg" if tc>avg else "↓ below avg"}</span></div></div><div class="t-row"><span class="t-name">14-day avg</span><span class="t-val">{avg}</span></div>',
                        unsafe_allow_html=True)
        else: st.markdown("<div style='color:var(--tx3);padding:12px 0'>No data</div>", unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    col_c,col_d = st.columns(2, gap="large")

    with col_c:
        st.markdown('<div class="chart-card"><div class="chart-title">MTTR by team</div>', unsafe_allow_html=True)
        if mttr_r:
            mx_h=max(float(r.get("hrs",0)) for r in mttr_r) or 1
            for row in mttr_r:
                team=str(row.get("team_name","—")); hrs=float(row.get("hrs",0))
                color="var(--red)" if hrs==mx_h else "var(--gold)"
                st.markdown(f'<div class="bar-row"><span class="bar-lbl">{team[:14]}</span><div class="bar-track"><div class="bar-fill" style="width:{int(hrs/mx_h*100)}%;background:{color}"></div></div><span class="bar-val">{hrs}h</span></div>',
                            unsafe_allow_html=True)
        else: st.markdown("<div style='color:var(--tx3);padding:12px 0'>No resolved data yet</div>", unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col_d:
        st.markdown('<div class="chart-card"><div class="chart-title">Workspace health</div>', unsafe_allow_html=True)
        if ws_r:
            total_ws=len(ws_r); healthy=sum(1 for r in ws_r if int(r.get("open",0))==0)
            degraded=sum(1 for r in ws_r if int(r.get("open",0))>0 and int(r.get("crit",0))==0)
            critical=sum(1 for r in ws_r if int(r.get("crit",0))>0)
            h_pct=int(healthy/total_ws*100) if total_ws else 0
            circ,dash=188,int(h_pct/100*188)
            st.markdown(f'<div style="display:flex;align-items:center;gap:18px;margin-bottom:14px"><svg width="72" height="72" viewBox="0 0 80 80"><circle cx="40" cy="40" r="30" fill="none" stroke="rgba(255,255,255,.06)" stroke-width="10"/><circle cx="40" cy="40" r="30" fill="none" stroke="#F59E0B" stroke-width="10" stroke-dasharray="{dash} {circ-dash}" stroke-dashoffset="47" stroke-linecap="round"/><text x="40" y="45" text-anchor="middle" font-family="Inter,sans-serif" font-size="14" font-weight="700" fill="#fff">{h_pct}%</text></svg><div><div style="display:flex;align-items:center;gap:7px;margin-bottom:5px"><div style="width:7px;height:7px;border-radius:50%;background:var(--green)"></div><span style="font-size:11px;color:var(--tx2);flex:1">Healthy</span><span style="font-size:11px;font-weight:600">{healthy}</span></div><div style="display:flex;align-items:center;gap:7px;margin-bottom:5px"><div style="width:7px;height:7px;border-radius:50%;background:var(--amber)"></div><span style="font-size:11px;color:var(--tx2);flex:1">Degraded</span><span style="font-size:11px;font-weight:600">{degraded}</span></div><div style="display:flex;align-items:center;gap:7px"><div style="width:7px;height:7px;border-radius:50%;background:var(--red)"></div><span style="font-size:11px;color:var(--tx2);flex:1">Critical</span><span style="font-size:11px;font-weight:600">{critical}</span></div></div></div>',
                        unsafe_allow_html=True)
            for row in ws_r:
                ws_id=str(row.get("workspace_id","—")); o,c=int(row.get("open",0)),int(row.get("crit",0))
                cls="b-red" if c>0 else "b-amb" if o>0 else "b-grn"
                lbl="Critical" if c>0 else "Degraded" if o>0 else "Healthy"
                st.markdown(f'<div class="t-row"><span class="t-name">{ws_id}</span>{b(lbl,cls)}</div>', unsafe_allow_html=True)
        else: st.markdown("<div style='color:var(--tx3);padding:12px 0'>No workspace data</div>", unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════
# ROUTER
# ══════════════════════════════════════════════════════════
render_sidebar()

if st.session_state.selected_inc is not None:
    render_investigate_detail(st.session_state.selected_inc)
elif st.session_state.selected_metric is not None:
    render_metric_detail(st.session_state.selected_metric)
elif st.session_state.page == "Overview":
    render_overview()
elif st.session_state.page == "Investigate":
    render_investigate_list()
elif st.session_state.page == "Metrics":
    render_metrics_list()
elif st.session_state.page == "Insights":
    render_insights()
else:
    st.session_state.page = "Overview"
    render_overview()
