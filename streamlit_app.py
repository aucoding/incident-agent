# streamlit_app.py
import streamlit as st
import requests

API = "http://localhost:8000"   # FastAPI runs on port 8000 inside the app

st.set_page_config(page_title="Incident Copilot 🚨", layout="wide")
st.title("🚨 Incident Copilot")

# Sidebar
page = st.sidebar.radio("Navigate", ["📊 Dashboard", "🔥 Incidents", "🤖 AI Diagnose"])

# ---- DASHBOARD ----
if page == "📊 Dashboard":
    stats = requests.get(f"{API}/stats").json()
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total (7d)",     stats.get("total", 0))
    c2.metric("Open",           stats.get("open_count", 0))
    c3.metric("Auto-resolved",  stats.get("auto_resolved", 0))
    c4.metric("Categories",     stats.get("unique_categories", 0))
    
    st.divider()
    st.subheader("Recent Incidents")
    data = requests.get(f"{API}/incidents", params={"limit": 20}).json()
    st.dataframe(data, use_container_width=True)

# ---- INCIDENTS ----
elif page == "🔥 Incidents":
    team = st.selectbox("Filter by team", ["All", "DE", "ML", "Analytics", "Platform"])
    data = requests.get(f"{API}/incidents", params={"team": team, "limit": 50}).json()
    
    if not data:
        st.info("No incidents found")
    else:
        for inc in data:
            status = "🟢 Resolved" if inc.get("resolved_at") else "🔴 Open"
            with st.expander(f"{status} | {inc['job_name']} | {inc['cause_category']} | {inc['detected_at']}"):
                st.write(f"**Root Cause:** {inc['root_cause']}")
                st.write(f"**Fix:** {inc['suggested_fix']}")
                st.write(f"**Impact:** {inc['business_impact']} | **Confidence:** {inc['confidence']}")
                
                if not inc.get("resolved_at"):
                    if st.button("✅ Resolve", key=inc["incident_id"]):
                        requests.post(f"{API}/incidents/{inc['incident_id']}/resolve")
                        st.success("Resolved!")
                        st.rerun()

# ---- AI DIAGNOSE ----
elif page == "🤖 AI Diagnose":
    st.subheader("Paste any error and get an AI diagnosis instantly")
    
    job = st.text_input("Job name")
    sig = st.selectbox("Signal type", ["JOB_FAILURE", "CLUSTER_FAILURE", "PIPELINE_ERROR", "OOM"])
    err = st.text_area("Error message", height=100)
    log = st.text_area("Log snippet (optional)", height=150)
    
    if st.button("🔍 Diagnose", type="primary"):
        with st.spinner("AI is analyzing..."):
            result = requests.post(f"{API}/diagnose", json={
                "signal_type": sig,
                "job_name": job,
                "error_message": err,
                "log_snippet": log
            }).json()
        
        st.success("Diagnosis complete!")
        st.json(result)
