# app.py
from fastapi import FastAPI
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
import json, uuid
from datetime import datetime

app = FastAPI()

# This automatically connects using the app's built-in credentials
spark = DatabricksSession.builder.getOrCreate()
w = WorkspaceClient()

CATALOG = "your_catalog_name"
SCHEMA  = "your_schema_name"

@app.get("/")
def health():
    return {"status": "running", "app": "incident-copilot"}

@app.get("/incidents")
def list_incidents(team: str = None, limit: int = 50):
    where = "WHERE 1=1"
    if team and team != "All":
        where += f" AND team_name = '{team}'"
    
    df = spark.sql(f"""
        SELECT * FROM {CATALOG}.{SCHEMA}.incidents
        {where}
        ORDER BY detected_at DESC
        LIMIT {limit}
    """)
    return df.toPandas().fillna("").to_dict(orient="records")

@app.get("/incidents/{incident_id}")
def get_incident(incident_id: str):
    df = spark.sql(f"""
        SELECT * FROM {CATALOG}.{SCHEMA}.incidents
        WHERE incident_id = '{incident_id}'
    """)
    rows = df.toPandas().fillna("").to_dict(orient="records")
    return rows[0] if rows else {"error": "not found"}

@app.post("/incidents/{incident_id}/resolve")
def resolve_incident(incident_id: str):
    spark.sql(f"""
        UPDATE {CATALOG}.{SCHEMA}.incidents
        SET resolved_at = current_timestamp()
        WHERE incident_id = '{incident_id}'
    """)
    return {"status": "resolved", "incident_id": incident_id}

@app.get("/stats")
def get_stats():
    df = spark.sql(f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN resolved_at IS NULL THEN 1 ELSE 0 END) as open_count,
            SUM(CASE WHEN auto_resolved = true THEN 1 ELSE 0 END) as auto_resolved,
            COUNT(DISTINCT cause_category) as unique_categories
        FROM {CATALOG}.{SCHEMA}.incidents
        WHERE detected_at >= current_date - 7
    """)
    return df.toPandas().fillna(0).to_dict(orient="records")[0]