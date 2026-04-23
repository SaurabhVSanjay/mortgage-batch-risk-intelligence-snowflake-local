import os
from fastapi import FastAPI, HTTPException
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
app = FastAPI(title="Mortgage Risk API")

def conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="MORTGAGE_RISK_DB",
        schema="MART",
    )

@app.get("/portfolio-risk-summary")
def portfolio_risk_summary():
    q = """
    SELECT risk_band, COUNT(*) borrowers, ROUND(AVG(risk_score),2) avg_risk
    FROM borrower_risk_scores
    WHERE as_of_date = CURRENT_DATE()
    GROUP BY risk_band
    ORDER BY borrowers DESC
    """
    c = conn(); cur = c.cursor()
    cur.execute(q)
    rows = cur.fetchall()
    cur.close(); c.close()
    return [{"risk_band": r[0], "borrowers": int(r[1]), "avg_risk": float(r[2])} for r in rows]

@app.get("/borrower/{borrower_id}/risk")
def borrower_risk(borrower_id: str):
    q = """
    SELECT borrower_id, mortgage_id, region, risk_score, risk_band, as_of_date
    FROM borrower_risk_scores
    WHERE borrower_id = %s
    ORDER BY as_of_date DESC
    LIMIT 1
    """
    c = conn(); cur = c.cursor()
    cur.execute(q, (borrower_id,))
    r = cur.fetchone()
    cur.close(); c.close()
    if not r:
        raise HTTPException(status_code=404, detail="Borrower not found")
    return {
        "borrower_id": r[0], "mortgage_id": r[1], "region": r[2],
        "risk_score": float(r[3]), "risk_band": r[4], "as_of_date": str(r[5])
    }

@app.post("/pipeline/run")
def run_pipeline():
    c = conn(); cur = c.cursor()
    cur.execute("CALL MART.sp_score_borrowers()")
    msg = cur.fetchone()[0]
    cur.close(); c.close()
    return {"status": "ok", "message": msg}
