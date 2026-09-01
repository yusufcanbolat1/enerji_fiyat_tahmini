"""Lightweight FastAPI API Server for Production Docker Deployment.

Replicates the Vite dev-mode middleware plugin (energyDataPlugin in vite.config.ts)
so that the frontend can fetch data in production via Nginx reverse proxy.

Endpoints:
  GET /api/db-data?date=...&type=...  — Energy data from PostgreSQL
  GET /api/fx                         — Live USD/TRY & EUR/TRY from Yahoo Finance
"""

import sys
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import asyncio
import json
import time
import logging
from typing import Optional
import httpx
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

from db.connection import get_db_engine
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("APIServer")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 API Server starting up...")
    yield
    logger.info("🛑 API Server shutting down...")


app = FastAPI(title="Enerji Fiyat Tahmini API", lifespan=lifespan)

# Module-level engine for the endpoints outside db_data(). get_db_engine() is a cached
# singleton and SQLAlchemy connects lazily, so this opens no socket at import time.
# Previously /api/fx referenced a bare `engine` that only existed as a local inside
# db_data(), so every DB read there raised NameError into a silent except.
engine = get_db_engine()

# ─────────────────────────────────────────────────────────────────────────────
# Shadow-run koruması. gold.ptf_predictions_daily'nin PK'sı (target_ts, model_name)
# — tabloda birden fazla model YAN YANA yaşayabilir. Bu dosyadaki sorgular eskiden
# model_name filtrelemiyordu; ikinci bir model yazıldığı anda LEFT JOIN'ler saat
# başına iki satır, AVG() iki modelin ortalamasını, next_day_forecast 48 satır
# döndürürdü — sessizce, hata vermeden. Dashboard'ın hangi seriyi gösterdiği artık
# TEK yerden belirleniyor; ensemble'a geçişte yalnızca bu sabit değişir.
ACTIVE_MODEL_NAME = "ensemble_v1"

# Bind parametresi yerine literal: aşağıdaki sorguların bir kısmı hiç params sözlüğü
# almıyor, bir kısmı sql_map içinde düz string. Değer kullanıcı girdisi değil, bu
# dosyada tanımlı sabit — enjeksiyon yüzeyi yok.
MODEL_FILTER = f"model_name = '{ACTIVE_MODEL_NAME}'"
MODEL_FILTER_G = f"g.model_name = '{ACTIVE_MODEL_NAME}'"
MODEL_FILTER_G2 = f"g2.model_name = '{ACTIVE_MODEL_NAME}'"
# ─────────────────────────────────────────────────────────────────────────────


@app.get("/api/db-data")
async def db_data(date: str = Query(..., description="Date param or 'latest'"),
                  type: str = Query(..., description="Query type"),
                  group_by: Optional[str] = Query(None, description="Aggregation level: hour, day, month")):
    """Serves energy data from PostgreSQL, same logic as query_db.py."""
    try:
        engine = get_db_engine()

        if type == "next_day_forecast":
            sql = f"""
                SELECT 
                    TO_CHAR(target_ts, 'HH24:00') as hour, 
                    ROUND(predicted_mcp_try, 2) as lightgbm_forecast,
                    ROUND(predicted_mcp_usd, 2) as lightgbm_forecast_usd,
                    ROUND(predicted_mcp_try_p10, 2) as price_p10,
                    ROUND(predicted_mcp_try_p90, 2) as price_p90,
                    ROUND(predicted_mcp_usd_p10, 2) as price_usd_p10,
                    ROUND(predicted_mcp_usd_p90, 2) as price_usd_p90,
                    TO_CHAR(target_ts, 'YYYY-MM-DD') as target_date
                FROM gold.ptf_predictions_daily 
                WHERE {MODEL_FILTER}
                  AND target_ts::date = (SELECT MAX(target_ts::date) FROM gold.ptf_predictions_daily WHERE {MODEL_FILTER}) 
                ORDER BY target_ts;
            """
            with engine.connect() as conn:
                res = conn.execute(text(sql)).mappings().all()
                data = [dict(r) for r in res]
                return JSONResponse(content=json.loads(json.dumps(data, default=str)))

        elif type == "pre_forecasts":
            if "_to_" in date:
                parts = date.split("_to_")
                start_dt, end_dt = parts[0], parts[1]
                target_clause = "pf.target_ts::date >= :start_dt AND pf.target_ts::date <= :end_dt"
                params = {"start_dt": start_dt, "end_dt": end_dt}
            elif date in ["latest", "today", "1d"]:
                target_clause = "pf.target_ts::date = (SELECT MAX(target_ts::date) FROM gold.kgup_load_pre_forecasts)"
                params = {}
            elif date in ["7d", "1m", "3m", "6m", "1y", "2y"]:
                days_map = {"7d": 7, "1m": 30, "3m": 90, "6m": 180, "1y": 365, "2y": 730}
                days = days_map.get(date, 30)
                target_clause = f"pf.target_ts::date >= ((SELECT MAX(target_ts::date) FROM gold.kgup_load_pre_forecasts) - INTERVAL '{days} days')"
                params = {}
            else:
                target_clause = "pf.target_ts::date = :dt"
                params = {"dt": date}

            sql = f"""
                SELECT 
                    TO_CHAR(pf.target_ts, 'YYYY-MM-DD HH24:00') as timestamp,
                    TO_CHAR(pf.target_ts, 'YYYY-MM-DD') as date,
                    TO_CHAR(pf.target_ts, 'HH24:00') as hour,
                    ROUND(pf.predicted_load_lag0, 2) AS pred_load_mw,
                    ROUND(l.load_forecast_mw, 2) AS actual_load_mw,
                    ROUND(pf.predicted_solar_lag0, 2) AS pred_solar_mw,
                    ROUND(k.solar_mw, 2) AS actual_solar_mw,
                    ROUND(pf.predicted_wind_lag0, 2) AS pred_wind_mw,
                    ROUND(k.wind_mw, 2) AS actual_wind_mw
                FROM gold.kgup_load_pre_forecasts pf
                LEFT JOIN raw_load_forecast_hourly l ON pf.target_ts = l.ts
                LEFT JOIN raw_kgup_hourly k ON pf.target_ts = k.ts
                WHERE {target_clause}
                ORDER BY pf.target_ts;
            """
            
            metrics_sql = f"""
                SELECT 
                    COUNT(*) as total_hours,
                    ROUND(AVG(ABS(pf.predicted_load_lag0 - l.load_forecast_mw)), 2) as load_mae_mw,
                    ROUND((SUM(ABS(pf.predicted_load_lag0 - l.load_forecast_mw)) / NULLIF(SUM(ABS(l.load_forecast_mw)), 0) * 100), 2) as load_wape_pct,
                    ROUND(AVG(ABS(pf.predicted_solar_lag0 - k.solar_mw)), 2) as solar_mae_mw,
                    ROUND((SUM(ABS(pf.predicted_solar_lag0 - k.solar_mw)) / NULLIF(SUM(ABS(k.solar_mw)), 0) * 100), 2) as solar_wape_pct,
                    ROUND(AVG(ABS(pf.predicted_wind_lag0 - k.wind_mw)), 2) as wind_mae_mw,
                    ROUND((SUM(ABS(pf.predicted_wind_lag0 - k.wind_mw)) / NULLIF(SUM(ABS(k.wind_mw)), 0) * 100), 2) as wind_wape_pct
                FROM gold.kgup_load_pre_forecasts pf
                LEFT JOIN raw_load_forecast_hourly l ON pf.target_ts = l.ts
                LEFT JOIN raw_kgup_hourly k ON pf.target_ts = k.ts
                WHERE {target_clause};
            """
            with engine.connect() as conn:
                res = conn.execute(text(sql), params).mappings().all()
                series_data = [dict(r) for r in res]
                
                m_res = conn.execute(text(metrics_sql), params).mappings().first()
                metrics_data = dict(m_res) if m_res else {}

                return JSONResponse(content=json.loads(json.dumps({
                    "series": series_data,
                    "metrics": metrics_data
                }, default=str)))

        elif type == "prediction_bounds":
            # Only expose dates where both a prediction and a realised PTF exist;
            # the UI uses this to prevent selections outside the model history.
            sql = f"""
                SELECT
                    MIN(g.target_ts::date) AS first_date,
                    MAX(g.target_ts::date) AS last_date
                FROM gold.ptf_predictions_daily g
                JOIN raw_mcp_hourly m ON m.ts = g.target_ts
                WHERE {MODEL_FILTER_G};
            """
            with engine.connect() as conn:
                row = conn.execute(text(sql)).mappings().first()
                data = dict(row) if row else {}
                return JSONResponse(content=json.loads(json.dumps(data, default=str)))

        elif type == "performance":
            if "_to_" in date:
                parts = date.split("_to_")
                start_dt, end_dt = parts[0], parts[1]
                sql = f"""
                    SELECT 
                        COUNT(*) as total_hours,
                        ROUND(AVG(ABS(g.predicted_mcp_try - m.price_try) / NULLIF(m.price_try, 0) * 100), 2) as mape,
                        ROUND((SUM(ABS(g.predicted_mcp_try - m.price_try)) / NULLIF(SUM(ABS(m.price_try)), 0) * 100), 2) as wape,
                        ROUND(AVG(ABS(g.predicted_mcp_try - m.price_try)), 2) as mae,
                        ROUND(AVG(g.predicted_mcp_try), 2) as avg_predicted,
                        ROUND(AVG(m.price_try), 2) as avg_actual,
                        ROUND(AVG(ABS(g.predicted_mcp_usd - m.price_usd) / NULLIF(m.price_usd, 0) * 100), 2) as mape_usd,
                        ROUND((SUM(ABS(g.predicted_mcp_usd - m.price_usd)) / NULLIF(SUM(ABS(m.price_usd)), 0) * 100), 2) as wape_usd,
                        ROUND(AVG(ABS(g.predicted_mcp_usd - m.price_usd)), 2) as mae_usd,
                        ROUND(AVG(g.predicted_mcp_usd), 2) as avg_predicted_usd,
                        ROUND(AVG(m.price_usd), 2) as avg_actual_usd
                    FROM gold.ptf_predictions_daily g
                    JOIN raw_mcp_hourly m ON g.target_ts = m.ts
                    WHERE {MODEL_FILTER_G}
                      AND DATE(g.target_ts AT TIME ZONE 'Europe/Istanbul') >= :start_dt AND DATE(g.target_ts AT TIME ZONE 'Europe/Istanbul') <= :end_dt;
                """
                with engine.connect() as conn:
                    res = conn.execute(text(sql), {"start_dt": start_dt, "end_dt": end_dt}).mappings().all()
                    data = [dict(r) for r in res]
                    return JSONResponse(content=json.loads(json.dumps(data, default=str)))
        if type == "performance":
            if date in ["latest", "today", "1d"]:
                target_clause = f"DATE(g.target_ts AT TIME ZONE 'Europe/Istanbul') = (SELECT MAX(DATE(g2.target_ts AT TIME ZONE 'Europe/Istanbul')) FROM gold.ptf_predictions_daily g2 JOIN raw_mcp_hourly m2 ON g2.target_ts = m2.ts WHERE {MODEL_FILTER_G2})"
                params = {}
            elif date in ["7d", "1m", "3m", "6m", "1y", "2y"]:
                days_map = {"7d": 7, "1m": 30, "3m": 90, "6m": 180, "1y": 365, "2y": 730}
                days = days_map.get(date, 365)
                target_clause = f"DATE(g.target_ts AT TIME ZONE 'Europe/Istanbul') >= ((SELECT MAX(DATE(g2.target_ts AT TIME ZONE 'Europe/Istanbul')) FROM gold.ptf_predictions_daily g2 JOIN raw_mcp_hourly m2 ON g2.target_ts = m2.ts WHERE {MODEL_FILTER_G2}) - INTERVAL '{days} days')"
                params = {}
            else:
                target_clause = "DATE(g.target_ts AT TIME ZONE 'Europe/Istanbul') = :dt"
                params = {"dt": date}

            sql = f"""
                SELECT 
                    COUNT(*) as total_hours,
                    ROUND(AVG(ABS(g.predicted_mcp_try - m.price_try) / NULLIF(m.price_try, 0) * 100), 2) as mape,
                    ROUND((SUM(ABS(g.predicted_mcp_try - m.price_try)) / NULLIF(SUM(ABS(m.price_try)), 0) * 100), 2) as wape,
                    ROUND(AVG(ABS(g.predicted_mcp_try - m.price_try)), 2) as mae,
                    ROUND(AVG(g.predicted_mcp_try), 2) as avg_predicted,
                    ROUND(AVG(m.price_try), 2) as avg_actual,
                    ROUND(AVG(ABS(g.predicted_mcp_usd - m.price_usd) / NULLIF(m.price_usd, 0) * 100), 2) as mape_usd,
                    ROUND((SUM(ABS(g.predicted_mcp_usd - m.price_usd)) / NULLIF(SUM(ABS(m.price_usd)), 0) * 100), 2) as wape_usd,
                    ROUND(AVG(ABS(g.predicted_mcp_usd - m.price_usd)), 2) as mae_usd,
                    ROUND(AVG(g.predicted_mcp_usd), 2) as avg_predicted_usd,
                    ROUND(AVG(m.price_usd), 2) as avg_actual_usd
                FROM gold.ptf_predictions_daily g

                JOIN raw_mcp_hourly m ON g.target_ts = m.ts
                WHERE {MODEL_FILTER_G} AND ({target_clause});
            """
            with engine.connect() as conn:
                res = conn.execute(text(sql), params).mappings().all()
                return JSONResponse(content=json.loads(json.dumps([dict(r) for r in res], default=str)))

        elif type == "today_performance" or type == "range_performance":
            if "_to_" in date:
                parts = date.split("_to_")
                start_dt, end_dt = parts[0], parts[1]
                target_clause = "m.ts::date >= :start_dt AND m.ts::date <= :end_dt"
                params = {"start_dt": start_dt, "end_dt": end_dt}
            elif date in ["latest", "today", "1d"]:
                target_clause = "m.ts::date = (SELECT MAX(ts::date) FROM raw_mcp_hourly WHERE price_try IS NOT NULL)"
                params = {}
            elif date in ["7d", "1m", "3m", "6m", "1y", "2y"]:
                days_map = {"7d": 7, "1m": 30, "3m": 90, "6m": 180, "1y": 365, "2y": 730}
                days = days_map.get(date, 365)
                target_clause = f"m.ts::date >= ((SELECT MAX(ts::date) FROM raw_mcp_hourly WHERE price_try IS NOT NULL) - INTERVAL '{days} days')"
                params = {}
            else:
                target_clause = "m.ts::date = :dt"
                params = {"dt": date}

            if group_by == 'day':
                sql = f"""
                    SELECT 
                        TO_CHAR(m.ts, 'YYYY-MM-DD') as timestamp,
                        TO_CHAR(m.ts, 'YYYY-MM-DD') as date,
                        TO_CHAR(m.ts, 'DD.MM.YYYY') as hour,
                        ROUND(AVG(m.price_try), 2) as ptf,
                        ROUND(AVG(m.price_usd), 2) as ptf_usd,
                        ROUND(AVG(g.predicted_mcp_try), 2) as lightgbm_forecast,
                        ROUND(AVG(g.predicted_mcp_usd), 2) as lightgbm_forecast_usd,
                        ROUND(AVG(g.predicted_mcp_try_p10), 2) as lightgbm_forecast_p10,
                        ROUND(AVG(g.predicted_mcp_try_p90), 2) as lightgbm_forecast_p90,
                        ROUND(AVG(g.predicted_mcp_usd_p10), 2) as lightgbm_forecast_usd_p10,
                        ROUND(AVG(g.predicted_mcp_usd_p90), 2) as lightgbm_forecast_usd_p90
                    FROM raw_mcp_hourly m
                    LEFT JOIN gold.ptf_predictions_daily g ON m.ts = g.target_ts AND {MODEL_FILTER_G}
                    WHERE {target_clause}
                    GROUP BY TO_CHAR(m.ts, 'YYYY-MM-DD'), TO_CHAR(m.ts, 'DD.MM.YYYY')
                    ORDER BY timestamp;
                """
            elif group_by == 'month':
                sql = f"""
                    SELECT 
                        TO_CHAR(m.ts, 'YYYY-MM') as timestamp,
                        TO_CHAR(m.ts, 'YYYY-MM') as date,
                        TO_CHAR(m.ts, 'YYYY-MM') as hour,
                        ROUND(AVG(m.price_try), 2) as ptf,
                        ROUND(AVG(m.price_usd), 2) as ptf_usd,
                        ROUND(AVG(g.predicted_mcp_try), 2) as lightgbm_forecast,
                        ROUND(AVG(g.predicted_mcp_usd), 2) as lightgbm_forecast_usd,
                        ROUND(AVG(g.predicted_mcp_try_p10), 2) as lightgbm_forecast_p10,
                        ROUND(AVG(g.predicted_mcp_try_p90), 2) as lightgbm_forecast_p90,
                        ROUND(AVG(g.predicted_mcp_usd_p10), 2) as lightgbm_forecast_usd_p10,
                        ROUND(AVG(g.predicted_mcp_usd_p90), 2) as lightgbm_forecast_usd_p90
                    FROM raw_mcp_hourly m
                    LEFT JOIN gold.ptf_predictions_daily g ON m.ts = g.target_ts AND {MODEL_FILTER_G}
                    WHERE {target_clause}
                    GROUP BY TO_CHAR(m.ts, 'YYYY-MM')
                    ORDER BY timestamp;
                """
            else:
                sql = f"""
                    SELECT 
                        TO_CHAR(m.ts, 'YYYY-MM-DD HH24:00') as timestamp,
                        TO_CHAR(m.ts, 'YYYY-MM-DD') as date,
                        TO_CHAR(m.ts, 'HH24:00') as hour,
                        ROUND(m.price_try, 2) as ptf,
                        ROUND(m.price_usd, 2) as ptf_usd,
                        ROUND(g.predicted_mcp_try, 2) as lightgbm_forecast,
                        ROUND(g.predicted_mcp_usd, 2) as lightgbm_forecast_usd,
                        ROUND(g.predicted_mcp_try_p10, 2) as lightgbm_forecast_p10,
                        ROUND(g.predicted_mcp_try_p90, 2) as lightgbm_forecast_p90,
                        ROUND(g.predicted_mcp_usd_p10, 2) as lightgbm_forecast_usd_p10,
                        ROUND(g.predicted_mcp_usd_p90, 2) as lightgbm_forecast_usd_p90
                    FROM raw_mcp_hourly m
                    LEFT JOIN gold.ptf_predictions_daily g ON m.ts = g.target_ts AND {MODEL_FILTER_G}
                    WHERE {target_clause}
                    ORDER BY m.ts;
                """
            metrics_sql = f"""
                SELECT 
                    COUNT(*) as total_hours,
                    ROUND(AVG(ABS(g.predicted_mcp_try - m.price_try) / NULLIF(m.price_try, 0) * 100), 2) as mape,
                    ROUND((SUM(ABS(g.predicted_mcp_try - m.price_try)) / NULLIF(SUM(ABS(m.price_try)), 0) * 100), 2) as wape,
                    ROUND((SUM(CASE WHEN m.price_try < g.predicted_mcp_try_p10 THEN g.predicted_mcp_try_p10 - m.price_try WHEN m.price_try > g.predicted_mcp_try_p90 THEN m.price_try - g.predicted_mcp_try_p90 ELSE 0 END) / NULLIF(SUM(ABS(m.price_try)), 0) * 100), 2) as wape_oob,
                    ROUND(AVG(ABS(g.predicted_mcp_try - m.price_try)), 2) as mae,
                    ROUND(AVG(g.predicted_mcp_try), 2) as avg_predicted,
                    ROUND(AVG(m.price_try), 2) as avg_actual,
                    ROUND(AVG(ABS(g.predicted_mcp_usd - m.price_usd) / NULLIF(m.price_usd, 0) * 100), 2) as mape_usd,
                    ROUND((SUM(ABS(g.predicted_mcp_usd - m.price_usd)) / NULLIF(SUM(ABS(m.price_usd)), 0) * 100), 2) as wape_usd,
                    ROUND((SUM(CASE WHEN m.price_usd < g.predicted_mcp_usd_p10 THEN g.predicted_mcp_usd_p10 - m.price_usd WHEN m.price_usd > g.predicted_mcp_usd_p90 THEN m.price_usd - g.predicted_mcp_usd_p90 ELSE 0 END) / NULLIF(SUM(ABS(m.price_usd)), 0) * 100), 2) as wape_oob_usd,
                    ROUND(AVG(ABS(g.predicted_mcp_usd - m.price_usd)), 2) as mae_usd,
                    ROUND(AVG(g.predicted_mcp_usd), 2) as avg_predicted_usd,
                    ROUND(AVG(m.price_usd), 2) as avg_actual_usd
                FROM gold.ptf_predictions_daily g
                JOIN raw_mcp_hourly m ON g.target_ts = m.ts
                WHERE {MODEL_FILTER_G} AND ({target_clause});
            """
            with engine.connect() as conn:
                res = conn.execute(text(sql), params).mappings().all()
                data = [dict(r) for r in res]
                
                m_res = conn.execute(text(metrics_sql), params).mappings().first()
                metrics = dict(m_res) if m_res else {}

                payload = {
                    "series": data,
                    "metrics": metrics
                }
                return JSONResponse(content=json.loads(json.dumps(payload, default=str)))

        else:
            if "_to_" in date:
                parts = date.split("_to_")
                start_dt, end_dt = parts[0], parts[1]

                if group_by == 'day':
                    # Group by day (1..31) for monthly comparison
                    sql_map = {
                        "mcp": "SELECT TO_CHAR(ts, 'DD') as label, TO_CHAR(ts, 'DD.MM.YYYY') as date_str, ROUND(AVG(price_try), 2) as price FROM raw_mcp_hourly WHERE ts::date >= :start_dt AND ts::date <= :end_dt GROUP BY TO_CHAR(ts, 'DD'), TO_CHAR(ts, 'DD.MM.YYYY') ORDER BY date_str",
                        "smp": "SELECT TO_CHAR(ts, 'DD') as label, TO_CHAR(ts, 'DD.MM.YYYY') as date_str, ROUND(AVG(system_marginal_price_try), 2) as price FROM raw_smp_hourly WHERE ts::date >= :start_dt AND ts::date <= :end_dt GROUP BY TO_CHAR(ts, 'DD'), TO_CHAR(ts, 'DD.MM.YYYY') ORDER BY date_str",
                        "kgup": "SELECT TO_CHAR(ts, 'DD') as label, TO_CHAR(ts, 'DD.MM.YYYY') as date_str, ROUND(AVG(total_mw), 2) as toplam FROM raw_kgup_hourly WHERE ts::date >= :start_dt AND ts::date <= :end_dt GROUP BY TO_CHAR(ts, 'DD'), TO_CHAR(ts, 'DD.MM.YYYY') ORDER BY date_str",
                        "load_forecast": "SELECT TO_CHAR(ts, 'DD') as label, TO_CHAR(ts, 'DD.MM.YYYY') as date_str, ROUND(AVG(load_forecast_mw), 2) as lep FROM raw_load_forecast_hourly WHERE ts::date >= :start_dt AND ts::date <= :end_dt GROUP BY TO_CHAR(ts, 'DD'), TO_CHAR(ts, 'DD.MM.YYYY') ORDER BY date_str",
                        "actual_generation": "SELECT TO_CHAR(ts, 'DD') as label, TO_CHAR(ts, 'DD.MM.YYYY') as date_str, ROUND(AVG(total_mw), 2) as total FROM raw_actual_generation_hourly WHERE ts::date >= :start_dt AND ts::date <= :end_dt GROUP BY TO_CHAR(ts, 'DD'), TO_CHAR(ts, 'DD.MM.YYYY') ORDER BY date_str",
                        "lightgbm": f"SELECT TO_CHAR(target_ts, 'DD') as label, TO_CHAR(target_ts, 'DD.MM.YYYY') as date_str, ROUND(AVG(predicted_mcp_try), 2) as price FROM gold.ptf_predictions_daily WHERE {MODEL_FILTER} AND target_ts::date >= :start_dt AND target_ts::date <= :end_dt GROUP BY TO_CHAR(target_ts, 'DD'), TO_CHAR(target_ts, 'DD.MM.YYYY') ORDER BY date_str",
                    }
                elif group_by == 'month':
                    # Group by month (01..12) for yearly comparison
                    sql_map = {
                        "mcp": "SELECT TO_CHAR(ts, 'MM') as label, ROUND(AVG(price_try), 2) as price FROM raw_mcp_hourly WHERE ts::date >= :start_dt AND ts::date <= :end_dt GROUP BY TO_CHAR(ts, 'MM') ORDER BY label",
                        "smp": "SELECT TO_CHAR(ts, 'MM') as label, ROUND(AVG(system_marginal_price_try), 2) as price FROM raw_smp_hourly WHERE ts::date >= :start_dt AND ts::date <= :end_dt GROUP BY TO_CHAR(ts, 'MM') ORDER BY label",
                        "kgup": "SELECT TO_CHAR(ts, 'MM') as label, ROUND(AVG(total_mw), 2) as toplam FROM raw_kgup_hourly WHERE ts::date >= :start_dt AND ts::date <= :end_dt GROUP BY TO_CHAR(ts, 'MM') ORDER BY label",
                        "load_forecast": "SELECT TO_CHAR(ts, 'MM') as label, ROUND(AVG(load_forecast_mw), 2) as lep FROM raw_load_forecast_hourly WHERE ts::date >= :start_dt AND ts::date <= :end_dt GROUP BY TO_CHAR(ts, 'MM') ORDER BY label",
                        "actual_generation": "SELECT TO_CHAR(ts, 'MM') as label, ROUND(AVG(total_mw), 2) as total FROM raw_actual_generation_hourly WHERE ts::date >= :start_dt AND ts::date <= :end_dt GROUP BY TO_CHAR(ts, 'MM') ORDER BY label",
                        "lightgbm": f"SELECT TO_CHAR(target_ts, 'MM') as label, ROUND(AVG(predicted_mcp_try), 2) as price FROM gold.ptf_predictions_daily WHERE {MODEL_FILTER} AND target_ts::date >= :start_dt AND target_ts::date <= :end_dt GROUP BY TO_CHAR(target_ts, 'MM') ORDER BY label",
                    }
                else:
                    # Default: 24-hour profile average across the range
                    sql_map = {
                        "mcp": "SELECT TO_CHAR(ts, 'HH24:00') as hour, ROUND(AVG(price_try), 2) as price FROM raw_mcp_hourly WHERE ts::date >= :start_dt AND ts::date <= :end_dt GROUP BY TO_CHAR(ts, 'HH24:00') ORDER BY hour",
                        "smp": "SELECT TO_CHAR(ts, 'HH24:00') as hour, ROUND(AVG(system_marginal_price_try), 2) as price FROM raw_smp_hourly WHERE ts::date >= :start_dt AND ts::date <= :end_dt GROUP BY TO_CHAR(ts, 'HH24:00') ORDER BY hour",
                        "kgup": "SELECT TO_CHAR(ts, 'HH24:00') as hour, ROUND(AVG(total_mw), 2) as toplam FROM raw_kgup_hourly WHERE ts::date >= :start_dt AND ts::date <= :end_dt GROUP BY TO_CHAR(ts, 'HH24:00') ORDER BY hour",
                        "load_forecast": "SELECT TO_CHAR(ts, 'HH24:00') as hour, ROUND(AVG(load_forecast_mw), 2) as lep FROM raw_load_forecast_hourly WHERE ts::date >= :start_dt AND ts::date <= :end_dt GROUP BY TO_CHAR(ts, 'HH24:00') ORDER BY hour",
                        "actual_generation": "SELECT TO_CHAR(ts, 'HH24:00') as hour, ROUND(AVG(total_mw), 2) as total FROM raw_actual_generation_hourly WHERE ts::date >= :start_dt AND ts::date <= :end_dt GROUP BY TO_CHAR(ts, 'HH24:00') ORDER BY hour",
                        "lightgbm": f"SELECT TO_CHAR(target_ts, 'HH24:00') as hour, ROUND(AVG(predicted_mcp_try), 2) as price FROM gold.ptf_predictions_daily WHERE {MODEL_FILTER} AND target_ts::date >= :start_dt AND target_ts::date <= :end_dt GROUP BY TO_CHAR(target_ts, 'HH24:00') ORDER BY hour",
                    }

                if type in sql_map:
                    with engine.connect() as conn:
                        res = conn.execute(text(sql_map[type]), {"start_dt": start_dt, "end_dt": end_dt}).mappings().all()
                        data = [dict(r) for r in res]
                        return JSONResponse(content=json.loads(json.dumps(data, default=str)))
                else:
                    return JSONResponse(content=[])
            else:
                sql_map = {
                    "mcp": "SELECT TO_CHAR(ts, 'HH24:00') as hour, price_try as price FROM raw_mcp_hourly WHERE ts::date = :dt ORDER BY ts",
                    "smp": "SELECT TO_CHAR(ts, 'HH24:00') as hour, system_marginal_price_try as price FROM raw_smp_hourly WHERE ts::date = :dt ORDER BY ts",
                    "kgup": "SELECT TO_CHAR(ts, 'HH24:00') as hour, total_mw as toplam FROM raw_kgup_hourly WHERE ts::date = :dt ORDER BY ts",
                    "load_forecast": "SELECT TO_CHAR(ts, 'HH24:00') as hour, load_forecast_mw as lep FROM raw_load_forecast_hourly WHERE ts::date = :dt ORDER BY ts",
                    "actual_generation": "SELECT TO_CHAR(ts, 'HH24:00') as hour, total_mw as total FROM raw_actual_generation_hourly WHERE ts::date = :dt ORDER BY ts",
                    "lightgbm": f"SELECT TO_CHAR(target_ts, 'HH24:00') as hour, predicted_mcp_try as price, predicted_mcp_try_p10 as price_p10, predicted_mcp_try_p90 as price_p90, predicted_mcp_usd as price_usd, predicted_mcp_usd_p10 as price_usd_p10, predicted_mcp_usd_p90 as price_usd_p90 FROM gold.ptf_predictions_daily WHERE {MODEL_FILTER} AND target_ts::date = :dt ORDER BY target_ts",
                }
                if type in sql_map:
                    with engine.connect() as conn:
                        res = conn.execute(text(sql_map[type]), {"dt": date}).mappings().all()
                        data = [dict(r) for r in res]
                        return JSONResponse(content=json.loads(json.dumps(data, default=str)))
                else:
                    return JSONResponse(content=[])

    except Exception as e:
        logger.error(f"DB query error: {e}")
        return JSONResponse(content=[])


_FX_CACHE = {"timestamp": 0, "response": None}

_YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


async def _fetch_yahoo_quote(client: httpx.AsyncClient, symbol: str) -> Optional[dict]:
    """Reads regularMarketPrice/previousClose for one Yahoo Finance symbol."""
    try:
        resp = await client.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}")
        if resp.status_code == 200:
            meta = resp.json()["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice")
            if price is not None:
                return {
                    "price": round(float(price), 4),
                    "prevClose": round(float(meta.get("previousClose") or price), 4),
                }
    except Exception as e:
        logger.warning(f"Yahoo quote fetch failed for {symbol}: {e}")
    return None


def _brent_from_db() -> Optional[dict]:
    """Last two ingested Brent closes from raw_macro_daily, used when Yahoo is unreachable."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT brent_oil_usd FROM raw_macro_daily WHERE brent_oil_usd IS NOT NULL "
                "ORDER BY entry_date DESC LIMIT 2"
            )).fetchall()
            if rows:
                price = float(rows[0][0])
                prev = float(rows[1][0]) if len(rows) > 1 else price
                return {"price": round(price, 4), "prevClose": round(prev, 4)}
    except Exception as e:
        logger.warning(f"Brent DB fallback query error: {e}")
    return None


def _ptf_daily_average() -> Optional[dict]:
    """Latest daily average MCP (TRY/MWh) with the previous day as prevClose."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT AVG(price_try) AS avg_try
                FROM raw_mcp_hourly
                GROUP BY (ts AT TIME ZONE 'Europe/Istanbul')::date
                ORDER BY (ts AT TIME ZONE 'Europe/Istanbul')::date DESC
                LIMIT 2
            """)).fetchall()
            if rows and rows[0][0] is not None:
                price = float(rows[0][0])
                prev = float(rows[1][0]) if len(rows) > 1 and rows[1][0] is not None else price
                return {"price": round(price, 2), "prevClose": round(prev, 2)}
    except Exception as e:
        logger.warning(f"PTF average query error: {e}")
    return None


def _yf_last_two_closes(symbol: str) -> Optional[dict]:
    """Last close and the one before it for a Yahoo symbol, via yfinance.

    Uses the yfinance library rather than the raw query1 chart endpoint because the
    latter answers 429 without cookie/crumb handling. Blocking — call in a thread.
    """
    try:
        import yfinance as yf
        df = yf.download(symbol, period="7d", interval="1d", progress=False,
                         timeout=10, auto_adjust=False)
        if df is None or df.empty or "Close" not in df.columns:
            return None
        close = df["Close"]
        if hasattr(close, "columns"):
            close = close.iloc[:, 0]
        close = close.dropna()
        if close.empty:
            return None
        price = float(close.iloc[-1])
        prev = float(close.iloc[-2]) if len(close) > 1 else price
        return {"price": round(price, 4), "prevClose": round(prev, 4)}
    except Exception as e:
        logger.warning(f"yfinance quote failed for {symbol}: {e}")
    return None


async def _fetch_commodities() -> dict:
    """Brent (BZ=F), Dutch TTF gas (TTF=F) and the daily PTF average.

    EU ETS carbon is deliberately absent: no free provider exposes a usable EUA
    price (EUA=F/KEUA/CFI2 return nothing, ^ICEEUA is an index, not €/t).
    """
    out = {}
    try:
        # Sequential on purpose: concurrent yf.download() calls clobber yfinance's
        # shared cookie/crumb state and both requests fail.
        quotes = await asyncio.to_thread(
            lambda: {s: _yf_last_two_closes(s) for s in ("BZ=F", "TTF=F")}
        )
        brent, ttf = quotes["BZ=F"], quotes["TTF=F"]
    except Exception as e:
        logger.warning(f"Commodity fetch failed: {e}")
        brent, ttf = None, None

    brent = brent or _brent_from_db()
    if brent:
        out["BRENT"] = brent
    if ttf:
        out["TTF"] = ttf

    ptf = _ptf_daily_average()
    if ptf:
        out["PTF_AVG"] = ptf
    return out


@app.get("/api/fx")
async def get_live_fx_rate():
    """Real-time USD/TRY, EUR/TRY plus Brent, TTF gas and daily PTF average (5-minute cache).

    Uses ExchangeRate-API as primary FX provider to prevent Yahoo Finance 429 rate limiting.
    Commodities always come from Yahoo Finance, with a PostgreSQL fallback for Brent.
    """
    now_ts = time.time()
    if _FX_CACHE["response"] is not None and (now_ts - _FX_CACHE["timestamp"]) < 300:
        return _FX_CACHE["response"]

    # Fetch yesterday's USD/TRY from DB for accurate prevClose
    db_prev_usd_try = None
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT usd_try FROM raw_macro_daily WHERE usd_try IS NOT NULL AND entry_date < CURRENT_DATE ORDER BY entry_date DESC LIMIT 1"
            )).fetchone()
            if row and row[0]:
                db_prev_usd_try = float(row[0])
    except Exception:
        pass

    payload = None

    # 1. Primary FX Provider: Open ExchangeRate API (Clean, fast, no 429 rate limiting)
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get("https://api.exchangerate-api.com/v4/latest/USD")
            if resp.status_code == 200:
                rates = resp.json().get("rates", {})
                usd_try = rates.get("TRY")
                if not usd_try:
                    raise ValueError("TRY rate missing from ExchangeRate API")
                eur_val = rates.get("EUR", 0.92)
                eur_try = usd_try / eur_val if eur_val else usd_try * 1.08
                usd_prev = db_prev_usd_try if db_prev_usd_try else round(usd_try * 0.998, 4)
                eur_prev = round(usd_prev / eur_val, 4) if (db_prev_usd_try and eur_val) else round(eur_try * 0.998, 4)
                payload = {
                    "USD": {"price": round(usd_try, 4), "prevClose": usd_prev},
                    "EUR": {"price": round(eur_try, 4), "prevClose": eur_prev},
                }
    except Exception as e:
        logger.warning(f"Primary FX fetch failed: {e}. Trying secondary Yahoo Finance API...")

    # 2. Secondary Fallback FX Provider: Yahoo Finance
    if payload is None:
        try:
            async with httpx.AsyncClient(timeout=8.0, headers=_YAHOO_HEADERS, follow_redirects=True) as client:
                usd_quote, eur_quote = await asyncio.gather(
                    _fetch_yahoo_quote(client, "USDTRY=X"),
                    _fetch_yahoo_quote(client, "EURTRY=X"),
                )
            if usd_quote and eur_quote:
                payload = {"USD": usd_quote, "EUR": eur_quote}
        except Exception as e:
            logger.error(f"Secondary FX fetch error: {e}")

    # 3. DB Safety Fallback: fetch latest real ingested exchange rate from PostgreSQL
    if payload is None:
        try:
            with engine.connect() as conn:
                row = conn.execute(text("SELECT usd_try FROM raw_macro_daily WHERE usd_try IS NOT NULL ORDER BY entry_date DESC LIMIT 1;")).fetchone()
                if row and row[0]:
                    db_usd = float(row[0])
                    usd_prev = db_prev_usd_try if db_prev_usd_try else round(db_usd * 0.998, 4)
                    payload = {
                        "USD": {"price": round(db_usd, 4), "prevClose": usd_prev},
                        "EUR": {"price": round(db_usd * 1.08, 4), "prevClose": round(usd_prev * 1.08, 4)},
                    }
        except Exception as e_db:
            logger.error(f"DB FX fallback query error: {e_db}")

    if payload is None:
        return JSONResponse(status_code=503, content={"error": "Live FX rates unavailable from APIs and Database"})

    payload.update(await _fetch_commodities())
    res = JSONResponse(content=payload)
    _FX_CACHE["timestamp"] = now_ts
    _FX_CACHE["response"] = res
    return res


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
