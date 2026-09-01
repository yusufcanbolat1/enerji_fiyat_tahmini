"""
`gold.ptf_predictions_daily` tohumlama (seed/backfill) script'i.

Dashboard'un geçmiş model performansını ve gerçekleşen-vs-tahmin grafiklerini
kesintisiz gösterebilmesi için son 730 günün walk-forward tahminlerini üretir.

Ensemble'a geçişten sonra ikinci bir görevi daha var: **konformal bandın
kalibrasyon geçmişini kurmak.** Canlı pipeline yarının bandını son 60 günün
(ensemble P50 − gerçekleşen) hatalarından hesaplıyor; o geçmiş burada üretiliyor.
Backfill koşulmadan canlıya geçilirse band 60 gün boyunca sabit ±$25 fallback'inde
kalır. Bkz. `ENSEMBLE_AKSIYON_PLANI.md`.

İki geçişli:
  1. (paralel) Her gün için 3 ensemble üyesi eğitilir, P50 + anlaşmazlık üretilir.
     Pahalı kısım burası — 790 gün x 3 model.
  2. (seri, saniyeler) Günler kronolojik yürünür, her günün bandı YALNIZCA
     kendinden önceki günlerin P50 hatalarından kurulur. Nedenselliği koruyan
     kısım bu; model eğitimi içermediği için paralelleştirmeye gerek yok.
"""

import logging
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from sqlalchemy import text

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from db.connection import get_db_engine
from src.features.feature_engineering import build_robust_features, get_feature_columns
from src.models import ensemble as ens_mod
from predict_daily_pipeline import create_gold_schema_if_not_exists, load_all_historical_data

logger = logging.getLogger("GoldBackfill")

CHUNK_SIZE = 1000

INSERT_SQL = text("""
    INSERT INTO gold.ptf_predictions_daily (
        target_ts, predicted_mcp_usd, predicted_mcp_try,
        predicted_mcp_usd_p10, predicted_mcp_try_p10,
        predicted_mcp_usd_p90, predicted_mcp_try_p90,
        model_name
    )
    VALUES (
        :target_ts, :predicted_mcp_usd, :predicted_mcp_try,
        :predicted_mcp_usd_p10, :predicted_mcp_try_p10,
        :predicted_mcp_usd_p90, :predicted_mcp_try_p90,
        :model_name
    )
    ON CONFLICT (target_ts, model_name)
    DO UPDATE SET
        predicted_mcp_usd = EXCLUDED.predicted_mcp_usd,
        predicted_mcp_try = EXCLUDED.predicted_mcp_try,
        predicted_mcp_usd_p10 = EXCLUDED.predicted_mcp_usd_p10,
        predicted_mcp_try_p10 = EXCLUDED.predicted_mcp_try_p10,
        predicted_mcp_usd_p90 = EXCLUDED.predicted_mcp_usd_p90,
        predicted_mcp_try_p90 = EXCLUDED.predicted_mcp_try_p90,
        created_at = CURRENT_TIMESTAMP;
""")


def _preflight_write_check(engine):
    """Yazma yolunu SAATLERCE süren hesaptan ÖNCE dene.

    Eski sürüm tüm günleri hesaplayıp en sonda tek seferde yazıyordu; insert'te
    bir engel çıkarsa (kısıt ihlali, izin, bağlantı) saatlerce süren hesap çöpe
    gidiyordu. Gerçekte kullanılacak tablo/kısıt yolunu tek satırla sınayıp
    hemen geri alıyoruz.
    """
    probe = {
        'target_ts': pd.Timestamp('1970-01-01 00:00:00+00:00'),
        'predicted_mcp_usd': 0.0, 'predicted_mcp_try': 0.0,
        'predicted_mcp_usd_p10': 0.0, 'predicted_mcp_try_p10': 0.0,
        'predicted_mcp_usd_p90': 0.0, 'predicted_mcp_try_p90': 0.0,
        'model_name': '__preflight__',
    }
    with engine.connect() as conn:
        try:
            conn.execute(INSERT_SQL, [probe])
        finally:
            conn.execute(text("DELETE FROM gold.ptf_predictions_daily WHERE model_name = '__preflight__'"))
            conn.commit()
    logger.info("✅ Yazma ön-kontrolü geçti.")


def _predict_day(df_model, feature_cols, target_col, test_start, test_end, threads):
    """Tek gün: 3 üyeyi eğit, P50 + anlaşmazlık döndür. Band YOK (2. geçişte)."""
    train_end = test_start - pd.Timedelta(hours=1)
    tr_df = df_model.loc[:train_end]
    te_df = df_model.loc[test_start:test_end]
    if len(tr_df) < 1000 or len(te_df) < 12:
        return None

    models = ens_mod.fit_members(df_model, feature_cols, target_col=target_col,
                                 train_end=train_end, n_jobs_per_model=threads)
    member_preds = ens_mod.predict_members(models, te_df, feature_cols)
    p50, disagreement = ens_mod.combine(member_preds)

    # Tahmin anında BİLİNEBİLİR kur: eğitim penceresinin son değeri, yani hedef
    # günden bir önceki gün. te_df['usd_try'] (hedef günün kendi kuru) ileriye
    # bakış olurdu — 04:00'te yarının kuru henüz yok.
    day_usd_try = None
    if 'usd_try' in tr_df.columns:
        known_fx = tr_df['usd_try'].dropna()
        if len(known_fx) > 0:
            day_usd_try = float(known_fx.iloc[-1])
    return te_df.index, p50, disagreement, day_usd_try


def backfill_historical_predictions(num_days: int = 730, n_jobs: int = 6,
                                    checkpoint_path: str | None = None):
    """Son `num_days` gün için ensemble tahminlerini üretir ve DB'ye yazar."""
    logger.info("📦 Veritabanından tüm geçmiş veri yükleniyor...")
    create_gold_schema_if_not_exists(run_backfill_if_empty=False)

    engine = get_db_engine()
    _preflight_write_check(engine)

    df_raw = load_all_historical_data()
    df_feat = build_robust_features(df_raw)
    feature_cols = get_feature_columns('robust', df_feat)
    target_col = 'mcp_price_usd'
    df_model = df_feat.dropna(subset=feature_cols + [target_col]).copy()

    max_ts = df_model.index.max()
    warmup = ens_mod.CALIBRATION_DAYS

    # Isınma: konformal band ilk `warmup` günde geçmişe sahip olmadığı için
    # fallback'e düşer. O günleri KOŞUP YAZMIYORUZ — böylece yazılan ilk günün
    # bile tam kalibrasyon penceresi olur, "ilk günler geniş band" sorunu
    # backfill'in içinde kalır ve canlıya taşmaz.
    first_written_start = max_ts - pd.Timedelta(days=num_days - 1) - pd.Timedelta(hours=23)

    spans = []
    for day_idx in range(num_days - 1 + warmup, -1, -1):
        test_end = max_ts - pd.Timedelta(days=day_idx)
        spans.append((test_end - pd.Timedelta(hours=23), test_end))

    # İş parçacığı bütçesi. LightGBM varsayılanı "tüm çekirdekler"; joblib zaten
    # `n_jobs` işçi açtığı için çarpım makineyi kilitler. Toplamı çekirdek-1 ile
    # sınırlıyoruz ki makine kullanılabilir kalsın.
    cores = os.cpu_count() or 4
    n_jobs = max(1, min(n_jobs, cores - 1))
    threads = max(1, (cores - 1) // n_jobs)
    logger.info("⏳ %d gün koşulacak (%d yazılacak + %d ısınma) — %d işçi x %d iş parçacığı "
                "= %d/%d çekirdek", len(spans), num_days, warmup, n_jobs, threads,
                n_jobs * threads, cores)

    # ── 1. geçiş: paralel P50 ────────────────────────────────────────────────
    raw = Parallel(n_jobs=n_jobs, verbose=5)(
        delayed(_predict_day)(df_model, feature_cols, target_col, s, e, threads) for s, e in spans)
    days = [r for r in raw if r is not None]
    if not days:
        raise RuntimeError("Hiçbir gün tahmin edilemedi — veri aralığını kontrol et.")
    logger.info("✅ 1. geçiş bitti: %d gün tahmin edildi.", len(days))

    # ── 2. geçiş: seri, nedensel band ────────────────────────────────────────
    actuals = df_model[target_col].astype(float)
    p50_history: dict = {}
    band_sources: dict = {}
    all_records = []

    for idx, p50, disagreement, day_usd_try in days:
        test_start = idx.min()
        p50_series = pd.Series(list(p50_history.values()), dtype=float,
                               index=pd.DatetimeIndex(list(p50_history.keys())))
        err_hist = ens_mod.build_error_history(p50_series, actuals, before=test_start)
        p10, p90, band_source, _ = ens_mod.conformal_band(
            p50, list(idx.hour), disagreement, err_hist)
        band_sources[band_source] = band_sources.get(band_source, 0) + 1

        # Sonraki günün kalibrasyonu için biriktir — ısınma günleri de dahil,
        # yazılmasalar bile kalibrasyona katkıları var (zaten amaçları bu).
        for ts_i, p_i in zip(idx, p50):
            p50_history[ts_i] = float(p_i)

        if test_start < first_written_start:
            continue

        if day_usd_try is None:
            from fetch_epias_data import resolve_usd_try_rate
            day_usd_try = resolve_usd_try_rate(df=df_model.loc[:test_start], engine=engine)

        for ts_val, u, lo, hi in zip(idx, p50, p10, p90):
            all_records.append({
                'target_ts': ts_val,
                'predicted_mcp_usd': float(np.round(u, 4)),
                'predicted_mcp_try': float(np.round(u * day_usd_try, 4)),
                'predicted_mcp_usd_p10': float(np.round(lo, 4)),
                'predicted_mcp_try_p10': float(np.round(lo * day_usd_try, 4)),
                'predicted_mcp_usd_p90': float(np.round(hi, 4)),
                'predicted_mcp_try_p90': float(np.round(hi * day_usd_try, 4)),
                'model_name': ens_mod.MODEL_NAME,
            })

    logger.info("📐 Band kaynakları: %s  (yazılan günlerde fallback beklenmez)", band_sources)

    if checkpoint_path:
        # Hesap pahalı, yazma ucuz — insert bir şekilde düşerse tekrar koşmayalım.
        pd.DataFrame(all_records).to_csv(checkpoint_path, index=False)
        logger.info("💾 Kontrol noktası: %s", checkpoint_path)

    logger.info("💾 %d kayıt gold.ptf_predictions_daily'ye yazılıyor (model=%s)...",
                len(all_records), ens_mod.MODEL_NAME)
    with engine.connect() as conn:
        for i in range(0, len(all_records), CHUNK_SIZE):
            conn.execute(INSERT_SQL, all_records[i:i + CHUNK_SIZE])
            conn.commit()          # parça parça commit: yarıda kalırsa yazılan durur
    logger.info("🎉 Backfill tamamlandı.")


# Backward compatibility alias
backfill_365_days_predictions = backfill_historical_predictions


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=730)
    ap.add_argument("--n-jobs", type=int, default=6)
    ap.add_argument("--checkpoint", default=None, help="Kayıtları CSV'ye de yaz (insert düşerse kurtarma).")
    a = ap.parse_args()
    backfill_historical_predictions(num_days=a.days, n_jobs=a.n_jobs, checkpoint_path=a.checkpoint)
