"""Çok-pencereli LightGBM ensemble + yuvarlanan konformal band.

`lgb_lag0_v2`'nin tek-model halinin yerini alır. İki tüketicisi var —
`scripts/predict_daily_pipeline.py` (günlük 04:00 koşusu) ve
`scripts/backfill_gold_predictions.py` (geçmiş yeniden üretimi). İkisi de bu
modülü çağırır; mantık BİR yerde yaşar, çünkü backfill'in ürettiği P50'ler
canlının band kalibrasyonunu besliyor — ikisi ayrışırsa band sessizce bozulur.

Kaynak: deney reposu Faz C.5.3' / C.5 konformal taraması
(`../electricity_price_forecasting_in_turkish_day_ahead_market/ENSEMBLE_CANLI_GECIS.md`),
doğrulama `../electricity_price_forecasting_in_turkish_day_ahead_market/ENSEMBLE_AKSIYON_PLANI.md` §1.

P50 (nokta tahmin)
------------------
Üç LightGBM, aynı `lgb_lag0_v2` konfigü, farklı eğitim pencereleri:

    A = son 90 gün    güncel rejim, hızlı uyum
    B = son 150 gün   güncel rejim, daha kararlı
    C = tüm geçmiş    yapı + mevsimsellik (mevcut canlı davranış)

    P50 = (A + B + C) / 3       eşit ağırlık

Uyarlanabilir ağırlık test edildi, katkı vermedi. Çöküş rejiminde MAE
$10.53 -> $9.65, BIAS +$3.59 -> +$1.64; normal rejimde nötr.

P10/P90 (band)
--------------
Quantile head'leri (alpha=0.10/0.90) KALDIRILDI — kapsamaları çöküşte %59.7'ye
düşüyordu. Yerine nedensel split-conformal, saat bazlı:

    err[d',h] = ensemble_P50[d',h] - gerçek[d',h]      d' ∈ son 60 gün
    L(d,h) = P50(d,h) - quantile_0.90(err[:,h]) - 0.5·std({A,B,C})(d,h)
    U(d,h) = P50(d,h) - quantile_0.10(err[:,h]) + 0.5·std({A,B,C})(d,h)
    L(d,h) = max(L(d,h), FLOOR)

`err`'in işareti bilinçli: model fazla-tahmin biaslı, band aşağı kayıyor.
`std({A,B,C})` = üç üyenin o saatteki anlaşmazlığı; 60 günlük hata geçmişi
rejim geçişinde geç kaldığı için erken uyarı terimi olarak ekleniyor. w=0 saf
konformal %74-77'de kalıyor, w=0.5 her rejimde %80-82 veriyor.

FLOOR = $0: TR MCP tarihinde hiç negatif olmamış (DB'de 0 negatif saat, 455
saat tam $0). Negatif fiyat mevzuatı değişirse burası gözden geçirilmeli.
"""

import logging
from dataclasses import dataclass
from typing import Dict, Optional, Sequence

import numpy as np
import pandas as pd

from src.models.lightgbm_model import LightGBMForecaster

logger = logging.getLogger("Ensemble")

# lgb_lag0_v2 konfigü. Üç üye de AYNI hiperparametreleri kullanır — aralarındaki
# tek fark eğitim penceresi. LightGBMForecaster reproducibility guard'larını
# (random_state / deterministic / force_col_wise) setdefault ile ekliyor.
MEMBER_PARAMS = {
    'objective': 'quantile', 'alpha': 0.50,
    'n_estimators': 300, 'learning_rate': 0.03, 'max_depth': 8, 'num_leaves': 63,
    'min_child_samples': 10,
    'verbose': -1, 'random_state': 42,
}

# Eğitim pencereleri (gün). None = tüm geçmiş.
MEMBER_WINDOWS: Sequence[Optional[int]] = (90, 150, None)

CALIBRATION_DAYS = 60          # konformal hata geçmişi penceresi
MIN_CALIBRATION_DAYS = 20      # bunun altında band güvenilir değil -> fallback
DISAGREEMENT_WEIGHT = 0.5      # üye anlaşmazlığının band genişletme katsayısı
COVERAGE_LO, COVERAGE_HI = 0.10, 0.90
PRICE_FLOOR_USD = 0.0
FALLBACK_HALF_WIDTH_USD = 25.0  # soğuk başlangıç: P50 ± $25

MODEL_NAME = 'ensemble_v1'


@dataclass
class EnsemblePrediction:
    """Bir hedef gün için ensemble çıktısı (USD/MWh)."""
    p50: np.ndarray            # (n,) ortalama
    p10: np.ndarray            # (n,) konformal alt sınır
    p90: np.ndarray            # (n,) konformal üst sınır
    disagreement: np.ndarray   # (n,) üye std'si — teşhis için
    member_preds: Dict[str, np.ndarray]
    band_source: str           # 'conformal' | 'fallback'
    calibration_days: int


def member_label(window: Optional[int]) -> str:
    return 'full' if window is None else f'roll{window}'


def fit_members(df_model: pd.DataFrame, feature_cols: Sequence[str],
                target_col: str = 'mcp_price_usd',
                train_end: Optional[pd.Timestamp] = None,
                windows: Sequence[Optional[int]] = MEMBER_WINDOWS,
                n_jobs_per_model: Optional[int] = None) -> Dict[str, LightGBMForecaster]:
    """Üç üyeyi eğitir.

    `train_end` verilirse eğitim orada kesilir (walk-forward backfill için).
    Pencereler `train_end`'den GERİYE sayılır, takvim gününden değil — backfill
    geçmişte yürürken pencerenin hedef güne göre konumlanması şart.

    `n_jobs_per_model`: LightGBM'in kullanacağı iş parçacığı sayısı. Varsayılan
    (None) LightGBM'e bırakır = tüm çekirdekler; günlük 04:00 koşusu için doğru,
    çünkü dışarıda paralellik yok. AMA backfill günleri joblib ile paralel
    koşuyor — orada bu ayarlanmazsa 6 işçi x 8 çekirdek = 48 iş parçacığı olur
    ve makine kilitlenir (bir kez yaşandı). Dış paralellik varsa MUTLAKA ver.
    """
    if train_end is None:
        train_end = df_model.index.max()
    full = df_model.loc[:train_end]

    models: Dict[str, LightGBMForecaster] = {}
    for w in windows:
        tr = full if w is None else full.loc[full.index >= train_end - pd.Timedelta(days=w)]
        label = member_label(w)
        if len(tr) < 500:
            # Pencere veriyi kapsamıyor (ör. eğitim penceresinin en başındayız).
            # Sessizce atlamak ensemble'ı iki üyeye düşürür ve bunu kimse fark
            # etmez; o yüzden gürültülü log.
            logger.warning("Üye '%s' atlandı: yalnızca %d saat var (train_end=%s)",
                           label, len(tr), train_end)
            continue
        params = dict(MEMBER_PARAMS)
        if n_jobs_per_model is not None:
            params['n_jobs'] = n_jobs_per_model
        m = LightGBMForecaster(params=params)
        m.fit(tr[list(feature_cols)], tr[target_col].values)
        models[label] = m
    if not models:
        raise RuntimeError(f"Hiçbir ensemble üyesi eğitilemedi (train_end={train_end}).")
    return models


def predict_members(models: Dict[str, LightGBMForecaster], X: pd.DataFrame,
                    feature_cols: Sequence[str]) -> Dict[str, np.ndarray]:
    return {label: m.predict(X[list(feature_cols)]) for label, m in models.items()}


def combine(member_preds: Dict[str, np.ndarray]) -> tuple[np.ndarray, np.ndarray]:
    """Eşit ağırlıklı ortalama ve üye anlaşmazlığı (std)."""
    stack = np.vstack([member_preds[k] for k in sorted(member_preds)])
    # ddof=0: üç üye bir örneklem değil, popülasyonun tamamı.
    return stack.mean(axis=0), stack.std(axis=0, ddof=0)


def conformal_band(p50: np.ndarray, hours: Sequence[int], disagreement: np.ndarray,
                   error_history: Optional[pd.DataFrame]) -> tuple[np.ndarray, np.ndarray, str, int]:
    """Saat bazlı nedensel split-conformal band.

    `error_history`: sütunları ['hour', 'error'] olan, hedef günden ÖNCEKİ
    günlere ait (tahmin - gerçek) kayıtları. Boş/yetersizse fallback banda düşer.

    Döner: (L, U, band_source, kalibrasyon_gün_sayısı)
    """
    n_days = 0
    if error_history is not None and not error_history.empty:
        n_days = int(error_history['target_date'].nunique())

    if n_days < MIN_CALIBRATION_DAYS:
        logger.warning(
            "Konformal kalibrasyon geçmişi yetersiz (%d gün < %d) -> sabit ±$%.0f band. "
            "Günlük koşuda bunu görüyorsan backfill eksik demektir; band bugünkünden GENİŞ "
            "ve kalibrasyonsuz olacak.",
            n_days, MIN_CALIBRATION_DAYS, FALLBACK_HALF_WIDTH_USD)
        lo = np.maximum(p50 - FALLBACK_HALF_WIDTH_USD, PRICE_FLOOR_USD)
        hi = p50 + FALLBACK_HALF_WIDTH_USD
        return lo, hi, 'fallback', n_days

    by_hour = error_history.groupby('hour')['error']
    q_hi = by_hour.quantile(COVERAGE_HI)   # L için
    q_lo = by_hour.quantile(COVERAGE_LO)   # U için
    # Bir saat için hiç kayıt yoksa (ör. DST) tüm saatlerin medyanına düş.
    fill_hi, fill_lo = float(q_hi.median()), float(q_lo.median())

    shift_hi = np.array([q_hi.get(h, fill_hi) for h in hours], dtype=float)
    shift_lo = np.array([q_lo.get(h, fill_lo) for h in hours], dtype=float)

    lo = p50 - shift_hi - DISAGREEMENT_WEIGHT * disagreement
    hi = p50 - shift_lo + DISAGREEMENT_WEIGHT * disagreement

    lo = np.maximum(lo, PRICE_FLOOR_USD)
    # Monotonluk: kalibrasyon ve anlaşmazlık terimleri birlikte sınırları
    # çaprazlayabilir (özellikle FLOOR kırpmasından sonra).
    lo = np.minimum(lo, p50)
    hi = np.maximum(hi, p50)
    return lo, hi, 'conformal', n_days


def build_error_history(p50_history: pd.Series, actuals: pd.Series,
                        before: pd.Timestamp,
                        days: int = CALIBRATION_DAYS) -> pd.DataFrame:
    """Kalibrasyon setini kurar: (tahmin - gerçek), saat etiketli.

    `before`: hedef günün başlangıcı. Pencere [before - days, before) — hedef
    günün KENDİSİ dışarıda, sızıntı olmasın.
    """
    start = before - pd.Timedelta(days=days)
    idx = p50_history.index.intersection(actuals.index)
    idx = idx[(idx >= start) & (idx < before)]
    if len(idx) == 0:
        return pd.DataFrame(columns=['target_date', 'hour', 'error'])
    err = p50_history.reindex(idx).astype(float) - actuals.reindex(idx).astype(float)
    err = err.dropna()
    return pd.DataFrame({
        'target_date': err.index.normalize(),
        'hour': err.index.hour,
        'error': err.to_numpy(),
    })


def predict(df_model: pd.DataFrame, feature_cols: Sequence[str], X_future: pd.DataFrame,
            error_history: Optional[pd.DataFrame] = None,
            target_col: str = 'mcp_price_usd',
            train_end: Optional[pd.Timestamp] = None,
            models: Optional[Dict[str, LightGBMForecaster]] = None,
            n_jobs_per_model: Optional[int] = None) -> EnsemblePrediction:
    """Uçtan uca: üyeleri eğit (ya da hazır al), birleştir, bandı kur.

    `models` verilirse yeniden eğitim yapılmaz — backfill aynı günü birden çok
    kez tahmin etmiyor ama çağıran tarafın modelleri yeniden kullanma seçeneği
    olsun diye açık bırakıldı.
    """
    if models is None:
        models = fit_members(df_model, feature_cols, target_col, train_end,
                             n_jobs_per_model=n_jobs_per_model)
    member_preds = predict_members(models, X_future, feature_cols)
    p50, disagreement = combine(member_preds)
    hours = list(pd.DatetimeIndex(X_future.index).hour)
    p10, p90, source, n_days = conformal_band(p50, hours, disagreement, error_history)
    return EnsemblePrediction(p50=p50, p10=p10, p90=p90, disagreement=disagreement,
                              member_preds=member_preds, band_source=source,
                              calibration_days=n_days)
