# Çok-pencereli ensemble + konformal band — implementasyon brief

**Bu dosya kimin için:** canlı repoda (`enerji_fiyat_tahmini`) implementasyonu yapacak olan.
**Kaynak karar:** deney reposu `../electricity_price_forecasting_in_turkish_day_ahead_market/`
— `ENSEMBLE_CANLI_GECIS.md` (tasarım) + `experiments/notebooks/07_lago_protocol/08_collapse_fix_experiments.ipynb` (kanıt).
**Önceki plan:** `ENSEMBLE_AKSIYON_PLANI.md` (31 Ağu, Opus) — hâlâ geçerli; bu dosya onu
**güncelliyor ve daraltıyor**: Faz 0 bitti, konformal parametreler kesinleşti, C.5.5 kapandı.

> **GÜNCEL DURUM (1 Eyl 2026, 12:40 — Opus):** implementasyon **TAMAMLANDI ve canlıya alındı.**
> `src/models/ensemble.py` yazıldı, iki pipeline bağlandı, öksüz UNIQUE index düşürüldü,
> 730 günlük backfill koşuldu (728 gün / 17.446 satır, `model_name='ensemble_v1'`),
> `api_server.ACTIVE_MODEL_NAME` çevrildi. Aşağıdaki "kod yazımı başlamadı" ifadesi
> ARTIK GEÇERSİZ. Ölçülen sonuçlar için commit mesajına ve `ENSEMBLE_AKSIYON_PLANI.md`'ye bak.

**Durum (1 Eyl 2026):** tasarım kesin, karar kapısı geçildi, kod yazımı **başlamadı**.
Sıradaki iş = Faz 1.

---

## 0. Karar özeti (neyi neden yapıyoruz)

Canlı model tek LightGBM, tüm geçmişle eğitiliyor. 2026 fiyat çöküşünde **sistematik yukarı
bias** veriyor (çöküş BIAS +$3.76, MAE $10.54). Kök neden: sabit özellik seti + tüm-geçmiş
eğitimi merit-order kırılmasını izleyemiyor.

**Çözüm:** LEAR ensemble mantığının LightGBM karşılığı — 3 pencere, eşit ağırlık ortalama.
Uzun üye yapı/mevsimsellik/sıfır-saat, kısa üyeler güncel rejim.

Deney reposunda 2-yıl walk-forward (proxy'siz, kaskad bug düzeltildikten sonra `_v2`):

| | canlı DB | ensemble | fark |
|---|---|---|---|
| 2-yıl MAE / rMAE | $7.27 / 0.645 | **$7.04 / 0.628** | −$0.23 (−%3) |
| normal MAE | $6.23 | $6.21 | nötr (sıfır-saat 9.8→12.2, kabul) |
| **çöküş MAE / BIAS** | $10.54 / +$3.76 | **$9.62 / +$1.54** | −$0.92, BIAS −%59 |
| toparlanma MAE / BIAS | $8.63 / +$1.51 | $8.20 / −$0.81 | −$0.43 |

Çerçeve doğrulaması: deney reposu proxy'siz `base` MAE $7.23 ≈ canlı DB $7.27 — WF harness
canlıyı sadık üretiyor.

**DM testi (base vs eşit ensemble, multivariate p=1):** TÜM dönem p<0.0001, çöküş p<0.0001,
çöküş-derin p<0.0001 — kazanç istatistiksel olarak gerçek. Normal p=0.61 (fark yok), toparlanma
p=0.11 (58g). Yani ensemble çöküşte kanıtlanmış biçimde daha iyi, normalde zarar vermiyor.

---

## 1. NE YAPILMAYACAK (sınırlar)

- **Rejim dedektörü YOK.** C.5.5 (çöküş sezilince ağırlık kaydır) elendi — çöküş MAE `base`
  ağırlığından bağımsız (~$9.6 sabit), ağırlık sadece bias kolu, ~$0.4 için dedektör riski
  alınmaz. Detay: `08 §8`.
- **`base 0.5×` default DEĞİL — muhtemelen hiç gerekmeyecek.** `(0.5·base + roll90 + roll150) / 2.5`
  çöküş bias'ında ~$0.4 kazandırır ama DM testinde çöküşte eşit ensemble'dan **ayırt edilemez**
  (p=0.90), normal/toparlanmada **DM-daha kötü** (p<0.001). Kodda `BASE_WEIGHT` sabiti olarak
  dursun (bedava kol), ama default 1.0 ve büyük ihtimalle öyle kalır.
- **Şema değişikliği YOK.** `gold.ptf_predictions_daily` sütunları aynı kalıyor
  (`predicted_mcp_usd`, `_p10`, `_p90`, TRY karşılıkları). PK zaten `(target_ts, model_name)`.
- **Rüzgar ön-tahmincisine dokunma.** Ayrı karar verildi — canlıda olduğu gibi kalıyor.
- **Deney CSV'lerini DB'ye import etme.** Özellik pipeline'ı / FX çözümleyici / pre-forecast
  kaynağı canlıda yaşıyor; CSV'ler yalnız doğrulama referansı.

---

## 2. Faz 1 — altyapı düzeltmeleri (ensemble'dan BAĞIMSIZ, zaten birer bug)

Bunlar ensemble reddedilse bile doğru. Shadow run'dan **önce** bitmeli.

### 2.1 `scripts/api_server.py` — `model_name` filtresi yok

`gold.ptf_predictions_daily`'ye vuran sorguların **hiçbirinde** `model_name` koşulu yok
(satırlar: 70, 148, 173, 207, 249, 269, 289, 308, 338, 348, 358, 375). İkinci bir `model_name`
(`ensemble_v1`) yazıldığı an:
- `LEFT JOIN ... ON m.ts = g.target_ts` → her saat **iki satır**
- `AVG(predicted_mcp_try)` → **iki modelin ortalaması**
- `next_day_forecast` → 48 satır

Dashboard **sessizce** bozulur.

**Yap:**
- Modül başına sabit: `ACTIVE_MODEL_NAME = "LightGBM_v1"`.
- 12 sorgunun hepsine `AND g.model_name = :active_model` (veya alias'sız olanlara
  `WHERE model_name = ...`). Parametreyi bind et.
- Alt-sorgulardaki `gold.ptf_predictions_daily g2` JOIN'lerine de ekle (satır 183, 188).
- Geçişte tek satır değişecek: `ACTIVE_MODEL_NAME = "ensemble_v1"`.

### 2.2 `scripts/backfill_gold_predictions.py` — `engine` used-before-defined

Satır 93: `resolve_usd_try_rate(df=tr_df, engine=engine)` döngü **içinde** çağrılıyor.
`engine = get_db_engine()` satır **119**'da, döngüden sonra. FX fallback tetiklenirse
(`tr_df['usd_try']` boşsa) `NameError`. Bugün latent çünkü `usd_try` hep dolu.

**Yap:** `engine = get_db_engine()`'i fonksiyon başına / döngü öncesine al. (`api_server`
engine bug'ıyla aynı sınıf — orada da varsa bak.)

---

## 3. Faz 2 — ensemble implementasyonu

### 3.1 Ortak modül: `src/models/ensemble.py` (YENİ)

Mantığı **tek yere** yaz; hem `predict_daily_pipeline.py` hem `backfill_gold_predictions.py`
bunu çağırsın. Kopyala-yapıştır yok.

**P50 — 3 pencere:**

```python
LGB_PARAMS = dict(objective="quantile", alpha=0.50, n_estimators=300, learning_rate=0.03,
                  max_depth=8, num_leaves=63, min_child_samples=10, verbose=-1, random_state=42)

# eğitim pencereleri (train_end = hedef gün − 1)
WINDOWS = {"A": 90, "B": 150, "C": None}   # gün; None = tüm geçmiş (2023-01-01'den)
BASE_WEIGHT = 1.0     # statik kol: çöküş uzarsa 0.5 yap. Default 1.0.

def fit_predict_ensemble(df_model, feature_cols, target_col, future_X):
    preds = {}
    for name, w in WINDOWS.items():
        tr = df_model if w is None else df_model.loc[df_model.index >= df_model.index.max() - pd.Timedelta(days=w)]
        m = LightGBMForecaster(params=LGB_PARAMS)
        m.fit(tr[feature_cols], tr[target_col].values)
        preds[name] = m.predict(future_X[feature_cols])       # shape (24,)
    wA = wB = 1.0
    wC = BASE_WEIGHT
    p50 = (wC*preds["C"] + wA*preds["A"] + wB*preds["B"]) / (wA + wB + wC)
    disagree = np.std(np.vstack([preds["A"], preds["B"], preds["C"]]), axis=0)   # (24,) saat bazlı
    return p50, disagree, preds
```

- `LightGBMForecaster` mevcut sınıf (`src/models/lightgbm_model.py`), aynen kullan.
- C üyesi = **bugünkü davranış** (tüm `df_model`). A/B küçük pencereler.
- `df_model` DatetimeIndex'li (mevcut pipeline'da öyle). `df_model.index.max()` = son bilinen gün.

**Konformal band — P10/P90:**

```python
FLOOR = 0.0        # TR MCP tabanı $0 (hiç negatif olmamış: 547 saat tam $0, min USD/TRY = 0)
N_CAL = 60         # kalibrasyon penceresi (gün)
W_DISAGREE = 0.5   # anlaşmazlık ağırlığı
ALPHA = 0.20       # hedef %80 kapsama
MIN_CAL_DAYS = 20  # bunun altında → fallback

def conformal_band(p50_24, disagree_24, err_hist):
    """
    err_hist: DataFrame, index=gün (son N_CAL gün, nedensel), columns=saat 0..23,
              değer = ensemble_P50 − gerçekleşen  (USD).
    p50_24, disagree_24: bugünün tahmini + üye std'si, shape (24,).
    """
    if err_hist is None or len(err_hist.dropna(how="all")) < MIN_CAL_DAYS:
        return np.maximum(p50_24 - 25.0, FLOOR), p50_24 + 25.0     # soğuk başlangıç fallback
    lo_q, hi_q = ALPHA/2, 1 - ALPHA/2
    qhi = err_hist.quantile(hi_q).to_numpy()     # (24,) — L için
    qlo = err_hist.quantile(lo_q).to_numpy()     # (24,) — U için
    L = np.maximum(p50_24 - qhi - W_DISAGREE * disagree_24, FLOOR)
    U = p50_24 - qlo + W_DISAGREE * disagree_24
    return L, U
```

- `err = tahmin − gerçek` (model fazla-tahmin biaslı → band aşağı kayıyor). İşaret önemli.
- **Saat bazlı** (24 ayrı dağılım) — gece/gündüz hata profili çok farklı.
- `FLOOR` sadece L'ye. U'ya tavan yok.
- Parametreler `conformal_final.py` taramasıyla kesinleşti: w=0 saf konformal %74–77 (hedefin
  altı), w=0.5 → her rejimde %80–82, w=0.7 fazla geniş. N 45/60/90 arası <%1 fark.

### 3.2 `err_hist` nereden geliyor

| bağlam | kaynak |
|---|---|
| **canlı günlük koşu** | `gold.ptf_predictions_daily` (`model_name = ACTIVE_MODEL_NAME`, `predicted_mcp_usd`) ⋈ `raw_mcp_hourly` (gerçekleşen USD fiyat), hedef günden önceki **son 60 gün**. `target_ts` saat bazlı pivotla. |
| **backfill (walk-forward)** | O ana kadar **kendi ürettiği** P50'ler (nedensel — gelecek satır okunamaz). Backfill döngüsünde `all_records`'u biriktirirken son 60 günün P50'sini in-memory tut, gerçekleşenle eşle. |

### 3.3 `scripts/predict_daily_pipeline.py` — `run_daily_prediction()`

Şu an (satır ~196–226): `X_train = df_model[feature_cols]` üzerinde **3 head P10/P50/P90**.

**Yeni:**
- ADIM 4: 3 head yerine → `ensemble.fit_predict_ensemble(df_model, feature_cols, target_col, future_df)`.
  (future_df ADIM 5d'de hazırlanıyor — çağrıyı oraya taşı ya da modeli 4'te fit edip 5d'de predict et.)
- ADIM 5d sonrası: `p50, disagree, _ = ...` → `preds_usd = p50`.
- Yeni ADIM 5e: son 60 günün `err_hist`'ini çek (§3.2) → `L, U = ensemble.conformal_band(p50, disagree, err_hist)`.
- `results_df`: `predicted_mcp_usd = p50`, `predicted_mcp_usd_p10 = L`, `predicted_mcp_usd_p90 = U`.
- **Monotonluk:** `L = min(L, p50)`, `U = max(U, p50)` (konformal normalde korur ama garanti et).
- TRY: `preds_try = preds_usd * day_usd_try` — `day_usd_try` = eğitim penceresinin son bilinen
  kuru (`resolve_usd_try_rate`, mevcut mantık). Kaydedilmiş try/usd oranından **asla** geri türetme.
- INSERT ... ON CONFLICT bloğu (satır ~415): `model_name` = `ACTIVE_MODEL_NAME` (shadow'da `'ensemble_v1'`).

### 3.4 `scripts/backfill_gold_predictions.py` — `backfill_historical_predictions()`

- Döngü içindeki 3 head (satır ~61–79) → `ensemble.fit_predict_ensemble(...)`.
- `tr_df` = train penceresi, `te_df` = hedef gün 24 saat. A/B için `tr_df.loc[tr_df.index >= tr_df.index.max() - Nd]`.
- Konformal: §3.2'deki in-memory nedensel `err_hist`. İlk 60 gün fallback band'e düşer (kabul).
- `model_name` = parametre (`--model-name`, default `'ensemble_v1'`).
- Faz 1.2'deki `engine` fix'i şart.

### 3.5 Eğitim maliyeti

3× model, ama A/B küçük (~2000–3600 saat). Toplam duvar-saati ~**2×** (04:00 koşusu ~1–2 dk →
~3–4 dk). Kabul.

---

## 4. Doğrulama (Faz 2 çıktısı için)

### 4.1 Birim — deney reposu CSV'lerine karşı

Referans (proxy'siz WF, 2024-08-01 → 2026-08-27, P50 USD, gün×24):
`../electricity_price_forecasting_in_turkish_day_ahead_market/experiments/notebooks/07_lago_protocol/`
- `wf_lgbm_bt2y_v2_base.csv` (C üyesi = tüm geçmiş)
- `wf_lgbm_bt2y_v2_roll90.csv` (A)
- `wf_lgbm_bt2y_v2_roll150.csv` (B)

**Tolerans:** saatlik `|fark| < $0.10`, günlük MAE farkı `< $0.05`. **Byte-yakın DEĞİL** —
`run_wf_lightgbm.PARAMS`'ta `random_state` yoktu, canlı `LightGBMForecaster` `random_state=42` +
`deterministic` + `force_col_wise` uyguluyor. Küçük stokastik fark beklenir.

### 4.2 Bütünsel — yeniden üretilmesi gereken sayılar

730 günlük backfill sonrası (yukarıdaki §0 tablosu):
- 2-yıl MAE ~$7.04 (canlı $7.27), rMAE ~0.628
- çöküş (2026-02→06) MAE ~$9.62, BIAS ~+$1.54
- toparlanma MAE ~$8.20
- normal MAE ~$6.21, sıfır-saat MAE ~12 (hafif regresyon, kabul)

**Konformal kapsama** (hedef %80), her rejim + son 30/90g'de **%80–82** (canlı 3-head %60–76).
Kırılım (`08 §7.1`): $0 civarı %92+, **$20–60 gerçekleşen aralıkta ~%70** (modelin kalıntı
yukarı biası — bilinen), $60–100 %84, $100+ spike'lar yukarı kaçıyor.

---

## 5. Faz 3 — shadow + geçiş

1. Branch `feat/multiwindow-ensemble`.
2. Faz 1 + Faz 2 merge (Faz 1 ayrı PR olabilir — ensemble'dan bağımsız).
3. **Shadow ≥ 14 gün:** günlük koşu `model_name='ensemble_v1'` ile `LightGBM_v1`'in YANINA
   yazsın. Dashboard Faz 1.1 sayesinde `LightGBM_v1`'i göstermeye devam eder.
4. **Kabul kriterleri (haftalık):**
   - ensemble P50 MAE ≤ tek-model MAE
   - P10–P90 kapsama ≥ %78 (ideal ~%80)
   - normal günlerde MAE regresyonu < +$0.30
5. **Geçiş:** onaylanırsa `backfill_gold_predictions.py --model-name ensemble_v1` 730 güne koş
   (canlı repodan, deney CSV'leri değil) → `ACTIVE_MODEL_NAME = 'ensemble_v1'` (tek satır) →
   dashboard yeni seriye döner.
6. **İzleme:** haftalık kapsama + BIAS paneli. Alarm: BIAS < −$3 veya kapsama < %70.

---

## 6. Bilinen sınırlar (geçişten sonra da açık)

- **Ani tek-gün aşağı-spike körlüğü** — 30 Ağu 2026 fiyat $40'a çöktü, ensemble $49 dedi. Tüm
  recency yöntemleri buna açık. Ayrı iş.
- **Konformal band hızlı rejim geçişinde geç** — anlaşmazlık terimi hafifletiyor, bitirmiyor.
- **$20–60 gerçekleşen aralıkta band kapsaması ~%70** — modelin kalıntı çöküş biası (+$1.54).
- **Spike'lar ($100+) bandın üstünden kaçıyor** — recency kör, 25 saat / 2 yıl.
- **Pre-forecast lag0 özellikleri** `gold.kgup_load_pre_forecasts`'a bağımlı, tablo yalnız
  2024-08-13'ten dolu. Daha eski backfill'de bu özellikler fallback (gerçek/aynı-gün) ile
  dolar — gerçek ön-tahmin değil. Ensemble bunu değiştirmiyor, mevcut davranış.
- `TRAINING_DATA_START = 2023-01-01` (2022 spike rejimini dışlıyor). C üyesi zaten bu; A/B
  otomatik 2023+.
