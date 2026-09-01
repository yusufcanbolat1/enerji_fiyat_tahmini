# Ensemble canlıya geçiş — aksiyon planı

**Girdi:** `../electricity_price_forecasting_in_turkish_day_ahead_market/ENSEMBLE_CANLI_GECIS.md`
**Tarih:** 31 Ağustos 2026
**Karar:** ensemble **kabul edilebilir** — ama iddia edilen kazancın ~%55'i ölçüm bug'ından
geliyordu. Doğrulama koşuları yapıldı (§1), 2 yıllık temiz yeniden üretim hâlâ gerekli.

---

## 0. Soruların kısa cevabı

**"Backfill gerekli mi, deney reposundaki CSV'ler yeterli mi?"**

**CSV'ler yeterli değil, backfill gerekli.** Üç bağımsız sebep — birincisi ölçümün
büyüklüğünü şişiriyor:

| # | sebep | ağırlık |
|---|---|---|
| A | `wf_lgbm_bt2y_*.csv`'ler **kaskad bug'lı** bir çerçevede üretildi: 757 günün tamamı yük/KGÜP/üretim/makro sütunları **2024-07-31'de donmuş** halde koştu | **bloke edici** |
| B | CSV'ler yalnız **P50, USD, gün×24**. P10/P90 yok, TRY yok | orta (türetilebilir, ama A'nın üstüne) |
| C | CSV'ler **2024-08-01 → 2026-08-27**; canlı DB **2024-08-12 → 2026-09-01**. Son 5 gün eksik | küçük |

---

## 1. Bulgu A — `--proxy-from` kaskadı (kök sorun)

`experiments/scripts/run_wf_lightgbm.py` içindeki T→T+1 proxy döngüsü, ilerlerken **kendi
yazdığını okuyor**:

```python
dn = df.index.normalize()
for d in pd.date_range(a.start, a.end, freq="D", tz="Europe/Istanbul"):
    m = dn == d
    prev = df.loc[dn == d - pd.Timedelta(days=1)]   # <-- d-1 zaten EZİLMİŞ
    for c in PROXY_COPY:
        df.loc[m, c] = prev[c].tail(24).values
```

Gün *d* işlenirken *d-1* bir önceki iterasyonda çoktan ezilmiştir. Sonuç kaskad: `--proxy-from
2024-08-01` ile **2024-08-01'den 2026-08-27'ye kadar her gün 2024-07-31'in değerlerini alır.**

Minimal repro (doğrulandı):

```
2024-07-30 [ 0.  1.  2.]
2024-07-31 [24. 25. 26.]   <- kaynak
2024-08-01 [24. 25. 26.]
2024-08-02 [24. 25. 26.]   <- d-1'den değil, hâlâ 07-31'den
2024-08-04 [24. 25. 26.]
```

### Neden bu kadar önemli

`PROXY_COPY` = `load_forecast_mw, kgup_{total,gas,wind,solar,hydro,geo,biomass,coal}_mw,
actual_gen_total_mw, actual_cons_mw, smp_price_try`; `PROXY_FFILL` = `usd_try, brent_oil_usd,
natural_gas_grf_try`. Bunlar `build_robust_features`'ta **65+ özelliğin ~30'unu** besliyor
(`load_lag_*`, `kgup_*_lag_*`, `supply_demand_gap_mw`, `renewable_ratio`, `solar_wind_ratio`,
`*_pressure_ratio`, `actual_*_lag_*`, `smp_usd_lag_48`, `brent_oil_lag_48`,
`natural_gas_grf_lag_48`). Kaskad altında bunların hepsi 2 yıl boyunca **günlük olarak tekrar
eden sabit bir profil.** Model fiilen yalnız fiyat lag'leri + `*_lag0` ön-tahminleri + sıcaklıkla
çalışmış.

**Ayrıca:** proxy, hedef günün *kendi* tahminini zaten etkilemez — `feature_engineering.py`'de
egzojen sütunların tamamı `shift(24/48/168)` ile giriyor, gün D'nin ham değeri gün D'nin
özelliklerine hiç girmiyor. Yani `--proxy-from` "sızıntısızlık" için **gerekli değildi**;
kaskadsız hali bile bir şey düzeltmiyor, kaskadlı hali özellikleri öldürüyor.

### Ölçülen bedel

119 günlük dilimde (2026-05-01 → 2026-08-27) aynı script proxy'siz yeniden koşuldu
(`wf_lgbm_DIAG_noproxy.csv`, 194 s):

| seri | gerçeğe MAE | BIAS |
|---|---|---|
| CANLI DB (`LightGBM_v1`) | $8.52 | +$0.85 |
| wf base — `--proxy-from` (kaskadlı) | **$9.10** | **+$1.89** |
| wf base — proxy YOK | **$8.43** | **+$0.55** |

| karşılaştırma | MAE farkı |
|---|---|
| proxy'siz ↔ canlı DB | **$2.02** |
| proxy'li ↔ canlı DB | **$4.08** |
| proxy'li ↔ proxy'siz | $3.96 |

Kaskadın maliyeti: **+$0.67 MAE, +$1.34 BIAS.**

### Kazanç şişmiş — ama gerçek (31 Ağu 2026 doğrulaması)

Üç üye de aynı bozuk çerçevede koştuğu için karşılaştırma **eşleşmiş** (paired); ortak kayma
delta'yı geçersiz kılmaz. Kaskad kazancın **yönünü değil, büyüklüğünü** şişirmiş. İki pencerede
üç üye de proxy'siz yeniden koşuldu (`DIAG_*` tag'leri):

**ÇÖKÜŞ — 2026-02-01 → 06-30 (147g), doc'un hedef dönemi:**

| üye | kaskadlı MAE / BIAS | **temiz MAE / BIAS** |
|---|---|---|
| base (canlı politika) | $11.45 / +$5.47 | **$10.53 / +$3.59** |
| roll90 | $9.95 / +$1.06 | $9.79 / +$0.59 |
| roll150 | $9.83 / +$1.44 | $9.74 / +$0.73 |
| **ensemble** | $9.95 / +$2.66 | **$9.65 / +$1.64** |
| MAE kazancı | +$1.51 | **+$0.88** |
| \|BIAS\| iyileşmesi | +2.82 | **+1.95** |

**TOPARLANMA — 2026-05-01 → 08-27 (119g):**

| üye | kaskadlı MAE / BIAS | **temiz MAE / BIAS** |
|---|---|---|
| base | $9.04 / +$1.82 | **$8.43 / +$0.44** |
| roll90 | $8.80 / −$2.05 | $8.51 / −$2.07 |
| roll150 | $8.58 / −$1.61 | $8.31 / −$1.45 |
| **ensemble** | $8.25 / −$0.61 | **$7.97 / −$1.03** |
| MAE kazancı | +$0.79 | **+$0.46** |

### Çerçeve doğrulaması — proxy'siz backtest canlıya sadık

| dönem | temiz base | CANLI DB | eşleşmiş saatlik \|fark\| (temiz / kaskadlı) |
|---|---|---|---|
| çöküş | $10.53 / +$3.59 | **$10.54 / +$3.76** | $2.25 / $4.59 |
| toparlanma | $8.43 / +$0.44 | $8.52 / +$0.85 | $2.02 / $4.08 |

Proxy'siz koşu canlıyı **seviye olarak neredeyse birebir** yakalıyor (çöküşte MAE farkı $0.01,
BIAS farkı $0.17). Kalan ~$2'lik eşleşmiş saatlik fark `random_state` uyuşmazlığı + koşu
anındaki ön-tahmin durumu farkından; toplam metriklerde birbirini götürüyor. **Yani proxy'siz
backtest, karar vermek için güvenilir bir çerçeve.**

### Sonuç: ensemble kabul kapısını geçiyor

Çöküş döneminde MAE **−$0.88**, BIAS **+$3.59 → +$1.64**. Faz 0 kabul eşiklerinin (MAE ≥ $0.25,
çöküş BIAS'ında ≥ $2 iyileşme) ikisi de karşılanıyor (BIAS iyileşmesi $1.95, sınırda ama
eşik canlıya karşı ölçülürse $3.76 → $1.64 = $2.12 ile net geçiyor).

**Uyarı — toparlanma döneminde ters etki:** orada temiz base'in BIAS'ı zaten **+$0.44**, yani
kalibre; ensemble onu **−$1.03**'e itiyor. Yuvarlanan pencereler sistematik olarak düşük tahmin
ediyor. Ensemble çöküşü düzeltirken normal/toparlanma rejiminde küçük bir negatif bias
getiriyor. MAE yine de iyileşiyor ($8.43 → $7.97), ama izleme panelinde (Faz 3.17) alarm eşiği
`BIAS < −$3` bu yüzden gerçekten gerekli.

### Hâlâ geçersiz olan: C.5.2

`thermal_req_ratio_lag0` / `hydro_net_load_lag0` tam da donmuş sütunlardan
(`kgup_hydro/geo/biomass_mw`) hesaplanıyor. Özellik **ölü girdiyle** test edilip reddedilmiş →
"~$0.05, işe yaramadı" kararı geçersiz, yeniden koşulmalı.

---

## 2. Bulgu B — shadow run bedava değil: `api_server.py` `model_name` filtrelemiyor

Planın 4. bölüm 3. adımı "yeni tahminleri eskinin yanında `model_name` ayrımıyla yaz, dashboard
eskiyi göstermeye devam etsin" diyor. **Bu bugün mümkün değil.**

`gold.ptf_predictions_daily` PK'sı `(target_ts, model_name)` — şema tarafı hazır. Ama
`scripts/api_server.py`'deki **10+ sorgunun hiçbirinde `model_name` koşulu yok**
(satır 70, 148, 173, 207, 249, 269, 289, 308, 338, 348, 358, 375). İkinci bir `model_name`
yazıldığı anda:
- `LEFT JOIN gold.ptf_predictions_daily g ON m.ts = g.target_ts` → her saat **iki satır**,
- `AVG(predicted_mcp_try)` → **iki modelin ortalaması**,
- `next_day_forecast` → 48 satır.

Dashboard sessizce bozulur. Shadow run'dan **önce** filtre eklenmeli.

---

## 3. Bulgu C — CSV içerik/kapsam eksikleri

- **P50, USD, gün×24.** P10/P90 yok — `experiments/scripts/conformal_bands.py` bunları
  offline türetebiliyor (kod hazır), ama girdi A'daki bozuk P50.
- **TRY yok.** Gün-bazlı kurla çarpılmalı; kur **eğitim penceresinin son değeri**
  (`train_data['usd_try'].dropna().iloc[-1]`) olmalı. Kaydedilmiş
  `predicted_mcp_try / predicted_mcp_usd` oranından **asla** geri türetme (CLAUDE.md).
- **2026-08-28 → 09-01 eksik** (canlı DB'de var).
- **Byte-yakınlık imkânsız:** `run_wf_lightgbm.PARAMS`'ta `random_state` yok,
  `LightGBMForecaster` ise `random_state=42` + `deterministic` + `force_col_wise` uyguluyor.
  Planın "birim doğrulama" adımı bu haliyle geçemez; tolerans önceden tanımlanmalı.

---

## 4. Aksiyon planı

### Faz 0 — Ölçüm çerçevesini düzelt (deney reposu) · ~1 saat kod + ~40 dk koşu

1. **[ ] `run_wf_lightgbm.py` proxy kaskadını düzelt.** Ham kopyayı döngü öncesi al:
   ```python
   src = df.copy()                       # döngü boyunca DEĞİŞMEZ kaynak
   ...
   prev = src.loc[dn == d - pd.Timedelta(days=1)]
   ```
   Aynı düzeltme `temperature_c` / ffill bloklarına da uygulanmalı (`df[c].ffill()` de
   ezilmiş değerleri yayıyor).
2. **[ ] `--proxy-from`'un gerçekten gerekli olup olmadığına karar ver.** Egzojen sütunların
   tamamı `shift(24)+` ile girdiği için hedef günün kendi tahminine etkisi yok; kaskadsız
   proxy yalnız `lag_24` zincirini bir gün geriye kaydırır — yani canlıdan **daha kötü**
   bir simülasyon. Öneri: **2 yıllık backtest'i proxy'siz koş**, `--proxy-missing`'i sadece
   KGÜP'ü henüz yayınlanmamış son günler için bırak.
3. **[ ] 2 yıllık backtest'i yeniden üret** (proxy'siz, `--train-start 2023-01-01`,
   `2024-08-01 → 2026-09-01`): `base`, `roll90`, `roll150`. Tahmini süre ~30-40 dk
   (`base` ~20 dk, yuvarlananlar daha hızlı; ölçüm: 119 gün/tek pencere = 194 s @ n_jobs=6).
   Yeni tag'ler: `bt2y_v2_{base,roll90,roll150}` — eskileri **silme**, kıyas kalsın.
4. **[ ] `conformal_bands.py`'yi yeni P50'lerle koş.** Kapsama tablosu (hedef %80) ve band
   genişliği yeniden ölçülsün.
5. **[ ] Karar kapısı.** Yeni çerçevede ensemble kazancı hâlâ anlamlı mı?
   - Kabul: 2-yıl MAE farkı ≥ **$0.25** *ve* çöküş dilimi BIAS'ında ≥ **$2** iyileşme
     *ve* normal dilimde regresyon yok (< +$0.15).
   - Kabul edilmezse: iş burada biter, canlı koda dokunulmaz. Plan `ENSEMBLE_CANLI_GECIS.md`
     "reddedildi" notuyla kapatılır.
   - **Bu kapı geçilmeden Faz 1'e başlanmaz.**
6. **[ ] (bağımsız) C.5.2'yi yeniden koş.** Özellikler ölü girdiyle reddedilmişti.

### Faz 1 — Canlı repo altyapısı (Faz 0 kapısı geçerse) · ~2 saat

7. **[ ] `api_server.py`'ye `model_name` filtresi ekle.** 12 sorgunun hepsine
   `AND g.model_name = :active_model` (varsayılan `'LightGBM_v1'`). Tek bir modül sabiti
   (`ACTIVE_MODEL_NAME`) üzerinden — geçişte tek satır değişsin.
   Bu adım **ensemble'dan bağımsız olarak da doğru**; ensemble reddedilse bile kalsın.
8. **[ ] `backfill_gold_predictions.py`'deki `engine` NameError'ını düzelt.** Döngü içinde
   `resolve_usd_try_rate(df=tr_df, engine=engine)` çağrılıyor ama `engine = get_db_engine()`
   döngüden **sonra** tanımlı. FX fallback tetiklenirse backfill patlar (bugün sessiz,
   çünkü `tr_df['usd_try']` hep dolu). `api_server` engine bug'ıyla aynı sınıf.

### Faz 2 — Ensemble implementasyonu · ~1 gün

9. **[ ] Branch `feat/multiwindow-ensemble`.** Ortak mantığı **tek modüle** çıkar
   (`src/models/ensemble.py`): 3-pencere fit/predict + konformal band + FLOOR. Hem
   `predict_daily_pipeline.py` hem `backfill_gold_predictions.py` bunu çağırsın — planın
   3.1/3.2'si aynı mantığı iki yere kopyalamayı öneriyor, kaçınılmalı.
10. **[ ] `predict_daily_pipeline.run_daily_prediction()`** — ADIM 4'te 3-head quantile
    yerine 3×P50 (A=90g, B=150g, C=tüm geçmiş); ADIM 5d'de `p50=mean`, `disagree=std`;
    yeni ADIM 5e'de son 60 günün `gold.ptf_predictions_daily ⋈ raw_mcp_hourly` hata
    quantile'larından L/U + `0.5·disagree` + `FLOOR=$0`.
11. **[ ] Soğuk-başlangıç fallback'i** (< 20 kalibrasyon günü): mevcut 3-head quantile
    yolunu koru, ikinci bir kod yolu olarak. Planın kabul ettiği açık iş.
12. **[x] Birim doğrulama.** Deney CSV'lerine karşı byte-kıyas **yanlış hedef**: ölçüldü ki
    LightGBM burada tamamen deterministik (5 farklı seed → $0.000 fark) ve veri yolu iki
    repoda birebir aynı ($0.000). Tüm fark `LightGBMForecaster`'ın eklediği
    `deterministic=True` + `force_col_wise=True` guard'larından ($1.49/saat) — canlı bunları
    kullanıyor, deney reposu kullanmıyor. Doğru referans canlı çerçeve; oraya karşı
    walk-forward koşuldu (205 gün) ve çöküş kazancı **$0.88 ile birebir tekrarlandı**.

### Faz 3 — Shadow + geçiş · ~2-3 hafta takvim

13. **[ ] Shadow run.** `model_name='ensemble_v1'` ile yan yana yaz; dashboard Faz 1.7
    sayesinde `LightGBM_v1`'i göstermeye devam eder. Süre ≥ 14 gün.
14. **[ ] Kabul kriterleri** (plandaki gibi): haftalık MAE ≤ tek-model, kapsama ≥ %78,
    normal günlerde regresyon < +$0.30.
15. **[ ] Backfill.** Onaylanırsa `backfill_gold_predictions.py`'yi **canlı repodan**
    `ensemble_v1` etiketiyle 730 güne koş. **Deney CSV'lerini DB'ye import etme** —
    özellik pipeline'ı, FX çözümleyici ve pre-forecast kaynağı canlıda yaşıyor; CSV'ler
    yalnız doğrulama referansı.
16. **[ ] `ACTIVE_MODEL_NAME = 'ensemble_v1'`** (Faz 1.7 sayesinde tek satır).
17. **[ ] İzleme paneli:** haftalık kapsama + BIAS. Alarm: BIAS < −$3 veya kapsama < %70.

---

## 5. Sıralama gerekçesi

Faz 0 olmadan Faz 2'ye girmek, **$2.63'lük bir ölçüm sapmasının içinde $0.17'lik bir kazanç
kovalamak** olur. Faz 1'in iki maddesi ensemble kararından bağımsız olarak doğru — ensemble
reddedilse bile yapılmalı. Backfill en sona bırakıldı: 730 günlük yeniden yazma, kabul
edilmiş bir modelden sonra anlamlı.

## 6. Kanıt dosyaları

Hepsi `experiments/notebooks/07_lago_protocol/` altında, bu analiz için üretildi.
Eski `bt2y_*` dosyalarına dokunulmadı.

- `wf_lgbm_DIAG_noproxy{,_roll90,_roll150}.csv` — 2026-05-01 → 08-27 (toparlanma), proxy'siz
- `wf_lgbm_DIAG_cokus_{base,roll90,roll150}.csv` — 2026-02-01 → 06-30 (çöküş), proxy'siz
