-- =============================================================================
-- ENERJİ FİYAT TAHMİNİ PROJESİ - POSTGRESQL VERİ TABANI BAŞLANGIÇ ŞEMASI (01_init_schema.sql)
-- Katmanlar: Bronze (API Ingestion & Audit Track) & Silver (Normalized Time Series)
-- =============================================================================

SET timezone = 'Europe/Istanbul';

-- -----------------------------------------------------------------------------
-- 1. BRONZE KATMANI: API ETL Ingestion Audit & Checksum Takip Tablosu
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingestion_batches (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,    -- Örn: 'mcp', 'smp', 'actual_generation', 'macro'
    period_key VARCHAR(50) NOT NULL,      -- Örn: '2026-07' veya '2026-07-27'
    checksum VARCHAR(64) NOT NULL,        -- EPİAŞ API'sinden gelen bellekteki verinin SHA-256 dijital imzası
    row_count INT DEFAULT 0,              -- Çekilen ve işlenen toplam kayıt sayısı
    status VARCHAR(20) DEFAULT 'SUCCESS', -- 'SUCCESS', 'FAILED', 'SKIPPED'
    error_message TEXT NULL,
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_source_checksum UNIQUE (source_name, checksum)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_source_period ON ingestion_batches(source_name, period_key);

-- -----------------------------------------------------------------------------
-- 2. SILVER KATMANI: Saatlik / Günlük Ham Zaman Serisi Tabloları
-- -----------------------------------------------------------------------------

-- 2.1 Piyasa Takas Fiyatı (PTF - MCP)
CREATE TABLE IF NOT EXISTS raw_mcp_hourly (
    ts TIMESTAMPTZ PRIMARY KEY,
    price_try NUMERIC(12, 4),
    price_usd NUMERIC(12, 4),
    price_eur NUMERIC(12, 4),
    ingestion_id INT REFERENCES ingestion_batches(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2.2 Sistem Marjinal Fiyatı (SMF - SMP)
CREATE TABLE IF NOT EXISTS raw_smp_hourly (
    ts TIMESTAMPTZ PRIMARY KEY,
    system_marginal_price_try NUMERIC(12, 4),
    ingestion_id INT REFERENCES ingestion_batches(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2.3 Yük Tahmini (LEP)
CREATE TABLE IF NOT EXISTS raw_load_forecast_hourly (
    ts TIMESTAMPTZ PRIMARY KEY,
    load_forecast_mw NUMERIC(12, 2),
    ingestion_id INT REFERENCES ingestion_batches(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2.4 Kesinleşmiş Gün Öncesi Üretim Planı (KGÜP)
CREATE TABLE IF NOT EXISTS raw_kgup_hourly (
    ts TIMESTAMPTZ PRIMARY KEY,
    total_mw NUMERIC(12, 2),
    natural_gas_mw NUMERIC(12, 2) DEFAULT 0,
    wind_mw NUMERIC(12, 2) DEFAULT 0,
    lignite_mw NUMERIC(12, 2) DEFAULT 0,
    black_coal_mw NUMERIC(12, 2) DEFAULT 0,
    import_coal_mw NUMERIC(12, 2) DEFAULT 0,
    fuel_oil_mw NUMERIC(12, 2) DEFAULT 0,
    geothermal_mw NUMERIC(12, 2) DEFAULT 0,
    dammed_hydro_mw NUMERIC(12, 2) DEFAULT 0,
    river_hydro_mw NUMERIC(12, 2) DEFAULT 0,
    naphtha_mw NUMERIC(12, 2) DEFAULT 0,
    biomass_mw NUMERIC(12, 2) DEFAULT 0,
    solar_mw NUMERIC(12, 2) DEFAULT 0,
    other_mw NUMERIC(12, 2) DEFAULT 0,
    ingestion_id INT REFERENCES ingestion_batches(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2.5 Gerçekleşen Üretim (Kaynak Bazlı)
CREATE TABLE IF NOT EXISTS raw_actual_generation_hourly (
    ts TIMESTAMPTZ PRIMARY KEY,
    total_mw NUMERIC(12, 2),
    natural_gas_mw NUMERIC(12, 2) DEFAULT 0,
    dammed_hydro_mw NUMERIC(12, 2) DEFAULT 0,
    lignite_mw NUMERIC(12, 2) DEFAULT 0,
    river_hydro_mw NUMERIC(12, 2) DEFAULT 0,
    import_coal_mw NUMERIC(12, 2) DEFAULT 0,
    wind_mw NUMERIC(12, 2) DEFAULT 0,
    solar_mw NUMERIC(12, 2) DEFAULT 0,
    fuel_oil_mw NUMERIC(12, 2) DEFAULT 0,
    geothermal_mw NUMERIC(12, 2) DEFAULT 0,
    asphaltite_coal_mw NUMERIC(12, 2) DEFAULT 0,
    black_coal_mw NUMERIC(12, 2) DEFAULT 0,
    biomass_mw NUMERIC(12, 2) DEFAULT 0,
    naphtha_mw NUMERIC(12, 2) DEFAULT 0,
    lng_mw NUMERIC(12, 2) DEFAULT 0,
    import_export_mw NUMERIC(12, 2) DEFAULT 0,
    waste_heat_mw NUMERIC(12, 2) DEFAULT 0,
    ingestion_id INT REFERENCES ingestion_batches(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2.6 Gerçekleşen Tüketim
CREATE TABLE IF NOT EXISTS raw_actual_consumption_hourly (
    ts TIMESTAMPTZ PRIMARY KEY,
    consumption_mw NUMERIC(12, 2),
    ingestion_id INT REFERENCES ingestion_batches(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2.7 GÖP Alış / Satış Teklif Miktarları
CREATE TABLE IF NOT EXISTS raw_bids_offers_hourly (
    ts TIMESTAMPTZ PRIMARY KEY,
    bid_quantity_mw NUMERIC(12, 2),
    offer_quantity_mw NUMERIC(12, 2),
    ingestion_id INT REFERENCES ingestion_batches(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2.8 Makro Göstergeler (Dolar & Brent Petrol)
CREATE TABLE IF NOT EXISTS raw_macro_daily (
    entry_date DATE PRIMARY KEY,
    usd_try NUMERIC(10, 4),
    brent_oil_usd NUMERIC(10, 4),
    ingestion_id INT REFERENCES ingestion_batches(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2.9 Doğalgaz Günlük Referans Fiyatı (GRF)
CREATE TABLE IF NOT EXISTS raw_natural_gas_daily (
    entry_date DATE PRIMARY KEY,
    gas_reference_price_try NUMERIC(12, 4),
    grf_usd NUMERIC(12, 4),
    grf_eur NUMERIC(12, 4),
    ingestion_id INT REFERENCES ingestion_batches(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2.10 Lisanslı Gerçekleşen Üretim (Kaynak Bazlı - Yenilenebilir ve Diğer)
CREATE TABLE IF NOT EXISTS raw_licensed_realtime_generation_hourly (
    ts TIMESTAMPTZ PRIMARY KEY,
    total_mw NUMERIC(12, 2),
    wind_mw NUMERIC(12, 2) DEFAULT 0,
    geothermal_mw NUMERIC(12, 2) DEFAULT 0,
    dammed_hydro_mw NUMERIC(12, 2) DEFAULT 0,
    canal_hydro_mw NUMERIC(12, 2) DEFAULT 0,
    river_hydro_mw NUMERIC(12, 2) DEFAULT 0,
    landfill_gas_mw NUMERIC(12, 2) DEFAULT 0,
    biogas_mw NUMERIC(12, 2) DEFAULT 0,
    solar_mw NUMERIC(12, 2) DEFAULT 0,
    biomass_mw NUMERIC(12, 2) DEFAULT 0,
    other_mw NUMERIC(12, 2) DEFAULT 0,
    ingestion_id INT REFERENCES ingestion_batches(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2.11 Günlük / Periyodik Kurulu Güç Dağılımı
CREATE TABLE IF NOT EXISTS raw_installed_capacity_daily (
    period_date DATE NOT NULL,
    energy_type VARCHAR(50) NOT NULL,
    licensed_capacity_mw NUMERIC(12, 4) DEFAULT 0,
    unlicensed_capacity_mw NUMERIC(12, 4) DEFAULT 0,
    total_capacity_mw NUMERIC(12, 4) DEFAULT 0,
    ingestion_id INT REFERENCES ingestion_batches(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT pk_installed_capacity PRIMARY KEY (period_date, energy_type)
);

-- 2.12 Türkiye Ağırlıklı Saatlik Sıcaklık (Open-Meteo Gerçekleşen Hava Durumu)
CREATE TABLE IF NOT EXISTS raw_weather_hourly (
    ts TIMESTAMPTZ PRIMARY KEY,
    turkey_weighted_temperature_c NUMERIC(5, 2),
    ingestion_id INT REFERENCES ingestion_batches(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2.12b Türkiye Ağırlıklı Saatlik Sıcaklık Tahminleri (Open-Meteo Weather Forecast)
CREATE TABLE IF NOT EXISTS raw_weather_forecast_hourly (
    ts TIMESTAMPTZ PRIMARY KEY,
    turkey_weighted_temperature_forecast_c NUMERIC(5, 2),
    forecast_run_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 2.13 Baraj Aktif Doluluk Oranları (Master Snapshot)
CREATE TABLE IF NOT EXISTS raw_master_active_fullness (
    dam_id INT NOT NULL,
    date_time TIMESTAMPTZ NOT NULL,
    record_id BIGINT,
    basin_name VARCHAR(100) NOT NULL,
    dam_name VARCHAR(100) NOT NULL,
    active_fullness_percent NUMERIC(8, 4) DEFAULT 0,
    ingestion_id INT REFERENCES ingestion_batches(id) ON DELETE SET NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT pk_dam_active_fullness PRIMARY KEY (dam_id, date_time)
);

-- 2.14 Baraj Su Enerji Karşılığı (Master Snapshot)
CREATE TABLE IF NOT EXISTS raw_master_water_energy_provision (
    id SERIAL PRIMARY KEY,
    date_time TIMESTAMPTZ,
    dam_name VARCHAR(100) NOT NULL,
    basin_name VARCHAR(100),
    water_energy_provision_mwh NUMERIC(14, 4),
    ingestion_id INT REFERENCES ingestion_batches(id) ON DELETE SET NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_dam_provision_date UNIQUE (dam_name, date_time)
);

-- -----------------------------------------------------------------------------
-- İNDEKS TASARIMLARI
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_mcp_ts ON raw_mcp_hourly(ts);
CREATE INDEX IF NOT EXISTS idx_smp_ts ON raw_smp_hourly(ts);
CREATE INDEX IF NOT EXISTS idx_load_forecast_ts ON raw_load_forecast_hourly(ts);
CREATE INDEX IF NOT EXISTS idx_kgup_ts ON raw_kgup_hourly(ts);
CREATE INDEX IF NOT EXISTS idx_actual_gen_ts ON raw_actual_generation_hourly(ts);
CREATE INDEX IF NOT EXISTS idx_actual_cons_ts ON raw_actual_consumption_hourly(ts);
CREATE INDEX IF NOT EXISTS idx_weather_ts ON raw_weather_hourly(ts);

-- -----------------------------------------------------------------------------
-- 3. GOLD LAYER TABLES
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.ptf_predictions_daily (
    target_ts TIMESTAMPTZ,
    predicted_mcp_usd NUMERIC(10, 4),
    predicted_mcp_try NUMERIC(10, 4),
    predicted_mcp_usd_p10 NUMERIC(10, 4),
    predicted_mcp_try_p10 NUMERIC(10, 4),
    predicted_mcp_usd_p90 NUMERIC(10, 4),
    predicted_mcp_try_p90 NUMERIC(10, 4),
    model_name VARCHAR(50) DEFAULT 'LightGBM_v1',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (target_ts, model_name)
);

CREATE TABLE IF NOT EXISTS gold.kgup_load_pre_forecasts (
    target_ts TIMESTAMPTZ PRIMARY KEY,
    predicted_load_lag0 NUMERIC(10, 4),
    predicted_solar_lag0 NUMERIC(10, 4),
    predicted_wind_lag0 NUMERIC(10, 4),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- target_ts index'i BENZERSİZ DEĞİLDİR ve öyle olmamalıdır. Tablonun PK'sı
-- (target_ts, model_name): aynı saat için birden fazla model (ör. shadow run'da
-- LightGBM_v1 + ensemble_v1) yan yana durabilmeli. Canlı DB'de bir noktada elle
-- yaratılmış `idx_gold_ptf_predictions_daily_target_ts` adlı BENZERSİZ bir index
-- bunu engelliyordu; bkz. migrations/02_drop_orphan_unique_index.sql.
-- Bu sütuna UNIQUE index EKLEME.
CREATE INDEX IF NOT EXISTS idx_gold_predictions_target_ts ON gold.ptf_predictions_daily(target_ts);


-- -----------------------------------------------------------------------------
-- 4. BRONZE KATMANI (HABER ARŞİVİ) — Kriz & Olay İstihbarat Sistemi
--    Bkz. CRISIS_ANALYSIS_PLAN.md. Bu katman HAM'dır ve değiştirilmez:
--    türetilmiş her şey (alaka filtresi, LLM etiketleri) silver/gold'a yazılır.
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.news_raw (
    article_id   BIGINT PRIMARY KEY,           -- URL'deki sıralı ID: ...-41334h.htm -> 41334
    url          TEXT NOT NULL UNIQUE,
    source       VARCHAR(50) NOT NULL DEFAULT 'enerjigunlugu',
    published_at TIMESTAMPTZ NOT NULL,         -- itemprop=datePublished — YETKİLİ yayın anı
    modified_at  TIMESTAMPTZ,                  -- itemprop=dateModified
    section      VARCHAR(100),                 -- itemprop=articleSection (Elektrik/Doğalgaz/Mevzuat/...)
    title        TEXT NOT NULL,                -- itemprop=headline
    description  TEXT,                         -- itemprop=description (spot)
    body         TEXT NOT NULL,                -- itemprop=articleBody, boşluk normalize
    body_chars   INT NOT NULL,
    keywords     TEXT[],                       -- itemprop=keywords — editör etiketleri, kural filtresini besler
    author       VARCHAR(200),
    content_hash CHAR(64) NOT NULL,            -- SHA-256(title|body) — mükerrer içerik tespiti
    http_status  SMALLINT,
    fetched_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_published_at ON bronze.news_raw(published_at);
CREATE INDEX IF NOT EXISTS idx_news_section      ON bronze.news_raw(section);
CREATE INDEX IF NOT EXISTS idx_news_hash         ON bronze.news_raw(content_hash);
CREATE INDEX IF NOT EXISTS idx_news_keywords     ON bronze.news_raw USING GIN(keywords);
-- Tam metin arama: LLM hiç çalışmasa bile anahtar kelimeyle vaka çalışması yapılabilsin.
-- 'simple' konfigürasyonu bilinçli: PostgreSQL'de Türkçe stemmer yok, yanlış kök bulmaktansa hiç bulma.
CREATE INDEX IF NOT EXISTS idx_news_fts ON bronze.news_raw
    USING GIN(to_tsvector('simple', coalesce(title,'') || ' ' || coalesce(body,'')));

-- Başarısız çekimler: hedefli yeniden deneme için. Başarılı çekimde satır silinir.
CREATE TABLE IF NOT EXISTS bronze.news_fetch_errors (
    url         TEXT PRIMARY KEY,
    article_id  BIGINT,
    http_status SMALLINT,
    error       TEXT,
    attempts    INT DEFAULT 1,
    last_try_at TIMESTAMPTZ DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- 5. SILVER: RESMÎ AZAMİ FİYAT LİMİTİ (TAVAN) SERİSİ
--    Kaynak: EPİAŞ/EPDK duyurularının haber arşivindeki karşılıkları
--    (bronze.news_raw). Her satırın provenance'ı var.
--
--    NEDEN GEREKLİ: Tavan daha önce aylık maksimumdan İSTATİSTİKLE çıkarılıyordu.
--    O yöntem 23 ayda birebir tuttu ama iki yerde sessizce yanlıştı:
--      (a) tavan ay ortasında değişebiliyor (2021-10-15, 2022-05-19,
--          2025-04-05, 2026-04-04) — aylık maksimum düşük olanı hiç görmüyor;
--      (b) fiyat tavana hiç değmediği aylarda maksimum tavan DEĞİL
--          (Şubat 2021: resmî 572, veri maksimumu 335).
--    Analiz modeli sansürsüz saatlerde eğitileceği için doğru maske şart.
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.price_cap_official (
    effective_from    DATE PRIMARY KEY,        -- yürürlük başlangıcı (dahil)
    cap_try           NUMERIC(12,2) NOT NULL,  -- TL/MWh, GÖP ve DGP'ye birlikte uygulanır
    source_article_id BIGINT,                  -- bronze.news_raw provenance
    note              TEXT,
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Herhangi bir saat için geçerli tavan + tavanda mı bayrağı.
-- Analiz modelinin eğitim maskesi bu görünümden gelir.
CREATE OR REPLACE VIEW silver.mcp_with_cap AS
SELECT m.ts, m.price_try, m.price_usd,
       c.cap_try, c.effective_from AS cap_effective_from,
       (c.cap_try IS NOT NULL AND m.price_try >= c.cap_try * 0.999) AS at_cap
FROM public.raw_mcp_hourly m
LEFT JOIN LATERAL (
    SELECT p.cap_try, p.effective_from FROM silver.price_cap_official p
    WHERE p.effective_from <= (m.ts AT TIME ZONE 'Europe/Istanbul')::date
    ORDER BY p.effective_from DESC LIMIT 1
) c ON TRUE;

-- Resmî tavan serisi — haber arşivinden derlendi, 67 ayın 67'sinde doğrulandı.
-- Yeniden derlemek için: CRISIS_CASE_IRAN_2022.md §3.2b
INSERT INTO silver.price_cap_official (effective_from, cap_try, source_article_id, note) VALUES
    ('2021-02-01', 572, 41077, NULL),
    ('2021-03-01', 569, 41563, NULL),
    ('2021-04-01', 567, 42050, NULL),
    ('2021-05-01', 578, 42445, 'haber "11 TL arttı" diyor: 567+11'),
    ('2021-06-01', 595, 42974, NULL),
    ('2021-07-01', 617, 43403, NULL),
    ('2021-08-01', 636, 43863, NULL),
    ('2021-09-01', 674, 44243, NULL),
    ('2021-10-01', 718, 44641, NULL),
    ('2021-10-15', 1078, 44932, 'AY ORTASI değişiklik; haber 728 TL diyor ama 44641 Ekim tavanını 718 vermişti'),
    ('2021-11-01', 1131, 45102, NULL),
    ('2021-12-01', 1217, 45589, NULL),
    ('2022-01-01', 1345, 46117, NULL),
    ('2022-02-01', 1524, 46596, NULL),
    ('2022-03-01', 1745, 47113, NULL),
    ('2022-04-01', 2500, 47685, 'kaynak bazlı teklif tavanı: gaz/ithal kömür 2,5 TL/kWh, diğer 1,2 TL/kWh. PTF tek fiyat olduğu için etkin tavan 2500; veride 1200 kümesi YOK (doğrulandı)'),
    ('2022-05-19', 2750, 48414, 'AY ORTASI değişiklik'),
    ('2022-06-01', 3200, 48558, NULL),
    ('2022-07-01', 3750, 49021, NULL),
    ('2022-08-01', 4000, 49427, NULL),
    ('2022-09-01', 4800, 49974, NULL),
    ('2023-01-01', 4200, 52175, NULL),
    ('2023-02-01', 3650, 52617, 'EPDK 26 Ocak 2023 duyurusu: 4.200 -> 3.650, Subat''tan itibaren. Kacirilmis DUSUS: fiyat tavani asmadigi icin oz-test bunu yakalamiyordu.'),
    ('2023-03-01', 3050, 53075, NULL),
    ('2023-04-01', 2600, 53499, NULL),
    ('2023-07-04', 2700, 54712, 'EPDK kararı, haber 2023-07-04'),
    ('2024-07-01', 3000, 59281, NULL),
    ('2025-04-05', 3400, 62872, 'AY ORTASI değişiklik'),
    ('2026-04-04', 4500, 67710, 'AY ORTASI değişiklik, +%32,4')
ON CONFLICT (effective_from) DO NOTHING;

-- -----------------------------------------------------------------------------
-- 6. GOLD: KRİZ KONTRAFAKTÜELİ (analiz modeli çıktısı)
--    Üreten: scripts/build_analysis_model.py  →  src/crisis/analysis_model.py
--
--    CANLIYA GİRMEZ. gold.ptf_predictions_daily ile karıştırma:
--    orası canlı fiyat tahmini (2023'ten eğitilir, T+1'i tahmin eder);
--    burası geçmişe dönük açıklayıcı model (2021'den eğitilir, sadece
--    sansürsüz saatlerde, o saatin gerçekleşen fundamentallerini görerek).
--
--    residual_usd = actual_usd - counterfactual_usd
--      = "bilinen fundamentaller verildiğinde fiyatın AÇIKLANAMAYAN kısmı".
--    Etki analizinin (CRISIS_ANALYSIS_PLAN.md §8 adım 7) girdisi budur.
--
--    is_lower_bound: tavandaki saatler için TRUE. Model eğitimde tavan üstü
--    fiyat hiç görmediği için ağaç oraya tahmin ÜRETEMEZ — o saatlerde
--    kontrafaktüel de kalıntı da ALT SINIRDIR. Yöntemin düzeltilebilir bir
--    hatası değil, doğasında olan kısıt; raporda öyle sunulmalı.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.crisis_counterfactual (
    ts                 TIMESTAMPTZ    NOT NULL,
    variant            VARCHAR(20)    NOT NULL,  -- fundamental | autoregressive
    model_name         VARCHAR(50)    NOT NULL DEFAULT 'crisis_cf_v1',
    actual_usd         NUMERIC(10,4),
    counterfactual_usd NUMERIC(10,4),
    residual_usd       NUMERIC(10,4),
    at_cap             BOOLEAN,
    is_lower_bound     BOOLEAN,
    fold_id            INT,                      -- bloklu OOF şemasındaki blok no
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ts, variant, model_name)
);

CREATE INDEX IF NOT EXISTS idx_crisis_cf_ts ON gold.crisis_counterfactual(ts);

-- -----------------------------------------------------------------------------
-- 7. SILVER: BOTAŞ ELEKTRİK ÜRETİM AMAÇLI DOĞAL GAZ TARİFESİ
--    Kaynak: BOTAŞ resmî tarife PDF'leri + haber arşivi (bronze.news_raw) zinciri
--
--    NEDEN GEREKLİ: Modele beslediğimiz natural_gas_grf_try, EPİAŞ'ın gaz piyasası
--    REFERANS fiyatı. Santrallerin fiilen ödediği ise BOTAŞ'ın idari tarifesi.
--    Normal aylarda ikisi yakın (oran 0,94-1,10) ve model sorunsuz çalışıyor.
--    İki dönemde ayrışıyorlar ve analiz modelinin kalıntısı tam o iki dönemde bozuluyor:
--      Ara 2021 - Mar 2022 : oran 0,55-0,77 (BOTAŞ piyasanın altında sattı) -> kalıntı NEGATİF
--      Kas 2022 - Mar 2023 : oran 1,01-1,28 (piyasa düştü, tarife inmedi)   -> kalıntı POZİTİF
--    Tarife/GRF oranı ile aylık kalıntı korelasyonu 0,852 (n=17 ay).
--    Ayrıntı: experiments/notebooks/05_crisis_analysis/03_gas_tariff_discovery.ipynb
--
--    BİRİM: TL / 1000 Sm3. price_try_kwh PDF'te doğrudan verilir, türetilmez.
--    Isıl değer 9155 Kcal/Sm3 (= 10,646 kWh) -> kwh/sm3 oranı 0,09393 sabittir;
--    bu, PDF ayrıştırıcısının sessiz hata yapmadığının testidir.
--
--    effective_from TARİH, ay değil: ay ortası yürürlükler gerçek
--    (5 Nisan 2025, 2 Temmuz 2025, 4 Nisan 2026). Aylık varsaymak
--    silver.price_cap_official'da Ekim 2021'i 3 kat yanlış ölçtürmüştü.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.gas_tariff_electricity (
    effective_from   DATE PRIMARY KEY,
    price_try_1000m3 NUMERIC(14,4) NOT NULL,  -- TL / 1000 Sm3
    price_try_kwh    NUMERIC(12,8),           -- PDF'ten gelir, yoksa NULL
    source           VARCHAR(16) NOT NULL,    -- botas_pdf | news_absolute | news_chain
    source_ref       TEXT,                    -- PDF dosya adı veya bronze.news_raw.article_id
    is_gap           BOOLEAN DEFAULT FALSE,   -- değeri bilinmiyor, önceki taşınıyor
    note             TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Herhangi bir saat için yürürlükteki tarife. silver.mcp_with_cap ile aynı desen.
CREATE OR REPLACE VIEW silver.gas_cost_hourly AS
SELECT m.ts,
       t.price_try_1000m3,
       t.price_try_kwh,
       t.effective_from AS tariff_effective_from,
       t.is_gap        AS tariff_is_gap
FROM public.raw_mcp_hourly m
LEFT JOIN LATERAL (
    SELECT g.price_try_1000m3, g.price_try_kwh, g.effective_from, g.is_gap
    FROM silver.gas_tariff_electricity g
    WHERE g.effective_from <= (m.ts AT TIME ZONE 'Europe/Istanbul')::date
    ORDER BY g.effective_from DESC LIMIT 1
) t ON TRUE;

-- Seri. Çıpalar resmî PDF veya haber gövdesindeki mutlak değer; aradaki aylar
-- duyurulan yüzdelerle zincirlenir. Zincirin kapanış testleri (hepsi tuttu):
--   Şub 2022 : zincir 6.298 -> resmî PDF 6.300            (%0,03)
--   Eyl 2022 : 13.750 x 1,50 = 20.625 -> Kas/Ara çıpası    (tam; haberdeki %49,5 yuvarlanmış)
--   Nis 2022 : 7.450 x 1,443 = 10.750 -> resmî PDF Mayıs   (tam)
--   Oca 2023 : 20.625 x 0,8727 = 18.000 -> resmî PDF       (tam)
INSERT INTO silver.gas_tariff_electricity
    (effective_from, price_try_1000m3, price_try_kwh, source, source_ref, is_gap, note) VALUES
    ('2021-01-01',  1414.00, NULL, 'news_absolute', '40682', FALSE, NULL),
    ('2021-02-01',  1428.14, NULL, 'news_chain',    '41158', FALSE, '+%1'),
    ('2021-03-01',  1442.42, NULL, 'news_chain',    '41610', FALSE, '+%1'),
    ('2021-04-01',  1456.85, NULL, 'news_chain',    '42129', FALSE, '+%1'),
    ('2021-05-01',  1631.67, NULL, 'news_absolute', '42577', FALSE, '+%12'),
    ('2021-06-01',  1713.25, NULL, 'news_absolute', '43026', FALSE, '+%5'),
    ('2021-07-01',  2055.90, NULL, 'news_chain',    '43481', FALSE, '+%20'),
    ('2021-08-01',  2060.00, NULL, 'news_absolute', '44348', FALSE, 'Eylul haberi Agustos degerini veriyor'),
    ('2021-09-01',  2369.00, NULL, 'news_chain',    '44348', FALSE, '+%15'),
    ('2021-10-01',  2724.35, NULL, 'news_chain',    '44725', FALSE, '+%15'),
    ('2021-11-01',  3999.34, NULL, 'news_chain',    '45165', FALSE, '+%46,8'),
    ('2021-12-01',  4800.00, NULL, 'news_absolute', '45690', FALSE, 'haber "Kasim tarifesi" diyor ama 4800 Aralik degeri: 3999x1,20=4799'),
    ('2022-01-01',  5520.00, NULL, 'news_chain',    '46182', FALSE, '+%15'),
    ('2022-02-01',  6300.00, 0.59210526, 'botas_pdf', '10212-ubat_2022_tarifesi.pdf', FALSE, 'zincir 6298 -> resmi 6300, %0,03 sapma'),
    ('2022-03-01',  7450.00, NULL, 'news_absolute', '47185', FALSE, '7,45 TL/Sm3, +%18,3'),
    ('2022-04-01', 10750.00, NULL, 'news_chain',    '47723', FALSE, '+%44,30; Mayis PDF ile birebir kapaniyor'),
    ('2022-05-01', 10750.00, 1.01033835, 'botas_pdf', '827613-mayis_2022_tarifesi.pdf', FALSE, 'degismedi (48210)'),
    ('2022-06-01', 12524.83, NULL, 'news_chain',    '48595', FALSE, '+%16,51'),
    ('2022-07-01', 12524.83, NULL, 'news_chain',    '49041', FALSE, 'degismedi'),
    ('2022-08-01', 13750.00, 1.29229323, 'botas_pdf', '949372-agustos_2022_tarifesi.pdf', FALSE, 'haber +%10 diyor, resmi deger 13750'),
    ('2022-09-01', 20625.00, NULL, 'news_chain',    '49973', FALSE, 'haber +%49,5 diyor; 13750x1,50=20625 Kas/Ara cipasiyla tam kapaniyor'),
    ('2022-11-01', 20625.00, NULL, 'news_absolute', '51622', FALSE, 'Kasim = Aralik, degismedi'),
    ('2023-01-01', 18000.00, 1.69172932, 'botas_pdf', '704191-ocak_2023_tarifesi.pdf', FALSE, '-%12,73'),
    ('2023-02-01', 15000.00, NULL, 'news_chain',    '52626', FALSE, '-%16,67'),
    ('2023-03-01', 12000.00, NULL, 'news_chain',    '53067', FALSE, '-%20. DIKKAT: haber basligindaki %26,12 SANAYI icin'),
    ('2023-04-01', 10000.00, NULL, 'news_absolute', '53897', FALSE, 'Nisan indirimi (53500); Mayis haberi 10 bin lira diyor'),
    ('2023-10-01', 12000.00, NULL, 'news_chain',    '55873', FALSE, '+%20'),
    ('2025-04-05', 14904.00, NULL, 'news_chain',    '62869', FALSE, 'haber 62869 govdesinin sonunda: elektrik uretim santralleri icin ortalama %24,2 artis, 5 Nisan 2025 itibariyle. 12000 x 1,242 = 14904. Temmuz resmi cipasi 15000 -> kalan belirsizlik %0,6. ILK DERLEMEDE KACIRILDI: haberin basligi elektrik perakende zammiyla ilgili, gaz tarifesi govdenin sonunda; baslik/bolum filtresi yetmiyor.'),
    ('2025-07-02', 15000.00, 1.40977444, 'botas_pdf', '128513-2_temmuz_2025_tarifesi.pdf', FALSE, NULL),
    ('2026-04-04', 18000.00, 1.69172932, 'botas_pdf', '139364-4-nisan_2026_tarife.pdf', FALSE, NULL)
ON CONFLICT (effective_from) DO NOTHING;
