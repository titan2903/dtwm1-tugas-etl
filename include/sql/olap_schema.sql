-- =============================================================================
-- Purpose : OLAP Star Schema DDL — jalankan di database OLAP sebelum DAG.
-- Schema  : marts  (sudah dibuat oleh init-db.sql)
-- Pattern : Hybrid Star/Snowflake — dim_product → dim_category (level-1 flake)
--           Semua fact table juga menyimpan category_sk langsung untuk
--           performa query agregasi per kategori tanpa join ke dim_product.
-- =============================================================================

SET search_path TO marts, public;

-- ─────────────────────────────────────────────────────────────────────────────
-- DIMENSIONS
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- dim_date
-- Tabel kalender — satu baris per hari.
-- date_id menggunakan integer format YYYYMMDD sebagai surrogate key alami
-- sehingga FK dari tabel fakta bisa diquery langsung tanpa join untuk filter
-- tahun/bulan sederhana (misal: WHERE order_date_id BETWEEN 20240101 AND 20241231).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS marts.dim_date (
    date_id       INTEGER      NOT NULL,   -- YYYYMMDD, misal 20240115
    full_date     DATE         NOT NULL,
    day_of_week   SMALLINT     NOT NULL,   -- 0 = Minggu … 6 = Sabtu (ISO: 1=Sen)
    day_name      VARCHAR(10)  NOT NULL,   -- 'Monday', 'Tuesday', …
    day_of_month  SMALLINT     NOT NULL,   -- 1 – 31
    week_of_year  SMALLINT     NOT NULL,   -- ISO week 1 – 53
    month_num     SMALLINT     NOT NULL,   -- 1 – 12
    month_name    VARCHAR(10)  NOT NULL,   -- 'January', …
    quarter       SMALLINT     NOT NULL,   -- 1 – 4
    year          SMALLINT     NOT NULL,
    is_weekend    BOOLEAN      NOT NULL    DEFAULT FALSE,

    CONSTRAINT pk_dim_date PRIMARY KEY (date_id)
);

COMMENT ON TABLE  marts.dim_date IS 'Tabel kalender — diisi satu baris per hari (minimal tahun 2024).';
COMMENT ON COLUMN marts.dim_date.date_id IS 'Surrogate key integer format YYYYMMDD.';


-- -----------------------------------------------------------------------------
-- dim_category
-- Natural key : category_id (dari OLTP tabel categories)
-- Surrogate   : category_sk (serial, stabil walaupun OLTP berubah)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS marts.dim_category (
    category_sk   SERIAL       NOT NULL,
    category_id   INTEGER      NOT NULL,   -- natural key dari OLTP
    name          VARCHAR(100) NOT NULL,
    description   TEXT,

    CONSTRAINT pk_dim_category      PRIMARY KEY (category_sk),
    CONSTRAINT uq_dim_category_nk   UNIQUE      (category_id)
);

COMMENT ON TABLE marts.dim_category IS 'Dimensi kategori produk — sumber oltp_categories.csv.';


-- -----------------------------------------------------------------------------
-- dim_product
-- FK ke dim_category (level-1 snowflake) karena atribut kategori lebih tepat
-- dikelola terpisah daripada direplikasi per produk.
-- Natural key : product_id
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS marts.dim_product (
    product_sk    SERIAL        NOT NULL,
    product_id    INTEGER       NOT NULL,   -- natural key dari OLTP
    category_sk   INTEGER       NOT NULL,
    name          VARCHAR(200)  NOT NULL,
    sku           VARCHAR(50),
    price         NUMERIC(12,2) NOT NULL,
    cost_price    NUMERIC(12,2) NOT NULL,
    is_active     BOOLEAN       NOT NULL    DEFAULT TRUE,

    CONSTRAINT pk_dim_product     PRIMARY KEY (product_sk),
    CONSTRAINT uq_dim_product_nk  UNIQUE      (product_id),
    CONSTRAINT fk_dim_product_cat FOREIGN KEY (category_sk)
        REFERENCES marts.dim_category (category_sk)
        ON UPDATE CASCADE ON DELETE RESTRICT
);

COMMENT ON TABLE marts.dim_product IS 'Dimensi produk — sumber oltp_products.csv. FK ke dim_category (snowflake level 1).';


-- -----------------------------------------------------------------------------
-- dim_customer
-- Natural key : customer_id
-- Atribut geografis (city, province) digunakan untuk segmentasi pelanggan.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS marts.dim_customer (
    customer_sk   SERIAL       NOT NULL,
    customer_id   INTEGER      NOT NULL,   -- natural key dari OLTP
    full_name     VARCHAR(200) NOT NULL,
    email         VARCHAR(200),
    city          VARCHAR(100),
    province      VARCHAR(100),

    CONSTRAINT pk_dim_customer    PRIMARY KEY (customer_sk),
    CONSTRAINT uq_dim_customer_nk UNIQUE      (customer_id)
);

COMMENT ON TABLE marts.dim_customer IS 'Dimensi pelanggan — sumber oltp_customers.csv.';


-- -----------------------------------------------------------------------------
-- dim_payment_method
-- Data statis — 4 metode yang dikenal sistem:
--   bank_transfer | ewallet | cod | credit_card
-- Natural key : method_code (varchar, lebih deskriptif dari integer ID)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS marts.dim_payment_method (
    method_sk     SERIAL       NOT NULL,
    method_code   VARCHAR(50)  NOT NULL,   -- natural key, misal 'ewallet'
    method_name   VARCHAR(100) NOT NULL,
    is_digital    BOOLEAN      NOT NULL    DEFAULT FALSE,

    CONSTRAINT pk_dim_payment_method    PRIMARY KEY (method_sk),
    CONSTRAINT uq_dim_payment_method_nk UNIQUE      (method_code)
);

COMMENT ON TABLE marts.dim_payment_method IS 'Dimensi metode pembayaran — data statis 4 metode.';


-- -----------------------------------------------------------------------------
-- dim_campaign
-- Sumber : promotions.csv (external storage / API)
-- Natural key : campaign_id
-- category_id / category_name bisa NULL untuk kampanye lintas kategori
-- (target_scope = 'all' atau 'product').
-- is_active digunakan langsung dalam query analitik Pertanyaan 3.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS marts.dim_campaign (
    campaign_sk       SERIAL        NOT NULL,
    campaign_id       INTEGER       NOT NULL,   -- natural key dari promotions.csv
    campaign_name     VARCHAR(200)  NOT NULL,
    category_id       INTEGER,                  -- NULL = lintas kategori
    category_name     VARCHAR(100),
    target_scope      VARCHAR(50),              -- 'category' | 'product' | 'all'
    discount_pct      NUMERIC(5,2),
    min_order_amount  NUMERIC(12,2),
    budget_idr        NUMERIC(15,2),
    start_date        DATE,
    end_date          DATE,
    channel           VARCHAR(100),
    is_active         BOOLEAN       NOT NULL    DEFAULT FALSE,

    CONSTRAINT pk_dim_campaign    PRIMARY KEY (campaign_sk),
    CONSTRAINT uq_dim_campaign_nk UNIQUE      (campaign_id)
);

COMMENT ON TABLE  marts.dim_campaign IS 'Dimensi kampanye marketing — sumber promotions.csv (external).';
COMMENT ON COLUMN marts.dim_campaign.is_active IS 'TRUE jika kampanye masih berjalan pada saat ETL dijalankan.';


-- ─────────────────────────────────────────────────────────────────────────────
-- FACTS
-- ─────────────────────────────────────────────────────────────────────────────

-- -----------------------------------------------------------------------------
-- fact_sales
-- Grain    : 1 baris = 1 item dalam 1 order (order_items level)
-- Composite PK : (order_id, item_id)
-- Measures : qty, unit_price, revenue, cost, gross_margin
--            Semua dihitung saat ETL: revenue = qty × unit_price
--                                    cost     = qty × cost_price
--                                    gross_margin = revenue − cost
-- category_sk di-denormalisasi langsung ke fakta (tidak hanya via product_sk)
-- untuk mendukung query agregasi per kategori tanpa join tambahan.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS marts.fact_sales (
    order_id       INTEGER       NOT NULL,
    item_id        INTEGER       NOT NULL,
    order_date_id  INTEGER,
    customer_sk    INTEGER,
    product_sk     INTEGER,
    category_sk    INTEGER,
    order_status   VARCHAR(50),
    qty            INTEGER       NOT NULL    DEFAULT 0,
    unit_price     NUMERIC(12,2) NOT NULL,
    revenue        NUMERIC(14,2) NOT NULL,   -- qty × unit_price
    cost           NUMERIC(14,2) NOT NULL,   -- qty × cost_price
    gross_margin   NUMERIC(14,2) NOT NULL,   -- revenue − cost

    CONSTRAINT pk_fact_sales          PRIMARY KEY (order_id, item_id),
    CONSTRAINT fk_fact_sales_date     FOREIGN KEY (order_date_id)
        REFERENCES marts.dim_date     (date_id),
    CONSTRAINT fk_fact_sales_customer FOREIGN KEY (customer_sk)
        REFERENCES marts.dim_customer (customer_sk),
    CONSTRAINT fk_fact_sales_product  FOREIGN KEY (product_sk)
        REFERENCES marts.dim_product  (product_sk),
    CONSTRAINT fk_fact_sales_category FOREIGN KEY (category_sk)
        REFERENCES marts.dim_category (category_sk)
);

COMMENT ON TABLE  marts.fact_sales IS 'Fakta penjualan — grain: 1 item per order. Sumber: oltp_order_items + oltp_orders + oltp_products.';
COMMENT ON COLUMN marts.fact_sales.revenue      IS 'qty × unit_price — dihitung saat load ETL.';
COMMENT ON COLUMN marts.fact_sales.cost         IS 'qty × cost_price — dihitung saat load ETL.';
COMMENT ON COLUMN marts.fact_sales.gross_margin IS 'revenue − cost — dihitung saat load ETL.';

-- Index pendukung query analitik Pertanyaan 1
CREATE INDEX IF NOT EXISTS idx_fact_sales_date_cat
    ON marts.fact_sales (order_date_id, category_sk);

-- Index pendukung query analitik Pertanyaan 2
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer
    ON marts.fact_sales (customer_sk);


-- -----------------------------------------------------------------------------
-- fact_payment
-- Grain    : 1 baris = 1 transaksi pembayaran
-- PK       : payment_id (natural key dari OLTP payments)
-- Measures : amount
-- payment_status dipakai filter langsung ('paid' | 'pending' | 'failed').
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS marts.fact_payment (
    payment_id      INTEGER       NOT NULL,
    order_id        INTEGER       NOT NULL,
    paid_date_id    INTEGER,
    customer_sk     INTEGER,
    method_sk       INTEGER,
    payment_status  VARCHAR(50),              -- 'paid' | 'pending' | 'failed'
    amount          NUMERIC(14,2) NOT NULL,

    CONSTRAINT pk_fact_payment          PRIMARY KEY (payment_id),
    CONSTRAINT fk_fact_payment_date     FOREIGN KEY (paid_date_id)
        REFERENCES marts.dim_date         (date_id),
    CONSTRAINT fk_fact_payment_customer FOREIGN KEY (customer_sk)
        REFERENCES marts.dim_customer    (customer_sk),
    CONSTRAINT fk_fact_payment_method   FOREIGN KEY (method_sk)
        REFERENCES marts.dim_payment_method (method_sk)
);

COMMENT ON TABLE  marts.fact_payment IS 'Fakta pembayaran — grain: 1 transaksi. Sumber: oltp_payments + oltp_orders.';
COMMENT ON COLUMN marts.fact_payment.payment_status IS 'Status pembayaran: paid | pending | failed.';

-- Index pendukung query Pertanyaan 2
CREATE INDEX IF NOT EXISTS idx_fact_payment_customer_status
    ON marts.fact_payment (customer_sk, payment_status);

CREATE INDEX IF NOT EXISTS idx_fact_payment_method
    ON marts.fact_payment (method_sk);


-- -----------------------------------------------------------------------------
-- fact_review
-- Grain    : 1 baris = 1 ulasan produk oleh 1 pelanggan
-- PK       : review_id (natural key dari API)
-- Measures : rating (skala 1.0 – 5.0), comment (teks)
-- Sumber   : api_product_reviews.csv (external REST API)
-- category_sk di-denormalisasi langsung dari dim_product saat ETL
-- untuk mendukung query avg_rating per kategori (Pertanyaan 3).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS marts.fact_review (
    review_id       INTEGER      NOT NULL,
    review_date_id  INTEGER,
    product_sk      INTEGER,
    category_sk     INTEGER,
    customer_sk     INTEGER,
    rating          NUMERIC(3,1),   -- 1.0 – 5.0
    comment         TEXT,

    CONSTRAINT pk_fact_review          PRIMARY KEY (review_id),
    CONSTRAINT fk_fact_review_date     FOREIGN KEY (review_date_id)
        REFERENCES marts.dim_date     (date_id),
    CONSTRAINT fk_fact_review_product  FOREIGN KEY (product_sk)
        REFERENCES marts.dim_product  (product_sk),
    CONSTRAINT fk_fact_review_category FOREIGN KEY (category_sk)
        REFERENCES marts.dim_category (category_sk),
    CONSTRAINT fk_fact_review_customer FOREIGN KEY (customer_sk)
        REFERENCES marts.dim_customer (customer_sk),
    CONSTRAINT chk_fact_review_rating  CHECK (rating BETWEEN 1.0 AND 5.0)
);

COMMENT ON TABLE  marts.fact_review IS 'Fakta ulasan produk — sumber: api_product_reviews.csv (external API).';
COMMENT ON COLUMN marts.fact_review.rating IS 'Rating numerik skala 1.0 – 5.0.';

-- Index pendukung query Pertanyaan 3
CREATE INDEX IF NOT EXISTS idx_fact_review_category
    ON marts.fact_review (category_sk);

CREATE INDEX IF NOT EXISTS idx_fact_review_product
    ON marts.fact_review (product_sk);


-- ─────────────────────────────────────────────────────────────────────────────
-- SEED DATA — dim_payment_method (statis, tidak dari staging CSV)
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO marts.dim_payment_method (method_code, method_name, is_digital)
VALUES
    ('bank_transfer', 'Bank Transfer',     FALSE),
    ('ewallet',       'E-Wallet',          TRUE),
    ('cod',           'Cash on Delivery',  FALSE),
    ('credit_card',   'Credit Card',       TRUE)
ON CONFLICT (method_code) DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────────────
-- VERIFIKASI — tampilkan tabel yang baru dibuat
-- ─────────────────────────────────────────────────────────────────────────────
DO $$
DECLARE
    tbl TEXT;
BEGIN
    FOREACH tbl IN ARRAY ARRAY[
        'dim_date', 'dim_category', 'dim_product', 'dim_customer',
        'dim_payment_method', 'dim_campaign',
        'fact_sales', 'fact_payment', 'fact_review'
    ] LOOP
        RAISE NOTICE 'Table marts.% ... OK', tbl;
    END LOOP;
END;
$$;
