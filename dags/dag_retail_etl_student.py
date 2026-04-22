"""
dag_retail_etl_student.py
=========================

Template untuk mahasiswa — PHASE 2 & 3 perlu dilengkapi.

Sumber Data:
  1. PostgreSQL OLTP  — 6 tabel: customers, categories, products,
                         orders, order_items, payments
  2. Product Review API — REST API dengan API Key
  3. Promotions CSV    — file CSV dari remote URL

Target:
  Star Schema OLAP (jalankan olap_schema.sql terlebih dahulu)

Flow:
  start → prepare_staging
    → [EXTRACT] 6 tabel OLTP + reviews API + promotions CSV (paralel)
      → [LOAD DIM] ← ISI DI SINI
        → [LOAD FACT] ← ISI DI SINI
          → end

PETUNJUK:
  - PHASE 1 (Extract) sudah selesai — jangan diubah
  - PHASE 2: Lengkapi load_dim_* menggunakan helper functions
  - PHASE 3: Lengkapi load_fact_* menggunakan helper functions
  - OLAP_DB perlu dikonfigurasi terlebih dahulu
  - olap_schema.sql harus sudah dijalankan di database OLAP
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta

import pandas as pd
import psycopg2
import requests
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

# ================================================================
#  KONFIGURASI — isi OLAP_DB dengan koneksi database masing-masing
# ================================================================

# Database sumber (OLTP) — sudah dikonfigurasi
OLTP_DB = {
    "host": os.getenv("OLTP_HOST", "127.0.0.1"),
    "port": int(os.getenv("OLTP_PORT", "5432")),
    "dbname": os.getenv("OLTP_DBNAME", "dbname"),
    "user": os.getenv("OLTP_USER", "dbuser"),
    "password": os.getenv("OLTP_PASSWORD", "password"),
}

# Database tujuan (OLAP) — membaca dari environment variable Docker (.env)
OLAP_DB = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_WAREHOUSE_DB", "warehouse_db"),
    "user": os.getenv("POSTGRES_USER", "etl_admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "etl_admin_123"),
    "options": "-c search_path=marts,public",
}

REVIEWS_URL = "https://product-review.devops-rightjet.workers.dev/api/v1/reviews"
PROMOTIONS_URL = (
    "https://product-review.devops-rightjet.workers.dev/api/v1/promotions/download"
)
API_KEY = "dev-api-key-retail-2024"
API_HEADERS = {"X-API-Key": API_KEY}

STAGING_DIR = "/tmp/retail_staging"

OLTP_TABLES = [
    "customers",
    "categories",
    "products",
    "orders",
    "order_items",
    "payments",
]

log = logging.getLogger(__name__)

default_args = {
    "owner": "cakrawala",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ================================================================
#  HELPER FUNCTIONS — fungsi pembantu yang bisa dipakai ulang
# ================================================================


def oltp_conn():
    """Buka koneksi ke database OLTP (sumber)."""
    return psycopg2.connect(**OLTP_DB)


def olap_conn():
    """Buka koneksi ke database OLAP (tujuan)."""
    return psycopg2.connect(**OLAP_DB)


def run_sql(sql, params=None):
    """Jalankan SQL tanpa return di database OLAP."""
    conn = olap_conn()
    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    cur.close()
    conn.close()


def fetch_all(sql):
    """SELECT query di database OLTP → (columns, rows)."""
    conn = oltp_conn()
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return cols, rows


def save_csv(path, columns, rows):
    """Simpan data ke file CSV."""
    import csv

    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows([columns] + rows)


def api_get(url):
    """HTTP GET dengan API key → JSON."""
    r = requests.get(url, headers=API_HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def upsert_many(sql, rows):
    """Batch INSERT … ON CONFLICT di database OLAP. Return jumlah rows."""
    if not rows:
        return 0
    conn = olap_conn()
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()
    return len(rows)


def upsert_sql(table, columns, conflict_key, updates=None):
    """
    Generate SQL: INSERT … ON CONFLICT.

    Parameter:
      table        — nama tabel
      columns      — list kolom
      conflict_key — kolom/komposit key untuk ON CONFLICT
      updates      — list kolom yang di-update saat konflik.
                     Gunakan format "col = EXPR" untuk ekspresi khusus
                     (misal: "updated_at = NOW()").
                     Jika None → DO NOTHING.

    Contoh:
      upsert_sql("dim_customer",
          ["customer_id","full_name","email","city","province"],
          "customer_id",
          ["full_name","city","province","updated_at = NOW()"])
    """
    cols = ", ".join(columns)
    vals = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table} ({cols}) VALUES ({vals})"

    if updates:
        parts = [c if "=" in c else f"{c} = EXCLUDED.{c}" for c in updates]
        sql += f" ON CONFLICT ({conflict_key}) DO UPDATE SET {', '.join(parts)}"
    else:
        sql += f" ON CONFLICT ({conflict_key}) DO NOTHING"

    return sql


def to_date_id(dt_str):
    """Konversi tanggal → integer YYYYMMDD. Return None kalau kosong."""
    s = str(dt_str).strip()
    if not s or s in ("nan", "None", "NaT"):
        return None
    return int(s[:10].replace("-", ""))


def sk_map(nk, sk, table):
    """Ambil mapping dari tabel OLAP: natural_key → surrogate_key."""
    conn = olap_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT {nk}, {sk} FROM {table}")
    result = {r[0]: r[1] for r in cur.fetchall()}
    cur.close()
    conn.close()
    return result


def csv_dict(csv_file, key):
    """Baca staging CSV → dict {key: row_dict} untuk lookup."""
    df = pd.read_csv(os.path.join(STAGING_DIR, csv_file))
    return df.set_index(key).to_dict("index")


def load_staging(csv_file, sql, transform):
    """
    Pola umum: baca staging CSV → transform tiap baris → upsert.
      transform(row_dict) → tuple | None
    """
    df = pd.read_csv(os.path.join(STAGING_DIR, csv_file))
    data = [transform(r) for r in df.to_dict("records")]
    data = [d for d in data if d is not None]
    n = upsert_many(sql, data)
    log.info("%-20s → %4d rows", csv_file, n)
    return n


def _safe_str(val):
    """
    Konversi nilai ke string yang bersih.
    Return None jika nilai adalah NaN, None, kosong, atau string 'nan'/'None'.

    Digunakan untuk kolom VARCHAR nullable agar tidak menyimpan NaN float
    dari pandas ke PostgreSQL (psycopg2 akan error jika menerima float NaN
    untuk kolom VARCHAR).
    """
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    stripped = str(val).strip()
    if stripped.lower() in ("nan", "none", "nat", ""):
        return None
    return stripped


def _safe_bool(val, default: bool = False) -> bool:
    """
    Parse nilai boolean dari CSV secara aman.

    MASALAH: pandas read_csv kadang membaca kolom boolean PostgreSQL sebagai
    string 'True'/'False'. Dalam kondisi itu, bool('False') == True — ini BUG!

    FIX: periksa string secara eksplisit, jangan pakai bool() langsung.

    Nilai yang dianggap False: 'false', '0', 'f', 'no', 'nan', 'none', ''
    Semua nilai lain dianggap True (termasuk 'true', '1', 't', 'yes').
    """
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        try:
            if pd.isna(val):
                return default
        except (TypeError, ValueError):
            pass
        return bool(val)
    # String case — JANGAN pakai bool() langsung
    return str(val).strip().lower() not in ("false", "0", "f", "no", "nan", "none", "")


# ================================================================
#  DAG
# ================================================================
@dag(
    dag_id="retail_etl_student",
    description="Pipeline ETL Retail: Extract → Load (Star Schema OLAP) — STUDENT VERSION",
    schedule_interval="0 1 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["retail", "etl", "olap", "student"],
)
def retail_etl_student():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ─────────────────────────────────────────────
    #  PHASE 1 — EXTRACT (sudah selesai, jangan diubah)
    # ─────────────────────────────────────────────

    @task
    def prepare_staging():
        """Buat folder staging jika belum ada."""
        os.makedirs(STAGING_DIR, exist_ok=True)
        return STAGING_DIR

    @task
    def extract_oltp_table(table_name, staging_dir):
        """Extract 1 tabel OLTP → CSV. Dipanggil berulang untuk setiap tabel."""
        sql = f"SELECT * FROM {table_name} ORDER BY 1"
        columns, rows = fetch_all(sql)
        path = os.path.join(staging_dir, f"oltp_{table_name}.csv")
        save_csv(path, columns, rows)
        log.info("[OLTP] %-15s → %4d rows", table_name, len(rows))
        return {"table": table_name, "rows": len(rows)}

    @task
    def extract_reviews(staging_dir):
        """Extract review produk dari REST API → CSV."""
        body = api_get(REVIEWS_URL)
        reviews = body.get("data", []) if isinstance(body, dict) else body

        if not reviews:
            return {"table": "product_reviews", "rows": 0}

        columns = list(reviews[0].keys())
        rows = [tuple(r[c] for c in columns) for r in reviews]
        path = os.path.join(staging_dir, "api_product_reviews.csv")
        save_csv(path, columns, rows)
        log.info("[API]  %-15s → %4d rows", "reviews", len(rows))
        return {"table": "product_reviews", "rows": len(rows)}

    @task
    def extract_promotions(staging_dir):
        """Download file CSV promosi dari remote URL."""
        resp = requests.get(PROMOTIONS_URL, headers=API_HEADERS, timeout=60)
        resp.raise_for_status()

        path = os.path.join(staging_dir, "promotions.csv")
        with open(path, "wb") as f:
            f.write(resp.content)

        count = len(pd.read_csv(path))
        log.info("[CSV]  %-15s → %4d rows", "promotions", count)
        return {"table": "promotions", "rows": count}

    # ─────────────────────────────────────────────
    #  PHASE 2 — LOAD DIMENSIONS
    # ─────────────────────────────────────────────

    @task
    def load_dim_date():
        """
        Generate tabel kalender tahun 2020–2026 → insert ke dim_date.

        Rentang 2020–2026 dipilih agar FK dari fact_sales, fact_payment,
        dan fact_review tidak gagal meski data OLTP memiliki tanggal di
        luar tahun 2024 (misal order lama atau review mendatang).

        Kolom dim_date:
          date_id (int YYYYMMDD), full_date, day_of_week, day_name,
          day_of_month, week_of_year, month_num, month_name,
          quarter, year, is_weekend

        Konvensi day_of_week: 0 = Minggu (Sunday) … 6 = Sabtu (Saturday)
          Python weekday(): 0=Sen … 6=Min → geser dengan (weekday+1)%7
        """
        cal_start = date(2020, 1, 1)
        cal_end = date(2026, 12, 31)

        rows = []
        current = cal_start
        while current <= cal_end:
            # Konversi Python weekday (Mon=0…Sun=6) → (Sun=0…Sat=6)
            dow = (current.weekday() + 1) % 7

            rows.append(
                (
                    int(current.strftime("%Y%m%d")),  # date_id  (YYYYMMDD)
                    current,  # full_date
                    dow,  # day_of_week (0=Sun…6=Sat)
                    current.strftime("%A"),  # day_name   ('Monday'…)
                    current.day,  # day_of_month
                    current.isocalendar()[1],  # week_of_year (ISO)
                    current.month,  # month_num
                    current.strftime("%B"),  # month_name  ('January'…)
                    (current.month - 1) // 3 + 1,  # quarter
                    current.year,  # year
                    dow in (0, 6),  # is_weekend (Sun=0, Sat=6)
                )
            )
            current += timedelta(days=1)

        sql = upsert_sql(
            "dim_date",
            [
                "date_id",
                "full_date",
                "day_of_week",
                "day_name",
                "day_of_month",
                "week_of_year",
                "month_num",
                "month_name",
                "quarter",
                "year",
                "is_weekend",
            ],
            "date_id",
            updates=None,  # DO NOTHING — data kalender tidak pernah berubah
        )
        n = upsert_many(sql, rows)
        log.info("load_dim_date → %4d rows (2020–2026)", n)
        return n

    @task
    def load_dim_category():
        """
        Baca staging oltp_categories.csv → insert ke dim_category.

        Kolom staging : category_id, name, description, created_at
        Kolom target  : category_id, name, description

        Catatan: description bisa NULL di OLTP — gunakan _safe_str agar
        tidak menyimpan string 'nan' ke kolom TEXT PostgreSQL.
        """
        sql = upsert_sql(
            table="dim_category",
            columns=["category_id", "name", "description"],
            conflict_key="category_id",
            updates=["name", "description"],
        )
        return load_staging(
            "oltp_categories.csv",
            sql,
            lambda r: (
                int(r["category_id"]),
                r["name"],
                _safe_str(r.get("description")),  # NULL jika kosong / NaN
            ),
        )

    @task
    def load_dim_product():
        """
        Baca staging oltp_products.csv → insert ke dim_product.
        Memerlukan category_sk lookup dari dim_category yang sudah di-load.

        Kolom staging : product_id, category_id, name, sku, price, cost_price,
                        stock, is_active, created_at, updated_at
        Kolom target  : product_id, category_sk, name, sku, price, cost_price,
                        is_active

        Bug yang diperbaiki:
          - bool(r["is_active"]) salah jika pandas membaca 'False' sebagai string
            karena bool('False') == True. Gunakan _safe_bool() sebagai gantinya.
          - sku bisa NaN float — gunakan _safe_str agar tidak error di VARCHAR.
        """
        # Surrogate key mapping: category_id (int) → category_sk (int)
        cat_sk = sk_map("category_id", "category_sk", "dim_category")

        sql = upsert_sql(
            table="dim_product",
            columns=[
                "product_id",
                "category_sk",
                "name",
                "sku",
                "price",
                "cost_price",
                "is_active",
            ],
            conflict_key="product_id",
            updates=["category_sk", "name", "sku", "price", "cost_price", "is_active"],
        )

        def transform(r):
            cat_id_raw = r.get("category_id")
            if not pd.notna(cat_id_raw):
                return None  # skip: category_id kosong

            cat_id = int(cat_id_raw)
            if cat_id not in cat_sk:
                log.warning(
                    "product_id=%s: category_id=%s tidak ada di dim_category — dilewati",
                    r.get("product_id"),
                    cat_id,
                )
                return None  # skip: FK tidak ditemukan

            return (
                int(r["product_id"]),
                cat_sk[cat_id],  # category_sk (FK)
                r["name"],
                _safe_str(r.get("sku")),  # NULL jika kosong / NaN
                float(r["price"]),
                float(r["cost_price"]),
                _safe_bool(
                    r.get("is_active"), default=True
                ),  # FIX: bool('False')==True bug
            )

        return load_staging("oltp_products.csv", sql, transform)

    @task
    def load_dim_customer():
        """
        Baca staging oltp_customers.csv → insert ke dim_customer.

        Kolom staging : customer_id, full_name, email, phone, city, province,
                        created_at, updated_at
        Kolom target  : customer_id, full_name, email, city, province

        Catatan: email, city, province bisa NULL — gunakan _safe_str agar
        psycopg2 tidak error saat menerima float NaN untuk kolom VARCHAR.
        """
        sql = upsert_sql(
            table="dim_customer",
            columns=["customer_id", "full_name", "email", "city", "province"],
            conflict_key="customer_id",
            updates=["full_name", "email", "city", "province"],
        )
        return load_staging(
            "oltp_customers.csv",
            sql,
            lambda r: (
                int(r["customer_id"]),
                r["full_name"],
                _safe_str(r.get("email")),  # NULL jika kosong / NaN
                _safe_str(r.get("city")),
                _safe_str(r.get("province")),
            ),
        )

    @task
    def load_dim_payment_method():
        """
        Insert 4 metode pembayaran statis ke dim_payment_method.
        Data tidak bersumber dari staging CSV — didefinisikan langsung di sini.

        Kolom target: method_code, method_name, is_digital

        Nilai method_code HARUS sama persis dengan nilai kolom payments.method
        di OLTP agar lookup di load_fact_payment() berhasil.
        """
        rows = [
            ("bank_transfer", "Bank Transfer", False),
            ("ewallet", "E-Wallet", True),
            ("cod", "Cash on Delivery", False),
            ("credit_card", "Credit Card", True),
        ]
        sql = upsert_sql(
            table="dim_payment_method",
            columns=["method_code", "method_name", "is_digital"],
            conflict_key="method_code",
            updates=["method_name", "is_digital"],
        )
        n = upsert_many(sql, rows)
        log.info("load_dim_payment_method → %4d rows", n)
        return n

    @task
    def load_dim_campaign():
        """
        Baca staging promotions.csv → insert ke dim_campaign.

        Kolom staging : campaign_id, campaign_name, category_id, category_name,
                        target_scope, discount_pct, min_order_amount, budget_idr,
                        start_date, end_date, channel, is_active
        Kolom target  : (sama dengan staging)

        Catatan:
          - category_id bisa NULL (kampanye lintas kategori / target_scope='all')
          - budget_idr bisa NULL
          - is_active: gunakan _safe_bool — sama seperti dim_product, pandas
            bisa membaca boolean sebagai string 'True'/'False' dari CSV.
        """
        sql = upsert_sql(
            table="dim_campaign",
            columns=[
                "campaign_id",
                "campaign_name",
                "category_id",
                "category_name",
                "target_scope",
                "discount_pct",
                "min_order_amount",
                "budget_idr",
                "start_date",
                "end_date",
                "channel",
                "is_active",
            ],
            conflict_key="campaign_id",
            updates=[
                "campaign_name",
                "category_id",
                "category_name",
                "target_scope",
                "discount_pct",
                "min_order_amount",
                "budget_idr",
                "start_date",
                "end_date",
                "channel",
                "is_active",
            ],
        )

        def transform(r):
            return (
                int(r["campaign_id"]),
                r["campaign_name"],
                int(r["category_id"]) if pd.notna(r.get("category_id")) else None,
                _safe_str(r.get("category_name")),
                _safe_str(r.get("target_scope")),
                float(r["discount_pct"]) if pd.notna(r.get("discount_pct")) else None,
                float(r["min_order_amount"])
                if pd.notna(r.get("min_order_amount"))
                else None,
                float(r["budget_idr"]) if pd.notna(r.get("budget_idr")) else None,
                _safe_str(r.get("start_date")),
                _safe_str(r.get("end_date")),
                _safe_str(r.get("channel")),
                _safe_bool(
                    r.get("is_active"), default=False
                ),  # FIX: bool('False')==True bug
            )

        return load_staging("promotions.csv", sql, transform)

    # ─────────────────────────────────────────────
    #  PHASE 3 — LOAD FACTS
    # ─────────────────────────────────────────────

    @task
    def load_fact_sales():
        """
        Baca staging order_items + orders + products → insert ke fact_sales.

        Grain    : 1 baris = 1 item dalam 1 order (order_items level)
        Measures :
          revenue      = qty × unit_price  (unit_price dari order_items — harga saat order)
          cost         = qty × cost_price  (cost_price dari products — harga pokok master)
          gross_margin = revenue − cost

        Kolom fact_sales: order_id, item_id, order_date_id, customer_sk,
                          product_sk, category_sk, order_status, qty,
                          unit_price, revenue, cost, gross_margin

        Mapping kolom OLTP → OLAP:
          orders.ordered_at  → order_date_id (YYYYMMDD via to_date_id)
          orders.customer_id → customer_sk   (via sk_map dim_customer)
          orders.status      → order_status
          products.cost_price → cost         (bukan unit_price dari order_items)
          products.category_id → category_sk (via sk_map dim_category)
        """
        # Baca staging CSV
        orders = csv_dict("oltp_orders.csv", "order_id")
        products = csv_dict("oltp_products.csv", "product_id")
        items = pd.read_csv(os.path.join(STAGING_DIR, "oltp_order_items.csv"))

        # Surrogate key mappings dari OLAP
        cust_sk = sk_map("customer_id", "customer_sk", "dim_customer")
        prod_sk_map = sk_map("product_id", "product_sk", "dim_product")
        cat_sk = sk_map("category_id", "category_sk", "dim_category")

        rows = []
        skipped = 0
        for r in items.to_dict("records"):
            order_id = int(r["order_id"])
            item_id = int(r["item_id"])
            product_id = int(r["product_id"])

            order = orders.get(order_id)
            if not order:
                log.warning(
                    "item_id=%s: order_id=%s tidak ditemukan — dilewati",
                    item_id,
                    order_id,
                )
                skipped += 1
                continue

            product = products.get(product_id)
            if not product:
                log.warning(
                    "item_id=%s: product_id=%s tidak ditemukan — dilewati",
                    item_id,
                    product_id,
                )
                skipped += 1
                continue

            qty = int(r["qty"])
            unit_price = float(r["unit_price"])
            cost_price = float(product["cost_price"])  # harga pokok dari master produk

            revenue = round(qty * unit_price, 2)
            cost = round(qty * cost_price, 2)
            gross_margin = round(revenue - cost, 2)

            rows.append(
                (
                    order_id,
                    item_id,
                    to_date_id(order.get("ordered_at")),  # order_date_id
                    cust_sk.get(int(order["customer_id"])) if pd.notna(order.get("customer_id")) else None,  # customer_sk
                    prod_sk_map.get(product_id),  # product_sk
                    cat_sk.get(int(product["category_id"])) if pd.notna(product.get("category_id")) else None,  # category_sk
                    order.get("status"),  # order_status
                    qty,
                    unit_price,
                    revenue,
                    cost,
                    gross_margin,
                )
            )

        sql = upsert_sql(
            table="fact_sales",
            columns=[
                "order_id",
                "item_id",
                "order_date_id",
                "customer_sk",
                "product_sk",
                "category_sk",
                "order_status",
                "qty",
                "unit_price",
                "revenue",
                "cost",
                "gross_margin",
            ],
            conflict_key="order_id, item_id",
            updates=[
                "order_date_id",
                "customer_sk",
                "product_sk",
                "category_sk",
                "order_status",
                "qty",
                "unit_price",
                "revenue",
                "cost",
                "gross_margin",
            ],
        )

        n = upsert_many(sql, rows)
        log.info("load_fact_sales → %4d rows inserted, %d skipped", n, skipped)
        return n

    @task
    def load_fact_payment():
        """
        Baca staging payments + orders → insert ke fact_payment.

        Grain   : 1 baris = 1 transaksi pembayaran
        Measure : amount

        customer_sk di-lookup melalui orders.customer_id karena tabel
        payments di OLTP tidak menyimpan customer_id secara langsung.

        Kolom fact_payment: payment_id, order_id, paid_date_id, customer_sk,
                            method_sk, payment_status, amount

        Mapping kolom OLTP → OLAP:
          payments.paid_at   → paid_date_id  (YYYYMMDD via to_date_id)
          payments.method    → method_sk     (via sk_map dim_payment_method.method_code)
          payments.status    → payment_status
          orders.customer_id → customer_sk   (lookup melalui order_id)
        """
        payments = pd.read_csv(os.path.join(STAGING_DIR, "oltp_payments.csv"))
        orders = csv_dict("oltp_orders.csv", "order_id")

        # Surrogate key mappings
        cust_sk = sk_map("customer_id", "customer_sk", "dim_customer")
        # payments.method (varchar) → method_sk via method_code natural key
        meth_sk = sk_map("method_code", "method_sk", "dim_payment_method")

        rows = []
        skipped = 0
        for r in payments.to_dict("records"):
            payment_id = int(r["payment_id"])
            order_id = int(r["order_id"])

            order = orders.get(order_id)
            if not order:
                log.warning(
                    "payment_id=%s: order_id=%s tidak ditemukan — dilewati",
                    payment_id,
                    order_id,
                )
                skipped += 1
                continue

            method_code = _safe_str(r.get("method"))  # OLTP kolom: payments.method

            rows.append(
                (
                    payment_id,
                    order_id,
                    to_date_id(r.get("paid_at")),  # paid_date_id
                    cust_sk.get(int(order["customer_id"])) if pd.notna(order.get("customer_id")) else None,  # customer_sk (via orders)
                    meth_sk.get(method_code),  # method_sk
                    _safe_str(r.get("status")),  # payment_status
                    float(r["amount"]),  # amount
                )
            )

        sql = upsert_sql(
            table="fact_payment",
            columns=[
                "payment_id",
                "order_id",
                "paid_date_id",
                "customer_sk",
                "method_sk",
                "payment_status",
                "amount",
            ],
            conflict_key="payment_id",
            updates=[
                "order_id",
                "paid_date_id",
                "customer_sk",
                "method_sk",
                "payment_status",
                "amount",
            ],
        )

        n = upsert_many(sql, rows)
        log.info("load_fact_payment → %4d rows inserted, %d skipped", n, skipped)
        return n

    @task
    def load_fact_review():
        """
        Baca staging api_product_reviews.csv → insert ke fact_review.

        Grain   : 1 baris = 1 ulasan produk oleh 1 pelanggan
        Measure : rating (1.0–5.0)

        category_sk di-denormalisasi dari dim_product (prod_cat mapping)
        agar fact_review langsung mengetahui kategori tanpa join ke dim_product.

        Kolom fact_review: review_id, review_date_id, product_sk,
                           category_sk, customer_sk, rating, comment

        Mapping kolom API → OLAP:
          reviewed_at  → review_date_id (YYYYMMDD via to_date_id)
          product_id   → product_sk    (via sk_map dim_product)
          product_id   → category_sk   (via sk_map dim_product.category_sk — denormalisasi)
          customer_id  → customer_sk   (via sk_map dim_customer, nullable)
          rating       → rating        (divalidasi range 1.0–5.0 sebelum INSERT)

        Bug yang diperbaiki:
          Rating yang keluar dari range 1.0–5.0 akan menyebabkan pelanggaran
          CHECK constraint di DDL. Validasi dilakukan di sini, nilai invalid
          di-set NULL dan di-log sebagai warning.
        """
        reviews = pd.read_csv(os.path.join(STAGING_DIR, "api_product_reviews.csv"))

        # Surrogate key mappings
        prod_sk = sk_map("product_id", "product_sk", "dim_product")
        cust_sk = sk_map("customer_id", "customer_sk", "dim_customer")
        prod_cat = sk_map("product_id", "category_sk", "dim_product")  # denormalisasi

        rows = []
        for r in reviews.to_dict("records"):
            review_id = int(r["review_id"])
            product_id = int(r["product_id"])

            # Validasi rating: CHECK constraint di DDL mensyaratkan 1.0 – 5.0
            rating_raw = r.get("rating")
            if pd.notna(rating_raw):
                rating = float(rating_raw)
                if not (1.0 <= rating <= 5.0):
                    log.warning(
                        "review_id=%s: rating=%.1f di luar range 1.0–5.0, diset NULL",
                        review_id,
                        rating,
                    )
                    rating = None
            else:
                rating = None

            rows.append(
                (
                    review_id,
                    to_date_id(r.get("reviewed_at")),  # review_date_id
                    prod_sk.get(product_id),  # product_sk
                    prod_cat.get(product_id),  # category_sk
                    cust_sk.get(int(r["customer_id"]))
                    if pd.notna(r.get("customer_id"))
                    else None,
                    rating,  # rating (validated)
                    _safe_str(r.get("comment")),  # comment
                )
            )

        sql = upsert_sql(
            table="fact_review",
            columns=[
                "review_id",
                "review_date_id",
                "product_sk",
                "category_sk",
                "customer_sk",
                "rating",
                "comment",
            ],
            conflict_key="review_id",
            updates=[
                "review_date_id",
                "product_sk",
                "category_sk",
                "customer_sk",
                "rating",
                "comment",
            ],
        )

        n = upsert_many(sql, rows)
        log.info("load_fact_review → %4d rows", n)
        return n

    # ─────────────────────────────────────────────
    #  WIRES — jangan diubah
    # ─────────────────────────────────────────────
    staging = prepare_staging()

    oltp_results = [
        extract_oltp_table.override(task_id=f"extract_{t}")(
            table_name=t, staging_dir=staging
        )
        for t in OLTP_TABLES
    ]
    reviews = extract_reviews(staging_dir=staging)
    promos = extract_promotions(staging_dir=staging)

    d_date = load_dim_date()
    d_cat = load_dim_category()
    d_prod = load_dim_product()
    d_cust = load_dim_customer()
    d_pay = load_dim_payment_method()
    d_camp = load_dim_campaign()

    f_sales = load_fact_sales()
    f_payment = load_fact_payment()
    f_review = load_fact_review()

    # ── Dependency graph ─────────────────────────
    start >> staging

    for ext in oltp_results + [reviews, promos]:
        ext >> d_date

    d_date >> [d_cat, d_cust, d_pay, d_camp]
    d_cat >> d_prod

    for d in [d_date, d_cat, d_prod, d_cust, d_pay, d_camp]:
        d >> [f_sales, f_payment, f_review]

    [f_sales, f_payment, f_review] >> end


retail_etl_student()
