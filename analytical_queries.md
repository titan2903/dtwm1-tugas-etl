# Bagian A & B — OLAP Schema: Justifikasi & Query Analitik

**Mata Kuliah:** Data Warehouse & Data Mining
**Schema Target:** `marts` (warehouse_db)

---

## Bagian A.3 — Justifikasi Desain Schema

### Pilihan: Hybrid Star / Snowflake Schema (Snowflake Level-1)

Schema yang dirancang menggunakan pendekatan **hybrid** yang secara dominan mengikuti pola **Star Schema**, dengan satu tingkat normalisasi ala *Snowflake* pada relasi `dim_product → dim_category`.

**Alasan pemilihan hybrid:**
Produk (`dim_product`) memiliki relasi many-to-one yang kuat dengan kategori (`dim_category`), dan atribut kategori (nama, deskripsi) tidak layak diduplikasi di setiap baris `dim_product` karena redundansi data yang tinggi. Normalisasi ini hanya menambah **satu join** dan tidak mengorbankan performa secara signifikan. Di sisi lain, tabel fakta (`fact_sales`, `fact_review`) menyimpan `category_sk` secara langsung (di-denormalisasi ke fakta) agar query agregasi per kategori bisa berjalan tanpa harus join ke `dim_product` terlebih dahulu — pola ini mempertahankan keunggulan Star Schema untuk query analitik yang cepat dan sederhana.

**Integrasi sumber eksternal:**
Dua sumber data eksternal diintegrasikan sebagai berikut. `promotions.csv` (kampanye marketing) dimodelkan sebagai tabel dimensi `dim_campaign`, bukan fakta, karena data ini bersifat *deskriptif* dan digunakan sebagai konteks analitik (filter `is_active`, relasi ke `category_id`) — bukan sebagai event transaksi yang memiliki measure. `api_product_reviews.csv` (ulasan produk) dimodelkan sebagai tabel fakta `fact_review` karena setiap baris merepresentasikan satu *event* yang memiliki measure terukur (rating), terjadi pada suatu waktu (review_date_id), dan dilakukan oleh pelanggan terhadap produk tertentu — sesuai definisi grain fakta dalam metodologi Kimball.

---

## Review Schema — Kesesuaian OLTP → OLAP

Berdasarkan OLTP schema aktual, berikut pemetaan kolom sumber ke kolom OLAP:

### Pemetaan Kolom Kritis

| OLTP Tabel      | OLTP Kolom      | OLAP Tabel       | OLAP Kolom         | Catatan                                              |
| --------------- | --------------- | ---------------- | ------------------ | ---------------------------------------------------- |
| `order_items` | `unit_price`  | `fact_sales`   | `unit_price`     | Harga saat transaksi (bukan current price)           |
| `order_items` | `qty`         | `fact_sales`   | `qty`            | Langsung                                             |
| `order_items` | `subtotal`    | `fact_sales`   | `revenue`        | subtotal = qty × unit_price, di-recompute           |
| `products`    | `cost_price`  | `dim_product`  | `cost_price`     | Disimpan di dim, dipakai hitung `fact_sales.cost`  |
| `orders`      | `status`      | `fact_sales`   | `order_status`   | Rename kolom saat ETL                                |
| `orders`      | `ordered_at`  | `fact_sales`   | `order_date_id`  | Dikonversi ke YYYYMMDD integer                       |
| `payments`    | `method`      | `fact_payment` | `method_sk`      | Di-lookup ke `dim_payment_method.method_code`      |
| `payments`    | `status`      | `fact_payment` | `payment_status` | Rename kolom saat ETL                                |
| `payments`    | `paid_at`     | `fact_payment` | `paid_date_id`   | Dikonversi ke YYYYMMDD integer                       |
| `categories`  | `category_id` | `dim_campaign` | `category_id`    | **Integer key** penghubung ke `dim_category` |

### Kalkulasi Measures di fact_sales (dihitung saat ETL Load)

```
revenue      = order_items.qty × order_items.unit_price
cost         = order_items.qty × products.cost_price
gross_margin = revenue − cost
```

> **Catatan:** `revenue` menggunakan `unit_price` dari `order_items` (harga saat order),
> sedangkan `cost` menggunakan `cost_price` dari `products` (harga pokok saat ETL).
> Ini adalah pendekatan umum dalam retail DWH — harga jual di-capture saat transaksi,
> harga pokok mengikuti data master terkini.

### Hasil Review: Schema OLAP Dapat Menjawab Semua Pertanyaan Bisnis

| Pertanyaan                                | Status     | Kolom Kunci                                                                                                        |
| ----------------------------------------- | ---------- | ------------------------------------------------------------------------------------------------------------------ |
| P1 — Revenue & Margin per Kategori/Bulan | ✅ Lengkap | `fact_sales.{revenue, gross_margin}` + `dim_date.{month_num}` + `dim_category.name`                          |
| P2 — Top 5 Pelanggan & Metode Pembayaran | ✅ Lengkap | `fact_payment.{amount, payment_status}` + `dim_customer.{city, province}` + `dim_payment_method.method_name` |
| P3 — Efektivitas Kampanye vs Rating      | ✅ Lengkap | `fact_review.rating` + `dim_campaign.{is_active, category_id}` + `dim_category.category_sk`                  |

---

## Bagian B — Query Analitik

---

### Pertanyaan 1 — Tren Revenue & Margin per Kategori Produk

**Pertanyaan bisnis:**

> Berapa total revenue dan gross margin per kategori produk, dikelompokkan per bulan?
> Kategori mana yang paling profitable dan apakah ada tren penurunan margin di bulan-bulan tertentu?

**Tabel yang digunakan:** `fact_sales` ← `dim_date`, `dim_category`

#### Query 1a — Revenue & Margin per Kategori per Bulan (Utama)

```sql
SELECT
    dd.year,
    dd.month_num,
    dd.month_name,
    dc.name                                                      AS category_name,

    -- Measures utama
    SUM(fs.revenue)                                              AS total_revenue,
    SUM(fs.cost)                                                 AS total_cost,
    SUM(fs.gross_margin)                                         AS total_gross_margin,

    -- Persentase margin (dibulatkan 2 desimal)
    ROUND(
        SUM(fs.gross_margin)::NUMERIC
        / NULLIF(SUM(fs.revenue), 0) * 100,
        2
    )                                                            AS margin_pct,

    -- Ringkasan volume transaksi
    SUM(fs.qty)                                                  AS total_qty_sold,
    COUNT(DISTINCT fs.order_id)                                  AS total_orders

FROM  marts.fact_sales   fs
JOIN  marts.dim_date     dd  ON fs.order_date_id = dd.date_id
JOIN  marts.dim_category dc  ON fs.category_sk   = dc.category_sk

-- Filter hanya order yang valid (opsional — sesuaikan dengan data)
-- WHERE fs.order_status NOT IN ('cancelled', 'refunded')

GROUP BY
    dd.year,
    dd.month_num,
    dd.month_name,
    dc.name

ORDER BY
    margin_pct   DESC NULLS LAST,   -- kategori paling profitable di atas
    dd.year      ASC,
    dd.month_num ASC;
```

**Click Result Query dibawah ini 👇**
**[Result Query Revenue &amp; Margin per Kategori per Bulan (Utama)](https://popsql.com/queries/-OqkT4jEQ62Q0fLoZCzu/revenue-margin-per-kategori-per-bulan-utama?access_token=070118c35a2498deafc6198ae113bda8)**

#### Query 1b — Deteksi Tren Penurunan Margin (Window Function LAG)

```sql
WITH monthly_margin AS (
    SELECT
        dd.year,
        dd.month_num,
        dd.month_name,
        dc.name                                                  AS category_name,
        SUM(fs.revenue)                                          AS total_revenue,
        SUM(fs.gross_margin)                                     AS total_gross_margin,
        ROUND(
            SUM(fs.gross_margin)::NUMERIC
            / NULLIF(SUM(fs.revenue), 0) * 100, 2
        )                                                        AS margin_pct
    FROM  marts.fact_sales   fs
    JOIN  marts.dim_date     dd  ON fs.order_date_id = dd.date_id
    JOIN  marts.dim_category dc  ON fs.category_sk   = dc.category_sk
    GROUP BY dd.year, dd.month_num, dd.month_name, dc.name
),
margin_with_trend AS (
    SELECT
        *,
        LAG(margin_pct) OVER (
            PARTITION BY category_name
            ORDER BY year, month_num
        )                                                        AS prev_month_margin_pct,

        margin_pct - LAG(margin_pct) OVER (
            PARTITION BY category_name
            ORDER BY year, month_num
        )                                                        AS margin_pct_change
    FROM monthly_margin
)
SELECT
    year,
    month_num,
    month_name,
    category_name,
    total_revenue,
    total_gross_margin,
    margin_pct,
    prev_month_margin_pct,
    ROUND(margin_pct_change, 2)                                  AS margin_pct_change,
    CASE
        WHEN margin_pct_change < 0 THEN 'TURUN'
        WHEN margin_pct_change > 0 THEN 'NAIK'
        WHEN margin_pct_change = 0 THEN 'STABIL'
        ELSE '-'                                                 -- bulan pertama (tidak ada data sebelumnya)
    END                                                          AS margin_trend
FROM  margin_with_trend
ORDER BY category_name, year, month_num;
```

**Click Result Query dibawah ini 👇**
**[Result Query Deteksi Tren Penurunan Margin (Window Function LAG)](https://popsql.com/queries/-OqkVooYrxWiU3vP-BHn/deteksi-tren-penurunan-margin-window-function-lag?access_token=c8caa477d16800acbe836befc473e173)**

---

### Pertanyaan 2 — Segmentasi Pelanggan & Metode Pembayaran Favorit

**Pertanyaan bisnis:**

> Siapa 5 pelanggan dengan total transaksi (paid) tertinggi? Dari kota/provinsi mana
> mereka berasal, dan metode pembayaran apa yang paling sering mereka gunakan?
> Apakah ada korelasi antara nilai transaksi dan pilihan metode pembayaran?

**Tabel yang digunakan:** `fact_payment` ← `dim_customer`, `dim_payment_method`

> **Catatan ETL:** Kolom `payments.method` dari OLTP (varchar) di-lookup ke
> `dim_payment_method.method_code` saat load `fact_payment`. Nilai yang dikenali:
> `bank_transfer`, `ewallet`, `cod`, `credit_card`.

#### Query 2a — Top 5 Pelanggan dengan Metode Pembayaran Favorit (Utama)

```sql
WITH customer_totals AS (
    -- Agregasi total transaksi paid per pelanggan
    SELECT
        fp.customer_sk,
        SUM(fp.amount)               AS total_paid_amount,
        COUNT(DISTINCT fp.order_id)  AS total_orders,
        ROUND(AVG(fp.amount), 2)     AS avg_order_value
    FROM  marts.fact_payment fp
    WHERE fp.payment_status = 'paid'
    GROUP BY fp.customer_sk
),
method_ranked AS (
    -- Ranking metode pembayaran per pelanggan berdasarkan frekuensi penggunaan
    SELECT
        fp.customer_sk,
        dpm.method_name,
        dpm.is_digital,
        COUNT(*)                                                 AS usage_count,
        ROW_NUMBER() OVER (
            PARTITION BY fp.customer_sk
            ORDER BY COUNT(*) DESC
        )                                                        AS rn
    FROM  marts.fact_payment       fp
    JOIN  marts.dim_payment_method dpm  ON fp.method_sk = dpm.method_sk
    WHERE fp.payment_status = 'paid'
    GROUP BY fp.customer_sk, dpm.method_name, dpm.is_digital
)
SELECT
    RANK() OVER (ORDER BY ct.total_paid_amount DESC)             AS ranking,
    dc.full_name,
    dc.city,
    dc.province,

    -- Measures transaksi
    ct.total_paid_amount,
    ct.total_orders,
    ct.avg_order_value,

    -- Metode pembayaran favorit (frekuensi tertinggi)
    mr.method_name                                               AS favorite_payment_method,
    mr.is_digital                                                AS is_digital_payment,
    mr.usage_count                                               AS method_usage_count

FROM  customer_totals    ct
JOIN  marts.dim_customer dc   ON ct.customer_sk = dc.customer_sk
JOIN  method_ranked      mr   ON ct.customer_sk = mr.customer_sk AND mr.rn = 1

ORDER BY ct.total_paid_amount DESC
LIMIT 5;
```

**Click Result Query dibawah ini 👇**
**[Result Query Top 5 Pelanggan dengan Metode Pembayaran Favorit (Utama)](https://popsql.com/queries/-OqkWjVSNm-ui41lpSY8/top-5-pelanggan-dengan-metode-pembayaran-favorit-utama?access_token=fa2717a59fb162c70d39fe5ad2db6dc0)**

#### Query 2b — Distribusi & Korelasi Metode Pembayaran vs Nilai Transaksi

```sql
SELECT
    dpm.method_name,
    dpm.is_digital,
    COUNT(DISTINCT fp.customer_sk)                               AS unique_customers,
    COUNT(*)                                                     AS total_transactions,
    ROUND(MIN(fp.amount), 2)                                     AS min_transaction_value,
    ROUND(AVG(fp.amount), 2)                                     AS avg_transaction_value,
    ROUND(MAX(fp.amount), 2)                                     AS max_transaction_value,
    SUM(fp.amount)                                               AS total_amount,

    -- Kontribusi persentase per metode terhadap total transaksi paid
    ROUND(
        COUNT(*)::NUMERIC
        / SUM(COUNT(*)) OVER () * 100,
        2
    )                                                            AS pct_of_total_transactions,

    ROUND(
        SUM(fp.amount)
        / SUM(SUM(fp.amount)) OVER () * 100,
        2
    )                                                            AS pct_of_total_amount

FROM  marts.fact_payment       fp
JOIN  marts.dim_payment_method dpm  ON fp.method_sk = dpm.method_sk
WHERE fp.payment_status = 'paid'
GROUP BY dpm.method_name, dpm.is_digital
ORDER BY total_amount DESC;
```

**Click Result Query dibawah ini 👇**
**[Result Query Distribusi &amp; Korelasi Metode Pembayaran vs Nilai Transaksi](https://popsql.com/queries/-OqkXKgJ6lVahILFmOQq/distribusi-korelasi-metode-pembayaran-vs-nilai-transaksi?access_token=928b47bc543ff336bb36b2ad2980714a)**

---

### Pertanyaan 3 — Efektivitas Kampanye Marketing terhadap Rating Produk

**Pertanyaan bisnis:**

> Apakah produk dalam kategori yang sedang mendapat promo aktif memiliki rata-rata
> rating ulasan yang lebih tinggi dibandingkan produk tanpa promo? Kampanye mana yang
> paling berkorelasi dengan kategori produk berperforma baik di ulasan pelanggan?

**Tabel yang digunakan:** `fact_review` ← `dim_category` ← `dim_campaign`

> **Catatan Join Kritis:** `dim_campaign.category_id` dan `dim_category.category_id`
> keduanya berasal dari natural key yang sama di OLTP tabel `categories.category_id`.
> Semua query Pertanyaan 3 menggunakan join integer `dcm.category_id = dc.category_id`
> — **bukan** string name — untuk menghindari bug akibat perbedaan kapitalisasi atau
> spasi pada nama kategori.

#### Query 3a — Rata-rata Rating per Kategori dengan Status Promo (Utama)

```sql
WITH category_ratings AS (
    -- Hitung avg rating dan jumlah review per kategori
    SELECT
        fr.category_sk,
        dc.category_id,
        dc.name                                                  AS category_name,
        ROUND(AVG(fr.rating), 2)                                 AS avg_rating,
        COUNT(fr.review_id)                                      AS review_count
    FROM  marts.fact_review  fr
    JOIN  marts.dim_category dc  ON fr.category_sk = dc.category_sk
    GROUP BY fr.category_sk, dc.category_id, dc.name
),
active_campaign_per_category AS (
    -- Kumpulkan kategori yang memiliki kampanye aktif
    -- Join via integer category_id (bukan string name) untuk keandalan
    SELECT DISTINCT
        dc.category_sk,
        dcm.campaign_name
    FROM  marts.dim_campaign dcm
    JOIN  marts.dim_category dc  
      ON (dcm.category_id = dc.category_id OR dcm.category_id IS NULL)
    WHERE dcm.is_active = TRUE
)
SELECT
    cr.category_name,
    cr.avg_rating,
    cr.review_count,
    CASE
        WHEN acp.category_sk IS NOT NULL THEN 'Dengan Promo Aktif'
        ELSE 'Tanpa Promo Aktif'
    END                                                          AS promo_status,
    COALESCE(acp.campaign_name, '-')                             AS active_campaign_name

FROM  category_ratings              cr
LEFT  JOIN active_campaign_per_category acp
      ON cr.category_sk = acp.category_sk    -- join via surrogate key (integer)

ORDER BY cr.avg_rating DESC NULLS LAST;
```

**Click Result Query dibawah ini 👇**
**[Result Query Rata-rata Rating per Kategori dengan Status Promo (Utama)](https://popsql.com/queries/-OqkXn9R7035jZW-d9Ad/rata-rata-rating-per-kategori-dengan-status-promo-utama?access_token=a546373f0b26e515aa3c375854b8a939)**

#### Query 3b — Perbandingan Agregat: Kategori Dengan Promo vs Tanpa Promo

```sql
WITH category_ratings AS (
    SELECT
        fr.category_sk,
        ROUND(AVG(fr.rating), 2)                                 AS avg_rating,
        COUNT(fr.review_id)                                      AS review_count
    FROM  marts.fact_review  fr
    GROUP BY fr.category_sk
),
promo_category_sks AS (
    -- Surrogate key kategori yang punya kampanye aktif
    -- Join via integer category_id untuk keandalan
    SELECT DISTINCT dc.category_sk
    FROM  marts.dim_campaign dcm
    JOIN  marts.dim_category dc  
      ON (dcm.category_id = dc.category_id OR dcm.category_id IS NULL)
    WHERE dcm.is_active = TRUE
)
SELECT
    CASE
        WHEN pc.category_sk IS NOT NULL THEN 'Dengan Promo Aktif'
        ELSE 'Tanpa Promo Aktif'
    END                                                          AS promo_group,
    COUNT(*)                                                     AS jumlah_kategori,
    ROUND(AVG(cr.avg_rating), 2)                                 AS rata_rata_rating_group,
    SUM(cr.review_count)                                         AS total_reviews

FROM  category_ratings  cr
LEFT  JOIN promo_category_sks pc  ON cr.category_sk = pc.category_sk

GROUP BY promo_group
ORDER BY rata_rata_rating_group DESC;
```

**Click Result Query dibawah ini 👇**
**[Perbandingan Agregat: Kategori Dengan Promo vs Tanpa Promo](https://popsql.com/queries/-OqkYi2kB5hzPsC4jPFr/perbandingan-agregat-kategori-dengan-promo-vs-tanpa-promo?access_token=39ce5f3882f2099f89e3bbe43f0efc3a)**

#### Query 3c — List Kampanye Beserta Kategori Target dan Avg Rating Produknya

```sql
SELECT
    dcm.campaign_name,
    COALESCE(dc.name, 'ALL CATEGORIES')                          AS target_category,
    dcm.target_scope,
    dcm.discount_pct,
    dcm.channel,
    dcm.start_date,
    dcm.end_date,
    dcm.is_active,

    -- Avg rating produk dalam kategori target kampanye ini
    ROUND(AVG(fr.rating), 2)                                     AS avg_product_rating,
    COUNT(fr.review_id)                                          AS total_reviews,

    -- Flag performa ulasan berdasarkan threshold rating
    CASE
        WHEN AVG(fr.rating) >= 4.5 THEN 'SANGAT BAIK'
        WHEN AVG(fr.rating) >= 4.0 THEN 'BAIK'
        WHEN AVG(fr.rating) >= 3.0 THEN 'CUKUP'
        WHEN AVG(fr.rating) IS NULL THEN 'BELUM ADA REVIEW'
        ELSE 'PERLU PERHATIAN'
    END                                                          AS rating_label

FROM  marts.dim_campaign  dcm

-- Join via integer category_id atau IS NULL jika kampanye lintas kategori
LEFT  JOIN marts.dim_category dc  
      ON (dcm.category_id = dc.category_id OR dcm.category_id IS NULL)

-- Join ke fact_review via surrogate key untuk mendapat data rating
LEFT  JOIN marts.fact_review  fr  ON dc.category_sk = fr.category_sk

GROUP BY
    dcm.campaign_sk,
    dcm.campaign_name,
    dc.name,
    dcm.target_scope,
    dcm.discount_pct,
    dcm.channel,
    dcm.start_date,
    dcm.end_date,
    dcm.is_active

ORDER BY avg_product_rating DESC NULLS LAST;
```

**Click Result Query dibawah ini 👇**
**[Result Query List Kampanye Beserta Kategori Target dan Avg Rating Produknya](https://popsql.com/queries/-OqkZ7rg50Ax7yVgGLst/list-kampanye-beserta-kategori-target-dan-avg-rating-produknya?access_token=543297c5c642cf20c747bc213c306f3d)**

---

## Ringkasan — Mapping Pertanyaan Bisnis ke Schema OLAP

| Pertanyaan                 | Fact Table       | Dimensi yang Dibutuhkan                  | Measures                                         | Filter Kunci                |
| -------------------------- | ---------------- | ---------------------------------------- | ------------------------------------------------ | --------------------------- |
| P1 — Revenue & Margin     | `fact_sales`   | `dim_date`, `dim_category`           | `revenue`, `gross_margin`, `margin_pct`    | —                          |
| P2 — Segmentasi Pelanggan | `fact_payment` | `dim_customer`, `dim_payment_method` | `amount`, `usage_count`, `avg_order_value` | `payment_status = 'paid'` |
| P3 — Efektivitas Kampanye | `fact_review`  | `dim_category`, `dim_campaign`       | `avg_rating`, `review_count`                 | `is_active = TRUE`        |

### Alur Join per Pertanyaan

```
P1:  fact_sales ──── dim_date        (order_date_id = date_id)
              └──── dim_category     (category_sk = category_sk)

P2:  fact_payment ── dim_customer          (customer_sk = customer_sk)
               └─── dim_payment_method    (method_sk = method_sk)

P3:  fact_review ─── dim_category          (category_sk = category_sk)
                       └── dim_campaign    (category_id = category_id) ← integer join
```
