# tugas_dwib_data_orchestration# 📦 Data Orchestration with Apache Airflow

Project ini merupakan implementasi orkestrasi ETL pipeline dan data warehouse menggunakan **Apache Airflow** dan **DuckDB**, sebagai bagian dari tugas Data Orchestration.

## 🧩 Struktur Proyek
```
├── dags/
│   ├── etl_pipeline.py             # DAG utama untuk ETL DuckDB
│   ├── quality_checks.py           # DAG untuk pengecekan kualitas data
├── docs/
│   ├── architecture_diagram.png    # Diagram arsitektur DAG
│   ├── scheduling_strategy.md      # Strategi penjadwalan dan partisi
│   ├── data_quality_metrics.md     # Dokumentasi metrik kualitas data
├── docker-compose.yaml            # Environment Apache Airflow
├── README.md                      # Dokumentasi proyek
```

## ⚙️ Teknologi yang Digunakan
- Apache Airflow (orchestration)
- DuckDB (data warehouse lokal)
- Pandas (transformasi data)
- Mermaid.js (diagram DAG)

## 🚀 Fitur Utama
- ETL pipeline terorkestrasi: extract, transform, load ke DuckDB
- DAG tambahan untuk pengecekan kualitas data
- Email notifikasi dan logika branching
- Strategi penjadwalan harian

## 🔄 DAG yang Tersedia

### 1. `etl_pipeline_full`
- Ekstraksi data CSV
- Transformasi menjadi star schema
- Load ke DuckDB

### 2. `data_quality_check`
- Cek null pada `CustomerKey`
- Cek harga negatif pada `UnitPrice`

## 📬 Notifikasi & Branching
- Email akan dikirim jika rata-rata umur pelanggan > 40 tahun
- Digunakan `BranchPythonOperator` untuk memutuskan perlu kirim email atau tidak

## 📅 Penjadwalan
- Semua DAG dijalankan setiap hari (`@daily`)
- `data_quality_check` tergantung pada keberhasilan DAG ETL

## 📈 Visualisasi
Data hasil ETL ini siap divisualisasikan menggunakan:
- **Metabase** atau **Superset** (untuk query langsung ke DuckDB)
- **Grafana** (jika DuckDB dipasangi plugin SQLite)

Contoh query untuk visualisasi:
```sql
SELECT Country, COUNT(*) AS TotalOrders
FROM FactSales
JOIN DimCustomer USING (CustomerKey)
GROUP BY Country
ORDER BY TotalOrders DESC;
```

## 🧪 Pengujian
- Semua DAG telah berhasil dieksekusi dan diuji via UI Airflow
- Tangkapan layar disimpan di laporan pengumpulan

## 📝 Author
Kahfi - [GitHub](https://github.com/kahfi117)
Satno — [GitHub](https://github.com/satnodeus77)
Zuri — [GitHub](https://github.com/riadizur)