# Running TPC-H Queries on Iceberg Tables from PostgreSQL

1. **Create Iceberg tables via** `create_iceberg_tables.sql`
2. (optional) **Download files to the NVMe drive via** `cache_iceberg_tables.sql`
3. **Create Postgres (heap) tables with indexes via** `create_heap_tables.sql`
4. **Set** `search_path` **to** `tpch_10_iceberg` **or** `tpch_10_heap`
5. **Run queries** (`q1.sql` ... `q22.sql`)
