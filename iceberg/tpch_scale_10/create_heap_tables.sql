-- create tables with heap storage (e.g., regular Postgres tables)
-- use the same data sets

DROP SCHEMA IF EXISTS tpch_10_heap CASCADE;

CREATE SCHEMA tpch_10_heap;
SET search_path TO tpch_10_heap;
CREATE TABLE lineitem()
WITH (load_from = 's3://crunchy-analytics-datasets-use1/tpch_iceberg/scale_10/public/lineitem/data/*.parquet');

CREATE TABLE nation()
WITH (load_from = 's3://crunchy-analytics-datasets-use1/tpch_iceberg/scale_10/public/nation/data/*.parquet');

CREATE TABLE customer()
WITH (load_from = 's3://crunchy-analytics-datasets-use1/tpch_iceberg/scale_10/public/customer/data/*.parquet');

CREATE TABLE orders()
WITH (load_from = 's3://crunchy-analytics-datasets-use1/tpch_iceberg/scale_10/public/orders/data/*.parquet');

CREATE TABLE part()
WITH (load_from = 's3://crunchy-analytics-datasets-use1/tpch_iceberg/scale_10/public/part/data/*.parquet');

CREATE TABLE partsupp()
WITH (load_from = 's3://crunchy-analytics-datasets-use1/tpch_iceberg/scale_10/public/partsupp/data/*.parquet');

CREATE TABLE region()
WITH (load_from = 's3://crunchy-analytics-datasets-use1/tpch_iceberg/scale_10/public/region/data/*.parquet');

CREATE TABLE supplier()
WITH (load_from = 's3://crunchy-analytics-datasets-use1/tpch_iceberg/scale_10/public/supplier/data/*.parquet');


-- create indexes
CREATE INDEX IDX_SUPPLIER_NATION_KEY ON SUPPLIER (S_NATIONKEY);
CREATE INDEX IDX_PARTSUPP_PARTKEY ON PARTSUPP (PS_PARTKEY);
CREATE INDEX IDX_PARTSUPP_SUPPKEY ON PARTSUPP (PS_SUPPKEY);
CREATE INDEX IDX_CUSTOMER_NATIONKEY ON CUSTOMER (C_NATIONKEY);
CREATE INDEX IDX_ORDERS_CUSTKEY ON ORDERS (O_CUSTKEY);
CREATE INDEX IDX_LINEITEM_ORDERKEY ON LINEITEM (L_ORDERKEY);
CREATE INDEX IDX_LINEITEM_PART_SUPP ON LINEITEM (L_PARTKEY,L_SUPPKEY);
CREATE INDEX IDX_NATION_REGIONKEY ON NATION (N_REGIONKEY);
CREATE INDEX IDX_LINEITEM_SHIPDATE ON LINEITEM (L_SHIPDATE, L_DISCOUNT, L_QUANTITY);
CREATE INDEX IDX_ORDERS_ORDERDATE ON ORDERS (O_ORDERDATE);