DROP SCHEMA IF EXISTS tpch_10_iceberg CASCADE;

CREATE SCHEMA tpch_10_iceberg;

SET search_path TO tpch_10_iceberg;

CREATE FOREIGN TABLE lineitem()
SERVER crunchy_lake_analytics OPTIONS(
    format 'iceberg',
    path 's3://crunchy-analytics-datasets-use1/tpch_iceberg/scale_10/public/lineitem/metadata/00000-4985a590-a845-407b-9b2d-5f8e104b4083.metadata.json'
);

CREATE FOREIGN TABLE nation()
SERVER crunchy_lake_analytics OPTIONS(
    format 'iceberg',
    path 's3://crunchy-analytics-datasets-use1/tpch_iceberg/scale_10/public/nation/metadata/00000-c5b512fb-930e-45d6-a2a5-344aa64f8de2.metadata.json'
);

CREATE FOREIGN TABLE customer()
SERVER crunchy_lake_analytics OPTIONS(
    format 'iceberg',
    path 's3://crunchy-analytics-datasets-use1/tpch_iceberg/scale_10/public/customer/metadata/00000-63b9cc3e-fe13-45b8-a502-89e931430965.metadata.json'
);

CREATE FOREIGN TABLE orders()
SERVER crunchy_lake_analytics OPTIONS(
    format 'iceberg',
    path 's3://crunchy-analytics-datasets-use1/tpch_iceberg/scale_10/public/orders/metadata/00000-52494914-8b94-4e10-a27f-025085adc602.metadata.json'
);

CREATE FOREIGN TABLE part()
SERVER crunchy_lake_analytics OPTIONS(
    format 'iceberg',
    path 's3://crunchy-analytics-datasets-use1/tpch_iceberg/scale_10/public/part/metadata/00000-21017d13-755a-4d08-aa64-466c53d5463d.metadata.json'
);

CREATE FOREIGN TABLE partsupp()
SERVER crunchy_lake_analytics OPTIONS(
    format 'iceberg',
    path 's3://crunchy-analytics-datasets-use1/tpch_iceberg/scale_10/public/partsupp/metadata/00000-b95482fd-2d39-44f7-b6c5-12a2dab3e0de.metadata.json'
);

CREATE FOREIGN TABLE region()
SERVER crunchy_lake_analytics OPTIONS(
    format 'iceberg',
    path 's3://crunchy-analytics-datasets-use1/tpch_iceberg/scale_10/public/region/metadata/00000-e08f411f-fea5-44f6-9c91-4078d5447cac.metadata.json'
);

CREATE FOREIGN TABLE supplier()
SERVER crunchy_lake_analytics OPTIONS(
    format 'iceberg',
    path 's3://crunchy-analytics-datasets-use1/tpch_iceberg/scale_10/public/supplier/metadata/00000-9924536c-5e2a-42a1-a81f-29faab535e0e.metadata.json'
);