SANDBOX RELEASE FILE MAKER
==========================


HOW TO USE?
-----------

1. Get the Clickhouse binary from https://github.com/ClickHouse/ClickHouse/releases
   (select the clickhouse-common-static file).


2. Run (needs ~100GB RAM):
   ```./run.sh \
          <PATH TO MUNGED (.txt.gz)> \
          <PATH TO TSV-GZIPPED OUTPUT (.txt.gz)> \
          <PATH TO PARQUET OUTPUT (.parquet)>
   ```
