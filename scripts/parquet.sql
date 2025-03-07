COPY (
  SELECT
    FINNGENID,
    SEX,
    CAST(EVENT_AGE AS FLOAT) AS EVENT_AGE,
    CAST(APPROX_EVENT_DATETIME AS TIMESTAMP) AS APPROX_EVENT_DATETIME,
    CAST("cleaned::TEST_NAME_ABBREVIATION" AS VARCHAR) AS TEST_NAME,
    TEST_ID,
    CAST(TEST_ID_IS_NATIONAL AS BOOLEAN) AS TEST_ID_IS_NATIONAL,
    CAST(NULLIF("harmonization_omop::OMOP_ID", -1) AS INT) AS OMOP_CONCEPT_ID
    FROM read_csv(
      '/mnt/disks/data/kanta/test/test.txt.gz',
      delim = '\t',
      header = true
    )
   ORDER BY 
     FINNGENID,
     SEX,
     APPROX_EVENT_DATETIME,
     EVENT_AGE,
     OMOP_CONCEPT_ID,
     TEST_NAME,
     TEST_ID,
     TEST_ID_IS_NATIONAL
)
  TO '/mnt/disks/data/kanta/test/test.parquet' (FORMAT 'parquet', COMPRESSION 'zstd');
