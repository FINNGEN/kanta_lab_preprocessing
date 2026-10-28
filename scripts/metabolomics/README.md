# metabolomics

Unpacks the Nightingale metabolomics panel, whose results exist only as free text, into two tables (issue #45).

## Main

**`unpack_metabolomics.py`**: handles the biomarker panel and the risk-score tests:

- S-NGMuut (TEST_ID 11282): split into one row per biomarker
- the 8 risk-score tests (11274–11281): one row per result, with the risk category, risk %, and comparison percentile parsed out

```
python3 unpack_metabolomics.py [--input <engine_output>.parquet] [--out DIR] [--test]
```

Outputs:

- `nightingale_results_metabolomics.tsv.gz`: one row per extracted biomarker, with the Finnish name, the English name, the value and the unit
- `nightingale_results_risks.tsv.gz`: one row per risk-score result

```
… TEST_NAME  RISK_CATEGORY  RISK_PCT  COMPARISON_PERCENTILE  AGE_GROUP  SEX_GROUP  RAW_TEXT  …
… (one row per result; values are kept as raw strings, e.g. ">6.5")
```
