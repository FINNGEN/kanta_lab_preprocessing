# KANTA LAB values preprocessing & QC

Based on Kira Detrois' [existing repo](https://github.com/detroiki/kanta_lab).

# TARGET OUTPUT

Based on Kira's work the expected output should be formatted in the following way.

## Output Columns

| #   | Column Name | Easy Description | General Notes | Technical Notes |
| --- | --- | --- | --- | --- |
| 1   | `FINREGISTRYID` | Pseudoanonimized IDs |     |     |
| 2   | `LAB_DATE_TIME` | Date and time of lab measurement |     |     |
| 3   | `LAB_SERVICE_PROVIDER` | Service provider | This is NOT the lab performing the test but the service provider that ordered the test see Section [Important Notes](#final_notes) for more details. | The original data contains uses OIDs  ([OID-yksilöintitunnukset](https://thl.fi/aiheet/tiedonhallinta-sosiaali-ja-terveysalalla/ohjeet-ja-soveltaminen/koodistopalvelun-ohjeet/oid-yksilointitunnukset)). These were mapped to a readable string based on the city where the service provider is registered i.e. *HUS is mapped to Helsinki_1301*. The map is in the [data folder](/finngen_qc/data/thl\_sote\_map_named.tsv) and based on [THL - SOTE-organisaatiorekisteri 2008](https://koodistopalvelu.kanta.fi/codeserver/pages/classification-view-page.xhtml?classificationKey=421&versionKey=501). |
| 4   | `LAB_ID` | National (THL) or local lab ID of the measurement |     |     |
| 5   | `LAB_ID_SOURCE` | Source of the lab ID | 0: local and 1: national (THL) |     |
| 6   | `LAB_ABBREVIATION` | Laboratory abbreviation of the measurement from the data (local) or mapped using the THL map (national) |     | The map for the national (THL) IDs is in the [data folder](/finngen_qc//data/thl_lab_id_abbrv_map.tsv),  and was downloaded from [Kuntaliitto - Laboratoriotutkimusnimikkeistö](https://koodistopalvelu.kanta.fi/codeserver/pages/classification-view-page.xhtml?classificationKey=88&versionKey=120) |
| 7   | `LAB_VALUE` | The value of the laboratory measurement |     |     |
| 8   | `LAB_UNIT` | The unit of the labroatroy measurement from the data |     |     |
| 9   | `LAB_ABNORMALITY` | Abnormality of the lab measurement | Describes whether the test is result is normal or abnormal i.e. too high or low low based on the laboratories reference values. This is **not** a quality control variable but to state it simply and inaccurately denotes whether the patient is healthy or not. See [AR/LABRA - Poikkeustilanneviestit](https://91.202.112.142/codeserver/pages/publication-view-page.xhtml?distributionKey=10329&versionKey=324&returnLink=fromVersionPublicationList) for the abbreviations meanings. | The column contains a lot of missingness. |
| 10  | `MEASUREMENT_STATUS` | The measurement status | The final data contains only `C` \- corrected results or `F` \- final result | See [Koodistopalvelu - AR/LABRA - Tutkimusvastauksien tulkintakoodit 1997](https://koodistopalvelu.kanta.fi/codeserver/pages/publication-view-page.xhtml?distributionKey=2637&versionKey=321&returnLink=fromVersionPublicationList) |
| 11  | `REFERENCE_VALUE_TEXT` | The reference values for the measurement in text form | This can be used to define the lab abnormality with better coverage using regex expressions (-to be implemented for the whole data). |     |

The raw to output column mapping is as follows:

| Column in raw file           | Description              |
|------------------------------|--------------------------|
| potilashenkilotunnus         | FINREGISTRYID            |
| tutkimusaika                 | TEST_DATE_TIME           |
| palvelutuottaja_organisaatio | TEST_SERVICE_PROVIDER    |
| paikallinentutkimusnimike    | TEST_NAME_ABBREVIATION   |
| tutkimustulosarvo            | MEASUREMENT_VALUE        |
| tutkimustulosyksikko         | MEASUREMENT_UNIT         |
| tutkimusvastauksentila       | MEASUREMENT_STATUS       |
| tuloksenpoikkeavuus          | RESULT_ABNORMALITY       |
| viitevaliteksti              | TEST_REFERENCE_TEXT      |
| viitearvoryhma               | TEST_REFERENCE_GROUP     |
| viitevalialkuarvo            | TEST_REFERENCE_MIN_VALUE |
| viitevalialkuyksikko         | TEST_REFERENCE_MIN_UNIT  |
| viitevaliloppuarvo           | TEST_REFERENCE_MAX_VALUE |
| viitevaliloppuyksikko        | TEST_REFERENCE_MAX_UNIT  |


The following columns are also needed for processing

| Column in raw file      | Usage                                                                                |
|-------------------------|--------------------------------------------------------------------------------------|
| hetu_root               | Filter out if current hetu root is not 1.2.246.21 (they are manually assigned hetus) |


Possible other columns to include?

| Column in raw file     | Description         |
|------------------------|---------------------|
| tietojarjestelmanimi   | DATA_SYSTEM_NAME    |
| tietojarjestelmaversio | DATA_SYSTEM_VERSION |

# TECHNICAL INFO


## Summary

There is a [config file](/finngen_qc/magic_config.py) that contains all the relevant "choices" about how to manipulate the data (e.g. which columns to include, how to rename columns, which values of which column to include etc.) so there are virtually no hard coded elements in the code it self.

First the [wdl](/finngen_qc/wdl/sort_dup.wdl) trims the data of only the relevant columns (taken from the config) and sorts it by a series of columns (also specified in the config) so we can also discard dupliate entries. ATM we work with patient ID,date and measurement abbreviation.

The code then performs the following actions:
- output columns are initialized
- spaces are removed everywhere in the text
- all type of NA/missing values (Puuttuu,"_" etc.) are replaced with "NA"
- entries with invalid hetu root are removed (and logged)
- entries with invalid measurement status are removed (and logged)
- TEST_ID_SOURCE is created checking if `laboratoriotutkimusnimikeid` is not NA (1 national/0 regional)
- TEST_ID is created assigning the regional id for regional labs and a 
- TEST_NAME_ABBREVIATION is updated for national labs [through a mapping](/finngen_qc/data/thl_lab_id_abbrv_map.tsv)
- MEASUREMENT_UNIT s are removed of problematich characters (!,?,etc)
- MEASUREMENT_UNIT is edited through an [approved mapping](/finngen_qc/data/unit_mapping.txt) and regex (logged)
- RESULT_ABNORMALITY values that are not in the accepted list are removed (and logged)

## How it works
The script reads in the data in chunks of  `--chunksize` length and it processes the lines with python's pandas. With the flag `--mp` and `--jobs` the script runs each chunk into other smaller subchunks in parallel (efficiency TBD). The [filter folder](/finngen_qc/filters/) contains separate scripts that perform conceptually separate tasks. Each of them contains a global function of the same name of the script that gathers all individual functions that populate the script. In this way we can easily compartmentalize the munging/qc and add new features.



```
usage: main.py [-h] [--raw-data RAW_DATA] [--log {critical,error,warn,warning,info,debug}] [--test] [--mp [MP]] [-o OUT] [--prefix PREFIX] [--sep SEP] [--chunk-size CHUNK_SIZE]

Kanta Lab preprocessing pipeline: raw data ⇒ clean data.

options:
  -h, --help            show this help message and exit
  --raw-data RAW_DATA   Path to input raw file. File should be tsv.
  --log {critical,error,warn,warning,info,debug}
                        Provide logging level. Example '--log debug', default = 'warning'
  --test                Reads first chunk only
  --mp [MP]             Flag for multiproc. Default is '0' (no multiproc). If passed it defaults to cpu count, but one can also specify the number of cpus to use: e.g. '--mp' or
                        '--mp 4'.
  -o OUT, --out OUT     Folder in which to save the results (default = current working directory)
  --prefix PREFIX       Prefix of the out files (default = 'kanta')
  --sep SEP             Separator (default = tab)
  --chunk-size CHUNK_SIZE
                        Number of rows to be processed by each chunk (default = '100').
```

E.g.

```
python3 main.py --log info --chunk-size 100000 --mp --out /mnt/disks/data/kanta/results/ --raw-data /mnt/disks/data/kanta/tests/mock_full.txt.gz --prefix kanta_full_mock 
```


## OMOP mapping (check)

`omop_check.py` is meant to have a preview of the future OMOP mapping.
It takes the mapping from [the data folder](/finngen_qc/data/mapping_abbreviation_and_unit.tsv) and pretty much replicates the main script by only applying the omop mapping to the desired columns. It splits the inputs in:
- [ROOT]_omop_success.txt
- [ROOT]_omop_failed.txt

with an added `OMOP_ID` column, which is `NA` for the failed file.


The usage is similar to the main script:
```
python3 ~/Dropbox/Projects/kanta_lab_preprocessing/finngen_qc/omop_check.py  --raw-data /mnt/disks/data/kanta/results/kanta_1M_munged.txt --chunk-size 320000 --mp
```

It will default produce the outputs in the same director of the input file and with the same prefix. Both parameters can be changed.