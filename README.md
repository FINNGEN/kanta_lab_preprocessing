
# KANTA LAB values preprocessing & QC

Based on Kira Detrois' [existing repo](https://github.com/detroiki/kanta_lab).

# OUTPUT



## Output Columns

| #   | Column Name | Easy Description | General Notes | Technical Notes |
| --- | --- | --- | --- | --- |
| 1   | `FINNGENID` | Pseudoanonimized IDs |     |     |
| 2   | `EVENT_AGE` | Age of the individual at the time fo the event |     |     |
| 3   | `APPROX_EVENT_DATETIME` | Approximate event (+- two weeks) of the event |
| 4   | `TEST_ID` | National (THL) or local lab ID of the measurement |     |     |
| 5   | `TEST_ID_SYSTEM` | Source of the lab ID | 0: local and 1: national (THL) |    
| 6   | `CODING_SYSTEM` | Service provider  | Used to be the provider ID, now it's a more complex ID that we haven't quite cracked yet.|  
| 7   | `RESULT_ABNORMALITY` | Abnormality of the lab measurement | Describes whether the test is result is normal or abnormal i.e. too high or low low based on the laboratories reference values. This is **not** a quality control variable but to state it simply and inaccurately denotes whether the patient is healthy or not. See [AR/LABRA - Poikkeustilanneviestit](https://91.202.112.142/codeserver/pages/publication-view-page.xhtml?distributionKey=10329&versionKey=324&returnLink=fromVersionPublicationList) for the abbreviations meanings. | The column contains a lot of missingness. |
| 8  | `MEASUREMENT_STATUS` | The measurement status | The final data contains only `C` \- corrected results or `F` \- final result | See [Koodistopalvelu - AR/LABRA - Tutkimusvastauksien tulkintakoodit 1997](https://koodistopalvelu.kanta.fi/codeserver/pages/publication-view-page.xhtml?distributionKey=2637&versionKey=321&returnLink=fromVersionPublicationList) |
| 9  | `REFERENCE_RANGE_GROUP` | The reference values for the measurement in text form | This can be used to define the lab abnormality with better coverage using regex expressions (-to be implemented for the whole data). |     |
| 10...13  | `REFERENCE_RANGE_[LOWER|UPPER]_[VALUE|UNIT]` | Reference lower|upper value|unit | Mostly mutually exclusive with `REFERENCE_RANGE_GROUP`     |
| 14| `TEST_NAME_ABBREVIATION` | Test abbreviation of the measurement from the data (local) or mapped using the THL map (national) |     | The map for the national (THL) IDs is in the [data folder](/finngen_qc//data/thl_lab_id_abbrv_map.tsv),  and was downloaded from [Kuntaliitto - Laboratoriotutkimusnimikkeistö](https://koodistopalvelu.kanta.fi/codeserver/pages/classification-view-page.xhtml?classificationKey=88&versionKey=120) |
| 15   | `MEASUREMENT_VALUE` | The  numeric value of the measurement |     |     |
| 16   | `MEASUREMENT_UNIT` | The unit of the measurement from the data |     |     |
| 17| `harmonization_omop::MEASUREMENT_VALUE` | The  harmonized numeric value of the measurement |     | The conversion happens from  [a table](/finngen_qc/data/quantity_source_unit_conversion.tsv) and by defining a target unit from [another table](/finngen_qc/data/harmonization_counts.txt) built using the most common unit for each concept ID|
| 18| `harmonization_omop::MEASUREMENT_UNIT` | The harmonized unit of the measurement from the data |     |     | |
| 19| `harmonization_omop::CONVERSION_FACTOR` | The conversion factor used to map the value columns |     |     | |
| 20| `harmonization_omop::IS_UNIT_VALID` | Boolean column with internal info about the unit |     | Internal use    | 
| 21| `harmonization_omop::mappingStatus` | String column with internal info about the mapping|   | Internal use   | 
| 22| `harmonization_omop::sourceCode` | String column with internal info about the abbreviation/unit|  |  Internal use   | 
| 23| `harmonization_omop::OMOP_ID` | OMOP id of the mapping |  | The mapping is done using [a table](/finngen_qc/data/LABfi_ALL.usagi.csv)   | 
| 24| `harmonization_omop::omopQuantity` | Quantity associated to the OMOP id of the mapping |  | The mapping is done using [a table](/finngen_qc/data/LABfi_ALL.usagi.csv)   |
| 25| `source::MEASUREMENT_VALUE` | The original numeric value of the measurement |     |     |
| 25| `source::MEASUREMENT_UNIT` | The original unit of the measurement |     |     |
| 25| `source::TEST_NAME_ABBREVIATION` | The original abbreviation of the measurement |     |     |

The raw to output column mapping is as follows:

| Column in raw file           | Description              |
|------------------------------|--------------------------|
|FINNGENID|FINNGENID|
|EVENT_AGE|EVENT_AGE|
|tutkimuskoodistonjarjestelmaid|CODING_SYSTEM|
|paikallinentutkimusnimike|TEST_NAME_ABBREVIATION|
|tutkimustulosarvo|MEASUREMENT_VALUE|
|tutkimustulosyksikko|MEASUREMENT_UNIT|
|tutkimusvastauksentilaid|MEASUREMENT_STATUS|
|tuloksenpoikkeavuusid|RESULT_ABNORMALITY|
|viitearvoryhma|REFERENCE_RANGE_GROUP|
|viitevalialkuarvo|REFERENCE_RANGE_LOWER_VALUE|
|viitevalialkuyksikko|REFERENCE_RANGE_LOWER_UNIT|
|viitevaliloppuarvo|REFERENCE_RANGE_UPPER_VALUE|
|viitevaliloppuyksikko|REFERENCE_RANGE_UPPER_UNIT|



# TECHNICAL INFO


## Summary

There is a [config file](/finngen_qc/magic_config.py) that contains all the relevant "choices" about how to manipulate the data (e.g. which columns to include, how to rename columns, which values of which column to include etc.) so there are virtually no hard coded elements in the code it self.

First the [wdl](/finngen_qc/wdl/sort_dup.wdl) trims the data of only the relevant columns (taken from the config) and sorts it by a series of columns (also specified in the config) so we can also discard dupliate entries. ATM we work with patient ID,date and measurement abbreviation.

The code then performs the following actions:
[MINIMAL](/finngen_qc/filters/filter_minimal.py)
- output columns are initialized
- spaces are removed everywhere in the text
- the date is built in the right format
- all type of NA/missing values (Puuttuu,"_" etc.) are replaced with "NA"
- entries with invalid hetu root are removed (and logged)
- entries with invalid measurement status are removed (and logged)
- TEST_ID_SYSTEM is created checking if `laboratoriotutkimusnimikeid` is not NA (1 national/0 regional)
- TEST_ID is created assigning the regional id for regional labs and a 
- TEST_NAME_ABBREVIATION is updated for national labs [through a mapping](/finngen_qc/data/thl_lab_id_abbrv_map.tsv)
- CODING_SYSTEM is updated when available (problematic ATM, see table above)
- TEST_NAME_ABBREVIATIONs with problematic characters are edited (see `abbreviation_replacements` in the config)
- RESULT_ABNORMALITY values that are not in the accepted list are removed (and logged)

[UNIT](/finngen_qc/filters/fix_unit.py)
-     Fixes strange characters in lab unit field. Also moves to lower case for non NA values.
- Mapping of units. This can be done either via regex (from config) or [through a mapping](/finngen_qc/data/unit_mapping.txt)
- RESULT_ABNORMALITY is edited to be consistent with the standard definition see AR/LABRA - Poikkeustilanneviestit. This means replacing `<` with `L`, `>` with `H`, `POS` with `A` and `NEG` with `N`.

[harmonization](/finngen_qc/filters/harmonization.py)
- Mapping status is updated (internal thing)
-   IS_UNIT_VALID column is populated based on whether the unit is in usagi list
-   Harmonizes units to make sure all abbreviations with similar units are mapped to same one (e.g. mg --> mg/24h for du-prot). Based on [a table](/finngen_qc/data/fix_unit_based_in_abbreviation.tsv) 
- OMOP mapping from [a table](/finngen_qc/data/LABfi_ALL.usagi.csv)
- unit harmonization (optional) from [a table](/finngen_qc/data/quantity_source_unit_conversion.tsv)

## How it works
The script reads in the data in chunks of  `--chunksize` length and it processes the lines with python's pandas. With the flag `--mp` and `--jobs` the script runs each chunk into other smaller subchunks in parallel (efficiency TBD). The [filter folder](/finngen_qc/filters/) contains separate scripts that perform conceptually separate tasks. Each of them contains a global function of the same name of the script that gathers all individual functions that populate the script. In this way we can easily compartmentalize the munging/qc and add new features.


```
usage: main.py [-h] [--raw-data RAW_DATA] [--log {critical,error,warn,warning,info,debug}] [--test] [--gz] [--mp [MP]] [-o OUT] [--prefix PREFIX] [--sep SEP] [--chunk-size CHUNK_SIZE] [--lines LINES] [--unit-map {regex,map,none}] [--harmonization [HARMONIZATION]]

Kanta Lab preprocessing pipeline: raw data ⇒ clean data.

options:
  -h, --help            show this help message and exit
  --raw-data RAW_DATA   Path to input raw file. File should be tsv.
 --log {critical,error,warn,warning,info,debug}Provide logging level. Example '--log debug', default = 'warning'
  --test                Reads first chunk only
  --gz                  Ouputs to gz
  --mp [MP]             Flag for multiproc. Default is '0' (no multiproc). If passed it defaults to cpu count, but one can also specify the number of cpus to use: e.g. '--mp' or '--mp 4'.
  -o OUT, --out OUT     Folder in which to save the results (default = current working directory)
  --prefix PREFIX       Prefix of the out files (default = 'kanta_YYYY_MM_DD')
  --sep SEP             Separator (default = tab)
  --chunk-size CHUNK_SIZE      Number of rows to be processed by each chunk (default = '100').
  --lines LINES         Number of lines in input file (calculated/estimated otherwise).
  --unit-map {regex,map,none}  How to replace units. Map uses the unit_mapping.txt mapping in data and regex after. Regex does only regex. none skips it entirely.
  --harmonization [HARMONIZATION]  Path to tsv with concept id and target unit.

```

