# KANTA LAB values preprocessing & QC

Based on Kira Detrois' [existing repo](https://github.com/detroiki/kanta_lab).

## Output Columns

| #   | Column Name | Easy Description | General Notes | Technical Notes |
| --- | --- | --- | --- | --- |
| 1   | `FINNGENID` | Pseudoanonimized IDs |     |     |
| 2   | `EVENT_AGE` | Age of the individual at the time fo the event |     |     |
| 3   | `APPROX_EVENT_DATETIME` | Approximate event (+- two weeks) of the event |
| 4   | `TEST_ID` | National (THL) or local lab ID of the measurement |     |     |
| 5   | `TEST_ID_SYSTEM` | Source of the lab ID | 0: local and 1: national (THL) |    
| 6   | `CODING_SYSTEM` | Service provider  | Used to be the provider ID, now it's a more complex ID that we haven't quite cracked yet.|  
| 7   | `CODING_SYSTEM_MAP` | Service provider mapped via Business Identity Code  |From a table in[data folder](/finngen_qc//data/thl_coding_manual_mapping.txt)| We noticed that the central digits in most codes were BIC  (sometimes missing leadin 0) and thus we could extract the macro information from the [bigger table](/finngen_qc//data/thl_sote_map_named.tsv). The result is not as granular as all subfields (e.g. ```Espoo_221```) are now merged into just `Espoon_Kaupunki (City of Espoo)`. |
| 8   | `TEST_OUTCOME` | Abnormality of the lab measurement | Describes whether the test result is normal or abnormal i.e. too high or low low based on the laboratories reference values. This is **not** a quality control variable but to state it simply and inaccurately denotes whether the patient is healthy or not. See [AR/LABRA - Poikkeustilanneviestit](https://91.202.112.142/codeserver/pages/publication-view-page.xhtml?distributionKey=10329&versionKey=324&returnLink=fromVersionPublicationList) for the abbreviations. | The column contains a lot of missingness. |
| 9  | `imputed::TEST_OUTCOME` | Imputed abnormality values of the lab measurement | Values are ```H,L,N```  for high,low and normal. All entries with valid OMOP ids with >100 counts were imputed, including already labelled ones. Entries with `TEST_OUTCOME` but with non-numeric measurement values were *not* taken into account. The `*` sign indicates that there were issues in defining the threshold for `H/L` thresholds. | In the data folder there's a [table](/finngen_qc/data/abnormality_estimation.table.tsv) that shows the values used to determine lower/higher limits for each OMOP id. `+-inf` values lack enough labels to define a threshold. The `PROBLEM` columns indicate instead the opposite issue, where is there's an imbalance between H/L labels and `N` so the median value of the OMOP id is returned. |
| 10  | `MEASUREMENT_STATUS` | The measurement status | The final data contains only `C` \- corrected results or `F` \- final result | See [Koodistopalvelu - AR/LABRA - Tutkimusvastauksien tulkintakoodit 1997](https://koodistopalvelu.kanta.fi/codeserver/pages/publication-view-page.xhtml?distributionKey=2637&versionKey=321&returnLink=fromVersionPublicationList) |
| 11| `REFERENCE_RANGE_GROUP` | The reference values for the measurement in text form | This can be used to define the lab abnormality with better coverage using regex expressions (-to be implemented for the whole data). |     |
| 12...15| `REFERENCE_RANGE_[LOWER|UPPER]_[VALUE|UNIT]` | Reference lower|upper value|unit | Mostly mutually exclusive with `REFERENCE_RANGE_GROUP`     |
| 16| `cleaned::TEST_NAME_ABBREVIATION` | Test abbreviation of the measurement from the data (local) or mapped using the THL map (national) |     | The map for the national (THL) IDs is in the [data folder](/finngen_qc//data/thl_lab_id_abbrv_map.tsv),  and was downloaded from [Kuntaliitto - Laboratoriotutkimusnimikkeistö](https://koodistopalvelu.kanta.fi/codeserver/pages/classification-view-page.xhtml?classificationKey=88&versionKey=120) |
| 17| `cleaned::MEASUREMENT_VALUE` | The  numeric value of the measurement |     |     |
| 18| `cleaned::MEASUREMENT_UNIT` | The unit of the measurement from the data |     |     |
| 19| `harmonization_omop::MEASUREMENT_VALUE` | The  harmonized numeric value of the measurement |     | The conversion happens from  [a table](/finngen_qc/data/quantity_source_unit_conversion.tsv) and by defining a target unit from [another table](/finngen_qc/data/harmonization_counts.txt) built using the most common unit for each concept ID|
| 20| `harmonization_omop::MEASUREMENT_UNIT` | The harmonized unit of the measurement from the data |     |     | |
| 21| `harmonization_omop::CONVERSION_FACTOR` | The conversion factor used to map the value columns |     |     | |
| 22| `harmonization_omop::IS_UNIT_VALID` | Boolean column with internal info about the unit |     | Internal use    | 
| 23| `harmonization_omop::mappingStatus` | String column with internal info about the mapping|   | Internal use   | 
| 24| `harmonization_omop::sourceCode` | String column with internal info about the abbreviation/unit|  |  Internal use   | 
| 25| `harmonization_omop::OMOP_ID` | OMOP id of the mapping |  | The mapping is done using [a table](/finngen_qc/data/LABfi_ALL.usagi.csv)   | 
| 26| `harmonization_omop::omopQuantity` | Quantity associated to the OMOP id of the mapping |  | The mapping is done using [a table](/finngen_qc/data/LABfi_ALL.usagi.csv)   |
| 27| `source::MEASUREMENT_VALUE` | The original numeric value of the measurement |     |     |
| 28| `source::MEASUREMENT_UNIT` | The original unit of the measurement |     |     |
| 29| `source::TEST_NAME_ABBREVIATION` | The original abbreviation of the measurement |     |     |
| 30 | `MEASUREMENT_EXTRA_INFO` | Contains some extra info about the measurement. Mostly short text descriptions of the measurement (e.g. _Lopullinenvastaus_ or _Keskisuihkuvirtsa>4h_ |
| 31 |`SERVICE_PROVIDER_ID` | Yet another column with information about the provider. It follows the [thl sote table](/finngen_qc//data/thl_sote_map_named.tsv)  | 
| 32 | `SEX` | Sex of the sample |     |     |


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
|tuloksenpoikkeavuusid|TEST_OUTCOME|
|viitearvoryhma|REFERENCE_RANGE_GROUP|
|viitevalialkuarvo|REFERENCE_RANGE_LOWER_VALUE|
|viitevalialkuyksikko|REFERENCE_RANGE_LOWER_UNIT|
|viitevaliloppuarvo|REFERENCE_RANGE_UPPER_VALUE|
|viitevaliloppuyksikko|REFERENCE_RANGE_UPPER_UNIT|
| tutkimuksenlisatiet | MEASUREMENT_EXTRA_INFO |
| antaja_organisaatioid | SERVICE_PROVIDER_ID|

## TECHNICAL INFO


### Summary

There is a [config file](/finngen_qc/magic_config.py) that contains all the relevant "choices" about how to manipulate the data (e.g. which columns to include, how to rename columns, which values of which column to include etc.) so there are virtually no hard coded elements in the code itself.

### DUPLICATE REMOVAL & SORTING

First the [wdl](/finngen_qc/wdl/sort_dup.wdl) trims the data of only the relevant columns (taken from the config) and sorts it by a series of columns (also specified in the config under `sort_cols`) so we can also discard duplicate entries. 

The code then performs the following actions:
### [MINIMAL](/finngen_qc/filters/filter_minimal.py)
- output columns are initialized
- spaces are removed everywhere in the text
- the date is built in the right format
- all type of NA/missing values (Puuttuu,"_" etc.) are replaced with "NA"
- entries with invalid hetu root are removed (and logged)
- entries with invalid measurement status are removed (and logged)
- TEST_ID_IS_NATIONAL is created checking if `laboratoriotutkimusnimikeid` is not NA (1 national/0 regional)
- TEST_ID is created assigning the regional id for regional labs and a 
- TEST_NAME_ABBREVIATION is updated for national labs [through a mapping](/finngen_qc/data/thl_lab_id_abbrv_map.tsv)
- CODING_SYSTEM is updated when available (problematic ATM, see table above)
- CODING_SYSTEM_MAP is created from y-tunnukset in table
- SERVICE_PROVIDER_ID is updated where the mapping can happen
- TEST_NAME_ABBREVIATIONs with problematic characters are edited (see `abbreviation_replacements` in the config)

### [UNIT](/finngen_qc/filters/fix_unit.py)
- Fixes strange characters in the lab unit field. Also moves to lower case for non NA values.
- Mapping of units. This can be done either via regex (from config) or [through a mapping](/finngen_qc/data/unit_mapping.txt)
- TEST_OUTCOME is edited to be consistent with the standard definition see AR/LABRA - Poikkeustilanneviestit. This means replacing `<` with `L`, `>` with `H`, `POS` with `A` and `NEG` with `N`.

#### [harmonization](/finngen_qc/filters/harmonization.py)
- Mapping status is updated (internal thing)
- IS_UNIT_VALID column is populated based on whether the unit is in usagi list
- Harmonizes units to make sure all abbreviations with similar units are mapped to the same one (e.g. mg --> mg/24h for du-prot). Based on [a table](/finngen_qc/data/fix_unit_based_in_abbreviation.tsv) 
- OMOP mapping from [a table](/finngen_qc/data/LABfi_ALL.usagi.csv)
- unit harmonization (optional) from [a table](/finngen_qc/data/quantity_source_unit_conversion.tsv)
- impute_abnormality  (with harmonization) creates the column `imputed::TEST_OUTCOME` based on lower/upper limits from [a table]((/finngen_qc/data/abnormality_estimation.table.tsv)). The limits for different definitions are built using [a script](/scripts/ab.py) and chosen by [another script](/scripts/impute_ab.py).

## How it works
The script reads in the data in chunks of  `--chunksize` length and it processes the lines with Python's pandas. With the flag `--mp` and `--jobs` the script runs each chunk into other smaller subchunks in parallel (efficiency TBD). The [filter folder](/finngen_qc/filters/) contains separate scripts that perform conceptually separate tasks. Each of them contains a global function of the same name of the script that gathers all individual functions that populate the script. In this way we can easily compartmentalize the munging/qc and add new features.


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


# Analysis

The `analysis` folder contains the pipeline that produces a similar file, which is used for analysis purpose. The idea behind this file is to focus more on the values and to use more "aggressive" choices with regards to which values to include/exclude in downstream analysis. 

The current version outputs 3 extra columns, which are taken from the `MEASUREMENT_FREE_TEXT` column, which cannot be shared due to data privacy reasons for the time being. The three columns are:

| Column Name | Easy Description | General Notes | Technical Notes |
| --- | --- | --- | --- |
| `extracted::MEASUREMENT_VALUE` | Numerical values |  The column is mutually exclusive with `harmonization_omop::MEASUREMENT_VALUE`. In our analysis the units seemed to be pretty much always coherent with our target OMOP units, albeit for occasional clusters of outliers that are present also in the core data.   |  The content of the `MEASUREMENT_FREE_TEXT` colum is cleaned by removal of spaces, converted to lower case, the target omop unit is removed if present, certain result strings are removed. If we're left with a pure float number, it's considered to be an extracted value.  |
| `extracted::IS_MEASUREMENT_EXTRACTED` | Boolean |   Boolean indicator of whether the values in the extracted column is original or extracted   |  In the very rare cases when both the original measurement column and a free text value can be extracted, the original one is preserved and this column is set to 0   |
| `extracted::IS_POS` | Boolean |   Pos(1) or Neg(0) status   | We extracted all strings containing the substring pos/neg and we manually mapped them to pos/neg statuses. The mapping is stored in the [data folder](/analysis/data/negpos_mapping.tsv)      |


## TECHNICAL INFO

The princples are identical to the `finngen_qc` pipeline, just with different filters being used.

### [EXTRACT](/analysis/filters/extract.py)
This filters works with the `FREE_TEXT` column:
- only fields with NA `MEASUREMENT_VALUE` and non NA `FREE_TEXT` are considered
- the free text is converted to lower case and spaces are removed
- the OMOP target unit string is removed
- string replacements from `magic_config.py` are applied (e.g. the string `tutkimuksentulos`)
- the remaining text is forced cast to float 
- where the conversion took place `extracted::IS_POS` is set to 1
- extracted and original measurements are merged into the `extracted::MEASUREMENT_VALUE` column

### [QC](/analysis/filters/qc.py)

This is for all sorts of QCing of the data, ideally for outlier filter/removal. For the time being the only QC in place is there for data privacy reasons:
- all extracted values that can be misinterpreted as dates are removed (DDMMYY) as they can either be birth dates or the exact date of the examination, which is shifted +- 2 weeks for each FINNGENID

## How it works
The princples are identical to the `finngen_qc` pipeline.

```
usage: main.py [-h] --raw-data RAW_DATA [--log {critical,error,warn,warning,info,debug}] [--test] [--gz] [--mp [MP]] [-o OUT] [--prefix PREFIX] [--sep SEP]
               [--chunk-size CHUNK_SIZE] [--lines LINES]

Kanta Lab analysis pipeline: clean data ⇒ analysis data.

options:
  -h, --help            show this help message and exit
  --raw-data RAW_DATA   Path to input raw file. File should be tsv.
  --log {critical,error,warn,warning,info,debug Provide logging level. Example '--log debug', default = 'warning'
  --test                Reads first chunk only
  --gz                  Ouputs to gz
  --mp [MP]             Flag for multiproc. Default is '0' (no multiproc). If passed it defaults to cpu count, but one can also specify the number of cpus  to use: e.g. '--mp' or '--mp 4'.
  -o OUT, --out OUT     Folder in which to save the results (default = current working directory)
  --prefix PREFIX       Prefix of the out files (default = 'kanta_YYYY_MM_DD')
  --sep SEP             Separator (default = tab)
  --chunk-size CHUNK_SIZE
                        Number of rows to be processed by each chunk (default = '1000*n_cpus').
  --lines LINES         Number of lines in input file (calculated/estimated otherwise).

```