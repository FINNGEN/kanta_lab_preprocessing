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

| Column in raw file                                      | Column in clean file | Comment |
|---------------------------------------------------------|----------------------|---------|
| potilashenkilotunnus                                    | FINREGISTRYID        |         |
| tutkimusaika                                            | LAB_DATE_TIME        |         |
| palverluntuottaja_organisaatio                          | LAB_SERVICE_PROVIDER |         |
| paikallinentutkimusnimikeid,laboratoriotutkimusnimikeid | LAB_ID               |         |
| paikallinentutkimusnimikeid,laboratoriotutkimusnimikeid | LAB_ID_SOURCE        |         |
| paikallinentutkimusnimike (ONLY IF LOCAL)               | LAB_ABBREVIATION     |         |
| tutkimustulosarvo                                       | LAB_VALUE            |         |
| tutkimustulosyksikko                                    | LAB_UNIT             |         |
| tuloksenpoikkeavuus                                     | LAB_ABNORMALITY      |         |
| viitevaliteksti                                         | REFERENCE_VALUE_TEXT |         |
| tutkimusvastauksentila                                  | MEASUREMENT_STATUS   |         |

The following columns are also needed for processing

| Column in raw file      | Usage                                                                                |
|-------------------------|--------------------------------------------------------------------------------------|
| hetu_root               | Filter out if current hetu root is not 1.2.246.21 (they are manually assigned hetus) |


Possible other columns to include?

| Column in raw file     | Description         |
|------------------------|---------------------|
| tietojarjestelmanimi   | DATA_SYSTEM_NAME    |
| tietojarjestelmaversio | DATA_SYSTEM_VERSION |

# How it works

The script reads in the data in chunks of  `--chunksize` length and it processes the lines with python's pandas. With the flag `--mp` and `--jobs` the script runs each chunk into other smaller subchunks in parallel (efficiency TBD). The [filter folder](/finngen_qc/filters/) contains separate scripts that perform conceptually separate tasks. Each of them contains a global function of the same name of the script that gathers all individual functions that populate the script. In this way we can easily compartmentalize the munging/qc and add new features.



## PRE-PROCECSSING STEPS

Given the structure of the input data, we decided to do some preprocessing steps that will allows to shrink the data considerably. 
- We should first only keep the relevant columns (~ 1/3: output + required for filtering)
- Then we can sort by FINNGENID/DATE (should be doable in bash) and remove duplicates (~ 1/2 according to Kira)

These steps are conceptually separate and should not interfere with the downstream analysis, but will help speed it up.
