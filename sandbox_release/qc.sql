select
    {filePath:String} as "File",
    count(*) as "N rows",
    countDistinct(FINNGENID) as "N FINNGENIDs",
    countDistinct(OMOP_CONCEPT_ID) as "N OMOP IDs"
from file({filePath:String})
format PrettyCompactMonoBlock;
