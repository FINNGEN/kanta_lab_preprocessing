#!/bin/bash


KANTA=$1
OMOP_ID=$2
NAME=$3
CHROM=$4
MIN_COUNT=$5
DIR=${6:-"/mnt/disks/data/kanta/analysis/"}


DATA_DIR=$DIR/data/
mkdir -p $DATA_DIR

echo "FILTER KANTA DATA"
echo "OMOP: " $OMOP_ID
echo "NAME: " $NAME
LAB_DATA=$DATA_DIR/min_count_$NAME.txt
TMP=$DIR/tmp.txt
if [ ! -f $LAB_DATA ]; then
    echo "$LAB_DATA  not found!"
    zcat -f $KANTA  |  awk -v var="$OMOP_ID" '23==var' | cut -f1,3,10,11 | grep -wv "NA" > $TMP && join -t $'\t' $TMP <(cut -f 1 $TMP | sort | uniq -c | sort -nr | awk '{print $1"\t"$2}' | awk '$1>=$MIN_COUNT' | cut -f2 | sort ) > $LAB_DATA 
else
    echo "$LAB_DATA ALREADY GENERATED"
fi
echo "GET AVERAGE FOR EACH SAMPLE"

#awk 'NR>1 && $1!=p{print p, s/c;a/c; a=c=s=0} {a+=$2;s+=$3;c++;p=$1} END {print p, s/c,a/c}' file


echo "AVERAGES"
AVG_FILE=$DATA_DIR/$NAME"_data.txt"
echo $AVG_FILE
if [ ! -f $AVG_FILE ];
then
    echo -e "FINNGENID\t$NAME\tAGE_AT_MEASUREMENT" > $AVG_FILE
cat $LAB_DATA | sed -E 1d | awk 'BEGIN {OFS="\t"} NR>1 && $1!=p{print p, s/c,a/c; a=c=s=0} {a+=$2;s+=$3;c++;p=$1} END {print p, s/c,a/c}' >> $AVG_FILE
else
    echo "Averages already calculated"
fi

echo "GET COV DATA"
COV=$DATA_DIR/cov.txt
echo $COV
if [ ! -f $COV ]; then
    zcat ~/r12/pheno/R12_COV_V2.FID.txt.gz | head -n1 > $COV && zcat ~/r12/pheno/R12_COV_V2.FID.txt.gz | sed -E 1d | sort >> $COV
else
    echo "COV already generated"
fi


echo "PHENO FILE"
PHENO_FILE=$DATA_DIR/pheno_$NAME.txt
if [ ! -f $PHENO_FILE ]; then
    join -t $'\t' --header $COV $AVG_FILE > $PHENO_FILE
else
    echo "$PHENO_FILE already generated"
fi


echo "REGENIE STEP1"
CPUS=`nproc --all`

REG_DIR=$DIR/regenie/
mkdir -p $REG_DIR
NULL_FILE=$REG_DIR/$NAME"_1.loco.gz"
echo $NULL_FILE
CWD=$PWD
cd $REG_DIR
if [ ! -f $NULL_FILE ]; then
    echo "Null missing"
    regenie --step 1 --bed ~/r12/grm/R12_GRM_V0_LD_0.2 --covarFile $PHENO_FILE --phenoFile $PHENO_FILE --phenoColList $NAME --covarColList SEX_IMPUTED,AGE_AT_MEASUREMENT,PC{1:10},IS_FINNGEN2_CHIP,BATCH_DS1_BOTNIA_Dgi_norm,BATCH_DS10_FINRISK_Palotie_norm,BATCH_DS11_FINRISK_PredictCVD_COROGENE_Tarto_norm,BATCH_DS12_FINRISK_Summit_norm,BATCH_DS13_FINRISK_Bf_norm,BATCH_DS14_GENERISK_norm,BATCH_DS15_H2000_Broad_norm,BATCH_DS16_H2000_Fimm_norm,BATCH_DS17_H2000_Genmets_norm_relift,BATCH_DS18_MIGRAINE_1_norm_relift,BATCH_DS19_MIGRAINE_2_norm,BATCH_DS2_BOTNIA_T2dgo_norm,BATCH_DS20_SUPER_1_norm_relift,BATCH_DS21_SUPER_2_norm_relift,BATCH_DS22_TWINS_1_norm,BATCH_DS23_TWINS_2_norm_nosymmetric,BATCH_DS24_SUPER_3_norm,BATCH_DS25_BOTNIA_Regeneron_norm,BATCH_DS26_DIREVA_norm,BATCH_DS27_NFBC66_norm,BATCH_DS28_NFBC86_norm,BATCH_DS3_COROGENE_Sanger_norm,BATCH_DS4_FINRISK_Corogene_norm,BATCH_DS5_FINRISK_Engage_norm,BATCH_DS6_FINRISK_FR02_Broad_norm_relift,BATCH_DS7_FINRISK_FR12_norm,BATCH_DS8_FINRISK_Finpcga_norm,BATCH_DS9_FINRISK_Mrpred_norm --bsize 1000 --lowmem --lowmem-prefix tmp_rg  --gz --threads $CPUS --out $REG_DIR/$NAME

else
    echo "NULL already generated"
fi

echo "REGENIE STEP2"
OUT_FILE=$REG_DIR/"kanta_"$CHROM"_"$NAME.regenie.gz
if [ ! -f $OUT_FILE ] ;then
    echo "STEP 2 missing"
    echo -e "$NAME\t$NULL_FILE"> $REG_DIR/pred.txt
    regenie --step 2 --bgen ~/r12/bgen/release_NO_PAR/data/chrom/finngen_R12_$CHROM.bgen --ref-first --sample ~/r12/bgen/release_NO_PAR/data/chrom/finngen_R12_$CHROM.bgen.sample --covarFile $PHENO_FILE --phenoFile $PHENO_FILE --phenoColList $NAME --covarColList SEX_IMPUTED,AGE_AT_MEASUREMENT,PC{1:10},IS_FINNGEN2_CHIP,BATCH_DS1_BOTNIA_Dgi_norm,BATCH_DS10_FINRISK_Palotie_norm,BATCH_DS11_FINRISK_PredictCVD_COROGENE_Tarto_norm,BATCH_DS12_FINRISK_Summit_norm,BATCH_DS13_FINRISK_Bf_norm,BATCH_DS14_GENERISK_norm,BATCH_DS15_H2000_Broad_norm,BATCH_DS16_H2000_Fimm_norm,BATCH_DS17_H2000_Genmets_norm_relift,BATCH_DS18_MIGRAINE_1_norm_relift,BATCH_DS19_MIGRAINE_2_norm,BATCH_DS2_BOTNIA_T2dgo_norm,BATCH_DS20_SUPER_1_norm_relift,BATCH_DS21_SUPER_2_norm_relift,BATCH_DS22_TWINS_1_norm,BATCH_DS23_TWINS_2_norm_nosymmetric,BATCH_DS24_SUPER_3_norm,BATCH_DS25_BOTNIA_Regeneron_norm,BATCH_DS26_DIREVA_norm,BATCH_DS27_NFBC66_norm,BATCH_DS28_NFBC86_norm,BATCH_DS3_COROGENE_Sanger_norm,BATCH_DS4_FINRISK_Corogene_norm,BATCH_DS5_FINRISK_Engage_norm,BATCH_DS6_FINRISK_FR02_Broad_norm_relift,BATCH_DS7_FINRISK_FR12_norm,BATCH_DS8_FINRISK_Finpcga_norm,BATCH_DS9_FINRISK_Mrpred_norm --pred $REG_DIR/pred.txt --bsize 400 --threads $CPUS --gz --out kanta_$CHROM

else
    echo "$OUT_FILE already generated"
fi


echo "MERGE RESULTS"
MERGED=$REG_DIR/$NAME"_regenie.txt"
echo $MERGED
rm -f $MERGED.tmp
while read f
do
    zcat $f | head -n1 > $MERGED.header
    zcat $f | sed -E 1d >>$MERGED.tmp
done < <( ls $REG_DIR/*"_"$NAME.regenie.gz)
cat $MERGED.header | tr ' ' '\t'  > $MERGED
cat  $MERGED.tmp | tr ' ' '\t' | sort -gk 1 -gk 2 >> $MERGED
rm $MERGED.tmp $MERGED.header

cd $CWD
