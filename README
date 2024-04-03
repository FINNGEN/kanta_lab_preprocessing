# KANTA LAB values preprocessing & QC

Based on Kira Detrois' [existing repo](https://github.com/detroiki/kanta_lab).

## How it works

The script reads in the data in chunks of  `--chunksize` length and it processes the lines with python's pandas. With the flag `--mp` and `--jobs` the script runs each chunk into other smaller subchunks in parallel (efficiency TBD).

## TO DO

- We realized that it's best to subset the columns and remove duplicates in a pre-processing step.
- We should first only keep the relevant columns (~ 1/3)
- Then we can sort by FINNGENID/DATE (should be doable in bash) and remove duplicates (~ 1/2 according to Kira)

These steps are conceptually separate and should not interfere with the downstream analysis, but will help speed it up.