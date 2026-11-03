# Bugs found in Real_kanta_labs_2016_DF14_toFG.R

## Fixed

**Real script has corrupted Finnish characters (U+FFFD replacement chars)**
`Lukumäärä`, `Yhteensä`, `Säde`, `Sjögren`, `Grön`, `Lång`, `Lähde` were all stored as `?`.
Fixed directly in `Real_kanta_labs_2016_DF14_toFG.R` and file re-saved as UTF-8.

**Real script is Latin-1 encoded — garbled Finnish chars in out.R**
`parametrize_r.py` now reads the Real script as Latin-1 and writes `out.R` as UTF-8.

**phone_regex gsub — missing `perl=TRUE` causes invalid regexp error**
```r
# before
gsub(phone_regex, "NNNNN", kanta_avo$tutkimustulosteksti)
# after
gsub(phone_regex, "NNNNN", kanta_avo$tutkimustulosteksti, perl=TRUE)
```

**Line 272 — missing `#` causes syntax error**
```r
# before
kanta %>% count(tutkimuksennaytelaatu, sort=T) 21 erilaista, many NA
# after
kanta %>% count(tutkimuksennaytelaatu, sort=T) #21 erilaista, many NA
```

**`rm(kanta, kanta_avo)` after chunk loop — variables live inside callback scope**
`kanta` and `kanta_avo` are local to the `SideEffectChunkCallback` function; they do not exist
in the outer scope after the loop. Changed to `gc()` in `Real_kanta_labs_2016_DF14_toFG.R`.

---

# Bugs found in `parametrize_r.py` chunk injection

**`data.table::fwrite` + `cat()` breaks `read_csv_chunked` iteration (1/3)**
`fwrite` with default multi-threading corrupts readr's internal gz stream state.
`cat()` of any kind inside the callback after `fwrite` then signals EOF to the reader.
Fixed by adding `nThread=1` to all patched `fwrite` calls.

**`cat()` on stdout inside readr chunk callback stops iteration after chunk 1 (2/3)**
`data.table::fwrite` (even with `nThread=1`) leaves stdout in a state where any
subsequent `cat()` inside the callback causes readr to see EOF.
`message()` (which writes to stderr via R's message connection) is unaffected.
Fixed by switching progress output from `cat(...)` to `message(...)`.

**Callback returning `FALSE` stops `readr::read_csv_chunked` iteration (3/3)**
`first_chunk <<- FALSE` was the last line of the callback; in readr ≥ 2.x,
a callback that returns `FALSE` signals the chunked reader to stop.
Fixed by adding `invisible(NULL)` as the final callback line.

---

## Not yet fixed in Real script

**Line 398 — wrong variable name for surnames**
```r
# bug: references undefined variable sukunimi_regex
name_regex <- sukunimi_regex$Sukunimi
# should be
name_regex <- name_regex$Sukunimi
```

**Lines 330–332 — phone_regex defined twice**
The first definition (DF14 version) is immediately overwritten by the second (DF15 version).
Harmless but the first line is dead code — can be removed.

**Line 427 — reads .txt.gz but fwrite writes .txt**
```r
kanta_avo2 <- fread("finngen_R14_kanta_laboratory_responses_freetext_2016_internal_1.0.txt.gz", data.table=F)
```
`fwrite` does not auto-gzip, so the `.gz` file won't exist unless compressed externally.
This line is a post-write check and will error if run as-is.
