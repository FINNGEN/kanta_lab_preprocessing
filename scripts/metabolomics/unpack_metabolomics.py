#!/usr/bin/env python3
"""
unpack_metabolomics.py

FINNGEN/kanta_lab_preprocessing#45 -- the Nightingale metabolomics panel is
delivered as 9 lab tests whose results live entirely inside free text, not
in MEASUREMENT_VALUE. This script unpacks all 9 into two standalone
datasets. (The remaining 3 test codes from the issue, 11270-11272, are report
links with nothing to unpack -- excluded, per the issue.)

1. S-NGMuut (TEST_ID 11282): ~31 concatenated "Name: Value Unit" biomarker
   entries with no separator between them (e.g. "...0.524 mmol/lGlysiini:
   0.437 mmol/l..."). Unpacked into a long-format table, one row per
   extracted biomarker, with the standard Nightingale English biomarker name
   alongside the raw Finnish one. Parsing has no hardcoded biomarker name
   list -- it just captures whatever text precedes a ": Value[ Unit]"
   pattern, which handles a real spelling-variant split in the source data
   (e.g. "Monityydyttymättömät" vs "Monityydyttymättämät" rasvahapot) for
   free, without normalizing anything. RAW_TEXT on each row is just that one
   biomarker's own matched "Name: Value Unit" substring, not the whole
   ~31-entry panel (which would otherwise get duplicated onto every one of
   its own extracted rows).

2. The 8 disease-risk-score tests (TEST_IDs 11274-11281: myocardial
   infarction, cardiovascular disease, type 2 diabetes, liver fibrosis and
   cirrhosis, chronic kidney disease, COPD, lung cancer, alcohol-related
   liver disease): one row per source row, with the free text's clearly
   structured parts (risk category, risk %, and -- when present -- a
   comparison-group percentile + age/sex reference group) pulled into
   columns, both translated to English. COPD/lung cancer's smoking-conditional
   risk multiplier is a disease-level constant, not per-patient data, so it's
   left in RAW_TEXT only, not extracted. The full raw text is always kept
   alongside too, so nothing is lost for whatever doesn't match a known
   sub-pattern.

Both: about 1-2% of rows are a whole-panel "Ei tehty" (not done) free-text
note (e.g. a lipemic sample) instead of a real result -- those are skipped,
not treated as errors. All extracted values are kept as raw strings exactly
as they appear in the source text (">6.5", "Ei tehty", etc.) -- there is no
other source for these values to compare against or clean against, so no
harmonization happens here.

Usage:
  python3 unpack_metabolomics.py [--input PATH] [--out PATH] [--chunksize N]
  python3 unpack_metabolomics.py --test   # run against the synthetic rows hardcoded below
"""

import argparse
import re
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm

DEFAULT_ENGINE_DIR = Path("/mnt/disks/data/kanta/engine/")
DEFAULT_OUT_DIR = Path("/mnt/disks/data/kanta/metabolomics/results/")

NGMUUT_TEST_ID = "11282"
RISK_TEST_IDS = ["11274", "11275", "11276", "11277", "11278", "11279", "11280", "11281"]
ALL_TEST_IDS = [NGMUUT_TEST_ID] + RISK_TEST_IDS

# Translation for RISK_CMP_RE's two possible captures (see below) -- COMPARISON
# is stored in English only, no Finnish is kept. The regex's third possible
# comparison, "Yhtä suuri" (equal risk), is translated inline instead, since
# RISK_EQ_RE doesn't capture it as a group.
COMPARISON_EN = {
    "Korkeampi": "Higher",
    "Matalampi": "Lower",
}

# All 10 raw RISK_CATEGORY values found across the full release (validated by
# scanning every risk-score row, not just a sample) -- translated in place,
# same convention as COMPARISON. Unrecognized future values are left as-is
# (see translate_risk_categories below) rather than silently dropped.
RISK_CATEGORY_EN = {
    "Matala": "Low",
    "Kohonnut": "Elevated",
    "Korkea": "High",
    "KohonJosTupak": "Elevated if you smoke",
    "KorkeaJosTupak": "High if you smoke",
    "Kohonnut, jos tupakoit": "Elevated if you smoke",
    "Korkea, jos tupakoit": "High if you smoke",
    "Ei Tehty": "Not done",
    "Ei Tehdä": "Not done",
    "Vastattu": "Answered",
}

SEX_GROUP_EN = {
    "naiset": "women",
    "miehet": "men",
}

# English names, straight from FINNGEN/kanta_lab_preprocessing#45 -- TEST_NAME
# itself is a Finnish compound word (e.g. "sydaninfarktiriski"), not obviously
# readable to a non-Finnish speaker.
TEST_NAME_EN = {
    "11274": "Myocardial infarction (MI)",
    "11275": "Cardiovascular disease (CVD)",
    "11276": "Type 2 diabetes (T2D)",
    "11277": "Liver fibrosis and cirrhosis",
    "11278": "Chronic kidney disease (CKD)",
    "11279": "Chronic obstructive pulmonary disease (COPD)",
    "11280": "Lung cancer",
    "11281": "Alcohol-related liver disease (ALD)",
    "11282": "S-NGMuut",
}

NEEDED_COLS = [
    "ROWID", "FINNGENID", "SEX", "APPROX_EVENT_DATETIME", "EVENT_AGE",
    "TEST_ID", "TEST_NAME", "CODING_SYSTEM_ORG", "MEASUREMENT_FREE_TEXT",
]

METADATA_COLS = ["FINNGENID", "SEX", "APPROX_EVENT_DATETIME", "EVENT_AGE", "TEST_ID", "TEST_NAME", "CODING_SYSTEM_ORG"]

# ---------------------------------------------------------------------------
# S-NGMuut (TEST_ID 11282): biomarker panel
# ---------------------------------------------------------------------------

NGMUUT_PREAMBLE_END = "merkitystä."  # fixed intro sentence ends here, precedes the first biomarker entry

# Matches one "Name: Value[ Unit]" entry. Name is whatever precedes the next
# colon (entries never contain a colon in the name), Value is either the
# literal "Ei tehty" or a number (optionally </> prefixed for censored
# values), Unit is an optional lowercase/symbol token immediately after --
# it naturally stops before the next entry's capitalized name.
NGMUUT_ENTRY_RE = re.compile(r"([A-ZÄÖÅ][^:]*?):\s*(Ei tehty|[<>]?\d[\d.,]*)(?:\s([a-zäöå%/]+))?")

# Standard Nightingale Health NMR biomarker panel nomenclature -- all 33 raw
# names found across the full release (both spelling variants of the two
# affected biomarkers map to the same English term, same convention as
# RISK_CATEGORY_EN/TEST_NAME_EN). Not normalized in place -- see
# add_ngmuut_name_en below, which adds this as its own column and keeps the
# raw source name too.
NGMUUT_NAME_EN = {
    "Alaniini": "Alanine",
    "Albumiini": "Albumin",
    "Dokosaheksaeenihappo": "Docosahexaenoic acid (DHA)",
    "Dokosaheksaeenihapon suhde kokonaisrasvahappoihin": "DHA to total fatty acids ratio (DHA %)",
    "Fenyylialaniini": "Phenylalanine",
    "Glukoosi": "Glucose",
    "Glykoproteiinin asetylaatio": "Glycoprotein acetylation (GlycA)",
    "Glysiini": "Glycine",
    "Haaraketjuisten aminohappojen (leusiini + isoleusiini + valiini) kokonaispitoisuus":
        "Total branched-chain amino acids: leucine + isoleucine + valine (BCAA)",
    "Histidiini": "Histidine",
    "Isoleusiini": "Isoleucine",
    "Kertatyydyttymättömät rasvahapot": "Monounsaturated fatty acids (MUFA)",
    "Kertatyydyttymättämät rasvahapot": "Monounsaturated fatty acids (MUFA)",
    "Kertatyydyttymättömien rasvahappojen suhde kokonaisrasvahappoihin": "MUFA to total fatty acids ratio (MUFA %)",
    "Kokonaisrasvahapot": "Total fatty acids",
    "Laktaatti": "Lactate",
    "Leusiini": "Leucine",
    "Linolihappo": "Linoleic acid (LA)",
    "Linolihapon suhde kokonaisrasvahappoihin": "LA to total fatty acids ratio (LA %)",
    "Monityydyttymättömät rasvahapot": "Polyunsaturated fatty acids (PUFA)",
    "Monityydyttymättämät rasvahapot": "Polyunsaturated fatty acids (PUFA)",
    "Monityydyttymättömien rasvahappojen suhde kertatyydyttymättömiin rasvahappoihin": "PUFA to MUFA ratio",
    "Monityydyttymättömien rasvahappojen suhde kokonaisrasvahappoihin": "PUFA to total fatty acids ratio (PUFA %)",
    "Omega-3-rasvahapot": "Omega-3 fatty acids",
    "Omega-3-rasvahappojen suhde kokonaisrasvahappoihin": "Omega-3 to total fatty acids ratio (Omega-3 %)",
    "Omega-6-rasvahapot": "Omega-6 fatty acids",
    "Omega-6-rasvahappojen suhde kokonaisrasvahappoihin": "Omega-6 to total fatty acids ratio (Omega-6 %)",
    "Omega-6-rasvahappojen suhde omega-3-rasvahappoihin": "Omega-6 to omega-3 fatty acids ratio",
    "Tyrosiini": "Tyrosine",
    "Tyydyttyneet rasvahapot": "Saturated fatty acids (SFA)",
    "Tyydyttyneiden rasvahappojen suhde kokonaisrasvahappoihin": "SFA to total fatty acids ratio (SFA %)",
    "VLDL-kolesteroli": "VLDL cholesterol",
    "Valiini": "Valine",
}


def unpack_ngmuut_text(text: str) -> list[tuple[str, str, str | None, str]]:
    """(name, value, unit, raw_entry_text) tuples extracted from one S-NGMuut
    free-text body -- raw_entry_text is just the matched "Name: Value Unit"
    substring for that one biomarker, not the whole ~31-entry panel (which
    would otherwise get duplicated onto every one of its ~31 output rows).
    Empty list for a whole-panel "not done" note (no structured entries)."""
    body = text.split(NGMUUT_PREAMBLE_END, 1)
    body = body[1] if len(body) == 2 else text
    return [(*m.groups(), m.group(0)) for m in NGMUUT_ENTRY_RE.finditer(body)]


def unpack_ngmuut_batch(batch: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for row in batch.itertuples(index=False):
        for name, value, unit, raw_entry in unpack_ngmuut_text(row.MEASUREMENT_FREE_TEXT):
            rows.append((
                *[getattr(row, c) for c in METADATA_COLS], name, unit, value,
                raw_entry, row.ROWID,
            ))
    return pd.DataFrame(rows, columns=[
        *METADATA_COLS,
        "TEST_NAME_ABBREVIATION", "MEASUREMENT_UNIT", "MEASUREMENT_VALUE",
        "RAW_TEXT", "SOURCE_ROWID",
    ])


# ---------------------------------------------------------------------------
# Risk-score tests (TEST_IDs 11274-11281)
# ---------------------------------------------------------------------------

# "<category> 10v riski riskiluokassa: <pct> %<tail>" -- category is whatever
# text precedes the fixed marker (covers e.g. "Matala", "Kohonnut",
# "KohonJosTupak", "Kohonnut, jos tupakoit" -- several label spellings for
# the same underlying categories, kept raw rather than normalized). The space
# before "10v" is inconsistent in the source data -- present for most labels,
# absent for the comma-style ones ("...tupakoit10v riski...") -- so it's optional.
RISK_HEAD_RE = re.compile(r"^(.+?)\s?10v riski riskiluokassa: (<?[\d.]+) %(.*)$", re.S)

# tail sub-pattern 1: compared against an age/sex reference group
RISK_CMP_RE = re.compile(
    r"^(Matalampi|Korkeampi) riski kuin (\d+) %:lla verrokkiryhmästä \((\d+-\d+) -vuotiaat (naiset|miehet)\)$"
)
# tail sub-pattern 2: equal risk to the reference group (no percentile given)
RISK_EQ_RE = re.compile(r"^Yhtä suuri riski kuin verrokkiryhmällä \((\d+-\d+) -vuotiaat (naiset|miehet)\)$")
# A third tail shape (COPD/lung cancer only) reads "Tupakoivilla on N-kertainen
# sairastumisriski tupakoimattomiin verrattuna. Jos et tupakoi, riskisi on
# matala." -- a disease-level smoking risk multiplier, not per-patient data,
# so nothing is extracted from it; it falls through to the "no sub-pattern
# matched" case below and is left in RAW_TEXT only.


def parse_risk_text(text: str) -> dict:
    """Structured fields pulled from one risk-score free-text body, when
    present -- always includes RAW_TEXT so nothing is lost. Returns {} for a
    whole-panel "not done" note or a missing free text (skip -- same
    convention as S-NGMuut)."""
    if pd.isna(text) or text.startswith("Ei tehty"):
        return {}

    fields = {"RISK_CATEGORY": None, "RISK_PCT": None, "COMPARISON": None,
              "COMPARISON_PERCENTILE": None, "AGE_GROUP": None, "SEX_GROUP": None, "RAW_TEXT": text}

    head = RISK_HEAD_RE.match(text)
    if not head:
        # category label only, e.g. "Matala" -- no percentage/tail at all
        fields["RISK_CATEGORY"] = text
        return fields

    category, pct, tail = head.groups()
    fields["RISK_CATEGORY"] = category
    fields["RISK_PCT"] = pct

    cmp_m = RISK_CMP_RE.match(tail)
    eq_m = RISK_EQ_RE.match(tail)
    if cmp_m:
        comparison, percentile, age_group, sex_group = cmp_m.groups()
        fields.update(COMPARISON=COMPARISON_EN[comparison], COMPARISON_PERCENTILE=percentile,
                      AGE_GROUP=age_group, SEX_GROUP=sex_group)
    elif eq_m:
        age_group, sex_group = eq_m.groups()
        fields.update(COMPARISON="Equal", AGE_GROUP=age_group, SEX_GROUP=sex_group)
    # else: either a smoking-conditional tail (see the comment above RISK_EQ_RE)
    # -- the multiplier is a disease-level constant, not per-patient data, so
    # nothing is extracted
    # from it -- or a tail that doesn't match any known sub-pattern. Either way
    # RAW_TEXT still carries the full original text, nothing is lost.

    return fields


RISK_COLUMNS = [
    *METADATA_COLS, "RISK_CATEGORY", "RISK_PCT",
    "COMPARISON", "COMPARISON_PERCENTILE", "AGE_GROUP", "SEX_GROUP",
    "RAW_TEXT", "SOURCE_ROWID",
]


def unpack_risk_batch(batch: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for row in batch.itertuples(index=False):
        fields = parse_risk_text(row.MEASUREMENT_FREE_TEXT)
        if fields:
            rows.append({**{c: getattr(row, c) for c in METADATA_COLS}, **fields, "SOURCE_ROWID": row.ROWID})
    return pd.DataFrame(rows, columns=RISK_COLUMNS)


# ---------------------------------------------------------------------------
# Shared machinery
# ---------------------------------------------------------------------------

def resolve_kanta_input() -> Path:
    """Default to the most recently modified *_RELEASE.parquet in the engine dir,
    since the dated filename changes with every new engine run."""
    candidates = sorted(DEFAULT_ENGINE_DIR.glob("*_RELEASE.parquet"), key=lambda p: p.stat().st_mtime)
    if not candidates:
        raise SystemExit(f"no *_RELEASE.parquet found under {DEFAULT_ENGINE_DIR} -- pass --input")
    return candidates[-1]


def add_test_name_en(unpacked: pd.DataFrame) -> pd.DataFrame:
    """Insert TEST_NAME_EN (see TEST_NAME_EN mapping above) right after TEST_NAME."""
    pos = unpacked.columns.get_loc("TEST_NAME") + 1
    unpacked.insert(pos, "TEST_NAME_EN", unpacked["TEST_ID"].map(TEST_NAME_EN))
    return unpacked


def add_ngmuut_name_en(ngmuut: pd.DataFrame) -> pd.DataFrame:
    """Insert TEST_NAME_ABBREVIATION_EN (see NGMUUT_NAME_EN mapping
    above) right after TEST_NAME_ABBREVIATION."""
    pos = ngmuut.columns.get_loc("TEST_NAME_ABBREVIATION") + 1
    ngmuut.insert(pos, "TEST_NAME_ABBREVIATION_EN",
                  ngmuut["TEST_NAME_ABBREVIATION"].map(NGMUUT_NAME_EN))
    return ngmuut


def translate_risk_categories(risk: pd.DataFrame) -> pd.DataFrame:
    """Translate RISK_CATEGORY and SEX_GROUP to English in place (same
    convention as COMPARISON) -- a value not in the mapping is left as-is
    rather than dropped, since Series.replace only touches matched values."""
    risk["RISK_CATEGORY"] = risk["RISK_CATEGORY"].replace(RISK_CATEGORY_EN)
    risk["SEX_GROUP"] = risk["SEX_GROUP"].replace(SEX_GROUP_EN)
    return risk


# Fully synthetic, hand-picked stand-in for release-parquet rows, for --test.
# Nothing here is derived from or resembles real data -- names/values are
# made up, but the structures (incl. a censored value, a per-biomarker "Ei
# tehty", a comparison-group risk, an equal-risk case with no percentile, a
# smoking-conditional risk, a category-only row, and a whole-panel "not
# done" row) mirror the real formats.
TEST_ROWS = [
    {
        "ROWID": 1, "FINNGENID": "TEST0001", "SEX": "female",
        "APPROX_EVENT_DATETIME": "2020-01-01", "EVENT_AGE": 40.0,
        "TEST_ID": NGMUUT_TEST_ID, "TEST_NAME": "s-ngmuut", "CODING_SYSTEM_ORG": "Test_Lab",
        "MEASUREMENT_FREE_TEXT": (
            "Tehty jotain tekstiä ennen tuloksia. " + NGMUUT_PREAMBLE_END +
            "Biomarkkeri A: 1.23 mmol/lBiomarkkeri B: >9.9 mmol/lBiomarkkeri C: Ei tehtyBiomarkkeri D: 4.56 %"
        ),
    },
    {
        # whole panel not performed -- must yield zero extracted rows, not an error
        "ROWID": 2, "FINNGENID": "TEST0002", "SEX": "male",
        "APPROX_EVENT_DATETIME": "2021-02-02", "EVENT_AGE": 55.0,
        "TEST_ID": NGMUUT_TEST_ID, "TEST_NAME": "s-ngmuut", "CODING_SYSTEM_ORG": "Test_Lab",
        "MEASUREMENT_FREE_TEXT": "Ei tehty. Näyte hylätty.",
    },
    {
        # a TEST_ID this script never touches -- must be filtered out entirely
        "ROWID": 3, "FINNGENID": "TEST0003", "SEX": "female",
        "APPROX_EVENT_DATETIME": "2022-03-03", "EVENT_AGE": 30.0,
        "TEST_ID": "99999", "TEST_NAME": "some-other-test", "CODING_SYSTEM_ORG": "Test_Lab",
        "MEASUREMENT_FREE_TEXT": "Biomarkkeri A: 1.00 mmol/l",
    },
    {
        # comparison-group risk (Korkeampi)
        "ROWID": 4, "FINNGENID": "TEST0004", "SEX": "female",
        "APPROX_EVENT_DATETIME": "2020-05-05", "EVENT_AGE": 50.0,
        "TEST_ID": "11274", "TEST_NAME": "sydaninfarktiriski", "CODING_SYSTEM_ORG": "Test_Lab",
        "MEASUREMENT_FREE_TEXT": "Kohonnut 10v riski riskiluokassa: 6 %Korkeampi riski kuin 80 %:lla "
                                  "verrokkiryhmästä (45-54 -vuotiaat naiset)",
    },
    {
        # equal-risk case -- no percentile in the tail
        "ROWID": 5, "FINNGENID": "TEST0005", "SEX": "male",
        "APPROX_EVENT_DATETIME": "2020-06-06", "EVENT_AGE": 45.0,
        "TEST_ID": "11275", "TEST_NAME": "sydan-javerisuonitautiriski", "CODING_SYSTEM_ORG": "Test_Lab",
        "MEASUREMENT_FREE_TEXT": "Matala 10v riski riskiluokassa: 2 %Yhtä suuri riski kuin "
                                  "verrokkiryhmällä (35-44 -vuotiaat miehet)",
    },
    {
        # smoking-conditional risk, comma-style category label
        "ROWID": 6, "FINNGENID": "TEST0006", "SEX": "male",
        "APPROX_EVENT_DATETIME": "2020-07-07", "EVENT_AGE": 60.0,
        "TEST_ID": "11279", "TEST_NAME": "keuhkoahtaumatautiriski", "CODING_SYSTEM_ORG": "Test_Lab",
        "MEASUREMENT_FREE_TEXT": "Kohonnut, jos tupakoit10v riski riskiluokassa: 3 %Tupakoivilla on "
                                  "8-kertainen sairastumisriski tupakoimattomiin verrattuna. "
                                  "Jos et tupakoi, riskisi on matala.",
    },
    {
        # category-only, no percentage/tail at all
        "ROWID": 7, "FINNGENID": "TEST0007", "SEX": "female",
        "APPROX_EVENT_DATETIME": "2020-08-08", "EVENT_AGE": 65.0,
        "TEST_ID": "11276", "TEST_NAME": "tyypin2diabetesriski", "CODING_SYSTEM_ORG": "Test_Lab",
        "MEASUREMENT_FREE_TEXT": "Matala",
    },
    {
        # whole panel not performed
        "ROWID": 8, "FINNGENID": "TEST0008", "SEX": "male",
        "APPROX_EVENT_DATETIME": "2020-09-09", "EVENT_AGE": 70.0,
        "TEST_ID": "11277", "TEST_NAME": "maksanfibroosijakirroosiriski", "CODING_SYSTEM_ORG": "Test_Lab",
        "MEASUREMENT_FREE_TEXT": "Ei tehty. Lipeeminen näyte.",
    },
]


def run(source_batches, source_desc: str, total: int | None = None):
    """Shared scan/route/unpack loop: `source_batches` yields (n_rows_in_batch, batch_df)."""
    ngmuut_parts, risk_parts = [], []
    n_ngmuut = n_risk = n_ngmuut_skipped = n_risk_skipped = 0

    with tqdm(total=total, desc=source_desc, unit="rows", unit_scale=True) as pbar:
        for n_rows, batch in source_batches:
            ngmuut_sub = batch.loc[batch["TEST_ID"] == NGMUUT_TEST_ID]
            n_ngmuut += len(ngmuut_sub)
            if not ngmuut_sub.empty:
                part = unpack_ngmuut_batch(ngmuut_sub)
                n_ngmuut_skipped += len(ngmuut_sub) - part["SOURCE_ROWID"].nunique()
                if not part.empty:
                    ngmuut_parts.append(part)

            risk_sub = batch.loc[batch["TEST_ID"].isin(RISK_TEST_IDS)]
            n_risk += len(risk_sub)
            if not risk_sub.empty:
                part = unpack_risk_batch(risk_sub)
                n_risk_skipped += len(risk_sub) - len(part)
                if not part.empty:
                    risk_parts.append(part)

            pbar.update(n_rows)

    empty_ngmuut = pd.DataFrame(columns=[
        *METADATA_COLS,
        "TEST_NAME_ABBREVIATION", "MEASUREMENT_UNIT", "MEASUREMENT_VALUE",
        "RAW_TEXT", "SOURCE_ROWID",
    ])
    empty_risk = pd.DataFrame(columns=RISK_COLUMNS)
    ngmuut = add_test_name_en(pd.concat(ngmuut_parts, ignore_index=True)) if ngmuut_parts else add_test_name_en(empty_ngmuut)
    ngmuut = add_ngmuut_name_en(ngmuut)
    risk = add_test_name_en(pd.concat(risk_parts, ignore_index=True)) if risk_parts else add_test_name_en(empty_risk)
    risk = translate_risk_categories(risk)

    print(f"S-NGMuut: {n_ngmuut} source rows scanned ({n_ngmuut_skipped} whole-panel \"not done\", skipped) "
          f"-> {len(ngmuut)} unpacked biomarker rows")
    print(f"risk scores: {n_risk} source rows scanned ({n_risk_skipped} whole-panel \"not done\", skipped) "
          f"-> {len(risk)} unpacked rows")
    return ngmuut, risk


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=None,
                         help=f"Kanta RELEASE parquet. Default: most recent *_RELEASE.parquet "
                              f"under {DEFAULT_ENGINE_DIR}. Ignored with --test.")
    parser.add_argument("--out", type=Path, default=None,
                         help=f"Output directory (ignored with --test, which prints to screen instead). "
                              f"Default: {DEFAULT_OUT_DIR}")
    parser.add_argument("--chunksize", type=int, default=2_000_000, help="Rows per read batch")
    parser.add_argument("--test", action="store_true",
                         help="Run against the synthetic rows hardcoded in this script instead of real data")
    args = parser.parse_args()

    if args.test:
        df = pd.DataFrame(TEST_ROWS)
        ngmuut, risk = run([(len(df), df)], source_desc="synthetic rows")
        print("\n-- S-NGMuut --")
        print(ngmuut.to_string(index=False))
        print("\n-- risk scores --")
        print(risk.to_string(index=False))
        return

    kanta_input = args.input or resolve_kanta_input()
    print(f"kanta input: {kanta_input}")
    pf = pq.ParquetFile(kanta_input)

    def batches():
        for record_batch in pf.iter_batches(batch_size=args.chunksize, columns=NEEDED_COLS):
            batch = record_batch.to_pandas()
            yield len(batch), batch.loc[batch["TEST_ID"].isin(ALL_TEST_IDS)]

    ngmuut, risk = run(batches(), source_desc="scanning for metabolomics tests", total=pf.metadata.num_rows)

    out_dir = args.out or DEFAULT_OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    ngmuut_path = out_dir / "nightingale_results_metabolomics.tsv.gz"
    risk_path = out_dir / "nightingale_results_risks.tsv.gz"
    ngmuut.to_csv(ngmuut_path, sep="\t", index=False, compression="gzip")
    risk.to_csv(risk_path, sep="\t", index=False, compression="gzip")
    print(f"-> {ngmuut_path}")
    print(f"-> {risk_path}")


if __name__ == "__main__":
    main()
