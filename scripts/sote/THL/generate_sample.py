"""Generate sample input files for THL_TEST.R

Produces all 7 files the R script reads:
  FD_1352_6460_2025_laboratoriovastaukset_2016.csv.gz  -- 29-col raw Kanta data
  Yhdistetty_biopankkikieltolista-09-09-2025_FINNGENID.txt  -- denial list (no header)
  r14_genotype_samples_from_FG.txt                     -- genotype QC list (no header)
  finngen_pic_and_finngenid_r14.txt                    -- hetu<->FINNGENID mapping
  R14_offset_090925.txt                                -- per-person day offset
  etunimitilasto-2025-08-13-dvv.xlsx                   -- first names (men: sheet 1, women: sheet 4)
  sukunimitilasto-2025-08-13-dvv.xlsx                  -- surnames (sheet 1)

Requires: openpyxl  (pip install openpyxl)
"""
import argparse
import csv
import gzip
import os
import random
import string
from pathlib import Path
import openpyxl

ap = argparse.ArgumentParser()
ap.add_argument("--N", type=int, default=1000, help="Number of lab result rows to generate")
args = ap.parse_args()

random.seed(42)

N_PERSONS = max(10, args.N // 10)
N_ROWS = args.N
OUT = Path.cwd() / "test"
os.makedirs(OUT, exist_ok=True)

# Finnish SSN checksum alphabet
_CHECKSUM = "0123456789ABCDEFHJKLMNPRSTUVWXY"


def _hetu_checksum(day, month, yy, individual):
    n = int(f"{day:02d}{month:02d}{yy:02d}{individual:03d}")
    return _CHECKSUM[n % 31]


_date_counters: dict = {}  # (day, month, yy, sep) -> next individual number


def next_hetu(birth_year):
    """Return a unique 11-char Finnish SSN using a sequential individual number per birth date."""
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    yy = birth_year % 100
    sep = "-" if birth_year < 2000 else "A"
    key = (day, month, yy, sep)
    individual = _date_counters.get(key, 2)
    _date_counters[key] = individual + 1
    check = _hetu_checksum(day, month, yy, individual)
    return f"{day:02d}{month:02d}{yy:02d}{sep}{individual:03d}{check}"


def birth_date_from_hetu(hetu):
    """Extract YYYY-MM-DD from hetu (century separator at position 6)."""
    dd, mm, yy = hetu[0:2], hetu[2:4], hetu[4:6]
    sep = hetu[6]
    century = {"+": "18", "-": "19", "A": "20"}[sep]
    return f"{century}{yy}-{mm}-{dd}"


def rand_finngenid(i):
    return f"FG{i:08d}"


def rand_tutkimusaika():
    """YYYY-MM-DD HH:MM:SS in 2016 (exactly 19 chars, as the R script requires)."""
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    h = random.randint(6, 20)
    m = random.randint(0, 59)
    s = random.randint(0, 59)
    return f"2016-{month:02d}-{day:02d} {h:02d}:{m:02d}:{s:02d}"


def rand_id(prefix, length=10):
    return prefix + "".join(random.choices(string.digits, k=length))


def rand_test_name():
    return random.choice([
        "P-Gluk", "B-Hb", "P-K", "P-Na", "P-Krea", "fP-Kol", "P-ALAT",
        "B-Leuk", "P-CRP", "P-TSH", "P-T4V", "fP-Trig", "P-Ca", "P-Urea",
    ])


def rand_selite(row_index):
    base = random.choice([
        "Glukoosi", "Hemoglobiini", "Kalium", "Natrium", "Kreatiniini",
        "Kokonaiskolesteroli", "Alaniiniaminotransferaasi", "Leukosyytit",
        "C-reaktiivinen proteiini", "Tyreotropiini", "Vapaa tyroksiini",
        "Triglyseridit", "Kalsium", "Urea",
    ])
    # Inject CR/CRLF noise in ~5% of rows to exercise normalize_newlines()
    if row_index % 20 == 0:
        return base + "\r\nLisätieto"
    if row_index % 30 == 0:
        return base + "\rhuomio"
    return base


# Name lists — defined here so both FREETEXT_WITH_PII and the xlsx files use them.
MEN_NAMES   = [("Mikael",600), ("Petteri",700), ("Juhani",800), ("Markku",500), ("Tapani",400)]
WOMEN_NAMES = [("Maarit",600), ("Pirjo",500), ("Kaarina",700), ("Tuulikki",400), ("Riitta",550)]
SURNAMES    = [("Virtanen",1500), ("Korhonen",1300), ("Leinonen",900), ("Saarinen",800), ("Mäkinen",700)]

# Freetext values with PII to exercise redaction in THL_TEST.R.
# Built from the name lists above so every name in the xlsx files also appears
# in the freetext — confirming end-to-end that redaction actually fires.
FREETEXT_WITH_PII = (
    ["Potilas 010150-123A kävi vastaanotolla"]       # hetu
    + ["Soita 0401234567 lisätietoja varten"]        # phone
    + [f"Lähete: {m} {s}" for m, _ in MEN_NAMES for s, _ in SURNAMES]
    + [f"Hoitaja {w} ilmoitti tulokset" for w, _ in WOMEN_NAMES]
    + ["Tulos OK"]                                   # no PII, unchanged
)


def rand_freetext(row_index):
    if row_index % 7 == 0:
        return random.choice(FREETEXT_WITH_PII)
    return random.choice(["", "", "", "96*", "138", "0.00", "NEGAT", "+/-"])


def rand_unit():
    return random.choice(["mmol/l", "g/l", "U/l", "µmol/l", "mg/l", "10^9/l", ""])


# ---------------------------------------------------------------------------
# Build persons
# ---------------------------------------------------------------------------
persons = []
for i in range(N_PERSONS):
    birth_year = random.randint(1930, 1975)
    hetu = next_hetu(birth_year)
    finngenid = rand_finngenid(i)
    persons.append({
        "hetu":      hetu,
        "FINNGENID": finngenid,
        "sex":       random.choice(["1", "2"]),
        "birthdate": birth_date_from_hetu(hetu),
        "offset":    random.randint(-15, 15),
    })

# 80% of persons get into fgid mapping; 20% have no FINNGENID (filtered out by R script)
n_fgid = int(N_PERSONS * 0.8)
# Of those, 87.5% are in the genotype QC list (70% of total)
n_keep = int(N_PERSONS * 0.7)
in_fgid = set(p["FINNGENID"] for p in persons[:n_fgid])
in_keep = set(p["FINNGENID"] for p in persons[:n_keep])

# ---------------------------------------------------------------------------
# 1. Denial list  (no header, one FINNGENID per line; none overlap with kanta)
# ---------------------------------------------------------------------------
with open(f"{OUT}/Yhdistetty_biopankkikieltolista-09-09-2025_FINNGENID.txt", "w") as f:
    for i in range(5):
        f.write(f"FG_DENIED_{i:04d}\n")

# ---------------------------------------------------------------------------
# 2. Genotype QC list  (no header, one FINNGENID per line)
# ---------------------------------------------------------------------------
with open(f"{OUT}/r14_genotype_samples_from_FG.txt", "w") as f:
    for p in persons:
        if p["FINNGENID"] in in_keep:
            f.write(p["FINNGENID"] + "\n")

# ---------------------------------------------------------------------------
# 3. hetu <-> FINNGENID mapping  (tab-separated, with header)
# ---------------------------------------------------------------------------
with open(f"{OUT}/finngen_pic_and_finngenid_r14.txt", "w") as f:
    f.write("pic\tFINNGENID\n")          # R script renames these to HETU/FINNGENID
    for p in persons:
        if p["FINNGENID"] in in_fgid:
            f.write(f"{p['hetu']}\t{p['FINNGENID']}\n")

# ---------------------------------------------------------------------------
# 4. Offset file  (tab-separated, with header; columns: FINNGENID, offset)
# ---------------------------------------------------------------------------
with open(f"{OUT}/R14_offset_090925.txt", "w") as f:
    f.write("FINNGENID\toffset\n")
    for p in persons:
        f.write(f"{p['FINNGENID']}\t{p['offset']}\n")

# ---------------------------------------------------------------------------
# 5. Raw Kanta CSV  (comma-separated, gzipped)
#
# 29 columns matching DF14 structure.  The R script:
#   - filters rows where nchar(hetu)==11 and nchar(tutkimusaika)==19
#   - joins FINNGENID from the mapping file
#   - filters to keep only FINNGENIDs in the genotype QC list
#   - derives SYNTPVM from hetu, APPROX_EVENT_DAY from tutkimusaika+offset
#   - drops: hetu, potilas_sukupuoli, potilassyntymaaika_pvm, tutkimusaika,
#            offset, SYNTPVM, tuloksenvalmistumisaika
# ---------------------------------------------------------------------------
RAW_COLS = [
    "hetu",
    "tutkimusaika",
    "potilas_sukupuoli",
    "potilassyntymaaika_pvm",
    "asiakirjaoid_pseudo",
    "merkintaoid_pseudo",
    "entryoid_pseudo",
    "file_name_pseudo",
    "load_id_pseudo",
    "laboratoriotutkimusoid",
    "laboratoriotutkimusnimike",
    "paikallinentutkimusnimike_koodi",
    "paikallinentutkimusnimike_selite",
    "tutkimuskoodistonjarjestelma",
    "tutkimuksentekotapa",
    "tutkimuksennaytelaatu",
    "tiedonlahde",
    "tutkimuksenlisatieto",       # new in DF14, freetext; excluded from base output
    "tuloksenvalmistumisaika",    # dropped from final output
    "tutkimusvastauksentila",
    "tutkimustulosarvo",
    "tutkimustulosyksikko",
    "tutkimustulosteksti",        # goes to freetext output file
    "tuloksenpoikkeavuus",
    "viitearvoryhma",
    "viitevalialkuarvo",
    "viitevalialkuyksikko",
    "viitevaliloppuarvo",
    "viitevaliloppuyksikko",
]
assert len(RAW_COLS) == 29

with gzip.open(
    f"{OUT}/FD_1352_6460_2025_laboratoriovastaukset_2016.csv.gz", "wt", encoding="utf-8"
) as fh:
    writer = csv.writer(fh, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(RAW_COLS)

    for row_idx in range(N_ROWS):
        p = random.choice(persons)
        tutkimusaika = rand_tutkimusaika()
        unit = rand_unit()
        val = round(random.uniform(0.1, 500.0), 3)
        lo = round(val * 0.5, 3)
        hi = round(val * 1.5, 3)

        writer.writerow([
            p["hetu"],
            tutkimusaika,
            p["sex"],
            p["birthdate"],
            rand_id("AO"),
            rand_id("MO"),
            rand_id("EO"),
            rand_id("FN"),
            rand_id("LI"),
            rand_id("LTO"),
            rand_test_name(),
            rand_id("TC", 6),
            rand_selite(row_idx),
            random.choice(["NordLab", "HUSLAB", "FIMLAB", ""]),
            random.choice(["1", ""]),
            random.choice(["B", "P", "U", "S", ""]),
            random.choice(["HOIORG", ""]),
            random.choice(["", "", "", "Lisätieto tähän", "Erityishuomio"]),
            tutkimusaika,   # tuloksenvalmistumisaika same as tutkimusaika
            random.choice(["F", "C", "P", ""]),
            val,
            unit,
            rand_freetext(row_idx),
            random.choice(["N", "A", "H", "L", ""]),
            random.choice(["Adult", ""]),
            lo,
            unit,
            hi,
            unit,
        ])

# ---------------------------------------------------------------------------
# 6. First-names Excel  (etunimitilasto-2025-08-13-dvv.xlsx)
#
# Men on sheet 1, women on sheet 4.  The R script filters Lukumäärä > 199
# and nchar(Etunimi) > 3, so all mock entries satisfy those thresholds.
# ---------------------------------------------------------------------------

wb_first = openpyxl.Workbook()

# Sheet 1: men
ws_men = wb_first.active
ws_men.title = "Miehet"
ws_men.append(["Etunimi", "Lukumäärä"])
for name, count in MEN_NAMES:
    ws_men.append([name, count])

# Sheets 2 and 3: placeholders so sheet 4 (index 3) lands at the right position
for title in ["Sheet2", "Sheet3"]:
    wb_first.create_sheet(title)

# Sheet 4: women
ws_women = wb_first.create_sheet("Naiset")
ws_women.append(["Etunimi", "Lukumäärä"])
for name, count in WOMEN_NAMES:
    ws_women.append([name, count])

wb_first.save(f"{OUT}/etunimitilasto-2025-08-13-dvv.xlsx")

# ---------------------------------------------------------------------------
# 7. Surnames Excel  (sukunimitilasto-2025-08-13-dvv.xlsx)
#
# Sheet 1.  R script filters Yhteensä > 299 and nchar(Sukunimi) > 3.
# ---------------------------------------------------------------------------

wb_last = openpyxl.Workbook()
ws_surn = wb_last.active
ws_surn.title = "Sukunimet"
ws_surn.append(["Sukunimi", "Yhteensä"])
for name, count in SURNAMES:
    ws_surn.append([name, count])

wb_last.save(f"{OUT}/sukunimitilasto-2025-08-13-dvv.xlsx")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
cr_count = sum(1 for i in range(N_ROWS) if "\r" in rand_selite(i))
pii_count = sum(1 for i in range(N_ROWS) if i % 7 == 0)

print(f"Written to {OUT}/")
print(f"  FD_...2016.csv.gz              {N_ROWS} rows, 29 cols")
print(f"    persons in fgid mapping:     {len(in_fgid)} / {N_PERSONS} ({100*len(in_fgid)//N_PERSONS}%)")
print(f"    persons in genotype QC:      {len(in_keep)} / {N_PERSONS} ({100*len(in_keep)//N_PERSONS}%)")
print(f"    rows with CR noise:          ~{cr_count}")
print(f"    rows with PII in freetext:   ~{pii_count}")
print(f"  r14_genotype_samples_from_FG.txt      {len(in_keep)} lines")
print(f"  finngen_pic_and_finngenid_r14.txt      {len(in_fgid)} lines")
print(f"  R14_offset_090925.txt                  {N_PERSONS} lines")
print(f"  Yhdistetty_biopankkikieltolista...     5 dummy denial IDs")
print(f"  etunimitilasto-2025-08-13-dvv.xlsx     {len(MEN_NAMES)} men, {len(WOMEN_NAMES)} women")
print(f"  sukunimitilasto-2025-08-13-dvv.xlsx    {len(SURNAMES)} surnames")
