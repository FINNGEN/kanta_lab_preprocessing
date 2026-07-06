"""
coding_map.py

Extract unique values of tutkimuskoodistonjarjestelma (raw CODING_SYSTEM column) from a
Kanta Lab parquet file, with their counts, via a clickhouse query. Then, for every value
under a known OID root (CODE_ROOTS), resolve the organization name from the embedded
Y-tunnus (business id), trying each of the following in order until one has it:
  1. PRH's full company registry (all_companies bulk export; private companies)
  2. PTV (Palvelutietovaranto), Finland's open public-sector service catalog, which covers
     municipalities/kuntayhtymät/hyvinvointialueet/state agencies that PRH's bulk export excludes
  3. a hand-maintained mapping (MANUAL_NAMES_BY_CODE) for codes neither of the above covers:
     hyvinvointialueet and their pre-2023 kuntayhtymä/sairaanhoitopiiri predecessors (public-law
     bodies never on the Trade Register), plus a few private companies dissolved/merged before
     the all_companies bulk export was taken
"NA" if none of the above has it, or if the value doesn't fall under a known root at all
(every distinct raw value still gets a row in the output, so none of them go missing
silently). Caches all results in the current working directory so re-running doesn't redo
work already done: a companies.pkl index of the full PRH registry (business_id -> name) so
it only has to be parsed out of the zip once, and a ptv.pkl answer cache (including confirmed
misses) that's incrementally expanded as new business ids are queried.

Usage:
    python3 coding_map.py <input_file.parquet>
"""

import argparse
import io
import json
import pickle
import re
import subprocess
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

import pandas as pd

COLUMN = "tutkimuskoodistonjarjestelma"

# Both OID roots embed the same kind of organization identifier (an integer derived from a
# Y-tunnus) right after the root; the trailing arcs differ but aren't otherwise meaningful here.
CODE_ROOTS = ["1.2.246.10.", "1.2.246.537.10."]
CODE_PATTERNS = [re.compile(rf"^{re.escape(root)}(\d+)(?:\.|$)") for root in CODE_ROOTS]
MAX_HTTP_RETRIES = 5
DEFAULT_RETRY_DELAY = 30
ALL_COMPANIES_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3/all_companies"
ALL_COMPANIES_CACHE_NAME = "all_companies.zip"
COMPANIES_PKL_NAME = "companies.pkl"
PTV_ORG_URL_TEMPLATE = "https://api.palvelutietovaranto.suomi.fi/api/v11/Organization/businesscode/{business_id}"
PTV_PKL_NAME = "ptv.pkl"

# Hyvinvointialueet (wellbeing services counties) aren't companies and PTV doesn't carry all of
# them either, so their OID codes are listed here by hand rather than resolved via an API.
HYVINVOINTIALUE_OIDS = {
    "1.2.246.10.7259373.10.0": "Etelä-Karjala",
    "1.2.246.10.32213238.10.0": "Etelä-Pohjanmaa",
    "1.2.246.10.32213158.10.0": "Etelä-Savo",
    "1.2.246.10.2012566.10.0": "Helsinki",
    "1.2.246.10.32213393.10.0": "Itä-Uusimaa",
    "1.2.246.10.19056522.10.0": "Kainuu",
    "1.2.246.10.32213078.10.0": "Kanta-Häme",
    "1.2.246.10.2164623.10.0": "Keski-Pohjanmaa",
    "1.2.246.10.32213182.10.0": "Keski-Suomi",
    "1.2.246.10.28449694.10.0": "Keski-Uusimaa",
    "1.2.246.10.7259015.10.0": "Kymenlaakso",
    "1.2.246.10.32213326.10.0": "Lappi",
    "1.2.246.10.32213473.10.0": "Länsi-Uusimaa",
    "1.2.246.10.32213086.10.0": "Pirkanmaa",
    "1.2.246.10.3493883.10.0": "Pohjanmaa",
    "1.2.246.10.32213174.10.0": "Pohjois-Karjala",
    "1.2.246.10.32213262.10.0": "Pohjois-Pohjanmaa",
    "1.2.246.10.32213166.10.0": "Pohjois-Savo",
    "1.2.246.10.32213094.10.0": "Päijät-Häme",
    "1.2.246.10.32213043.10.0": "Satakunta",
    "1.2.246.10.32213561.10.0": "Vantaa_ja_Kerava",
    "1.2.246.10.32210651.10.0": "Varsinais-Suomi",
}


def get_value_counts(input_file: Path) -> pd.DataFrame:
    """Return value counts for COLUMN, using a cached TSV in the current working dir if present."""
    cache_file = Path.cwd() / (input_file.name + ".coding_map_counts.tsv")

    if cache_file.exists():
        print(f"[coding_map] Using cached counts: {cache_file}")
        return pd.read_csv(cache_file, sep="\t", dtype=str, keep_default_na=False)

    print(f"[coding_map] Querying {COLUMN} value counts from {input_file} via clickhouse...")
    query = (
        f"SELECT {COLUMN}, count() AS count FROM '{input_file}' "
        f"GROUP BY {COLUMN} HAVING count > 10 ORDER BY count DESC"
    )
    result = subprocess.run(
        ["clickhouse", "-q", query, "--format", "TSVWithNames"],
        check=True,
        capture_output=True,
        text=True,
    )

    cache_file.write_text(result.stdout)
    counts = pd.read_csv(cache_file, sep="\t", dtype=str, keep_default_na=False)
    print(f"[coding_map] Found {len(counts)} distinct {COLUMN} value(s); cached to {cache_file}")
    return counts


def extract_code(value: str) -> str | None:
    """Extract the digit segment right after any of CODE_ROOTS, e.g.
    "1.2.246.10.23925196.6.3.2016" -> "23925196",
    "1.2.246.537.10.3575029.12.2.10.1102012566.6.17" -> "3575029", or a bare
    "1.2.246.10.24838684" (no trailing arc at all) -> "24838684"."""
    for pattern in CODE_PATTERNS:
        match = pattern.match(value)
        if match:
            return match.group(1)
    return None


HYVINVOINTIALUE_NAMES_BY_CODE = {
    extract_code(oid): name.replace(" ", "_") for oid, name in HYVINVOINTIALUE_OIDS.items()
}

# Codes that resolve to neither PRH's registry nor PTV: mostly public-law kuntayhtymät and
# sairaanhoitopiirit (pre-2023 predecessors of hyvinvointialueet, same blind spot) never on the
# Trade Register, plus a handful of private companies dissolved/merged before the all_companies
# bulk export was taken (still real, just absent from the live snapshot). Cross-checked against
# thl_coding_manual_mapping.txt and, individually, against PRH's per-company endpoint (which
# covers dissolved companies) and/or public business-registry lookups (see NordLab, Negen Oy,
# Helsingin yliopisto). Names are the historical trade name at the time, not necessarily the
# current one (some of these companies were later sold and the shell renamed to something
# unrelated).
MANUAL_NAMES_BY_CODE = {
    **HYVINVOINTIALUE_NAMES_BY_CODE,
    "24838684": "NordLab",
    "2156068": "Päijät-Hämeen_hyvinvointikuntayhtymä",
    "8282559": "VSSHP_ky",
    "1714953": "Äitiys-_ja_ehkäisyneuvola,_Juankosken_ta,_Kuopio_perusturva",
    "2159787": "KSSHP",
    "8259156": "SATSHP",
    "8182355": "K-HSHP",
    "2430960": "EPSHP",
    "27320952": "Pohjois-Karjalan_sosiaali-_ja_terveyspalvelujen_kuntayhtymä",
    "8255083": "Etelä-Savon_sosiaali_-_ja_terveyspalvelujen_kuntayhtymä",
    "22810709": "Yhtyneet_Medix_Laboratoriot_Oy",
    "2142950": "FSHKY",
    "23388722": "Perusturvakuntayhtymä_Akseli",
    "8196167": "Korvaushoitoklinikka,_Rovaniemen_kaupunki,_Perusturvapalv",
    "8286189": "Länsi-Pohjan_sairaanhoitopiirin_kuntayhtymä",
    "21256902": "Kallio_PPKY",
    "22658751": "Ylä-Savon_SOTE_ky",
    "2159250": "Hammashoitola,_Rantasalmi",
    "2044111": "LOPETETTU_Yläneen_hammashoitola,_Pöytyän_ktt:n_ky",
    "2058097": "Riihimäen_seudun_terveyskeskuksen_kuntayhtymä",
    "24787186": "SYNLAB_Finland_Oy",
    "10065385": "Oulunkaaren_ky",
    "2042132": "Paimion-Sauvon_ktky",
    "22206827": "JIK_ky",
    "2026378": "Ylioppilaiden_terveydenhoitosäätiö_sr",
    "22054886": "PoSa",
    "19832309": "Kuusiokuntien_sosiaali-ja_terveyskuntayhtymä",
    "21872801": "PTKY_Karviainen",
    "22042279": "Suupohjan_peruspalveluliikelaitoskuntayhtymä",
    "23065253": "Työterveys_Wellamo_Oy",
    "2085212": "Saarikka",
    "22654152": "Äitiysneuvola_Kärsämäki,_Kärsämäki",
    "2037384": "Keski-Satakunnan_thky",
    "20712777": "Ky_Kaksineuvoinen",
    "2149132": "Sisä-Savon_thky",
    "8693163": "Oulunkylän_kuntoutuskeskus_sr",
    "27715014": "Työterveys_Virta_Oy",
    "27735314": "Terveyspalvelu_Verso_Oy",
    "20506818": "Docrates_Oy",
    "27775869": "Turun_Seudun_Työterveystalo_Oy",
    "18750125": "Etelä-Karjalan_Työkunto_Oy",
    "6538773": "Pihlajalinna_Etelä-Savo_Oy",
    "9434972": "Vantaan_Työterveys_Oy",
    "2105260": "Pelkosenniemen-Savukosken_ktt:n_ky",
    "8494615": "City_Läkarna_Mariehamn_Ab",
    "1592300": "Imatran_Työterveys_ry",
    "2459771": "Eduskunnan_työterveys",
    "27626695": "SeiMedi_Oy",
    "2034538": "Oy_Porvoon_Lääkärikeskus_Borgå_Läkarcentral_Ab",
    "1702645": "Iisalmen_Työterveysasema_Oy",
    "19906785": "MedInari_Oy",
    "26062357": "Diagnos_Terveyspalvelut_Oy",
    "18597562": "Promedi_Oy/_Ab",
    "2078741": "Kuopion_Työterveys_ry",
    "8609654": "Oy_Femeda_Ab",
    "19918698": "Sairaala_Eira_Oy",
    "1074183": "Pihlajalinna_Lääkärikeskukset_Oy",
    "6794809": "PPSHP",
    "25813542": "Apila_Terveys_Oy",
    "24969860": "Lohtajan_koulun_kouluterv.huolto,_Kainuun_hva",
    "4325072": "Lahden_seudun_Yritysterveys_ry",
    "22727622": "Pikkujätti_lasten_ja_nuorten_lääkäriasema_Oy",
    "1497805": "Kipinä_Terveys_Osuuskunta",
    "27146405": "Lääkäriasema_Joutsen_Oy",
    "2068367": "Lappeenrannan_Työterveys_ry",
    "22952424": "NEO_Terveys_Oy",
    "27860107": "Pihlajalinna_Seppälääkärit_Oy",
    "26086463": "Cityterveys_Oy",
    "20658256": "Doctagon_Ab",
    "16186831": "Metso_Shared_Services_Oy",
    "2809030": "S-työterveys_Kuopio_ry",
    "21580649": "TyöSyke_Oy",
    "16115945": "Lääkäriasema_Cantti_Oy",
    "19637124": "COOR_SERVICE_MANAGEMENT_LP_OY",
    "2025113": "Urheilulääketieteen_säätiö_sr",
    "4600310": "Vakka-Suomen_Lääkärikeskus_Oy",
    "26121433": "Asemanpuiston_Lääkärikeskus_Oy",
    "26771567": "Negen_Oy",
    "3134717": "Helsingin_yliopisto",
}


def to_business_id(code: str) -> str | None:
    """Format an OID code as a Finnish Y-tunnus, e.g. "23925196" -> "2392519-6".

    OID components are plain integers, so a Y-tunnus starting with "0" loses that leading
    zero when embedded (e.g. "3575029" is really "03575029", i.e. "0357502-9"); zero-pad to
    8 digits before splitting into body + check digit.
    """
    if not code.isdigit() or len(code) > 8:
        return None
    padded = code.zfill(8)
    return f"{padded[:7]}-{padded[7]}"


def iter_json_array(fileobj, chunk_size=1 << 20):
    """Yield each top-level element of a JSON array read from fileobj, without loading it all
    into memory at once (the PRH bulk export is ~1.4GB of uncompressed JSON)."""
    decoder = json.JSONDecoder()
    buf = fileobj.read(chunk_size)
    buf = buf.lstrip()
    if buf.startswith("["):
        buf = buf[1:]

    while True:
        buf = buf.lstrip().lstrip(",")
        if not buf or buf == "]":
            chunk = fileobj.read(chunk_size)
            if not chunk:
                break
            buf += chunk
            continue
        try:
            obj, idx = decoder.raw_decode(buf)
        except json.JSONDecodeError:
            chunk = fileobj.read(chunk_size)
            if not chunk:
                break
            buf += chunk
            continue
        yield obj
        buf = buf[idx:]


def download_all_companies() -> bytes:
    """Return the PRH bulk company registry ZIP bytes, using a cache in the current working dir
    if present. The cache is shared across input files/runs (delete it to force a fresh
    download); retries with backoff on rate limiting (429)."""
    cache_path = Path.cwd() / ALL_COMPANIES_CACHE_NAME
    if cache_path.exists():
        print(f"[coding_map] Using cached company registry: {cache_path}")
        return cache_path.read_bytes()

    for attempt in range(1, MAX_HTTP_RETRIES + 1):
        try:
            print(
                f"[coding_map] Downloading PRH company registry from {ALL_COMPANIES_URL} "
                f"(attempt {attempt}/{MAX_HTTP_RETRIES})..."
            )
            with urllib.request.urlopen(ALL_COMPANIES_URL) as response:
                zip_bytes = response.read()
            cache_path.write_bytes(zip_bytes)
            print(f"[coding_map] Cached company registry to {cache_path}")
            return zip_bytes
        except urllib.error.HTTPError as e:
            if e.code != 429 or attempt == MAX_HTTP_RETRIES:
                raise
            retry_after = e.headers.get("Retry-After")
            delay = int(retry_after) if retry_after and retry_after.isdigit() else DEFAULT_RETRY_DELAY * attempt
            print(f"[coding_map] Rate limited (429). Waiting {delay}s before retrying...")
            time.sleep(delay)

    raise AssertionError("unreachable")


def fetch_json_with_retries(url: str):
    """GET url and parse the response as JSON, retrying with backoff on rate limiting (429),
    same as download_all_companies. Returns None on 404 (not found)."""
    for attempt in range(1, MAX_HTTP_RETRIES + 1):
        try:
            with urllib.request.urlopen(url) as response:
                return json.load(response)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code != 429 or attempt == MAX_HTTP_RETRIES:
                raise
            retry_after = e.headers.get("Retry-After")
            delay = int(retry_after) if retry_after and retry_after.isdigit() else DEFAULT_RETRY_DELAY * attempt
            print(f"[coding_map] Rate limited (429) on {url}. Waiting {delay}s before retrying...")
            time.sleep(delay)

    raise AssertionError("unreachable")


def build_companies_index() -> dict[str, str]:
    """Return a business_id -> name mapping for every company in the PRH bulk registry, using
    a cached pickle in the current working directory if present (avoids re-parsing the ~1.4GB
    JSON export on every run)."""
    pkl_path = Path.cwd() / COMPANIES_PKL_NAME
    if pkl_path.exists():
        print(f"[coding_map] Using cached company index: {pkl_path}")
        with pkl_path.open("rb") as f:
            return pickle.load(f)

    zip_bytes = download_all_companies()
    print(f"[coding_map] Downloaded {len(zip_bytes) / 1e6:.1f} MB, indexing all companies...")

    companies = {}
    scanned = 0
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        with zf.open(zf.namelist()[0]) as raw_file:
            text_stream = io.TextIOWrapper(raw_file, encoding="utf-8")
            for company in iter_json_array(text_stream):
                scanned += 1
                if scanned % 200_000 == 0:
                    print(f"[coding_map]   scanned {scanned:,} companies, indexed {len(companies)}...")

                business_id = (company.get("businessId") or {}).get("value")
                names = company.get("names") or []
                if business_id and names:
                    companies[business_id] = names[0]["name"].replace(" ", "_")

    print(f"[coding_map] Done scanning {scanned:,} companies: indexed {len(companies)}.")
    with pkl_path.open("wb") as f:
        pickle.dump(companies, f)
    print(f"[coding_map] Cached company index to {pkl_path}")
    return companies


def lookup_company_names_bulk(business_ids: set[str]) -> dict[str, str]:
    """Resolve business_ids against the full PRH company index (built/cached once as
    companies.pkl)."""
    if not business_ids:
        return {}
    companies = build_companies_index()
    return {bid: companies[bid] for bid in business_ids if bid in companies}


def lookup_public_organization_name(business_id: str) -> str | None:
    """Look up a Finnish public-sector organization (municipality, kuntayhtymä, hyvinvointialue,
    state agency, etc.) by Y-tunnus via PTV (Palvelutietovaranto), Finland's open public service
    catalog. Returns None if not found there either (e.g. a private company with no public
    service listed)."""
    url = PTV_ORG_URL_TEMPLATE.format(business_id=business_id)
    organizations = fetch_json_with_retries(url)

    if not organizations:
        return None

    names = organizations[0].get("organizationNames") or []
    for preferred_type in ("Name", "AlternativeName"):
        for entry in names:
            if entry.get("type") == preferred_type and entry.get("language") == "fi":
                return entry["value"].replace(" ", "_")
    if names:
        return names[0]["value"].replace(" ", "_")
    return None


def load_pkl_cache(name: str) -> dict[str, str | None]:
    """Load an answer cache (business_id -> name, or None for a confirmed miss) from a pickle
    file of the given name in the current working directory, if present."""
    pkl_path = Path.cwd() / name
    if pkl_path.exists():
        with pkl_path.open("rb") as f:
            return pickle.load(f)
    return {}


def save_pkl_cache(name: str, cache: dict[str, str | None], announce: bool = True) -> None:
    pkl_path = Path.cwd() / name
    with pkl_path.open("wb") as f:
        pickle.dump(cache, f)
    if announce:
        print(f"[coding_map] Cached answers to {pkl_path}")


def lookup_public_organization_names(business_ids: set[str]) -> dict[str, str]:
    """Resolve business_ids against PTV (no bulk export exists for this), reusing a cache of
    prior answers in ptv.pkl and only querying ids not already in it. Misses are cached too
    (as None) so they aren't re-queried on a later run."""
    cache = load_pkl_cache(PTV_PKL_NAME)
    uncached = business_ids - cache.keys()
    if uncached:
        print(f"[coding_map] Querying PTV for {len(uncached)} business id(s) not yet cached...")
        for business_id in sorted(uncached):
            cache[business_id] = lookup_public_organization_name(business_id)
        save_pkl_cache(PTV_PKL_NAME, cache)
    return {bid: cache[bid] for bid in business_ids if cache.get(bid)}


def build_coding_table(input_file: Path, counts: pd.DataFrame) -> pd.DataFrame:
    """Resolve a name for every value in counts, using/updating a cache.

    Each row's "SOURCE" column records which of PRH/PTV/MANUAL resolved the name, or "NA" if
    none did. Values under a known CODE_ROOTS prefix are grouped by their extracted org code
    (the same code can appear under either root, so those rows are merged); everything else (a
    handful of fixed/rootless values, plus the literal "NA") is kept as its own row keyed by the
    raw value,
    so every row of the input counts is accounted for in the output rather than silently dropped.

    The result is sorted by count (summed across raw values sharing the same code) descending,
    same convention as get_value_counts.
    """
    cache_file = Path.cwd() / (input_file.name + ".coding_table.txt")

    resolved = {}
    if cache_file.exists():
        print(f"[coding_map] Using cached coding table: {cache_file}")
        cached = pd.read_csv(cache_file, sep="\t", dtype=str, keep_default_na=False)
        resolved = {
            code: (name, source)
            for code, name, source in zip(cached["CODE"], cached["NAME"], cached["SOURCE"])
        }

    counts = counts.copy()
    counts["code"] = [extract_code(value) or value for value in counts[COLUMN]]
    code_counts = counts.astype({"count": "int64"}).groupby("code")["count"].sum()

    codes = sorted(code_counts.index)
    unresolved_codes = [code for code in codes if code not in resolved]

    if unresolved_codes:
        print(f"[coding_map] {len(unresolved_codes)} of {len(codes)} code(s) need resolving.")
        business_ids_by_code = {code: to_business_id(code) for code in unresolved_codes}
        business_ids = {bid for bid in business_ids_by_code.values() if bid}

        names_by_business_id = lookup_company_names_bulk(business_ids)
        source_by_business_id = {bid: "PRH" for bid in names_by_business_id}

        still_missing = business_ids - names_by_business_id.keys()
        if still_missing:
            print(
                f"[coding_map] {len(still_missing)} business id(s) not in PRH data; "
                f"checking PTV (public-sector organizations)..."
            )
            ptv_names = lookup_public_organization_names(still_missing)
            names_by_business_id.update(ptv_names)
            source_by_business_id.update({bid: "PTV" for bid in ptv_names})

        for code in unresolved_codes:
            if code in MANUAL_NAMES_BY_CODE:
                resolved[code] = (MANUAL_NAMES_BY_CODE[code], "MANUAL")
                continue
            business_id = business_ids_by_code[code]
            name = names_by_business_id.get(business_id) if business_id else None
            resolved[code] = (name, source_by_business_id[business_id]) if name else ("NA", "NA")
    else:
        print(f"[coding_map] All {len(codes)} code(s) already resolved.")

    result = pd.DataFrame(
        [(code, name, source) for code, (name, source) in sorted(resolved.items())],
        columns=["CODE", "NAME", "SOURCE"],
    )
    result["COUNT"] = result["CODE"].map(code_counts).fillna(0).astype("int64")
    result = result.sort_values("COUNT", ascending=False).reset_index(drop=True)
    result = result[["CODE", "NAME", "COUNT", "SOURCE"]]
    result.to_csv(cache_file, sep="\t", index=False)
    print(f"[coding_map] Wrote coding table ({len(result)} code(s)) to {cache_file}")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_file", type=Path, help="Path to the Kanta Lab parquet file")
    args = parser.parse_args()

    counts = get_value_counts(args.input_file)
    build_coding_table(args.input_file, counts)


if __name__ == "__main__":
    main()
