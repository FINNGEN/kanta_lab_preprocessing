#!/usr/bin/env python3
"""Add the maakunta of an organization OID column to a tab-separated (.gz) file, with how it was resolved.

Made for the statements file (finngen_R12_kanta_laboratory_reports_internal_1.0.txt.gz): its antaja_organisaatioid
gets antaja_organisaatio_maakunta (the --column name without its trailing "id", + "_maakunta") in its place,
followed by three columns to check the lookup:
    <new>_how     how it was resolved (see below)
    <column>      the original OID
    <new>_match   the register OID the maakunta came from

An OID gets a maakunta if it is exactly a register unit with a known municipality, or, as the one override, if it
is a unit missing from the register whose organization is entirely in one maakunta; everything else is NA. How each
row was resolved is in <new>_how:
    exact              a register unit with a known municipality: that municipality's maakunta
    exact_old_municipality  the same, for a unit recorded with a municipality that has since been merged away (e.g.
                       838 Tarvasjoki): the maakunta Statistics Finland assigned it in its last year
                       (data/municipality_history.tsv, from data/fetch_municipality_history.py)
    org_all_units      not a register unit, but the organization in its OID (by Y-tunnus) has all its units, at
                       least ORG_MIN_UNITS of them, in one maakunta: that maakunta; <new>_match is the organization's
                       top-level entry. Tested on the register (each unit left out of its organization's counts):
                       ~99.9% right
    head_office_multi  the top-level entry (head office) of an organization whose units are spread over several
                       maakunnat (e.g. a private chain): NA, the head office's municipality says nothing about where
    unit_no_region     a register unit whose municipality code isn't a municipality, current or old (999 Ei
                       tiedossa, 200 Ulkomaat, a mistyped code): NA
    professional       1.2.246.537.28.<n>, an individual healthcare professional (self-employed practitioner) not
                       in the register: NA; these OIDs identify people
    not_found          not a register unit, and its organization isn't entirely in one maakunta: NA
    missing            empty, NA or Puuttuu: NA
The rest of each line is copied byte for byte (lines are split on tabs only, as the old pre-merge WDL did). Lines
with too few fields (e.g. after a newline inside a text field) are copied unchanged and counted.

The file is streamed line by line, so memory doesn't grow with its size (only the register and one cached result
per distinct OID are kept). --test [N] stops after the first N lines (default 100k). Decompression uses rapidgzip
(parallel) when its Python module or CLI is installed, else Python's gzip; full runs show a progress bar.

Usage
-----
  python3 anonymize_sote_ids.py --input reports.txt.gz [--column antaja_organisaatioid] [--output out.txt.gz] [--test [N]]
"""

import argparse, collections, contextlib, csv, gzip, io, os, re, shutil, subprocess, sys, time
from pathlib import Path

try:  # parallel gzip decompression if installed (pip install rapidgzip); else the rapidgzip CLI, else gzip
    import rapidgzip
except ImportError:
    rapidgzip = None

DATA_DIR = Path(__file__).resolve().parents[1] / "data"  # scripts/sote/data
SOTE = DATA_DIR / "sote.txt.gz"  # sote register: unit OID -> municipality (';'-separated, ISO-8859-1)
REGION_TABLE = DATA_DIR / "municipality_to_region.tsv"  # municipality code -> Maakunta
HISTORY_TABLE = DATA_DIR / "municipality_history.tsv"  # every municipality code since 1990 -> its maakunta
NA = "NA"
MISSING = {"", "NA", "PUUTTUU"}  # compared upper-cased
PROFESSIONAL = "1.2.246.537.28."  # individual healthcare professionals
YTUNNUS_PURITY = 0.9  # an organization with less than this share of its units in one maakunta is multi-region
ORG_MIN_UNITS = 20  # org_all_units: the organization needs at least this many units, all in one maakunta
TEST_LINES = 100_000


def load_region_table(path: Path) -> dict:
    with open(path, encoding="utf-8", newline="") as f:
        return {row["MunicipalityCode"]: row for row in csv.DictReader(f, delimiter="\t")}


def read_register(path: Path) -> list[dict]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="iso-8859-1", newline="") as fin:
        return list(csv.DictReader(fin, delimiter=";"))


# --- Y-tunnus of an OID, the same rule as sp_region_cs.py / kanta.engine.filters.filter_minimal
def ytunnus_check_digit(body):
    """Check digit of a 7-digit Y-tunnus body (weights 7,9,10,5,8,4,2, mod 11); None if no valid one exists."""
    remainder = sum(int(d) * w for d, w in zip(body, (7, 9, 10, 5, 8, 4, 2))) % 11
    return None if remainder == 1 else (0 if remainder == 0 else 11 - remainder)


def complete_old_form_code(code):
    """Old-form OIDs embed the 7-digit Y-tunnus body without check digit, leading zero kept: complete it."""
    if len(code) == 7 and code.startswith("0") and code.isdigit():
        check = ytunnus_check_digit(code)
        if check is not None:
            return (code + str(check)).lstrip("0")
    return code


def org_code(oid: str):
    """Y-tunnus digits in an OID: 1.2.246.[537.]10.<code>...; used to group an organization's units."""
    m = re.match(r"1\.2\.246\.(?:537\.)?10\.(\d+)\.", oid + ".")
    return complete_old_form_code(m.group(1)).lstrip("0") if m else None


class Maakunta:
    """Organization OID -> (maakunta or NA, why, the register OID it came from)."""

    def __init__(self, rows: list[dict], region_table: dict, history_table: dict):
        def maakunta(row):
            code = (row["A:Sijainti kunta"] or "").strip().partition(" ")[0]
            region = region_table.get(code)
            if region and region["Maakunta"]:
                return region["Maakunta"]
            old = history_table.get(code)
            if old and old["Maakunta"]:
                self.old_municipality.add(row["OrganizationId"])
                return old["Maakunta"]
            return None

        self.old_municipality = set()  # units whose maakunta comes from a merged-away municipality
        self.by_oid = {r["OrganizationId"]: maakunta(r) for r in rows}
        self.level = {r["OrganizationId"]: r["HierarchyLevel"] for r in rows}
        # Per Y-tunnus, the maakunnat of the organization's units: to tell multi-region organizations' head offices.
        self.code_maakunnat = collections.defaultdict(collections.Counter)
        self.code_top = {}  # Y-tunnus -> the organization's top-level entry (or its first unit)
        for r in sorted(rows, key=lambda r: r["HierarchyLevel"] != "0"):
            code = org_code(r["OrganizationId"])
            if code:
                self.code_top.setdefault(code, r["OrganizationId"])
                if maakunta(r):
                    self.code_maakunnat[code][maakunta(r)] += 1
        self.cache = {}

    def resolve(self, oid: str) -> tuple[str, str, str]:
        if oid not in self.cache:
            self.cache[oid] = self._resolve(oid)
        return self.cache[oid]

    def multi_region(self, oid: str) -> bool:
        """The OID's organization has units in several maakunnat, none with >= YTUNNUS_PURITY of them."""
        counts = self.code_maakunnat.get(org_code(oid))
        return bool(counts) and counts.most_common(1)[0][1] / sum(counts.values()) < YTUNNUS_PURITY

    def _resolve(self, oid: str) -> tuple[str, str, str]:
        if oid.upper() in MISSING:
            return NA, "missing", NA
        if oid in self.by_oid:
            if not self.by_oid[oid]:
                return NA, "unit_no_region", oid
            if self.level.get(oid) == "0" and self.multi_region(oid):
                return NA, "head_office_multi", oid
            return self.by_oid[oid], "exact_old_municipality" if oid in self.old_municipality else "exact", oid
        code = org_code(oid)
        counts = self.code_maakunnat.get(code) if code else None
        if counts and len(counts) == 1 and sum(counts.values()) >= ORG_MIN_UNITS:
            return next(iter(counts)), "org_all_units", self.code_top[code]
        if oid.startswith(PROFESSIONAL):
            return NA, "professional", NA
        return NA, "not_found", NA


def count_lines(path: Path, backend: str) -> int | None:
    """Line count of a .gz file with rapidgzip (parallel, fast), or None with the gzip fallback."""
    if backend == "rapidgzip":
        with rapidgzip.open(str(path), parallelization=os.cpu_count()) as f:
            n = 0
            while chunk := f.read(1 << 24):
                n += chunk.count(b"\n")
            return n
    if backend == "rapidgzip-cli":
        out = subprocess.run(["rapidgzip", "--count-lines", "-P", str(os.cpu_count()), str(path)],
                             check=True, capture_output=True, text=True).stdout
        return int(out.split()[-1])
    return None


@contextlib.contextmanager
def open_input(path: Path, backend: str):
    """The input as a binary line iterator, and a function giving the share of the compressed file read so far
    (used for progress when the line count isn't known)."""
    if path.suffix != ".gz":
        with open(path, "rb") as f:
            size = max(os.path.getsize(path), 1)
            yield f, lambda: f.tell() / size
    elif backend == "rapidgzip":
        # Must be closed (with-statement): a RapidgzipFile left open aborts Python at exit.
        with rapidgzip.open(str(path), parallelization=os.cpu_count()) as f:
            yield io.BufferedReader(f, 1 << 20), None
    elif backend == "rapidgzip-cli":
        proc = subprocess.Popen(["rapidgzip", "-d", "-c", "-P", str(os.cpu_count()), str(path)],
                                stdout=subprocess.PIPE, bufsize=1 << 20)
        try:
            yield proc.stdout, None
        finally:
            proc.stdout.close()
            proc.kill() if proc.poll() is None else None
            proc.wait()
    else:
        with open(path, "rb") as raw, gzip.GzipFile(fileobj=raw, mode="rb") as f:
            size = max(os.path.getsize(path), 1)
            yield f, lambda: raw.tell() / size


def pick_backend(path: Path) -> str:
    if path.suffix != ".gz":
        return "plain"
    if rapidgzip is not None:
        return "rapidgzip"
    if shutil.which("rapidgzip"):
        return "rapidgzip-cli"
    return "gzip"


class Progress:
    """Progress of a full run: exact (lines out of the total) when the line count is known, else the share of the
    compressed file read. A bar redrawn in place on a terminal, a plain line every 10% otherwise (e.g. when stderr
    goes to a log file)."""

    def __init__(self, total_lines: int | None, read_fraction):
        self.total, self.read_fraction = total_lines, read_fraction
        self.tty = sys.stderr.isatty()
        self.t0 = self.last = time.time()
        self.next_decile = 1

    def fraction(self, lines: int) -> float:
        if self.total:
            return min(lines / self.total, 1.0)
        return min(self.read_fraction(), 1.0) if self.read_fraction else 0.0

    def update(self, lines: int, force: bool = False) -> None:
        now = time.time()
        if not force and now - self.last < 0.5:
            return
        self.last = now
        frac = self.fraction(lines)
        elapsed = now - self.t0
        eta = elapsed * (1 - frac) / frac if frac > 0 else 0
        of = f"/{self.total:,}" if self.total else ""
        text = (f"{100 * frac:5.1f}%  {lines:,}{of} lines  {lines / max(elapsed, 1e-9):,.0f} lines/s"
                f"  elapsed {elapsed / 60:.1f} min  eta {eta / 60:.1f} min")
        if self.tty:
            bar = "#" * int(30 * frac)
            print(f"\r  [{bar:<30}] {text}", end="", file=sys.stderr, flush=True)
        elif frac * 10 >= self.next_decile or force:
            print(f"  {text}", file=sys.stderr, flush=True)
            self.next_decile = int(frac * 10) + 1

    def done(self, lines: int) -> None:
        self.update(lines, force=True)
        if self.tty:
            print(file=sys.stderr)


def replace_column(path: Path, column: str, output: Path, lookup: Maakunta, max_lines: int | None = None) -> None:
    new_name = re.sub(r"id$", "", column) + "_maakunta"
    out_opener = gzip.open if output.suffix == ".gz" else open
    lines = short = 0
    by_how = collections.Counter()
    oids_by_how = collections.defaultdict(set)
    not_found = collections.Counter()
    t0 = time.time()
    backend = pick_backend(path)
    print(f"decompression: {backend}", file=sys.stderr)
    total = None
    if max_lines is None:
        t1 = time.time()
        total = count_lines(path, backend)
        if total is not None:
            print(f"  {total:,} lines (counted in {time.time() - t1:.0f}s; incl. the header)", file=sys.stderr)
    with open_input(path, backend) as (fin, read_fraction), out_opener(output, "wb") as fout:
        progress = Progress(total and total - 1, read_fraction) if max_lines is None else None
        header = fin.readline().rstrip(b"\r\n").split(b"\t")
        names = [h.decode("utf-8", "replace") for h in header]
        if column not in names:
            sys.exit(f"{path}: no column {column!r}; columns: {names}")
        idx = names.index(column)
        header[idx] = f"{new_name}\t{new_name}_how\t{column}\t{new_name}_match".encode()
        fout.write(b"\t".join(header) + b"\n")
        for line in fin:
            if max_lines is not None and lines >= max_lines:
                break
            lines += 1
            fields = line.split(b"\t")
            if len(fields) <= idx:
                short += 1
                fout.write(line)
                continue
            raw, eol = fields[idx], b""
            if idx == len(fields) - 1:  # the column is the line's last field: keep its line ending
                stripped = raw.rstrip(b"\r\n")
                raw, eol = stripped, raw[len(stripped):]
            oid = raw.decode("utf-8", "replace").strip()
            value, how, match = lookup.resolve(oid)
            by_how[how] += 1
            oids_by_how[how].add(oid)
            if how == "not_found":
                not_found[oid] += 1
            fields[idx] = value.encode("utf-8") + b"\t" + how.encode() + b"\t" + raw + b"\t" + match.encode() + eol
            fout.write(b"\t".join(fields))
            if progress and lines % 10_000 == 0:
                progress.update(lines)
        if progress:
            progress.done(lines)

    print(f"# {column} -> {new_name}: {lines:,} lines in {time.time() - t0:.0f}s", file=sys.stderr)
    for how in ["exact", "exact_old_municipality", "org_all_units", "head_office_multi", "unit_no_region", "professional",
                "not_found", "missing"]:
        n = by_how.get(how, 0)
        print(f"  {how:<23} {n:>14,} rows ({100 * n / max(lines, 1):5.1f}%), {len(oids_by_how[how]):,} distinct OIDs",
              file=sys.stderr)
    if short:
        print(f"  !!! {short:,} lines with fewer than {idx + 1} fields, copied unchanged", file=sys.stderr)
    if not_found:
        print("  top OIDs not found:", file=sys.stderr)
        for oid, n in not_found.most_common(10):
            print(f"    {n:>12,}  {oid}", file=sys.stderr)
    print(f"Written to {output}", file=sys.stderr)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", required=True, type=Path, help="Tab-separated (.gz) file with the OID column")
    p.add_argument("--column", default="antaja_organisaatioid", help="Organization OID column to replace")
    p.add_argument("--output", type=Path, default=None,
                   help="Output (default: <input name>_maakunta[_test].txt.gz in the current folder)")
    p.add_argument("--test", nargs="?", type=int, const=TEST_LINES, default=None, metavar="N",
                   help=f"Only the first N lines (N defaults to {TEST_LINES:,})")
    args = p.parse_args()
    if args.test is not None and args.test < 1:
        sys.exit("--test N must be positive")

    output = args.output or Path(args.input.name.split(".")[0] + f"_maakunta{'_test' if args.test else ''}.txt.gz")
    if output.exists():
        sys.exit(f"{output} already exists. Aborting.")
    if output.resolve() == args.input.resolve():
        sys.exit("--output must differ from --input")

    rows = read_register(SOTE)
    region_table = load_region_table(REGION_TABLE)
    history_table = load_region_table(HISTORY_TABLE)
    print(f"sote register: {len(rows):,} units; region table: {len(region_table)} municipalities; history: "
          f"{len(history_table)} municipality codes", file=sys.stderr)
    if args.test:
        print(f"TEST: first {args.test:,} lines only", file=sys.stderr)
    replace_column(args.input, args.column, output, Maakunta(rows, region_table, history_table), args.test)


if __name__ == "__main__":
    main()
