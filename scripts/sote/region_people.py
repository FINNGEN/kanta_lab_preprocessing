"""Most likely maakunta per individual, from the maakunta of the service providers (SP) of their lab events.

Each event's SERVICE_PROVIDER is mapped to a maakunta via sp_region.tsv (written by sp_region_cs.py: run that
first, on the same engine output). Events whose SP has no maakunta are dropped, so everything below is computed
over each person's regional events only. Per person, the most prevalent maakunta is returned, plus a `maakunta`
column holding it only if its share of the person's regional events is above --threshold, and last a JSON dict
of the person's top 3 maakunta with their shares.

Last, the assigned maakunta is compared with region of birth and with cohort (regionofbirthname, cohort in --pheno),
and as clinical checks, vitamin D by month is summarised per assigned maakunta with a fitted annual cycle (level,
peak month) and compared with latitude, and the % of tested people positive for Puumala hantavirus (known east/central
vs west-coast pattern) is computed per maakunta; and FinnGen endpoint prevalence (type 2 diabetes, coronary heart
disease; age-standardised) per assigned maakunta is compared with THL/Kela's age-standardised disease indices per
region (data/sotkanet_health_index_<year>.tsv, written by fetch_sotkanet.py). Figures are written next to the tables.

Caching: the steps that scan the engine parquet (per-person table, vitamin D and serology aggregations) are skipped
when their output file already exists, and that file is loaded instead, so an interrupted run picks up where it was.
Derived tables and figures are kept when their source table was loaded from cache, and rewritten when it was just
recomputed (they would be stale otherwise). --force recomputes and overwrites everything.
"""
import argparse, hashlib, json
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

# regionofbirthname uses English names for some maakunta: translate to the Finnish names used in sp_region.tsv
BIRTH_TO_MAAKUNTA = {
    'North_Ostrobothnia': 'Pohjois-Pohjanmaa', 'Central_Finland': 'Keski-Suomi', 'North_Karelia': 'Pohjois-Karjala',
    'Lapland': 'Lappi', 'South_Ostrobothnia': 'Etelä-Pohjanmaa', 'Ostrobothnia': 'Pohjanmaa',
    'South_Karelia': 'Etelä-Karjala', 'Central_Ostrobothnia': 'Keski-Pohjanmaa', 'Åland': 'Ahvenanmaa',
}
# birth regions with no current maakunta: left out of the comparison
BIRTH_OUTSIDE_FINLAND = {'Abroad', 'Region_ceded_to_Soviet'}

# approximate latitude of each maakunta's main town, to order regions south -> north in the vitamin D check
# maakunta with a sea coast; the rest are inland. Pohjois-Pohjanmaa and Lappi touch the sea but are mostly inland:
# kept as coastal (fixed rule, not adjusted to the data), which blurs the coastal/inland contrast somewhat
COASTAL = {'Ahvenanmaa', 'Uusimaa', 'Varsinais-Suomi', 'Satakunta', 'Pohjanmaa', 'Keski-Pohjanmaa', 'Pohjois-Pohjanmaa',
           'Lappi', 'Kymenlaakso'}

MAAKUNTA_LAT = {
    'Ahvenanmaa': 60.1, 'Uusimaa': 60.2, 'Varsinais-Suomi': 60.5, 'Kymenlaakso': 60.7, 'Päijät-Häme': 61.0,
    'Kanta-Häme': 61.0, 'Etelä-Karjala': 61.1, 'Satakunta': 61.5, 'Pirkanmaa': 61.5, 'Etelä-Savo': 61.7,
    'Keski-Suomi': 62.2, 'Pohjois-Karjala': 62.6, 'Etelä-Pohjanmaa': 62.8, 'Pohjois-Savo': 62.9, 'Pohjanmaa': 63.1,
    'Keski-Pohjanmaa': 63.8, 'Kainuu': 64.2, 'Pohjois-Pohjanmaa': 65.0, 'Lappi': 66.5,
}

ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
ap.add_argument('--input', required=True, type=Path, help="Engine output parquet (needs FINNGENID, SERVICE_PROVIDER)")
ap.add_argument('--out', type=Path, default=Path('/mnt/disks/data/kanta/service-provider'), help="Output directory; writes person_maakunta_t<threshold>.tsv and the other t<threshold>-tagged outputs there")
ap.add_argument('--sp-region', type=Path, default=None, help="sp_region.tsv from sp_region_cs.py (default: <out>/sp_region.tsv)")
ap.add_argument('--sp-min-pct', type=float, default=0, help="Only use SPs whose own maakunta purity (pct_maakunta) is at least this (default: 0, all SPs with a maakunta)")
ap.add_argument('--threshold', type=float, default=50,
                help="%% threshold: the person's top maakunta goes in column `maakunta` only if its share of their "
                     "regional events is > this (default: 50, i.e. a strict majority)")
ap.add_argument('--pheno', type=Path, default=Path('~/r14/pheno/R14_COV_PHENO_V0.FID.txt.gz'),
                help="FinnGen covariate + endpoint file: FID, regionofbirthname, cohort, age, sex and 0/1/NA endpoint columns "
                     "(default: ~/r14/pheno/R14_COV_PHENO_V0.FID.txt.gz)")
ap.add_argument('--vitd-omop', default='40771025', help="OMOP concept of 25-OH vitamin D (default: 40771025)")
ap.add_argument('--vitd-min-people', type=int, default=50, help="Hide maakunta x month cells with fewer person-months than this (default: 50)")
ap.add_argument('--sero-min-people', type=int, default=30, help="Hide maakunta x serology cells with fewer tested people than this (default: 30)")
ap.add_argument('--age-col', default='AGE_AT_DEATH_OR_END_OF_FOLLOWUP',
                help="Age in --pheno used to standardise endpoint prevalence: age reached by death or end of follow-up, since "
                     "endpoint status is 'ever by end of follow-up' (default: AGE_AT_DEATH_OR_END_OF_FOLLOWUP)")
ap.add_argument('--sex-col', default='SEX', help="Sex column in --pheno, also used to standardise endpoint prevalence (default: SEX)")
ap.add_argument('--sotkanet', type=Path, default=Path(__file__).resolve().parent / 'data' / 'sotkanet_health_index_2024.tsv',
                help="Reference disease indices per hyvinvointialue from fetch_sotkanet.py (default: data/sotkanet_health_index_2024.tsv next to this script)")
ap.add_argument('--exclude-cohorts', type=lambda s: sorted(c for c in s.split(',') if c), default=[],
                help="Comma-separated cohorts left out of the disease comparison, e.g. ones recruited for a disease "
                     "(THL_BIOBANK_BOTNIA,THL_BIOBANK_FUSION,THL_BIOBANK_T1D,THL_BIOBANK_COROGENE,THL_BIOBANK_HHS); default: none")
ap.add_argument('--disease-min-people', type=int, default=100, help="Leave out maakunta with fewer people with endpoint data than this (default: 100)")
ap.add_argument('--min-cell', type=int, default=5,
                help="Shareable tables show person counts from 1 to this minus 1 as '<N' and blank the values derived from "
                     "them (default: 5, i.e. no counts under 5)")
ap.add_argument('--force', action='store_true', help="Recompute and overwrite every output, ignoring existing files")
args = ap.parse_args()
pheno = args.pheno.expanduser()
if not pheno.exists():
    raise SystemExit(f"{pheno} not found (--pheno)")
sp_region = args.sp_region or args.out / 'sp_region.tsv'
if not sp_region.exists():
    raise SystemExit(f"{sp_region} not found: run sp_region_cs.py on the same engine output first")
args.out.mkdir(parents=True, exist_ok=True)
# every output below depends on --threshold (person_maakunta via its `maakunta` column, the rest by using only
# people above it), so its value goes into each file name, e.g. person_maakunta_t50.tsv
TAG = f"t{args.threshold:g}" + (f"_sp{args.sp_min_pct:g}" if args.sp_min_pct else "")  # --sp-min-pct changes every output too
THRESHOLD_DESC = f"people whose top maakunta has > {args.threshold:g}% of their regional events"
out_file = args.out / f'person_maakunta_{TAG}.tsv'


def cached(path):
    """True if `path` exists and may be reused (no --force)."""
    return path.exists() and not args.force


def is_small(values):
    """Person counts from 1 to --min-cell - 1 (0 is fine to show)."""
    return pd.Series(values).between(1, args.min_cell - 1).fillna(False).astype(bool)


def mask_small(df, count_cols, derived_cols=(), also_flag=None):
    """Copy of df for sharing: person counts from 1 to --min-cell - 1 in count_cols become '<N', and on any row with
    such a count, or flagged in also_flag (e.g. a small complement such as non-cases), the derived_cols (percentages,
    indices, intervals) are blanked, since they would give the small count back."""
    out = df.copy()
    flagged = pd.Series(False, index=out.index) if also_flag is None else also_flag.reindex(out.index, fill_value=False)
    for c in count_cols:
        small = is_small(out[c])
        flagged |= small
        out[c] = out[c].astype(object).where(~small, f"<{args.min_cell}")
    for c in derived_cols:
        out[c] = out[c].astype(object).where(~flagged, None)
    return out


def write_tsv_once(df, path, fresh=False):
    """Write a derived table unless it is already there (cached) and its source data was loaded from cache too;
    `fresh` = the source data was just recomputed, so an existing derived file is stale and gets rewritten."""
    if cached(path) and not fresh:
        print(f"Kept existing {path}")
    else:
        df.to_csv(path, sep='\t', index=False)
        print(f"Wrote {path}")

con = duckdb.connect()
P = f"read_parquet('{args.input}')"
missing = {'FINNGENID', 'SERVICE_PROVIDER'} - set(con.sql(f"select * from {P} limit 0").columns)
if missing:
    raise SystemExit(f"{args.input}: not an engine output file, missing columns {sorted(missing)}")

person_fresh = not cached(out_file)
if not person_fresh:
    # per-person table from an earlier run: load it instead of re-scanning the engine output
    con.sql(f"""create table person as
      select FINNGENID, n_events::bigint n_events, top_maakunta, n_top::bigint n_top, pct_top::double pct_top, maakunta, top3_maakunta
      from read_csv('{out_file}', delim='\t', header=true, all_varchar=true, quote='', escape='')""")
    n_with, k = con.sql("select count(*), count(maakunta) from person").fetchone()
    print(f"Loaded existing {out_file} (--force to recompute): {n_with:,} people with a regional event, "
          f"{k:,} with top maakunta > {args.threshold:g}% of their regional events")
else:
    # SP -> maakunta, only SPs that have one
    con.sql(f"""create table sp as select s, maakunta from read_csv('{sp_region}', delim='\t', header=true, all_varchar=true, quote='', escape='')
                where maakunta is not null and pct_maakunta::double >= {args.sp_min_pct}""")
    print(f"{con.sql('select count(*) from sp').fetchone()[0]:,} SPs mapped to a maakunta (pct_maakunta >= {args.sp_min_pct})")

    # events per (person, SP), then per (person, maakunta), dropping events whose SP has no maakunta
    con.sql(f"create table fs as select FINNGENID f, SERVICE_PROVIDER s, count(*)::bigint n from {P} group by all")
    con.sql("create table fm as select f, maakunta, sum(n)::bigint n from fs join sp using (s) group by all")

    # per person: top maakunta and its share of their regional events; ties broken by name
    con.sql(f"""create table person as
      with ranked as (
        select f, maakunta, n, sum(n) over (partition by f) n_events, 100.0 * n / sum(n) over (partition by f) pct,
               row_number() over (partition by f order by n desc, maakunta) rk
        from fm)
      select r.f FINNGENID, n_events, maakunta top_maakunta, n n_top, round(pct, 2) pct_top,
             case when pct > {args.threshold} then maakunta end maakunta,
             t.top3_maakunta
      from ranked r join (
        -- JSON dict of the person's top 3 maakunta -> % of their regional events, in decreasing order
        select f, '{{' || string_agg('"' || maakunta || '": ' || round(pct, 2), ', ' order by rk) || '}}' top3_maakunta
        from ranked where rk <= 3 group by f) t using (f)
      where rk = 1""")
    con.sql(f"copy (select * from person order by FINNGENID) to '{out_file}' (delimiter '\t', header, quote '')")
    print(f"Wrote {out_file}")

    # summary: how many people get a maakunta, out of everyone in the input
    n_people, n_events = con.sql("select count(distinct f), sum(n) from fs").fetchone()
    n_regional = con.sql("select coalesce(sum(n), 0) from fm").fetchone()[0]
    n_with = con.sql("select count(*) from person").fetchone()[0]
    print(f"\nEvents: {n_events:,} total, {n_regional:,} ({100 * n_regional / n_events:.1f}%) from SPs with a maakunta")
    print(f"People: {n_people:,} total, {n_with:,} ({100 * n_with / n_people:.1f}%) with at least one regional event")
    k = con.sql("select count(maakunta) from person").fetchone()[0]
    print(f"  top maakunta > {args.threshold:g}% of their regional events: {k:,} people ({100 * k / n_people:.1f}% of all)")

# --- Assigned maakunta vs pheno columns that are independent of the lab data: region of birth and cohort
def cramers_v(ct):
    expected = np.outer(ct.sum(axis=1), ct.sum(axis=0)) / ct.values.sum()
    chi2 = ((ct.values - expected) ** 2 / expected).sum()
    k = min(ct.shape) - 1
    return np.sqrt(chi2 / (ct.values.sum() * k)) if k else float('nan')


def compare(both, col, out_name, same=None):
    """Cross `col` (one value per person) with the assigned maakunta; `same` flags rows that count as a match."""
    print(f"  Cramér's V ({col} x assigned maakunta): {cramers_v(pd.crosstab(both[col], both['maakunta'])):.3f}")
    if same is not None:
        both = both.assign(same=same)
        print(f"  same region: {both['same'].sum():,} ({100 * both['same'].mean():.1f}%)")
    rows = []
    for v, g in both.groupby(col):
        vc = g['maakunta'].value_counts()
        pct = (100 * vc / len(g)).round(1)
        rows.append({col: v, 'n': len(g), **({'pct_same': round(100 * g['same'].mean(), 1)} if same is not None else {}),
                     'top_maakunta': vc.index[0], 'pct_top': pct.iloc[0],
                     'next3': json.dumps(dict(pct.iloc[1:4]), ensure_ascii=False)})
    print(pd.DataFrame(rows).sort_values('n', ascending=False).to_string(index=False, max_colwidth=90))
    ct = both.groupby([col, 'maakunta']).size().rename('n').reset_index().sort_values([col, 'n'], ascending=[True, False])
    write_tsv_once(mask_small(ct, ['n']), args.out / out_name, fresh=person_fresh)


# one read of the (wide) pheno file: only the columns used below
# FinnGen endpoint -> (THL/Kela index column in --sotkanet, or None if there is none, label)
ENDPOINT_TO_INDEX = {
    'T2D': ('diabetes_index', 'type 2 diabetes'),
    'I9_CHD': ('coronary_index', 'coronary heart disease'),
    'I9_STR': ('cerebrovascular_index', 'stroke'),
    'J10_COPD': ('lung_index', 'COPD'),
    'J10_ASTHMA': ('lung_index', 'asthma'),
    'F5_DEMENTIA': ('memory_index', 'dementia'),
    'G6_MS': (None, 'multiple sclerosis'),
}
# endpoints with no THL/Kela index but a well-documented regional hotspot: is that maakunta the highest?
HOTSPOTS = {'G6_MS': 'Etelä-Pohjanmaa'}
PH = f"read_csv('{pheno}', delim='\t', header=true, all_varchar=true)"
pcols = set(con.sql(f"select * from {PH} limit 0").columns)
id_col = next((c for c in ('FINNGENID', 'FID') if c in pcols), None)
if id_col is None:
    raise SystemExit(f"{pheno}: no FINNGENID/FID column")
opt = lambda c, expr: expr if c in pcols else 'null'
ph_endpoints = [e for e in ENDPOINT_TO_INDEX if e in pcols]
pheno_cols = [
    f'"{id_col}" f',
    opt('regionofbirthname', "nullif(nullif(trim(regionofbirthname), ''), 'NA')") + ' birth',
    opt('cohort', "nullif(nullif(trim(cohort), ''), 'NA')") + ' cohort',
    opt(args.age_col, 'try_cast("' + args.age_col + '" as double)') + ' age',
    opt(args.sex_col, "nullif(nullif(trim(\"" + args.sex_col + "\"), ''), 'NA')") + ' sex',
    *['try_cast("' + e + '" as int) "ep_' + e + '"' for e in ph_endpoints],
]
con.sql(f"create table pheno as select {', '.join(pheno_cols)} from {PH}")
missing_ph = [c for c in ('regionofbirthname', 'cohort', args.age_col, args.sex_col) if c not in pcols]
if missing_ph:
    print(f"Note: {pheno.name} lacks {missing_ph}; the steps using them are skipped or run unstandardised")
assigned = con.sql("""select p.FINNGENID f, p.maakunta, ph.birth, ph.cohort from person p join pheno ph on ph.f = p.FINNGENID
                      where p.maakunta is not null""").df()

# region of birth: translate English names, leave out births outside current Finland
births = assigned.dropna(subset=['birth'])
print(f"\nRegion of birth: {births['f'].nunique():,} people with both a region of birth and an assigned maakunta")
outside = births['birth'].isin(BIRTH_OUTSIDE_FINLAND)
if outside.any():
    print(f"  left out, born outside current Finland: {outside.sum():,} ({', '.join(f'{k}: {v:,}' for k, v in births.loc[outside, 'birth'].value_counts().items())})")
births = births[~outside].assign(birth=lambda d: d['birth'].replace(BIRTH_TO_MAAKUNTA))
if len(births):
    same = births['birth'].str.lower() == births['maakunta'].str.lower()
    unmatched = sorted(set(births['birth']) - set(births['maakunta']))
    if unmatched:
        print(f"  birth regions never matching an assigned maakunta name (check naming): {unmatched}")
    compare(births, 'birth', f'birth_vs_maakunta_{TAG}.tsv', same)

# cohort (biobank / study of recruitment): no expected maakunta, so just the distribution per cohort
cohorts = assigned.dropna(subset=['cohort'])
print(f"\nCohort: {cohorts['f'].nunique():,} people with both a cohort and an assigned maakunta")
if len(cohorts):
    compare(cohorts, 'cohort', f'cohort_vs_maakunta_{TAG}.tsv')

# --- Clinical check: vitamin D seasonal cycle per assigned maakunta (the person's region, not the lab's)
# One value per person and month (median of their tests that month), so heavily re-tested people don't dominate.
# Pre-specified summary, no hand-picked season windows: per maakunta, one annual sine curve is fitted to the 12
# monthly medians (and 10th / 90th percentiles), giving the annual level and the month of the peak.
VAL, OMOP = '"harmonization_omop::MEASUREMENT_VALUE"', '"harmonization_omop::OMOP_ID"'
MONTHS = [f'{m:02d}' for m in range(1, 13)]
# figure colours: dataviz reference palette (light surface); categorical slots 1-3 validated as a set
SURFACE, INK, INK2, GRID, REF, BLUE = '#fcfcfb', '#0b0b0b', '#52514e', '#e4e3df', '#a3a29d', '#2a78d6'
PERCENTILE_COLORS = {'median': '#2a78d6', 'p10': '#eb6834', 'p90': '#1baf7a'}
PERCENTILE_LABELS = {'median': 'median', 'p10': '10th percentile', 'p90': '90th percentile'}
PERCENTILES = {'median': '', 'p10': '_p10', 'p90': '_p90'}  # monthly statistic -> column suffix in the fitted-cycle table


def spearman(x, y):
    """Spearman rank correlation (Pearson on ranks), without needing scipy."""
    return x.rank().corr(y.rank())


def fit_annual_cycle(monthly_values):
    """Least-squares fit of level + amplitude*cos(2*pi*(month - peak)/12) to the available months (>= 8 needed);
    returns (level, peak month)."""
    y = monthly_values.reindex(MONTHS).astype(float)
    ok = y.notna().to_numpy()
    if ok.sum() < 8:
        return np.nan, np.nan
    t = 2 * np.pi * (np.arange(12) + 0.5) / 12  # mid-month
    b = np.linalg.lstsq(np.column_stack([np.ones(12), np.cos(t), np.sin(t)])[ok], y.to_numpy()[ok], rcond=None)[0]
    peak_month = (np.degrees(np.arctan2(b[2], b[1])) % 360) / 30 + 0.5
    return b[0], peak_month


def save_fig_once(fig, path, fresh=False):
    """Save a figure unless the file is already there (cached); `fresh` as in write_tsv_once."""
    import matplotlib.pyplot as plt
    if cached(path) and not fresh:
        print(f"Kept existing {path}")
    else:
        fig.savefig(path, dpi=150, bbox_inches='tight')
        print(f"Wrote {path}")
    plt.close(fig)


def setup_matplotlib():
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    plt.rcParams.update({'font.size': 9, 'axes.edgecolor': GRID, 'axes.labelcolor': INK2, 'xtick.color': INK2,
                         'ytick.color': INK2, 'axes.titlecolor': INK, 'figure.facecolor': SURFACE, 'axes.facecolor': SURFACE})
    return plt


def plot_vitd(med, season, path, fresh=False):
    """Two figures: monthly curves per maakunta (small multiples, shared y axis) and annual level per maakunta."""
    plt = setup_matplotlib()
    blue, ink, ink2, grid, ref = BLUE, INK, INK2, GRID, REF
    regions = [r for r in season.sort_values('lat').index if r != 'ALL']
    if not regions:
        return
    x = np.arange(1, 13)
    ncol = 5
    nrow = -(-len(regions) // ncol)
    fig, axes = plt.subplots(nrow, ncol, figsize=(2.6 * ncol, 2.1 * nrow), sharex=True, sharey=True, squeeze=False)
    for ax, reg in zip(axes.flat, regions):
        if 'ALL' in med.index:
            ax.plot(x, med.loc['ALL'].reindex(MONTHS), color=ref, lw=1.5, ls='--', label='national')
        ax.plot(x, med.loc[reg].reindex(MONTHS), color=blue, lw=2, marker='o', ms=3, label='maakunta')
        ax.set_title(f"{reg} ({MAAKUNTA_LAT.get(reg, float('nan')):.1f}°N)", fontsize=9, loc='left')
        ax.grid(axis='y', color=grid, lw=0.8)
        ax.set_xticks([1, 4, 7, 10], ['Jan', 'Apr', 'Jul', 'Oct'])
        ax.tick_params(length=0)
        for side in ('top', 'right'):
            ax.spines[side].set_visible(False)
    for ax in axes.flat[len(regions):]:
        ax.set_visible(False)
    for ax in axes[:, 0]:
        ax.set_ylabel('median 25-OH-D, nmol/l')
    handles, labels = axes.flat[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper center', ncol=2, frameon=False, bbox_to_anchor=(0.5, 1.0))
    fig.suptitle(f'Vitamin D by month and assigned maakunta, south to north\n{THRESHOLD_DESC}', y=1.06, color=ink, fontsize=11)
    fig.tight_layout()
    f1 = path / f'vitd_month_maakunta_{TAG}.png'
    save_fig_once(fig, f1, fresh)

    # annual level per maakunta, regions listed south -> north (a scatter on latitude overlaps labels): the p10 and
    # p90 dots sit on a thin p10-p90 range line, the median dot between them
    d = season.reindex(regions)
    y = np.arange(len(regions))
    fig, ax = plt.subplots(figsize=(7.5, 0.32 * len(regions) + 1.6))
    ax.hlines(y, d['annual_level_p10'], d['annual_level_p90'], color=grid, lw=2, zorder=1)
    for stat, sfx in PERCENTILES.items():
        ok = d.dropna(subset=['lat', 'annual_level' + sfx])
        rho = f"{spearman(ok['lat'], ok['annual_level' + sfx]):+.2f}" if len(ok) >= 3 else "n/a"
        ax.scatter(d['annual_level' + sfx], y, s=36, color=PERCENTILE_COLORS[stat], edgecolor='#fcfcfb', linewidth=1.5, zorder=3,
                   label=f"{PERCENTILE_LABELS[stat]}  (Spearman ρ with latitude {rho})")
    ax.set_yticks(y, [f"{r} ({MAAKUNTA_LAT.get(r, float('nan')):.1f}°N)" for r in regions])
    ax.invert_yaxis()  # south at the top, north at the bottom, as in the tables
    ax.set_xlabel('annual level of the fitted monthly curve, nmol/l')
    ax.grid(axis='x', color=grid, lw=0.8)
    ax.tick_params(length=0)
    for side in ('top', 'right', 'left'):
        ax.spines[side].set_visible(False)
    handles, labels = ax.get_legend_handles_labels()
    order = [list(PERCENTILES).index(k) for k in ('p10', 'median', 'p90')]  # legend low -> high, as the dots read
    ax.legend([handles[i] for i in order], [labels[i] for i in order],
              loc='upper center', bbox_to_anchor=(0.4, -0.08 - 0.6 / len(regions)), ncol=1, frameon=False)
    fig.suptitle(f'Vitamin D per assigned maakunta, south to north\n{THRESHOLD_DESC}', color=ink, fontsize=11)
    fig.tight_layout()
    f2 = path / f'vitd_latitude_{TAG}.png'
    save_fig_once(fig, f2, fresh)


cols = set(con.sql(f"select * from {P} limit 0").columns)
need = {'harmonization_omop::MEASUREMENT_VALUE', 'harmonization_omop::OMOP_ID', 'QC_PASS', 'APPROX_EVENT_DATETIME'}
monthly_file, season_file = args.out / f'vitd_month_maakunta_{TAG}.tsv', args.out / f'vitd_season_maakunta_{TAG}.tsv'
monthly, vitd_fresh = None, False
if cached(monthly_file) and not person_fresh:  # built from the per-person table: redo when that was recomputed
    monthly = pd.read_csv(monthly_file, sep='\t', dtype={'maakunta': str, 'month': str})
    print(f"\nVitamin D: loaded existing {monthly_file} (--force to recompute)")
elif need - cols:
    print(f"\nVitamin D: skipped, input lacks {sorted(need - cols)}")
else:
    con.sql(f"""create table vitd_events as
      select p.maakunta, r.FINNGENID f, r.APPROX_EVENT_DATETIME dt, try_cast(r.{VAL} as double) v
      from {P} r join person p on p.FINNGENID = r.FINNGENID
      where r.{OMOP} = '{args.vitd_omop}' and r.QC_PASS <> '0' and p.maakunta is not null and try_cast(r.{VAL} as double) is not null""")
    con.sql("create table vitd as select maakunta, f, substr(dt, 6, 2) mon, median(v) v from vitd_events group by all")
    n_people = con.sql("select count(distinct f) from vitd_events").fetchone()[0]
    print(f"\nVitamin D (OMOP {args.vitd_omop}, nmol/l): {n_people:,} people with an assigned maakunta and a passing test")
    stats = ("count(*) n_person_months, median(v) median, quantile_cont(v, 0.25) q1, quantile_cont(v, 0.75) q3, "
             "quantile_cont(v, 0.1) p10, quantile_cont(v, 0.9) p90")
    monthly = con.sql(f"""
      select * from (
        select maakunta, mon as "month", {stats} from vitd group by all
        union all
        select 'ALL', mon, {stats} from vitd group by all)
      where n_person_months >= {args.vitd_min_people} order by 1, 2""").df()
    monthly.round(1).to_csv(monthly_file, sep='\t', index=False)
    vitd_fresh = True
    print(f"Wrote {monthly_file}")

if monthly is not None and len(monthly):
    med = monthly.pivot(index='maakunta', columns='month', values='median')
    # the annual cycle is fitted to the monthly median and, to see whether the low and high ends behave alike,
    # to the monthly 10th and 90th percentiles; columns for those carry a _p10 / _p90 suffix
    fits = []
    for stat, sfx in PERCENTILES.items():
        curves = monthly.pivot(index='maakunta', columns='month', values=stat)
        fits.append(curves.apply(lambda r: pd.Series(fit_annual_cycle(r), index=[f'annual_level{sfx}', f'peak_month{sfx}']), axis=1))
    season = pd.concat(fits, axis=1).assign(lat=med.index.map(MAAKUNTA_LAT), n_person_months=monthly.groupby('maakunta')['n_person_months'].sum())
    season = season[['lat', 'n_person_months', *[c for f in fits for c in f.columns]]].sort_values('lat', na_position='last').round(1)
    print("Median 25-OH-D per person-month, by month (rows south -> north; cells with too few person-months empty):")
    print(med.reindex(season.index).round(0).to_string(na_rep=''))
    print("Fitted annual cycle (annual_level = mean of the fitted curve; peak_month 1.0 = start of January):")
    print(season.to_string())
    regional = season.drop(index='ALL', errors='ignore').dropna(subset=['lat', 'annual_level'])
    if len(regional) >= 3:
        sp = lambda c: spearman(regional['lat'], regional[c])
        print(f"Spearman with latitude over {len(regional)} maakunta:")
        for stat, sfx in PERCENTILES.items():
            print(f"  {stat:<6}  annual level {sp(f'annual_level{sfx}'):+.2f}, peak month {sp(f'peak_month{sfx}'):+.2f}")
    write_tsv_once(season.reset_index(names='maakunta'), season_file, fresh=vitd_fresh)
    plot_vitd(med, season, args.out, fresh=vitd_fresh)

# --- Clinical check: % of tested people positive for Puumala hantavirus, per assigned maakunta
# Risk depends on where people live (high in east/central Finland, low on the west and south-west coast), the result
# is positive/negative so lab calibration barely matters, and using % of tested people (not counts) removes how often
# each region tests. Only Puumala IgM is kept: Borrelia and TBE tests had too many results with no usable encoding,
# or too few people tested, to be read per region.
#
# Each test row is classified as positive, negative or unknown (no usable result), by any of three encodings:
#   TEST_OUTCOME (abnormality flag), extracted::IS_POS (free-text pos/neg mapping), or the source-unit value
#   MEASUREMENT_VALUE against REFERENCE_RANGE_UPPER_VALUE (same unit as the value; the harmonized value may not be).
# A row is positive if any encoding says positive, else negative if any says negative, else unknown. Per person and
# test: positive if any row is, else negative if any row is, else unknown. % positive is over people with a usable
# result (positive + negative); unknowns are reported per cell so encoding gaps don't masquerade as negatives.
SEROLOGY = {  # test label -> OMOP concepts (see LABfi.tsv)
    'Puumala IgM': ['42868544'],
}
POSITIVE_OUTCOMES = ('A', 'AA', 'H', 'HH', 'POS', '+')
NEGATIVE_OUTCOMES = ('N', 'NEG', '-', 'L', 'LL')
SERO_COLUMNS = ['test', 'maakunta', 'n_people', 'n_pos', 'n_neg', 'n_unknown', 'pos_by_outcome', 'pos_by_ispos', 'pos_by_value']


def wilson(k, n, z=1.96):
    """95% Wilson interval for a proportion, in %."""
    p = k / n
    centre = (p + z * z / (2 * n)) / (1 + z * z / n)
    half = z * np.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
    return 100 * (centre - half), 100 * (centre + half)


def coast_group(maakunta):
    return 'coastal' if maakunta in COASTAL else 'inland'


def pooled_by_coast(sero):
    """Per test: people tested / with a usable result / positive summed over the coastal and the inland maakunta."""
    d = sero[sero['maakunta'] != 'ALL'].assign(group=lambda x: x['maakunta'].map(coast_group))
    g = d.groupby(['test', 'group'], as_index=False)[['n_people', 'n_pos', 'n_neg', 'n_unknown']].sum()
    g['n_tested'] = g['n_pos'] + g['n_neg']
    g['pct_unknown'] = 100 * g['n_unknown'] / g['n_people']
    g['pct_pos'] = 100 * g['n_pos'] / g['n_tested']
    g['ci_low'], g['ci_high'] = wilson(g['n_pos'], g['n_tested'])
    return g


def plot_serology(usable, pooled, path, fresh=False):
    """One panel per test: maakunta grouped coastal / inland (south -> north within each group), % positive per
    maakunta (dot) with 95% CI (line), the pooled group % (diamond), national % as a dashed reference."""
    plt = setup_matplotlib()
    tests = [t for t in SEROLOGY if t in set(usable['test'])]
    if not tests:
        return
    # rows: pooled group row, then its maakunta; a blank row between the groups
    rows = []
    for group in ('coastal', 'inland'):
        members = sorted({r for r in usable['maakunta'] if r != 'ALL' and coast_group(r) == group}, key=lambda r: MAAKUNTA_LAT.get(r, 99))
        if rows:
            rows.append(None)
        rows.append(('pooled', group))
        rows += [('maakunta', r) for r in members]
    y = np.arange(len(rows))
    fig, axes = plt.subplots(1, len(tests), figsize=(3.2 * len(tests) + 2.4, 0.32 * len(rows) + 1.8), sharey=True, squeeze=False)
    for ax, test in zip(axes[0], tests):
        d = usable[usable['test'] == test].set_index('maakunta')
        pg = pooled[pooled['test'] == test].set_index('group')
        for yi, row in zip(y, rows):
            if row is None:
                continue
            kind, name = row
            src = pg if kind == 'pooled' else d
            if name not in src.index or pd.isna(src.loc[name, 'pct_pos']):
                continue
            r = src.loc[name]
            ax.hlines(yi, r['ci_low'], r['ci_high'], color=INK2 if kind == 'pooled' else GRID, lw=2, zorder=1)
            ax.scatter(r['pct_pos'], yi, s=60 if kind == 'pooled' else 36, marker='D' if kind == 'pooled' else 'o',
                       color=INK if kind == 'pooled' else BLUE, edgecolor=SURFACE, linewidth=1.5, zorder=3)
        if 'ALL' in d.index:
            ax.axvline(d.loc['ALL', 'pct_pos'], color=REF, lw=1.5, ls='--', zorder=0)
        n = int(d.loc['ALL', 'n_tested']) if 'ALL' in d.index else int(pg['n_tested'].sum())
        ax.set_title(f"{test}\n{n:,} people with a result", fontsize=9, loc='left')
        ax.set_xlabel('% positive')
        ax.set_xlim(left=0)
        ax.grid(axis='x', color=GRID, lw=0.8)
        ax.tick_params(length=0)
        for side in ('top', 'right', 'left'):
            ax.spines[side].set_visible(False)
    labels = ['' if row is None else (f"{row[1]}, pooled" if row[0] == 'pooled' else f"{row[1]} ({MAAKUNTA_LAT.get(row[1], float('nan')):.1f}°N)")
              for row in rows]
    axes[0][0].set_yticks(y, labels)
    for tick, row in zip(axes[0][0].get_yticklabels(), rows):
        if row is not None and row[0] == 'pooled':
            tick.set_fontweight('bold')
            tick.set_color(INK)
    axes[0][0].invert_yaxis()
    fig.suptitle(f"% positive among people with a usable result,\ncoastal vs inland maakunta\n{THRESHOLD_DESC}", color=INK, fontsize=11)
    fig.text(0.5, -0.01, "dot: maakunta · diamond: pooled group · line: 95% CI · dashed: national",
             ha='center', va='top', color=INK2, fontsize=8)
    fig.tight_layout()
    save_fig_once(fig, path / f'serology_maakunta_{TAG}.png', fresh)


sero_file = args.out / f'serology_counts_{TAG}.tsv'  # internal cache with exact counts: not for sharing
need = {'harmonization_omop::OMOP_ID', 'MEASUREMENT_VALUE', 'QC_PASS', 'TEST_OUTCOME', 'extracted::IS_POS', 'REFERENCE_RANGE_UPPER_VALUE'}
sero, sero_fresh = None, False
if cached(sero_file) and not person_fresh:  # built from the per-person table: redo when that was recomputed
    sero = pd.read_csv(sero_file, sep='\t')
    print(f"\nSerology: loaded existing {sero_file} (--force to recompute)")
elif need - cols:
    print(f"\nSerology: skipped, input lacks {sorted(need - cols)}")
else:
    omop_to_test = " ".join(f"when '{o}' then '{t}'" for t, os_ in SEROLOGY.items() for o in os_)
    omops = ", ".join(f"'{o}'" for os_ in SEROLOGY.values() for o in os_)
    pos_out = ", ".join(f"'{o}'" for o in POSITIVE_OUTCOMES)
    neg_out = ", ".join(f"'{o}'" for o in NEGATIVE_OUTCOMES)
    con.sql(f"""create table sero_rows as
      select r.FINNGENID f, p.maakunta, case r.{OMOP} {omop_to_test} end test,
             coalesce(r.TEST_OUTCOME in ({pos_out}), false) pos_outcome,
             coalesce(r.TEST_OUTCOME in ({neg_out}), false) neg_outcome,
             coalesce(r."extracted::IS_POS" = '1', false) pos_ispos,
             coalesce(r."extracted::IS_POS" = '0', false) neg_ispos,
             coalesce(try_cast(r.MEASUREMENT_VALUE as double) > try_cast(r.REFERENCE_RANGE_UPPER_VALUE as double), false) pos_value,
             coalesce(try_cast(r.MEASUREMENT_VALUE as double) <= try_cast(r.REFERENCE_RANGE_UPPER_VALUE as double), false) neg_value
      from {P} r join person p on p.FINNGENID = r.FINNGENID
      where r.{OMOP} in ({omops}) and r.QC_PASS <> '0' and p.maakunta is not null""")
    con.sql("""create table sero_people as
      select f, maakunta, test,
             case when bool_or(pos_outcome or pos_ispos or pos_value) then 'pos'
                  when bool_or(neg_outcome or neg_ispos or neg_value) then 'neg' else 'unknown' end result,
             bool_or(pos_outcome) by_outcome, bool_or(pos_ispos) by_ispos, bool_or(pos_value) by_value
      from sero_rows group by all""")
    agg = """count(*) n_people, count(*) filter (result = 'pos') n_pos, count(*) filter (result = 'neg') n_neg,
             count(*) filter (result = 'unknown') n_unknown, count(*) filter (by_outcome) pos_by_outcome,
             count(*) filter (by_ispos) pos_by_ispos, count(*) filter (by_value) pos_by_value"""
    sero = con.sql(f"""
      select test, maakunta, {agg} from sero_people group by all
      union all
      select test, 'ALL', {agg} from sero_people group by all
      order by 1, 2""").df()
    sero[SERO_COLUMNS].to_csv(sero_file, sep='\t', index=False)
    sero_fresh = True
    print(f"\nSerology: wrote {sero_file}")

if sero is not None and len(sero):
    sero['n_tested'] = sero['n_pos'] + sero['n_neg']  # people with a usable result
    sero['pct_unknown'] = 100 * sero['n_unknown'] / sero['n_people']
    order_tests = [t for t in SEROLOGY if t in set(sero['test'])]
    order = sorted([r for r in set(sero['maakunta']) if r != 'ALL'], key=lambda r: (coast_group(r), MAAKUNTA_LAT.get(r, 99))) + ['ALL']
    pooled = pooled_by_coast(sero)
    usable = sero[sero['n_tested'] >= args.sero_min_people].copy()
    usable['pct_pos'] = 100 * usable['n_pos'] / usable['n_tested']
    usable['ci_low'], usable['ci_high'] = wilson(usable['n_pos'], usable['n_tested'])
    # one table per test, one row per maakunta: % positive (with 95% CI) shown only with enough people with a result
    for test in order_tests:
        t = sero[sero['test'] == test].set_index('maakunta').reindex(order)
        u = usable[usable['test'] == test].set_index('maakunta').reindex(order)
        # pooled coastal / inland rows go after the maakunta rows
        pg = pooled[pooled['test'] == test].set_index('group').rename(index=lambda g: f"{g}, pooled")
        t = pd.concat([t, pg])
        u = pd.concat([u, pg])
        table = pd.DataFrame({
            'group': [coast_group(r) if r != 'ALL' and not r.endswith(', pooled') else '' for r in t.index],
            'tested': t['n_people'].astype('Int64'),
            '% no usable result': t['pct_unknown'].round(1),
            'with result': t['n_tested'].astype('Int64'),
            '% positive': u['pct_pos'].round(1),
            '95% CI': [f"{lo:.1f}-{hi:.1f}" if pd.notna(lo) else '' for lo, hi in zip(u['ci_low'], u['ci_high'])],
        })
        print(f"{test}: % positive among people with a usable result (no outcome flag, pos/neg text or value + reference "
              f"limit = no usable result; % positive hidden with < {args.sero_min_people} people with a result)")
        print(table.to_string(na_rep=''))
    print("How positives were recognised, national (people; one person can be counted by several encodings):")
    print(sero[sero['maakunta'] == 'ALL'].set_index('test').reindex(order_tests)[
        ['n_pos', 'pos_by_outcome', 'pos_by_ispos', 'pos_by_value']].to_string())
    # shareable table and figure (serology_counts_* stays internal): no count from 1 to --min-cell - 1 may be shown
    # or be derivable by subtracting shown counts. So a maakunta is published only if its positives and negatives
    # are both 0 or >= --min-cell, and the ALL and pooled coastal / inland rows sum the published maakunta only,
    # which leaves nothing to subtract. People with no usable result are shown per published maakunta only (masked
    # when small), not in the summed rows; n_people is left out, since with n_tested it would give them back.
    regional = sero[sero['maakunta'] != 'ALL']
    published = ~is_small(regional['n_pos']).values & ~is_small(regional['n_neg']).values
    pub, suppressed = regional[published], regional[~published]
    pub_pooled = pooled_by_coast(pub)
    summed = pd.concat([pub.groupby('test', as_index=False)[['n_pos', 'n_tested']].sum().assign(maakunta='ALL'),
                        pub_pooled[['test', 'group', 'n_pos', 'n_tested']].rename(columns={'group': 'maakunta'})
                        .assign(maakunta=lambda d: d['maakunta'] + ', pooled')])
    share = pd.concat([pub.assign(pct_unknown=100 * pub['n_unknown'] / pub['n_people']), summed,
                       suppressed[['test', 'maakunta']].assign(note='left out: small count')], ignore_index=True)
    share['pct_pos'] = (100 * share['n_pos'] / share['n_tested']).where(share['n_tested'] >= args.sero_min_people)
    share['ci_low'], share['ci_high'] = wilson(share['n_pos'], share['n_tested'])
    share[['ci_low', 'ci_high']] = share[['ci_low', 'ci_high']].where(share['pct_pos'].notna())
    rank = {r: i for i, r in enumerate(order + ['coastal, pooled', 'inland, pooled'])}
    share = share.assign(_r=share['maakunta'].map(rank)).sort_values(['test', '_r'])
    share = share[['test', 'maakunta', 'n_tested', 'n_pos', 'pct_pos', 'ci_low', 'ci_high', 'n_unknown', 'pct_unknown', 'note']]
    for c in ['n_tested', 'n_pos', 'n_unknown']:
        share[c] = share[c].astype('Int64')
    write_tsv_once(mask_small(share.round(2), ['n_unknown'], ['pct_unknown']),
                   args.out / f'serology_maakunta_{TAG}.tsv', fresh=sero_fresh)
    if len(suppressed):
        print(f"Shareable serology table and figure: left out for a small count (also from the ALL and pooled rows): "
              f"{', '.join(suppressed['test'] + ' ' + suppressed['maakunta'])}")
    plot_serology(share[share['pct_pos'].notna() & ~share['maakunta'].str.endswith(', pooled')], pub_pooled, args.out,
                  fresh=sero_fresh)

# --- Clinical check: FinnGen endpoint prevalence per assigned maakunta vs THL/Kela's regional disease indices
# Endpoint status is 'ever diagnosed by death or end of follow-up', so prevalence depends on the age people have
# reached (not age at diagnosis, which controls don't have): FinnGen's age and sex mix differ by region (they depend
# on which biobank/cohort recruited there), so prevalence is indirectly standardised over 10-year bands of --age-col x
# --sex-col (observed / expected cases x 100, expected from all assigned people's stratum rates), giving a FinnGen
# index on the same scale as THL/Kela's (all = 100). THL/Kela measure the whole population differently, so the
# regional ranking is what is compared (Spearman). Uusimaa's
# index is the population-weighted mean of Helsinki and its four hyvinvointialue.
AGE_BANDS = list(range(20, 100, 10))  # lower edges; < 20 joins the first band, >= 90 the last


def thl_index_by_maakunta(path):
    t = pd.read_csv(path, sep='\t')
    cols = sorted({c for c, _ in ENDPOINT_TO_INDEX.values() if c and c in t.columns})
    weighted = t[cols].mul(t['population'], axis=0).groupby(t['maakunta']).sum()
    return weighted.div(t.groupby('maakunta')['population'].sum(), axis=0)


def plot_disease(cmp, path, fresh=False):
    """One panel per endpoint: FinnGen age/sex-standardised index (x) vs THL/Kela index (y), one labelled dot per maakunta."""
    plt = setup_matplotlib()
    eps = [e for e in ENDPOINT_TO_INDEX if e in set(cmp['endpoint']) and ENDPOINT_TO_INDEX[e][0]]
    if not eps:
        return
    ncol = min(3, len(eps))
    nrow = -(-len(eps) // ncol)
    fig, axes = plt.subplots(nrow, ncol, figsize=(5.2 * ncol, 4.6 * nrow), squeeze=False)
    for ax in axes.flat[len(eps):]:
        ax.set_visible(False)
    for ax, ep in zip(axes.flat, eps):
        d = cmp[(cmp['endpoint'] == ep) & cmp['shareable']].dropna(subset=['finngen_index', 'thl_index'])
        ax.scatter(d['finngen_index'], d['thl_index'], s=36, color=BLUE, edgecolor=SURFACE, linewidth=1.5, zorder=3)
        for _, r in d.iterrows():
            ax.annotate(r['maakunta'], (r['finngen_index'], r['thl_index']), xytext=(4, 3), textcoords='offset points', fontsize=7, color=INK2)
        rho = f"{spearman(d['finngen_index'], d['thl_index']):+.2f}" if len(d) >= 3 else "n/a"
        ax.set_title(f"{ENDPOINT_TO_INDEX[ep][1]} ({ep})\nSpearman ρ {rho} over {len(d)} maakunta", fontsize=9, loc='left')
        ax.set_xlabel('FinnGen index, age- and sex-standardised (all assigned people = 100)')
        ax.axvline(100, color=REF, lw=1, ls='--', zorder=0)
        ax.axhline(100, color=REF, lw=1, ls='--', zorder=0)
        ax.set_ylabel(f"THL/Kela {ENDPOINT_TO_INDEX[ep][0].replace('_', ' ')} (country = 100)")
        ax.grid(color=GRID, lw=0.8)
        ax.tick_params(length=0)
        for side in ('top', 'right'):
            ax.spines[side].set_visible(False)
    fig.suptitle(f"FinnGen endpoint index per assigned maakunta vs THL/Kela regional index (both age-standardised)\n{THRESHOLD_DESC}", color=INK, fontsize=11)
    fig.tight_layout()
    save_fig_once(fig, path / f'disease_vs_thl_{DTAG}.png', fresh)


# leaving cohorts out changes the counts, so it goes into the file names (short hash of the sorted list)
DTAG = TAG + (f"_excl{hashlib.md5(','.join(args.exclude_cohorts).encode()).hexdigest()[:6]}" if args.exclude_cohorts else "")
disease_file = args.out / f'disease_counts_{DTAG}.tsv'
counts, disease_fresh = None, False
if cached(disease_file) and not person_fresh:  # built from the per-person table: redo when that was recomputed
    counts = pd.read_csv(disease_file, sep='\t', dtype={'maakunta': str, 'endpoint': str, 'stratum': str})
    print(f"\nDisease prevalence: loaded existing {disease_file} (--force to recompute)")
elif not ph_endpoints:
    print(f"\nDisease prevalence: skipped, none of {sorted(ENDPOINT_TO_INDEX)} in {pheno.name}")
elif not args.sotkanet.exists():
    print(f"\nDisease prevalence: skipped, {args.sotkanet} not found (run fetch_sotkanet.py)")
else:
    if set(ENDPOINT_TO_INDEX) - set(ph_endpoints):
        print(f"\nDisease prevalence: endpoints not in {pheno.name}, left out: {sorted(set(ENDPOINT_TO_INDEX) - set(ph_endpoints))}")
    # stratum = age band | sex ('NA' parts when unknown; strata with an NA part are left out of the standardisation)
    stratum = (f"coalesce(lpad((least(greatest(floor(ph.age / 10) * 10, {AGE_BANDS[0]}), {AGE_BANDS[-1]}))::int::varchar, 2, '0'), 'NA')"
               " || '|' || coalesce(ph.sex, 'NA')")
    excl_sql = (" and coalesce(ph.cohort, '') not in (" + ", ".join(f"'{c}'" for c in args.exclude_cohorts) + ")"
                if args.exclude_cohorts else "")
    if args.exclude_cohorts:
        print(f"\nDisease prevalence: leaving out cohorts {args.exclude_cohorts} (file tag {DTAG})")
    unions = " union all ".join(
        f"select '{e}' endpoint, p.maakunta, {stratum} stratum, ph.\"ep_{e}\" case_status "
        f"from person p join pheno ph on ph.f = p.FINNGENID where p.maakunta is not null{excl_sql}" for e in ph_endpoints)
    counts = con.sql(f"""
      select endpoint, maakunta, stratum, count(*) n, sum(case_status) n_cases from ({unions})
      where case_status in (0, 1) group by all order by 1, 2, 3""").df()
    counts.to_csv(disease_file, sep='\t', index=False)
    disease_fresh = True
    print(f"\nDisease prevalence: wrote {disease_file}")

if counts is not None and len(counts):
    thl = thl_index_by_maakunta(args.sotkanet)
    # indirect standardisation: expected cases = each stratum's people x the national (all assigned people) rate in
    # that stratum; index = 100 x observed / expected, on the same scale as THL/Kela's (country = 100). Stabler than
    # direct standardisation when a maakunta has few people in some strata. Strata with unknown age or sex left out.
    known = counts[~counts['stratum'].str.contains('NA')]
    nat = known.groupby(['endpoint', 'stratum'])[['n', 'n_cases']].sum()
    known = known.join((nat['n_cases'] / nat['n']).rename('rate'), on=['endpoint', 'stratum'])
    expected = (known['n'] * known['rate']).groupby([known['endpoint'], known['maakunta']]).sum()
    observed = known.groupby(['endpoint', 'maakunta'])['n_cases'].sum()
    rows = []
    for (ep, mk), g in counts.groupby(['endpoint', 'maakunta']):
        n, cases = g['n'].sum(), g['n_cases'].sum()
        e = expected.get((ep, mk), np.nan)
        rows.append({'endpoint': ep, 'maakunta': mk, 'n_people': int(n), 'n_cases': int(cases), 'pct_crude': 100 * cases / n,
                     'finngen_index': 100 * observed.get((ep, mk), np.nan) / e if e else np.nan,
                     'thl_index': thl.loc[mk, ENDPOINT_TO_INDEX[ep][0]]
                                  if mk in thl.index and ENDPOINT_TO_INDEX[ep][0] in thl.columns else np.nan})
    cmp = pd.DataFrame(rows)
    cmp = cmp[cmp['n_people'] >= args.disease_min_people]
    # a maakunta whose cases or non-cases are from 1 to --min-cell - 1 has its index blanked in the shareable table, so
    # it is also left out of the correlations, the hotspot rank and the figure (all shared)
    cmp['shareable'] = ~is_small(cmp['n_cases']).values & ~is_small(cmp['n_people'] - cmp['n_cases']).values
    if cmp['finngen_index'].isna().all():
        print("  (no age/sex available: no standardised index, comparing crude prevalence)")
        cmp['finngen_index'] = cmp['pct_crude']
    cmp = cmp.assign(lat=cmp['maakunta'].map(MAAKUNTA_LAT)).sort_values(['endpoint', 'lat']).drop(columns='lat')
    summary = []
    for ep in [e for e in ENDPOINT_TO_INDEX if e in set(cmp['endpoint'])]:
        d = cmp[cmp['endpoint'] == ep]
        index_col, label = ENDPOINT_TO_INDEX[ep]
        table = d.drop(columns=['endpoint', 'shareable']).set_index('maakunta').round({'pct_crude': 1, 'finngen_index': 1, 'thl_index': 1})
        if index_col:
            ok = d[d['shareable']].dropna(subset=['finngen_index', 'thl_index'])
            rho = spearman(ok['finngen_index'], ok['thl_index']) if len(ok) >= 3 else np.nan
            print(f"\n{label} ({ep}): FinnGen age/sex-standardised index (observed / expected x 100) per assigned maakunta vs "
                  f"THL/Kela {index_col.replace('_', ' ')} — Spearman ρ {rho:+.2f} over {len(ok)} maakunta "
                  f"(maakunta with < {args.disease_min_people} people, or < {args.min_cell} cases or non-cases, left out)")
            summary.append({'endpoint': ep, 'disease': label, 'compared with': index_col, 'maakunta': len(ok), 'spearman': round(rho, 2)})
        else:
            hot = HOTSPOTS.get(ep)
            ranked = d[d['shareable']].dropna(subset=['finngen_index']).sort_values('finngen_index', ascending=False)['maakunta'].tolist()
            where = f"{hot} ranks {ranked.index(hot) + 1} of {len(ranked)}" if hot in ranked else f"{hot} not in the table"
            print(f"\n{label} ({ep}): FinnGen age/sex-standardised index per assigned maakunta (no THL/Kela index; "
                  f"expected hotspot {hot}: {where})")
            table = table.drop(columns='thl_index')
            summary.append({'endpoint': ep, 'disease': label, 'compared with': f'hotspot {hot}', 'maakunta': len(ranked),
                            'spearman': np.nan, 'hotspot rank': ranked.index(hot) + 1 if hot in ranked else np.nan})
        print(table.to_string())
    print("\nSummary:")
    print(pd.DataFrame(summary).to_string(index=False, na_rep=''))
    # shareable: small case counts masked, with the prevalence and index derived from them blanked; with few
    # non-cases, n_cases is blanked too, since n_people - n_cases would give them back
    few_noncases = pd.Series(is_small(cmp['n_people'] - cmp['n_cases']).values, index=cmp.index)
    shared = mask_small(cmp.drop(columns='shareable').round(2), ['n_people', 'n_cases'], ['pct_crude', 'finngen_index'], few_noncases)
    shared['n_cases'] = shared['n_cases'].where(~few_noncases, None)
    write_tsv_once(shared,
                   args.out / f'disease_vs_thl_{DTAG}.tsv', fresh=disease_fresh)
    plot_disease(cmp, args.out, fresh=disease_fresh)
