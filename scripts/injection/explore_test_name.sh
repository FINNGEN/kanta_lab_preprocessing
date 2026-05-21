#!/bin/bash

F="$1"
MIN_COUNT="$2"

OUT_COUNTS="test_name_id_counts.tsv"
OUT_DETAILS="test_name_id_details.tsv"
PLOT="test_names_exploration_scatter.png"

# --- Query 1: TEST_NAME, TEST_ID, COUNT for all pairs with count > threshold ---
if [[ ! -f "$OUT_COUNTS" ]]; then
    clickhouse -q "
        SELECT
            TEST_NAME,
            TEST_ID,
            count() AS COUNT
        FROM file('$F')
        WHERE (MEASUREMENT_VALUE_EXTRACTED IS NOT NULL OR MEASUREMENT_VALUE_HARMONIZED IS NOT NULL)
          AND MEASUREMENT_UNIT_PRE_FIX IS NULL
        GROUP BY TEST_NAME, TEST_ID
        HAVING COUNT > $MIN_COUNT
        ORDER BY COUNT DESC
        FORMAT TSVWithNames
    " > "$OUT_COUNTS"
    echo "Wrote $OUT_COUNTS"
else
    echo "$OUT_COUNTS already exists, skipping."
fi

# --- Query 2: TEST_NAME, TEST_ID, COUNT, UNIT, PREVALENCE_DICT ---
# Include all TEST_NAMEs present in Query 1 output
if [[ ! -f "$OUT_DETAILS" ]]; then
    OUT_COUNTS_ABS="$(realpath "$OUT_COUNTS")"
    clickhouse -q "
        WITH
        global_names AS (
            SELECT DISTINCT TEST_NAME
            FROM file('$OUT_COUNTS_ABS', TSVWithNames)
        ),
        top3_units AS (
            SELECT
                TEST_NAME, TEST_ID,
                argMax(MEASUREMENT_UNIT_PRE_FIX, unit_cnt) AS UNIT,
                concat('{', arrayStringConcat(groupArray(unit_json), ','), '}') AS PREVALENCE_DICT
            FROM (
                SELECT TEST_NAME, TEST_ID, MEASUREMENT_UNIT_PRE_FIX, unit_cnt, unit_json
                FROM (
                    SELECT
                        TEST_NAME, TEST_ID, MEASUREMENT_UNIT_PRE_FIX, unit_cnt,
                        ROW_NUMBER() OVER (PARTITION BY TEST_NAME, TEST_ID ORDER BY unit_cnt DESC) AS rn,
                        concat(
                            MEASUREMENT_UNIT_PRE_FIX, ':',
                            toString(round(100.0 * unit_cnt / SUM(unit_cnt) OVER (PARTITION BY TEST_NAME, TEST_ID), 2))
                        ) AS unit_json
                    FROM (
                        SELECT TEST_NAME, TEST_ID, MEASUREMENT_UNIT_PRE_FIX, count() AS unit_cnt
                        FROM file('$F')
                        WHERE MEASUREMENT_VALUE_MERGED IS NOT NULL
                          AND MEASUREMENT_UNIT_PRE_FIX IS NOT NULL
                        GROUP BY TEST_NAME, TEST_ID, MEASUREMENT_UNIT_PRE_FIX
                    ) AS sub
                ) AS ranked
                WHERE rn <= 3
            ) AS top3
            GROUP BY TEST_NAME, TEST_ID
        ),
        total_counts AS (
            SELECT TEST_NAME, TEST_ID, count() AS COUNT
            FROM file('$F')
            WHERE MEASUREMENT_VALUE_MERGED IS NOT NULL
              AND MEASUREMENT_UNIT_PRE_FIX IS NOT NULL
            GROUP BY TEST_NAME, TEST_ID
        )
        SELECT
            t.TEST_NAME       AS TEST_NAME,
            t.TEST_ID         AS TEST_ID,
            t.COUNT           AS COUNT,
            u.UNIT            AS UNIT,
            u.PREVALENCE_DICT AS PREVALENCE_DICT
        FROM total_counts t
        LEFT JOIN top3_units u USING (TEST_NAME, TEST_ID)
        INNER JOIN global_names g ON t.TEST_NAME = g.TEST_NAME
        ORDER BY t.COUNT DESC
        FORMAT TSVWithNames
    " > "$OUT_DETAILS"
    echo "Wrote $OUT_DETAILS"
else
    echo "$OUT_DETAILS already exists, skipping."
fi

# --- Python: build plotting tables and scatter plots ---
if [[ -f "$PLOT" ]]; then
    echo "Plot '$PLOT' already exists, skipping."
    exit 0
fi

python3 - <<'EOF'
import re
import pandas as pd
import matplotlib.pyplot as plt

def parse_top_prevalence(d):
    if pd.isna(d):
        return None
    pcts = re.findall(r':(\d+\.?\d*)[,}]', str(d))
    if not pcts:
        return None
    return max(float(p) for p in pcts)

counts  = pd.read_csv("test_name_id_counts.tsv", sep="\t")
details = pd.read_csv("test_name_id_details.tsv", sep="\t")

# restrict details to (TEST_NAME, TEST_ID) pairs present in Query 1
details = details.merge(counts[["TEST_NAME", "TEST_ID"]], on=["TEST_NAME", "TEST_ID"], how="inner")

# ---- TEST_NAME + TEST_ID level ----
details["top_prevalence"] = details["PREVALENCE_DICT"].apply(parse_top_prevalence)

plot_name_id = counts.merge(
    details[["TEST_NAME", "TEST_ID", "top_prevalence"]],
    on=["TEST_NAME", "TEST_ID"], how="left"
)
plot_name_id["has_unit_data"]  = plot_name_id["top_prevalence"].notna()
plot_name_id["top_prevalence"] = plot_name_id["top_prevalence"].fillna(0)
plot_name_id.to_csv("plot_name_id_level.tsv", sep="\t", index=False)
print(f"Built plot_name_id_level.tsv  ({len(plot_name_id)} rows)")

# ---- TEST_NAME level (COUNT-weighted mean over rows WITH unit data only) ----
def weighted_prevalence(d):
    valid = d[d["has_unit_data"]]
    if valid.empty:
        return 0.0
    return (valid["top_prevalence"] * valid["COUNT"]).sum() / valid["COUNT"].sum()

g = plot_name_id.groupby("TEST_NAME")
plot_name = pd.DataFrame({
    "total_count":    g["COUNT"].sum(),
    "top_prevalence": g.apply(weighted_prevalence),
}).reset_index()
plot_name.to_csv("plot_name_level.tsv", sep="\t", index=False)
print(f"Built plot_name_level.tsv  ({len(plot_name)} rows)")

# ---- Scatter plots ----
fig, axes = plt.subplots(1, 2, figsize=(18, 7))

ax1 = axes[0]
ax1.scatter(plot_name_id["COUNT"], plot_name_id["top_prevalence"], alpha=0.5, s=20)
ax1.set_xlabel("Count (value, no unit)")
ax1.set_ylabel("Top unit prevalence (%)")
ax1.set_title("TEST_NAME + TEST_ID level")
ax1.set_xscale("log")

ax2 = axes[1]
ax2.scatter(plot_name["total_count"], plot_name["top_prevalence"], alpha=0.5, s=20)
ax2.set_xlabel("Total count (value, no unit)")
ax2.set_ylabel("Weighted top unit prevalence (%)")
ax2.set_title("TEST_NAME level")
ax2.set_xscale("log")

plt.tight_layout()
plt.savefig("test_names_exploration_scatter.png", dpi=150)
print("Saved test_names_exploration_scatter.png")
EOF
