#!/bin/bash

# --- 1. CONFIG & ARGS ---
INPUT_RAW=""
MAP_FILE=""
PN_ORIG=""
PLUS_ORIG=""
PN_SUMMARY="pos_neg_summary.tsv"
PLUS_SUMMARY="plusplus_summary.tsv"
PN_WARN="PN_RECONCILIATION_WARNINGS.tsv"
PLUS_WARN="PLUS_RECONCILIATION_WARNINGS.tsv"
TEST_LINES=1000000
MODE="full"
FORCE=0

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --map) MAP_FILE="$2"; shift 2 ;;
        --pn_orig) PN_ORIG="$2"; shift 2 ;;
        --plus_orig) PLUS_ORIG="$2"; shift 2 ;;
        --force) FORCE=1; shift 1 ;;
        --test) MODE="test"; [[ "$2" =~ ^[0-9]+$ ]] && TEST_LINES="$2" && shift 2 || shift 1 ;;
        *) INPUT_RAW="$1"; shift ;;
    esac
done

export MAP_FILE PN_ORIG PLUS_ORIG PN_SUMMARY PLUS_SUMMARY PN_WARN PLUS_WARN

# --- 2. EXTRACTION LOGIC ---
LAST_FILE_LOG=".last_input_source"
LAST_INPUT=$(cat "$LAST_FILE_LOG" 2>/dev/null)

needs_extraction() {
    if [[ $FORCE -eq 1 ]]; then return 0; fi
    if [[ "$INPUT_RAW" != "$LAST_INPUT" ]]; then return 0; fi
    if [[ ! -f "tmp_pn.raw" ]] || [[ ! -f "tmp_pl.raw" ]]; then return 0; fi
    return 1
}

if needs_extraction; then
    echo "Step 1: Extracting matching lines from $INPUT_RAW..."
    echo "$INPUT_RAW" > "$LAST_FILE_LOG"
    rm -f subset_pn.tsv subset_pl.tsv tmp_pn.raw tmp_pl.raw
    
    [[ "$MODE" == "test" ]] && STREAM="zcat -f \"$INPUT_RAW\" | head -n $TEST_LINES" || STREAM="zcat -f \"$INPUT_RAW\""

    eval "$STREAM" | awk -F'\t' '
    function clean(s) { gsub(/^[[:space:]]+|[[:space:]]+$/, "", s); gsub(/\t|\r|\n/, " ", s); return s }
    FNR == 1 {
        for(i=1; i<=NF; i++) {
            h = tolower(clean($i))
            if(h == "finngenid") c_id = i
            if(h == "measurement_free_text") c_tx = i
            if(h == "extracted::is_pos") c_p = i
            if(h == "harmonization_omop::omop_id") c_o = i
        }
        next
    }
    {
        tx = clean($c_tx); id = clean($c_id); pos = clean($c_p); om = clean($c_o)
        if (tx == "") next
        l_tx = tolower(tx); p_val = (pos == "" ? "NA" : pos)
        if (l_tx ~ /pos|neg/) print tx "\t" p_val "\t" id > "subset_pn.tsv"
        if (tx ~ /\+/) print (om==""?"NA":om) "\t" tx "\t" p_val "\t" id > "subset_pl.tsv"
    }'

    echo "Step 2: Sorting and aggregating (Npeople >= 5)..."
    sort -t$'\t' -k1,3 subset_pn.tsv | awk -F'\t' '
    {
        key = $1 "\t" $2
        if (key != last_key && last_key != "") {
            if (nppl >= 5) print last_key "\t" cnt "\t" nppl
            cnt = 0; nppl = 0; last_id = ""
        }
        cnt++; if ($3 != last_id) { nppl++; last_id = $3 }; last_key = key
    }
    END { if (nppl >= 5) print last_key "\t" cnt "\t" nppl }' > tmp_pn.raw

    sort -t$'\t' -k1,4 subset_pl.tsv | awk -F'\t' '
    {
        key = $1 "\t" $2 "\t" $3
        if (key != last_key && last_key != "") {
            if (nppl >= 5) print last_key "\t" cnt "\t" nppl
            cnt = 0; nppl = 0; last_id = ""
        }
        cnt++; if ($4 != last_id) { nppl++; last_id = $4 }; last_key = key
    }
    END { if (nppl >= 5) print last_key "\t" cnt "\t" nppl }' > tmp_pl.raw
    
    rm subset_pn.tsv subset_pl.tsv
else
    echo "Using existing aggregated data for $INPUT_RAW."
fi

# --- 3. RECONCILE (Python) ---
echo "Step 3: Reconciling and appending warnings..."

python3 -c "
import csv, os

def load_ref(path, use_omop=False):
    data = {}
    if not path or not os.path.exists(path): return data
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            txt, pos = row.get('MEASUREMENT_FREE_TEXT', '').strip(), row.get('extracted::IS_POS', '').strip()
            om = row.get('harmonization_omop::OMOP_ID', '').strip() if use_omop else None
            key = (om, txt) if use_omop else txt
            if key not in data: data[key] = {}
            if pos: data[key][pos] = row.get('NOTES', '').strip()
    return data

def check_row(key, current_pos, nppl, ref_dict, warn_list):
    # 1. NEW ENTRY
    if key not in ref_dict:
        msg = '!! WARNING: NEW ENTRY !!'
        warn_list.append([str(key), current_pos, nppl, msg])
        return msg
    
    # 2. EXACT MATCH (Keep existing note)
    if current_pos in ref_dict[key]:
        return ref_dict[key][current_pos]
    
    # 3. STATUS MISMATCH (Keep existing note and append warning)
    known_statuses = sorted([s for s in ref_dict[key].keys() if s])
    known_str = '/'.join(known_statuses)
    
    # Get any pre-existing note from any status for this text to preserve it
    existing_notes = [n for n in ref_dict[key].values() if n]
    base_note = existing_notes[0] if existing_notes else ''
    
    msg = f'!! WARNING: Status Mismatch (Ref has {known_str}) !!'
    warn_list.append([str(key), current_pos, nppl, msg])
    
    return f'{base_note} {msg}'.strip()

omop_map = {}
if os.path.exists(os.environ.get('MAP_FILE', '')):
    with open(os.environ['MAP_FILE'], 'r') as f:
        for row in csv.DictReader(f): omop_map[row.get('conceptId', '').strip()] = row.get('conceptName', '').strip()

pn_refs, pl_refs = load_ref(os.environ.get('PN_ORIG', '')), load_ref(os.environ.get('PLUS_ORIG', ''), True)
pn_warns, pl_warns = [], []

if os.path.exists('tmp_pn.raw'):
    res = []
    with open('tmp_pn.raw', 'r') as f:
        for line in f:
            tx, pos, cnt, nppl = line.strip('\n').split('\t')
            res.append([tx, pos, cnt, nppl, check_row(tx, pos, nppl, pn_refs, pn_warns)])
    res.sort(key=lambda x: int(x[2]), reverse=True)
    with open(os.environ['PN_SUMMARY'], 'w') as f:
        f.write('MEASUREMENT_FREE_TEXT\textracted::IS_POS\tCOUNT\tNpeople\tNOTES\n')
        for r in res: f.write('\t'.join(r) + '\n')

if os.path.exists('tmp_pl.raw'):
    res = []
    with open('tmp_pl.raw', 'r') as f:
        for line in f:
            om, tx, pos, cnt, nppl = line.strip('\n').split('\t')
            desc = omop_map.get(om, 'NOT_IN_MAP') if om != 'NA' else 'NO_OMOP_ID'
            res.append([om, tx, pos, desc, cnt, nppl, check_row((om, tx), pos, nppl, pl_refs, pl_warns)])
    res.sort(key=lambda x: int(x[4]), reverse=True)
    with open(os.environ['PLUS_SUMMARY'], 'w') as f:
        f.write('harmonization_omop::OMOP_ID\tMEASUREMENT_FREE_TEXT\textracted::IS_POS\tDESC\tCOUNT\tNpeople\tNOTES\n')
        for r in res: f.write('\t'.join(r) + '\n')

for w_file, w_list, head in [(os.environ['PN_WARN'], pn_warns, 'TEXT\tSTATUS\tNPEOPLE\tISSUE\n'), (os.environ['PLUS_WARN'], pl_warns, 'OMOP_TEXT_KEY\tSTATUS\tNPEOPLE\tISSUE\n')]:
    with open(w_file, 'w') as f:
        f.write(head)
        w_list.sort(key=lambda x: int(x[2]), reverse=True)
        for w in w_list: f.write('\t'.join(w) + '\n')
"
echo "Done. Notes preserved and warnings appended."
