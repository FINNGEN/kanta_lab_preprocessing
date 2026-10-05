import json
import subprocess
import tempfile
from itertools import zip_longest
from pathlib import Path

from tests import (
    fill_templates_with_mocks,
    generate_source_list,
    parquet_to_ppjson,
)

# One row per pos/neg lookup table, each using that table's top entry by COUNT so the expected
# result is very unlikely to change when the tables get curated:
# - negpos_mapping.tsv: free text "NEGAT" -> IS_POS 0
# - kanta_plusplus_abnormality.tsv: "+" on OMOP 3011397 (Hemoglobin [Presence] in Urine by Test
#   strip, reached via the APPROVED u-hb-o mapping in LABfi.tsv) -> IS_POS 1
# plus a control: "+" on an unmapped test must NOT match, since the plus table is keyed on the
# (free text, OMOP_ID) pair.
MOCK_MAIN = [
    {
        "FINNGENID": "FAKE_NEGPOS",
        "paikallinentutkimusnimike_koodi": "0001",
        "paikallinentutkimusnimike_selite": "some-test",
    },
    {
        "FINNGENID": "FAKE_PLUS",
        "paikallinentutkimusnimike_koodi": "0002",
        "paikallinentutkimusnimike_selite": "u-hb-o",
    },
    {
        "FINNGENID": "FAKE_PLUS_UNMAPPED",
        "paikallinentutkimusnimike_koodi": "0003",
        "paikallinentutkimusnimike_selite": "some-test",
    },
]
MOCK_FREETEXT = [
    {"FINNGENID": "FAKE_NEGPOS", "tutkimustulosteksti": "NEGAT"},
    {"FINNGENID": "FAKE_PLUS", "tutkimustulosteksti": "+"},
    {"FINNGENID": "FAKE_PLUS_UNMAPPED", "tutkimustulosteksti": "+"},
]
MOCK_PHENO_SEX = [
    {"FINNGENID": "FAKE_NEGPOS", "SEX": "female"},
    {"FINNGENID": "FAKE_PLUS", "SEX": "female"},
    {"FINNGENID": "FAKE_PLUS_UNMAPPED", "SEX": "female"},
]

# FINNGENID -> release fields that must hold, checked before the full golden comparison so a
# failure names the broken lookup instead of just "row N differs".
EXPECTED = {
    "FAKE_NEGPOS": {"OUTCOME_POS_EXTRACTED": 0},
    "FAKE_PLUS": {
        "OMOP_CONCEPT_ID": "3011397",
        "OUTCOME_POS_EXTRACTED": 1,
        "TEST_OUTCOME_TEXT_EXTRACTED": "+",
    },
    "FAKE_PLUS_UNMAPPED": {"OUTCOME_POS_EXTRACTED": None},
}


def test_finngen_qc_e2e():
    """End-to-end test that the negpos and plus-plus free-text lookups set OUTCOME_POS_EXTRACTED"""

    # Get paths relative to test file
    test_dir = Path(__file__).parent
    golden_file = test_dir / "output_GOLDEN.json"
    main_script = test_dir.parent.parent / "src" / "kanta" / "__main__.py"

    # Verify paths exist
    assert golden_file.exists(), f"Golden output file not found at {golden_file}"
    assert main_script.exists(), f"Main script not found at {main_script}"

    # Create temporary output directory
    tmpdir = tempfile.TemporaryDirectory(delete=False)

    path_main_gzip, path_freetext_gzip, path_pheno_sex_gzip = fill_templates_with_mocks(
        MOCK_MAIN, MOCK_FREETEXT, MOCK_PHENO_SEX, Path(tmpdir.name)
    )

    source_list = generate_source_list(
        path_main_gzip, path_freetext_gzip, Path(tmpdir.name)
    )

    try:
        # Run the CLI command
        command = [
            'uv', 'run', 'python', '-m', 'kanta',
            '--source-list-file', str(source_list),
            '--phenotype-file', path_pheno_sex_gzip,
            '--output-dir', tmpdir.name
        ]
        print("command=\n" + " ".join(map(str, command)))
        subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=60,
            check=True
        )

        output_files = list(Path(tmpdir.name).glob("finngen_R*_kanta_laboratory_responses_1.0_*.parquet"))
        actual_release_file = next(filter(lambda ff: "RELEASE" in ff.name, output_files))
        actual_release_ppjson_file = parquet_to_ppjson(actual_release_file)

        with open(actual_release_ppjson_file, 'r', encoding='utf-8') as ff:
            actual_data = json.load(ff)

        with open(golden_file, 'r', encoding='utf-8') as ff:
            golden_data = json.load(ff)

        # Targeted checks on the lookup results
        rows_by_id = {row["FINNGENID"]: row for row in actual_data}
        failures = []
        for finngenid, fields in EXPECTED.items():
            row = rows_by_id.get(finngenid)
            if row is None:
                failures.append(f"  {finngenid}: row missing from release output")
                continue
            for field, expected in fields.items():
                if row.get(field) != expected:
                    failures.append(f"  {finngenid}: {field} is {row.get(field)!r}, expected {expected!r}")
        assert not failures, "Pos/neg free-text lookup broken:\n" + "\n".join(failures)

        # Compare rows by rows
        differences = []
        for ii, (actual_row, golden_row) in enumerate(zip_longest(actual_data, golden_data), start=1):
            if actual_row is None:
                differences.append(f"  Actual data is missing row {ii}.")
            elif golden_row is None:
                differences.append(f"  Actual data has extra row {ii}.")
            elif actual_row != golden_row:
                differences.append(f"  Row {ii} differs from golden data.")

        if differences:
            error_msg = (
                f"Output differs from golden file in {len(differences)} line(s) " +
                "(showing max 10 lines):\n\n" +
                "\n\n".join(differences[:10]) +  # Show first 10 differences
                "\n\n" +
                "Check diff with\n"
                f"  diff {golden_file} {actual_release_ppjson_file}\n"
            )
            assert False, error_msg

    except subprocess.CalledProcessError as ee:
        print(f"Failure in the pipeline itself. Temporary directory preserved at: {tmpdir.name}")
        print(ee.stdout)
        print(ee.stderr)
        raise
    except:
        print(f"Test failed. Temporary directory preserved at: {tmpdir.name}")
        raise
    else:
        tmpdir.cleanup()
