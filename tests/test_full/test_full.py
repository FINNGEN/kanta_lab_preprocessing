import json
import subprocess
import tempfile
from itertools import zip_longest
from pathlib import Path

from tests import generate_source_list, gzip_file, parquet_to_ppjson


def test_finngen_qc_e2e():
    """End-to-end test running the full pipeline with mock data"""

    # Get paths relative to test file
    test_dir = Path(__file__).parent
    mock_data_main = test_dir / "input_main.tsv"
    mock_data_freetext = test_dir / "input_freetext.tsv"
    mock_data_pheno_sex = test_dir / "input_pheno.tsv"
    golden_file = test_dir / "output_GOLDEN.json"
    main_script = test_dir.parent.parent / "src" / "kanta" / "__main__.py"

    # Verify paths exist
    assert mock_data_main.exists(), f"Mock data main not found at {mock_data_main}"
    assert mock_data_freetext.exists(), f"Mock data freetext not found at {mock_data_freetext}"
    assert golden_file.exists(), f"Golden file not found at {golden_file}"
    assert main_script.exists(), f"Main script not found at {main_script}"

    # Create temporary output directory
    tmpdir =  tempfile.TemporaryDirectory(delete=False)

    path_main_gzip = gzip_file(mock_data_main, Path(tmpdir.name))
    path_freetext_gzip = gzip_file(mock_data_freetext, Path(tmpdir.name))
    source_list = generate_source_list(
        path_main_gzip,
        path_freetext_gzip,
        Path(tmpdir.name)
    )

    try:
        # Run the CLI command
        command = [
            'uv', 'run', 'python', '-m', 'kanta',
            '--source-list-file', str(source_list),
            '--phenotype-file', mock_data_pheno_sex,
            '--output-dir', tmpdir.name
        ]
        print("command=\n" + " ".join(map(str, command)))
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=60,
            check=True
        )

        # Check exit code
        assert result.returncode == 0, (
            f"Command failed with exit code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

        # Check that output file was created
        expected_n_output_files = 5
        output_files = list(Path(tmpdir.name).glob("finngen_R*_kanta_laboratory_responses_1.0_*.parquet"))
        assert len(output_files) == expected_n_output_files, \
            f"Different number of output files, expected 5, got {len(output_files)}"

        # Check that log file was created
        log_file = next(Path(tmpdir.name).glob("finngen_R*_kanta_laboratory_responses_1.0_*.log"))
        assert log_file.exists(), "No log file created"

        # Read the actual data
        # See NOTE(Vincent 2026-08-26) in test_simple.py
        actual_release_file = next(filter(lambda ff: "RELEASE" in ff.name, output_files))
        actual_release_ppjson_file = parquet_to_ppjson(actual_release_file)

        with open(actual_release_ppjson_file, 'r', encoding='utf-8') as ff:
            actual_data = json.load(ff)

        with open(golden_file, 'r', encoding='utf-8') as ff:
            golden_data = json.load(ff)

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
