import subprocess
import tempfile
from pathlib import Path


def test_finngen_qc_e2e():
    """E2E test running finngen_qc/main.py with mock data"""

    # Get paths relative to test file
    test_dir = Path(__file__).parent
    mock_data = test_dir / "laboratory_responses_internal_unique.tsv"
    golden_file = test_dir / "kanta_munged__GOLDEN.txt"
    main_script = test_dir.parent.parent / "src" / "kanta" / "finngen_qc" / "main.py"

    # Verify paths exist
    assert mock_data.exists(), f"Mock data not found at {mock_data}"
    assert golden_file.exists(), f"Golden file not found at {golden_file}"
    assert main_script.exists(), f"Main script not found at {main_script}"

    # Create temporary output directory
    tmpdir =  tempfile.TemporaryDirectory(delete=False)

    try:
        # Run the CLI command
        result = subprocess.run(
            [
                'python', str(main_script),
                '--raw-data', str(mock_data),
                '--out', tmpdir.name,
                '--log', 'info'
            ],
            capture_output=True,
            text=True,
            timeout=60
        )

        # Check exit code
        assert result.returncode == 0, (
            f"Command failed with exit code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

        # Check that output file was created
        output_files = list(Path(tmpdir.name).glob("*_munged.txt"))
        assert len(output_files) > 0, "No munged output file created"

        # Check that log file was created
        log_files = list(Path(tmpdir.name).glob("*_log.txt"))
        assert len(log_files) > 0, "No log file created"

        # Compare munged output with golden file
        actual_output = output_files[0]

        # Read both files
        with open(actual_output, 'r', encoding='utf-8') as f:
            actual_lines = f.readlines()

        with open(golden_file, 'r', encoding='utf-8') as f:
            golden_lines = f.readlines()

        # Compare line by line
        differences = []
        for i, (actual_line, golden_line) in enumerate(zip(actual_lines, golden_lines), start=1):
            if actual_line != golden_line:
                differences.append(
                    f"Line {i} differs:\n"
                    f"  Actual: {actual_line.rstrip()}\n"
                    f"  Golden: {golden_line.rstrip()}"
                )

        if differences:
            error_msg = (
                f"Output differs from golden file in {len(differences)} line(s):\n\n" +
                "\n\n".join(differences[:10])  # Show first 10 differences
            )
            assert False, error_msg

    except:
        print(f"Test failed. Temporary directory preserved at: {tmpdir.name}")
        raise
    else:
        tmpdir.cleanup()
