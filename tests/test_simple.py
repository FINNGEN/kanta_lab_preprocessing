import subprocess
import tempfile
from pathlib import Path


def test_finngen_qc_e2e():
    """E2E test running finngen_qc/main.py with mock data"""

    # Get paths relative to test file
    test_dir = Path(__file__).parent
    mock_data = test_dir / "mock_data" / "laboratory_responses_internal_unique.tsv"
    main_script = test_dir.parent / "src" / "kanta" / "finngen_qc" / "main.py"

    # Verify paths exist
    assert mock_data.exists(), f"Mock data not found at {mock_data}"
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

        # Check that output is not empty
        assert output_files[0].stat().st_size > 0, "Output file is empty"

    except:
        print(f"Test failed. Temporary directory preserved at: {tmpdir.name}")
        raise
    else:
        tmpdir.cleanup()
