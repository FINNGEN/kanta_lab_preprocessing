import shutil
import tempfile
import warnings
from pathlib import Path


def check_safe_write(output_file: Path) -> None:
    """Check it's safe to write to output file path."""
    if output_file.exists():
        raise FileExistsError(
            f"The output file already exists at {output_file}. Aborting."
        )

    free_space = check_free_space(output_file.parent)
    print(f"Free space in dir for output file {output_file.name}: {free_space}")


def create_tmp_dir():
    tmp_dir = Path(tempfile.mkdtemp())

    free_space = check_free_space(tmp_dir)
    print(f"Free space: {free_space} in {tmp_dir}")

    return tmp_dir


def check_free_space(dir, *, min_space_gb=50) -> str:
    disk_usage = shutil.disk_usage(dir)
    free_in_gib = disk_usage.free >> 30

    if free_in_gib < min_space_gb:
        warnings.warn(f"Only {free_in_gib} GiB of free disk space available.")

    return f"{free_in_gib} GiB"


def teardown_dir(dir):
    shutil.rmtree(dir)
