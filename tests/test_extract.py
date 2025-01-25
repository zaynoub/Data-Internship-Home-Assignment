import os
import pytest
from dags.etl_dag_single_file import do_extract

@pytest.fixture
def sample_csv(tmp_path):
    """
    Creates a temp CSV with 'context' lines that are already valid JSON strings
    without double-escaping.
    """
    csv_file = tmp_path / "jobs.csv"
    # We place the JSON object directly in the 'context' column,
    # e.g. {"title":"Data Engineer"} with no extra quoting or escaping.
    content = """context
{"title":"Data Engineer"}
{"title":"Software Developer"}
"""
    csv_file.write_text(content, encoding="utf-8")
    return str(csv_file)

def test_do_extract_creates_txt_files(sample_csv, tmp_path):
    """Test that do_extract reads CSV and writes .txt files containing the JSON."""
    extracted_dir = tmp_path / "extracted"
    extracted_dir_path = do_extract(sample_csv, str(extracted_dir))

    # Check the directory was returned and created
    assert extracted_dir_path == str(extracted_dir)
    assert os.path.isdir(extracted_dir_path)

    # We expect 2 .txt files
    files = os.listdir(extracted_dir_path)
    assert len(files) == 2

    # Check the contents
    for fname in files:
        with open(os.path.join(extracted_dir_path, fname), "r") as f:
            data = f.read().strip()
            # We expect e.g.: {"title":"Data Engineer"}
            assert '"title":"' in data

