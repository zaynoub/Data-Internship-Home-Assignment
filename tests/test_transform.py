import os
import json
import pytest
from dags.etl_dag_single_file import do_transform

@pytest.fixture
def extracted_txt_dir(tmp_path):
    """Creates .txt files containing sample JSON for transform step."""
    in_dir = tmp_path / "extracted"
    in_dir.mkdir()
    sample1 = {
        "title": "Senior Data Engineer",
        "description": "Salary Range: $120k - $150k<br>",
        "experienceRequirements": {"monthsOfExperience": 60}
    }
    sample2 = {
        "title": "Software Dev",
        "description": "Another desc",
        "estimatedSalary": {
            "@type": "MonetaryAmount",
            "currency": "USD",
            "value": {
                "@type": "QuantitativeValue", 
                "minValue": 130000, 
                "maxValue": 200000, 
                "unitText": "YEAR"
            }
        }
    }

    with open(in_dir / "job_0.txt", "w") as f:
        f.write(json.dumps(sample1))
    with open(in_dir / "job_1.txt", "w") as f:
        f.write(json.dumps(sample2))

    return str(in_dir)

def test_do_transform_creates_json_files(extracted_txt_dir, tmp_path):
    out_dir = tmp_path / "transformed"
    out_dir_path = do_transform(extracted_txt_dir, str(out_dir))

    assert out_dir_path == str(out_dir)
    assert os.path.isdir(out_dir_path)

    # We expect 2 JSON files
    files = os.listdir(out_dir_path)
    assert len(files) == 2

    # Inspect first file
    f0 = os.path.join(out_dir_path, files[0])
    with open(f0, "r") as ff:
        record = json.load(ff)
        # Because description had Salary Range: $120k - $150k
        assert record["salary"]["min_value"] == 120000
        assert record["salary"]["max_value"] == 150000

    # Inspect second file
    f1 = os.path.join(out_dir_path, files[1])
    with open(f1, "r") as ff:
        record = json.load(ff)
        sal = record["salary"]
        assert sal["min_value"] == 130000
        assert sal["max_value"] == 200000
        assert sal["unit"] == "YEAR"

