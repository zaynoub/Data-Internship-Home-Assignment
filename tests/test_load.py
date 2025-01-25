import os
import json
import pytest
from unittest.mock import patch, MagicMock

from dags.etl_dag_single_file import do_load

@pytest.fixture
def transformed_json_dir(tmp_path):
    """Creates sample .json files to simulate final transform step."""
    out_dir = tmp_path / "transformed"
    out_dir.mkdir()

    record1 = {
        "job": {
            "title": "Data Engineer",
            "industry": "Software",
            "description": "Cleaned desc",
            "employment_type": "FULL_TIME",
            "date_posted": "2021-07-01"
        },
        "company": {"name": "TestCo", "link": "http://example.com"},
        "education": {"required_credential": "Bachelor"},
        "experience": {"months_of_experience": 60, "seniority_level": "Senior"},
        "salary": {"currency": "USD", "min_value": 120000, "max_value": 150000, "unit": "YEAR"},
        "location": {"country": "US", "locality": "Alexandria", "region": "VA", 
                     "postal_code": "22336", "street_address": None, "latitude": 38.8, "longitude": -77.04}
    }

    with open(out_dir / "job_0.json", "w") as f:
        json.dump(record1, f)

    return str(out_dir)

def test_do_load_inserts_records(transformed_json_dir):
    """
    Test that do_load reads the .json files and inserts them into the DB.
    We patch the context manager so that the with-statement doesn't fail 
    and we can track calls.
    """
    with patch("dags.etl_dag_single_file.SqliteHook") as MockHook:
        hook_instance = MockHook.return_value
        mock_conn = MagicMock()
        # Make the mock_conn a valid context manager
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = False

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        hook_instance.get_conn.return_value = mock_conn

        message = do_load(transformed_json_dir)

        assert "Successfully loaded 1 job records" in message

        calls = mock_cursor.execute.call_args_list
        # We expect 6 inserts (job, company, education, experience, salary, location)
        assert len(calls) == 6

        # For example, check the first call is inserting into job
        first_call = calls[0]
        sql_statement, params = first_call[0]
        assert "INSERT INTO job" in sql_statement
        assert params[0] == "Data Engineer"

