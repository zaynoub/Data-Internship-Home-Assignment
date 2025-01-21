from datetime import timedelta, datetime
import os
import re
import json
import pandas as pd

# Airflow imports
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

###############################################################################
#                           SQL TABLE CREATION
###############################################################################

TABLES_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);

CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    currency VARCHAR(5),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS location (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    country VARCHAR(60),
    locality VARCHAR(60),
    region VARCHAR(60),
    postal_code VARCHAR(25),
    street_address VARCHAR(225),
    latitude NUMERIC,
    longitude NUMERIC,
    FOREIGN KEY (job_id) REFERENCES job(id)
);
"""

###############################################################################
#                         PURE PYTHON / LOGIC FUNCTIONS
###############################################################################

def _clean_html(text: str) -> str:
    """Remove simple HTML tags/escapes from text using regex/substitutions."""
    text = re.sub(r"<.*?>", "", text)
    text = text.replace("&lt;", "").replace("&gt;", "")
    text = text.replace("&amp;", "&")
    return text.strip()

def _extract_salary(raw_json: dict, description: str) -> dict:
    """
    Attempts to parse out salary info from:
     1) A structured field like "estimatedSalary" or "baseSalary"
     2) A "Salary Range: $120k - $150k" style snippet in the job description (regex)
     3) Returns None fields if nothing found
    """
    # 1) Check structured fields
    for key in ["estimatedSalary", "baseSalary"]:
        if key in raw_json:
            sal = raw_json[key]
            currency = sal.get("currency")
            val_info = sal.get("value", {})
            min_val = val_info.get("minValue")
            max_val = val_info.get("maxValue")
            unit = val_info.get("unitText")
            if currency or min_val or max_val:
                return {
                    "currency": currency,
                    "min_value": min_val,
                    "max_value": max_val,
                    "unit": unit
                }

    # 2) Check description snippet like "Salary Range: $120k - $150k"
    match = re.search(r"Salary Range:\s*\$?(\d+)\s*[kK]\s*-\s*\$?(\d+)\s*[kK]", description)
    if match:
        try:
            min_val = int(match.group(1)) * 1000
            max_val = int(match.group(2)) * 1000
            return {
                "currency": "USD",
                "min_value": min_val,
                "max_value": max_val,
                "unit": "YEAR"
            }
        except ValueError:
            pass

    # 3) Otherwise, no salary
    return {
        "currency": None,
        "min_value": None,
        "max_value": None,
        "unit": None
    }

def do_extract(csv_path: str, output_dir: str) -> str:
    """
    Pure function for extract logic (no @task).
    Reads 'context' from CSV, writes .txt files to output_dir, returns output_dir.
    """
    import numpy as np
    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_csv(csv_path, usecols=["context"])
    for idx, row in df.iterrows():
        val = row["context"]
        if pd.isna(val) or str(val).strip() == "":
            continue
        out_file = os.path.join(output_dir, f"job_{idx}.txt")
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(str(val))

    return output_dir

def do_transform(input_dir: str, output_dir: str) -> str:
    """
    Pure function for transform logic.
    Reads .txt JSON, cleans, sets final schema, writes .json to output_dir.
    """
    os.makedirs(output_dir, exist_ok=True)
    extracted_files = [f for f in os.listdir(input_dir) if f.endswith(".txt")]

    for idx, filename in enumerate(extracted_files):
        file_path = os.path.join(input_dir, filename)
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if not content:
            continue

        try:
            raw_json = json.loads(content)
        except json.JSONDecodeError:
            continue

        # Clean description
        description = raw_json.get("description", "")
        cleaned_description = _clean_html(description)

        # Experience
        exp_req = raw_json.get("experienceRequirements", {})
        months_of_experience = None
        if isinstance(exp_req, dict):
            months_of_experience = exp_req.get("monthsOfExperience")

        # Salary
        salary_info = _extract_salary(raw_json, cleaned_description)

        # Final schema
        transformed_record = {
            "job": {
                "title": raw_json.get("title"),
                "industry": raw_json.get("industry"),
                "description": cleaned_description,
                "employment_type": raw_json.get("employmentType"),
                "date_posted": raw_json.get("datePosted"),
            },
            "company": {
                "name": raw_json.get("hiringOrganization", {}).get("name"),
                "link": raw_json.get("hiringOrganization", {}).get("sameAs"),
            },
            "education": {
                "required_credential": (
                    raw_json.get("educationRequirements", {})
                    if isinstance(raw_json.get("educationRequirements"), dict)
                    else {}
                ).get("credentialCategory")
            },
            "experience": {
                "months_of_experience": months_of_experience,
                "seniority_level": raw_json.get("seniority_level"),
            },
            "salary": {
                "currency": salary_info["currency"],
                "min_value": salary_info["min_value"],
                "max_value": salary_info["max_value"],
                "unit": salary_info["unit"]
            },
            "location": {
                "country": raw_json.get("jobLocation", {}).get("address", {}).get("addressCountry"),
                "locality": raw_json.get("jobLocation", {}).get("address", {}).get("addressLocality"),
                "region": raw_json.get("jobLocation", {}).get("address", {}).get("addressRegion"),
                "postal_code": raw_json.get("jobLocation", {}).get("address", {}).get("postalCode"),
                "street_address": raw_json.get("jobLocation", {}).get("address", {}).get("streetAddress"),
                "latitude": raw_json.get("jobLocation", {}).get("latitude"),
                "longitude": raw_json.get("jobLocation", {}).get("longitude"),
            }
        }

        out_file = os.path.join(output_dir, f"job_{idx}.json")
        with open(out_file, "w", encoding="utf-8") as out_f:
            json.dump(transformed_record, out_f, indent=2)

    return output_dir

def do_load(input_dir: str) -> str:
    """
    Pure function for load logic.
    Reads .json from input_dir, does single-transaction insert.
    Returns a success message with record count.
    """
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')

    records = []
    for fname in os.listdir(input_dir):
        if fname.endswith(".json"):
            path = os.path.join(input_dir, fname)
            with open(path, "r", encoding="utf-8") as f:
                records.append(json.load(f))

    with sqlite_hook.get_conn() as conn:
        cur = conn.cursor()
        for record in records:
            # Insert into job
            job_sql = """
            INSERT INTO job (title, industry, description, employment_type, date_posted)
            VALUES (?, ?, ?, ?, ?)
            """
            job_vals = (
                record["job"]["title"],
                record["job"]["industry"],
                record["job"]["description"],
                record["job"]["employment_type"],
                record["job"]["date_posted"]
            )
            cur.execute(job_sql, job_vals)
            job_id = cur.lastrowid

            # child: company
            company_sql = """
            INSERT INTO company (job_id, name, link)
            VALUES (?, ?, ?)
            """
            company_vals = (job_id,
                            record["company"]["name"],
                            record["company"]["link"])
            cur.execute(company_sql, company_vals)

            # child: education
            edu_sql = """
            INSERT INTO education (job_id, required_credential)
            VALUES (?, ?)
            """
            edu_vals = (job_id, record["education"]["required_credential"])
            cur.execute(edu_sql, edu_vals)

            # child: experience
            exp_sql = """
            INSERT INTO experience (job_id, months_of_experience, seniority_level)
            VALUES (?, ?, ?)
            """
            exp_vals = (job_id,
                        record["experience"]["months_of_experience"],
                        record["experience"]["seniority_level"])
            cur.execute(exp_sql, exp_vals)

            # child: salary
            sal_sql = """
            INSERT INTO salary (job_id, currency, min_value, max_value, unit)
            VALUES (?, ?, ?, ?, ?)
            """
            sal_vals = (job_id,
                        record["salary"]["currency"],
                        record["salary"]["min_value"],
                        record["salary"]["max_value"],
                        record["salary"]["unit"])
            cur.execute(sal_sql, sal_vals)

            # child: location
            loc_sql = """
            INSERT INTO location (job_id, country, locality, region, postal_code,
                                  street_address, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            loc_vals = (job_id,
                        record["location"]["country"],
                        record["location"]["locality"],
                        record["location"]["region"],
                        record["location"]["postal_code"],
                        record["location"]["street_address"],
                        record["location"]["latitude"],
                        record["location"]["longitude"])
            cur.execute(loc_sql, loc_vals)

        conn.commit()

    return f"Successfully loaded {len(records)} job records (and child rows) into SQLite!"

###############################################################################
#                          DAG TASK WRAPPERS
###############################################################################
@task()
def extract(csv_path: str = "/usr/local/airflow/source/jobs.csv") -> str:
    extracted_dir = "/usr/local/airflow/staging/extracted"
    return do_extract(csv_path, extracted_dir)

@task()
def transform(extracted_dir: str) -> str:
    transformed_dir = "/usr/local/airflow/staging/transformed"
    return do_transform(extracted_dir, transformed_dir)

@task()
def load(transformed_dir: str) -> str:
    return do_load(transformed_dir)

###############################################################################
#                          DAG DEFINITION
###############################################################################
from airflow import DAG

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15),
}

@dag(
    dag_id="etl_dag_single_file",
    description="ETL pipeline w/ salary parsing & single transaction loading",
    schedule="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS,
    tags=["etl"]
)
def etl_dag_single_file():
    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY,
        split_statements=True
    )

    extracted_dir = extract()
    transformed_dir = transform(extracted_dir)
    load_done = load(transformed_dir)

    create_tables >> extracted_dir >> transformed_dir >> load_done

etl_dag_single_file = etl_dag_single_file()

