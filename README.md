# Website Analytics Pipeline — AWS S3

An Apache Airflow DAG that runs a daily Extract → Transform → Load pipeline using AWS S3 as both the source and destination store.

## Pipeline overview

```
                         ┌─ transform_data ─► load_data          → processed/data/transformed_data.csv
raw/data/*.csv → extract ┤
                         └─ country_report ─► load_country_report → processed/data/country_report.csv
```

| Step | Task ID | What it does |
|------|---------|--------------|
| Extract | `extract_data` | Copies CSV files from `raw/data/` to `staging/data/` |
| Transform | `transform_data` | Computes Daily Active Users (DAU) per day, writes to `/tmp` |
| Load | `load_data` | Uploads DAU report to `processed/data/transformed_data.csv` in S3 |
| Country Report | `country_report` | Counts unique users per country, takes top 10, writes to `/tmp` |
| Load Country | `load_country_report` | Uploads country report to `processed/data/country_report.csv` in S3 |

The two branches (`transform_data` and `country_report`) run in parallel after `extract_data`. The DAG is **manual-trigger only** — it has no schedule.

## S3 bucket structure

```
akb-test-airflow/
├── raw/
│   └── data/          ← drop source CSV files here
├── staging/
│   └── data/          ← intermediate copies (managed by Airflow)
└── processed/
    └── data/
        ├── transformed_data.csv   ← DAU report (date, dau)
        └── country_report.csv     ← top 10 countries (Country, total user)
```

## Required IAM permissions

```json
{
  "Effect": "Allow",
  "Action": ["s3:GetObject", "s3:PutObject", "s3:CopyObject", "s3:ListBucket"],
  "Resource": [
    "arn:aws:s3:::akb-test-airflow",
    "arn:aws:s3:::akb-test-airflow/*"
  ]
}
```

---

## Option A — WSL2 (Airflow 3.2.0)

### 1. Create a separate virtual environment

```bash
python3 -m venv ~/airflow3-venv
source ~/airflow3-venv/bin/activate
pip install --upgrade pip
```

### 2. Set AIRFLOW_HOME and install Airflow 3.x

```bash
export AIRFLOW_HOME=<project-path>/airflow3
echo 'export AIRFLOW_HOME=<project-path>/airflow3' >> ~/.bashrc

PYTHON_VERSION="3.12"   # adjust to your version
pip install "apache-airflow==3.2.0" \
  "apache-airflow-providers-amazon" \
  "pandas>=2.0.0" \
  "boto3>=1.28.0" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.0/constraints-${PYTHON_VERSION}.txt"
```

### 3. Initialize the database

```bash
airflow db migrate
```

Airflow 3.x generates a fresh `airflow.cfg` with `SimpleAuthManager` as the default — no FAB provider required.

> **Troubleshooting — `cannot use SQLite with the LocalExecutor`:** Airflow 3.x defaults to `LocalExecutor`. Change it to `SequentialExecutor`:
> ```bash
> sed -i 's/executor = LocalExecutor/executor = SequentialExecutor/' $AIRFLOW_HOME/airflow.cfg
> ```
> Then re-run `airflow db migrate`.

### 4. Set the JWT secret key

Airflow 3.x task subprocesses authenticate with the API server via JWT. All components must share the same secret:

```bash
# Generate a secret key
python -c "import secrets; print(secrets.token_hex(32))"

# Add to airflow.cfg under [api_auth]
# jwt_secret = <your-generated-key>
```

Or set it as an environment variable (add to `~/.bashrc`):

```bash
export AIRFLOW__API_AUTH__JWT_SECRET=<your-generated-key>
```

### 5. Point Airflow at the shared dags folder

No need to copy DAG files. Set `AIRFLOW__CORE__DAGS_FOLDER` to the project's existing `dags/` folder:

```bash
export AIRFLOW__CORE__DAGS_FOLDER=<project-path>/dags
echo 'export AIRFLOW__CORE__DAGS_FOLDER=<project-path>/dags' >> ~/.bashrc
```

### 6. Start Airflow

```bash
airflow standalone
```

Wait until you see:
```
standalone | Airflow is ready
standalone | Login with username: admin  password: <generated-password>
```

The password is also saved at:
```bash
cat $AIRFLOW_HOME/standalone_admin_password.txt
```

Open `http://localhost:8080` and login with `admin` and the password above.

### 7. Configure AWS connection in the UI

Go to **Admin → Connections → +**

| Field | Value |
|-------|-------|
| Connection ID | `aws_default` |
| Connection Type | `Amazon Web Services` |
| Extra | `{"aws_access_key_id": "your_key", "aws_secret_access_key": "your_secret", "region_name": "ap-southeast-1"}` |

> In Airflow 3.x the connection form has no dedicated key/secret fields — all credentials go in the **Extra** JSON field.

### 8. Generate and upload sample data

Run from the project root directory:

```bash
python generate_sample_data.py --files 5 --rows 1000

# Upload to S3
aws s3 cp sample_data/ s3://akb-test-airflow/raw/data/ --recursive
```

### 9. Trigger the DAG

The DAG is manual-trigger only. Trigger from the UI **▶** button, or:

```bash
airflow dags trigger website_analytics_pipeline
```

### 10. Verify S3 output

```bash
aws s3 ls s3://akb-test-airflow/processed/data/
aws s3 cp s3://akb-test-airflow/processed/data/country_report.csv -
```

---

## Option B — Docker (Airflow 3.x)

Runs Airflow 3.x with PostgreSQL via Docker Compose. AWS credentials are loaded from a `.env` file — never hardcoded.

### 1. Install Docker Desktop

Download and install Docker Desktop, enabling the WSL2 backend during setup.

### 2. Create required folders

```powershell
cd <project-path>
mkdir logs, plugins
```

### 3. Review docker-compose.yml

A `docker-compose.yml` is already included in this project. It runs Airflow 3.x with these services: `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`, and `postgres`. No changes needed — proceed to the next step.

### 4. Create the .env file

Copy the template and fill in your values:

```powershell
copy .env.example .env
```

Edit `.env`:

```env
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_DEFAULT_REGION=ap-southeast-1

# Generate with: python -c "import secrets; print(secrets.token_hex(32))"
AIRFLOW_SECRET_KEY=your_generated_secret_here
```

> **Why `AIRFLOW_SECRET_KEY`?** Airflow 3.x signs JWT tokens that task subprocesses use to authenticate with the API server. All containers must share the same key or token verification fails. Generate once and keep it consistent.

### 5. First-time database init

```powershell
docker compose up airflow-init
```

Wait for it to exit cleanly (exit code 0). This runs `airflow db migrate` and creates the `admin` user.

### 6. Start all services

```powershell
docker compose up -d
```

Wait ~60 seconds for pip dependencies to install inside containers, then open `http://localhost:8080`
- Username: `admin`
- Password: `admin123`

### 7. Configure AWS connection in the UI

Go to **Admin → Connections → +**

| Field | Value |
|-------|-------|
| Connection ID | `aws_default` |
| Connection Type | `Amazon Web Services` |
| Extra | `{"aws_access_key_id": "your_key", "aws_secret_access_key": "your_secret", "region_name": "ap-southeast-1"}` |

> In Airflow 3.x the connection form has no dedicated key/secret fields — all credentials go in the **Extra** JSON field.

### 8. Generate and upload sample data

Sample files live in `sample_data/`. Use `generate_sample_data.py` to create them:

```powershell
# Generate 5 files with 1000 rows each (starting 2024-01-01)
python generate_sample_data.py --files 5 --rows 1000

# All options
python generate_sample_data.py --files 5 --rows 1000 --start-date 2024-01-01 --users 500
```

| Flag | Default | Description |
|------|---------|-------------|
| `--files` | 2 | Number of CSV files; each covers one calendar day |
| `--rows` | 1000 | Rows per file |
| `--start-date` | `2024-01-01` | Date of the first file |
| `--users` | 500 | User pool size — smaller = higher DAU per day |

Then upload to S3:

```powershell
aws s3 cp sample_data/ s3://akb-test-airflow/raw/data/ --recursive
```

### 9. Trigger and monitor the DAG

The DAG is manual-trigger only. Click **▶** in the UI, or:

```powershell
docker compose exec airflow-scheduler airflow dags trigger website_analytics_pipeline
```

### 10. Verify S3 output

```powershell
# List output files
aws s3 ls s3://akb-test-airflow/processed/data/

# View DAU report
aws s3 cp s3://akb-test-airflow/processed/data/transformed_data.csv -

# View country report
aws s3 cp s3://akb-test-airflow/processed/data/country_report.csv -
```

### Docker — Key commands reference

```powershell
# Start all services
docker compose up -d

# Stop all services
docker compose down

# View scheduler logs
docker compose logs -f airflow-scheduler

# View dag-processor logs (DAG parsing)
docker compose logs -f airflow-dag-processor

# Trigger DAG manually
docker compose exec airflow-scheduler airflow dags trigger website_analytics_pipeline

# Check run status
docker compose exec airflow-scheduler airflow dags list-runs --dag-id website_analytics_pipeline

# Full reset — deletes all run history and the database
docker compose down -v
docker compose up airflow-init
docker compose up -d
```

### Docker — Airflow 3.x architecture

Airflow 3.x splits responsibilities across five containers:

| Service | Command | Role |
|---------|---------|------|
| `airflow-apiserver` | `api-server` | Serves the UI and REST API on port 8080; also handles task execution API |
| `airflow-scheduler` | `scheduler` | Schedules task runs; runs task subprocesses (LocalExecutor) |
| `airflow-dag-processor` | `dag-processor` | Parses DAG files and writes them to the DB — **new in 3.x, required** |
| `airflow-triggerer` | `triggerer` | Handles deferrable operators |
| `postgres` | — | Metadata database |

---

## Input CSV format

| Column | Type | Description |
|--------|------|-------------|
| `user_id` | string / int | Unique user identifier |
| `timestamp` | datetime string | Event timestamp (any pandas-parseable format) |
| `country` | string | ISO 3166-1 alpha-2 country code (e.g. `US`, `IN`, `GB`) |

Additional columns are allowed and ignored.

## Output CSV format

**`processed/data/transformed_data.csv`** — Daily Active Users

| Column | Type | Description |
|--------|------|-------------|
| `date` | date | Calendar date |
| `dau` | int | Unique users active that day |

**`processed/data/country_report.csv`** — Top 10 countries

| Column | Type | Description |
|--------|------|-------------|
| `Country` | string | ISO 3166-1 alpha-2 country code |
| `total user` | int | Unique users from that country (across all input files) |
