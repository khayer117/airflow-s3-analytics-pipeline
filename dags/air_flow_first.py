from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import io

BUCKET_NAME = 'akb-test-airflow'
RAW_PREFIX = 'raw/data/'
STAGING_PREFIX = 'staging/data/'
PROCESSED_PREFIX = 'processed/data/'
AWS_CONN_ID = 'aws_default'

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1,
}


def extract_data():
    """Copy raw CSV files from raw/ prefix into staging/ prefix."""
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=RAW_PREFIX)

    if not keys:
        raise ValueError(f"No files found at s3://{BUCKET_NAME}/{RAW_PREFIX}")

    for key in (k for k in keys if k.endswith('.csv')):
        filename = key.split('/')[-1]
        hook.copy_object(
            source_bucket_key=key,
            dest_bucket_key=f"{STAGING_PREFIX}{filename}",
            source_bucket_name=BUCKET_NAME,
            dest_bucket_name=BUCKET_NAME,
        )


def transform_data():
    """Read staged CSVs, compute daily active users, save result locally."""
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=STAGING_PREFIX)

    if not keys:
        raise ValueError(f"No files found at s3://{BUCKET_NAME}/{STAGING_PREFIX}")

    all_data = []
    for key in (k for k in keys if k.endswith('.csv')):
        s3_obj = hook.get_key(key=key, bucket_name=BUCKET_NAME)
        df = pd.read_csv(io.BytesIO(s3_obj.get()['Body'].read()))
        all_data.append({
            'date': pd.to_datetime(df['timestamp'].iloc[0]).date(),
            'dau': df['user_id'].nunique(),
        })

    transformed_df = pd.DataFrame(all_data)
    transformed_df.to_csv('/tmp/transformed_data.csv', index=False)


def load_data():
    """Upload the locally transformed CSV to the processed/ prefix in S3."""
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    hook.load_file(
        filename='/tmp/transformed_data.csv',
        key=f"{PROCESSED_PREFIX}transformed_data.csv",
        bucket_name=BUCKET_NAME,
        replace=True,
    )


def country_report():
    """Read staged CSVs, compute top 10 countries by unique user count."""
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=STAGING_PREFIX)

    if not keys:
        raise ValueError(f"No files found at s3://{BUCKET_NAME}/{STAGING_PREFIX}")

    all_data = []
    for key in (k for k in keys if k.endswith('.csv')):
        s3_obj = hook.get_key(key=key, bucket_name=BUCKET_NAME)
        df = pd.read_csv(io.BytesIO(s3_obj.get()['Body'].read()))
        all_data.append(df)

    combined = pd.concat(all_data, ignore_index=True)

    if 'country' not in combined.columns:
        raise ValueError("Input CSV is missing a 'country' column required for the country report.")

    report = (
        combined.groupby('country')['user_id']
        .nunique()
        .reset_index()
        .rename(columns={'country': 'Country', 'user_id': 'total user'})
        .sort_values('total user', ascending=False)
        .head(10)
    )
    report.to_csv('/tmp/country_report.csv', index=False)


def load_country_report():
    """Upload the country report CSV to the processed/ prefix in S3."""
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    hook.load_file(
        filename='/tmp/country_report.csv',
        key=f"{PROCESSED_PREFIX}country_report.csv",
        bucket_name=BUCKET_NAME,
        replace=True,
    )


with DAG(
    dag_id='website_analytics_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['s3', 'analytics'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    country_report_task = PythonOperator(
        task_id='country_report',
        python_callable=country_report,
    )

    load_country_report_task = PythonOperator(
        task_id='load_country_report',
        python_callable=load_country_report,
    )

    extract_task >> [transform_task, country_report_task]
    transform_task >> load_task
    country_report_task >> load_country_report_task
