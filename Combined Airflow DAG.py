# DAG Code for PART 1------------------------------------------------------------------

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os


# Function: Load CSV from GCS → Postgres

def load_csv_from_gcs_to_postgres(bucket_name, source_path, table_name, postgres_conn_id='pg-airbnb-manasvi'):
    """Download CSV from GCS and load it into Postgres"""
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    tmp_path = f"/tmp/{os.path.basename(source_path)}"
    gcs_hook.download(bucket_name=bucket_name, object_name=source_path, filename=tmp_path)

    print(f"Downloaded {source_path} from bucket {bucket_name}")

    df = pd.read_csv(tmp_path)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df.columns = [c.lower().strip() for c in df.columns]

    # Display first 5 rows in logs
    print(" First 5 rows of the dataset:")
    print(df.head().to_string())

    # Clean up nulls
    for c in df.columns:
        df[c] = df[c].astype(str).replace({'nan': None, 'NaT': None, '': None})

    # Parse dates safely to ISO format 
    for date_col in ['scraped_date', 'host_since']:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce', dayfirst=True)
            df[date_col] = df[date_col].dt.strftime('%Y-%m-%d')
            df[date_col] = df[date_col].where(pd.notnull(df[date_col]), None)

    # Boolean conversions
    bool_cols = ['host_is_superhost', 'has_availability']
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].map({'t': True, 'f': False, 'True': True, 'False': False}).fillna(False)

    # Connect to Postgres
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(f'TRUNCATE TABLE {table_name} CASCADE;')
    conn.commit()

    # Insert rows
    for _, row in df.iterrows():
        columns = ', '.join([f'"{col}"' for col in row.index])
        placeholders = ', '.join(['%s'] * len(row))
        sql = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
        cursor.execute(sql, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Loaded {len(df)} rows into {table_name}")



# DAG Definition
with DAG(
    dag_id='bronze_ingestion_gcs_sequential',
    start_date=datetime(2025, 10, 21),
    schedule_interval=None,  # Manual run
    catchup=False,
    tags=['bronze', 'gcs', 'ingestion', 'sequential']
) as dag:

    bucket_name = 'manasvi-airbnb-bucket'

    # Airbnb data
    load_airbnb = PythonOperator(
        task_id='load_airbnb',
        python_callable=load_csv_from_gcs_to_postgres,
        op_kwargs={
            'bucket_name': bucket_name,
            'source_path': 'airbnbdata/05_2020.csv',
            'table_name': 'bronze.airbnb_raw'
        }
    )

    # Census G01
    load_census_g01 = PythonOperator(
        task_id='load_census_g01',
        python_callable=load_csv_from_gcs_to_postgres,
        op_kwargs={
            'bucket_name': bucket_name,
            'source_path': 'censusdata/2016Census_G01_NSW_LGA.csv',
            'table_name': 'bronze.census_g01'
        }
    )

    # Census G02
    load_census_g02 = PythonOperator(
        task_id='load_census_g02',
        python_callable=load_csv_from_gcs_to_postgres,
        op_kwargs={
            'bucket_name': bucket_name,
            'source_path': 'censusdata/2016Census_G02_NSW_LGA.csv',
            'table_name': 'bronze.census_g02'
        }
    )

    # LGA Codes
    load_lga_codes = PythonOperator(
        task_id='load_lga_codes',
        python_callable=load_csv_from_gcs_to_postgres,
        op_kwargs={
            'bucket_name': bucket_name,
            'source_path': 'mappingsdata/NSW_LGA_CODE.csv',
            'table_name': 'bronze.lga_codes'
        }
    )

    # LGA Suburb Mapping
    load_lga_suburb_mapping = PythonOperator(
        task_id='load_lga_suburb_mapping',
        python_callable=load_csv_from_gcs_to_postgres,
        op_kwargs={
            'bucket_name': bucket_name,
            'source_path': 'mappingsdata/NSW_LGA_SUBURB.csv',
            'table_name': 'bronze.lga_suburb_mapping'
        }
    )

    
    # Sequential dependencies
    
    load_airbnb >> load_census_g01 >> load_census_g02 >> load_lga_codes >> load_lga_suburb_mapping



# DAG Code for PART 3------------------------------------------------------------------
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os


# Function to append CSV from GCS to Postgres

def load_csv_from_gcs_to_postgres(bucket_name, source_path, table_name, postgres_conn_id='pg-airbnb-manasvi'):
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    tmp_path = f"/tmp/{os.path.basename(source_path)}"

    # Download file from GCS
    gcs_hook.download(bucket_name=bucket_name, object_name=source_path, filename=tmp_path)

    # Read and clean CSV
    df = pd.read_csv(tmp_path)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df.columns = [c.lower().strip() for c in df.columns]

    # Clean nulls
    for c in df.columns:
        df[c] = df[c].astype(str).replace({'nan': None, 'NaT': None, '': None})

    # Parse date columns
    for date_col in ['scraped_date', 'host_since']:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce', dayfirst=True)
            df[date_col] = df[date_col].dt.strftime('%Y-%m-%d')
            df[date_col] = df[date_col].where(pd.notnull(df[date_col]), None)

    # Boolean conversions
    bool_cols = ['host_is_superhost', 'has_availability']
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].map({'t': True, 'f': False, 'True': True, 'False': False}).fillna(False)

    # Connect to Postgres
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Append mode (no truncation)
    for _, row in df.iterrows():
        columns = ', '.join([f'"{col}"' for col in row.index])
        placeholders = ', '.join(['%s'] * len(row))
        sql = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
        cursor.execute(sql, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Loaded {len(df)} rows from {source_path} into {table_name}")


# DAG Definition

with DAG(
    dag_id='airbnb_load_chronological_append',
    start_date=datetime(2025, 10, 24),
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=['bronze', 'airbnb', 'append']
) as dag:

    bucket_name = 'manasvi-airbnb-bucket'

    # List Airbnb months in correct chronological order
    airbnb_months = [
        '06_2020','07_2020','08_2020','09_2020',
        '10_2020','11_2020','12_2020','01_2021','02_2021',
        '03_2021','04_2021'
    ]

    previous_task = None
    for month in airbnb_months:
        task = PythonOperator(
            task_id=f'load_airbnb_{month}',
            python_callable=load_csv_from_gcs_to_postgres,
            op_kwargs={
                'bucket_name': bucket_name,
                'source_path': f'airbnbdata/{month}.csv',
                'table_name': 'bronze.airbnb_raw'
            }
        )

        # Run sequentially (05_2020 → 06_2020 → … → 04_2021)
        if previous_task:
            previous_task >> task
        previous_task = task
