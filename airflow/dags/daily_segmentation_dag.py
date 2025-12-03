from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'daily_user_segmentation',
    default_args=default_args,
    description='Daily User Segmentation and Reporting',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

# SQL for User Segmentation
# Window Shoppers: Views > 0, Purchases = 0
# Buyers: Purchases > 0
create_segmentation_table = PostgresOperator(
    task_id='create_segmentation_table',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS user_segments (
        user_id VARCHAR(50),
        segment VARCHAR(20),
        date DATE
    );
    """,
    dag=dag,
)

segment_users = PostgresOperator(
    task_id='segment_users',
    postgres_conn_id='postgres_default',
    sql="""
    INSERT INTO user_segments (user_id, segment, date)
    SELECT 
        user_id,
        CASE 
            WHEN sum(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) > 0 THEN 'Buyer'
            ELSE 'Window Shopper'
        END as segment,
        CURRENT_DATE
    FROM activity_logs
    GROUP BY user_id;
    """,
    dag=dag,
)

# SQL for Top 5 Products
create_top_products_table = PostgresOperator(
    task_id='create_top_products_table',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS top_products (
        product_id VARCHAR(50),
        total_views BIGINT,
        date DATE
    );
    """,
    dag=dag,
)

top_products = PostgresOperator(
    task_id='top_products',
    postgres_conn_id='postgres_default',
    sql="""
    INSERT INTO top_products (product_id, total_views, date)
    SELECT 
        product_id,
        sum(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as total_views,
        CURRENT_DATE
    FROM activity_logs
    GROUP BY product_id
    ORDER BY total_views DESC
    LIMIT 5;
    """,
    dag=dag,
)

# Python Task for Reporting
def generate_report_func():
    # Connect to Postgres
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
    
    # Get Conversion Rates
    query = """
    SELECT 
        product_id,
        sum(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
        sum(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
        CASE 
            WHEN sum(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) > 0 
            THEN CAST(sum(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS FLOAT) / sum(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END)
            ELSE 0 
        END as conversion_rate
    FROM activity_logs
    GROUP BY product_id;
    """
    df = pd.read_sql(query, engine)
    
    # Save Report
    report_path = "/tmp/analytic_report.csv"
    df.to_csv(report_path, index=False)
    print(f"Report generated at {report_path}")

generate_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report_func,
    dag=dag,
)

# Dependencies
create_segmentation_table >> segment_users
create_top_products_table >> top_products
[segment_users, top_products] >> generate_report
