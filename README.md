# E-Commerce Clickstream Analysis System

An end-to-end Big Data pipeline implementing a **Kappa Architecture** to process real-time e-commerce user events. The system ingests clickstream data, performs real-time aggregation for alerts, and orchestrates daily reporting.

## 🏗 Architecture

The pipeline consists of the following components:

1.  **Ingestion Layer**:
    -   **Source**: Python Producer simulating user events (`view`, `add_to_cart`, `purchase`).
    -   **Message Broker**: Apache Kafka (Topic: `clickstream`).

2.  **Stream Processing Layer**:
    -   **Engine**: Apache Spark Structured Streaming.
    -   **Logic**: Aggregates views/purchases in 10-minute sliding windows.
    -   **Alerting**: Triggers "Flash Sale" alerts for products with High Interest (>100 views) but Low Conversion (<5 purchases).
    -   **Sinks**:
        -   `activity_logs`: Raw event data stored in PostgreSQL.
        -   `product_stats`: Aggregated stats stored in PostgreSQL.
        -   Console: Alerts printed to logs.

3.  **Storage Layer**:
    -   **Database**: PostgreSQL.

4.  **Orchestration & Reporting Layer**:
    -   **Orchestrator**: Apache Airflow.
    -   **DAG**: `daily_user_segmentation`.
        -   Segments users into "Buyers" vs "Window Shoppers".
        -   Identifies Top 5 most viewed products.
        -   Generates a CSV report of Conversion Rates.

## 🚀 Getting Started

### Prerequisites
-   Docker
-   Docker Compose

### Installation

1.  **Clone the repository**:
    ```bash
    git clone <repository-url>
    cd big-data-mini-project
    ```

2.  **Build and Start Services**:
    This command builds the custom producer image and starts Kafka, Zookeeper, Spark, Postgres, and Airflow.
    ```bash
    docker-compose up -d --build
    ```

### 🏃‍♂️ Running the Pipeline

The system is designed to run automatically upon startup. Here is how to verify each stage:

#### 1. Verify Data Ingestion
The producer starts automatically. Check its logs to see data being generated:
```bash
docker logs big-data-mini-project_producer_1
```
*You should see logs like `Sent: {'user_id': '...', 'event_type': 'view', ...}`.*

#### 2. Verify Stream Processing
The Spark job is submitted automatically by the `spark-master` container (via manual submission in this demo setup, or you can submit it manually if needed).

To check if the Spark job is running and processing data:
```bash
docker exec big-data-mini-project_spark-master_1 ps aux | grep spark-submit
```

To view the job logs (if running in detached mode):
```bash
docker exec big-data-mini-project_spark-master_1 cat /opt/spark/work-dir/spark_job.log
```

**Verify Data in Postgres**:
Check if raw logs are landing in the database:
```bash
docker exec big-data-mini-project_postgres_1 psql -U airflow -d airflow -c "SELECT count(*) FROM activity_logs;"
```

#### 3. Trigger Orchestration (Airflow)
The Airflow Webserver is available at `http://localhost:8081`.
-   **Username**: `admin`
-   **Password**: `admin`

You can trigger the DAG from the UI or via the CLI:
```bash
docker exec big-data-mini-project_airflow-scheduler_1 airflow dags test daily_user_segmentation 2025-12-03
```

#### 4. View Reports
After the Airflow DAG completes, the analytic report is generated in the scheduler container.

Copy the report to your local machine:
```bash
docker cp big-data-mini-project_airflow-scheduler_1:/tmp/analytic_report.csv .
```

View the content:
```bash
cat analytic_report.csv
```

## 🛠 Troubleshooting

-   **Spark fails to connect to Kafka**: Ensure the Spark job is using the internal Docker network address for Kafka (`kafka:29092`), not localhost.
-   **Postgres tables missing**: The tables are created automatically by the Spark job (`activity_logs`) and Airflow (`user_segments`, `top_products`). Ensure the Spark job has processed at least one batch.
-   **Airflow DAG fails**: Check the Airflow scheduler logs. Ensure the connection `postgres_default` is configured (it is set up automatically in this project's instructions, but can be added via CLI if missing).

## 📂 Project Structure

```
big-data-mini-project/
├── airflow/
│   └── dags/
│       └── daily_segmentation_dag.py  # Airflow DAG
├── producers/
│   ├── Dockerfile                     # Producer image definition
│   ├── producer.py                    # Data generator script
│   └── requirements.txt
├── spark/
│   └── stream_processor.py            # Spark Structured Streaming job
├── docker-compose.yml                 # Infrastructure definition
└── README.md                          # Documentation
```