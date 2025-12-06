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

#### 1. Verify Data Ingestion
The producer starts automatically. Check its logs to see data being generated:
```bash
docker logs big-data-mini-project-producer-1 --tail 10
```
*You should see logs like `Sent: {'user_id': '...', 'event_type': 'view', ...}`.*

#### 2. Submit and Verify Spark Stream Processing

**Create the Ivy cache directory** (required for downloading Maven dependencies):
```bash
docker exec -u root big-data-mini-project-spark-master-1 bash -c "mkdir -p /home/spark/.ivy2/cache && chown -R spark:spark /home/spark/.ivy2"
```

**Submit the Spark streaming job**:
```bash
docker exec big-data-mini-project-spark-master-1 bash -c "nohup /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
  /opt/spark/work-dir/stream_processor.py > /opt/spark/work-dir/spark_job.log 2>&1 &"
```

**Verify the Spark job is running**:
```bash
docker exec big-data-mini-project-spark-master-1 ps aux | grep spark-submit
```

**View the Spark job logs**:
```bash
docker exec big-data-mini-project-spark-master-1 tail -30 /opt/spark/work-dir/spark_job.log
```

**Verify data is being written to PostgreSQL**:
```bash
docker exec big-data-mini-project-postgres-1 psql -U airflow -d airflow -c "SELECT count(*) FROM activity_logs;"

docker exec big-data-mini-project-postgres-1 psql -U airflow -d airflow -c "SELECT * FROM activity_logs LIMIT 5;"
```

#### 3. Configure Airflow PostgreSQL Connection

Before triggering the DAG, configure the PostgreSQL connection:
```bash
docker exec big-data-mini-project-airflow-scheduler-1 airflow connections add postgres_default \
  --conn-type postgres \
  --conn-host postgres \
  --conn-schema airflow \
  --conn-login airflow \
  --conn-password airflow \
  --conn-port 5432
```

**Verify the connection**:
```bash
docker exec big-data-mini-project-airflow-scheduler-1 airflow connections get postgres_default
```

#### 4. Trigger Airflow DAG

**Access the Airflow Webserver** at `http://localhost:8081`:
-   **Username**: `admin`
-   **Password**: `admin`

**Trigger the DAG from the UI** by clicking the "Trigger DAG" button on the `daily_user_segmentation` DAG.

**Or trigger via CLI**:
```bash
docker exec big-data-mini-project-airflow-scheduler-1 airflow dags trigger daily_user_segmentation
```

**Check DAG run status**:
```bash
docker exec big-data-mini-project-airflow-scheduler-1 airflow dags list-runs -d daily_user_segmentation -o table
```

#### 5. View Reports and Results

**Copy the analytic report to your local machine**:
```bash
docker cp big-data-mini-project-airflow-scheduler-1:/tmp/analytic_report.csv .
```

**View the report**:
```bash
cat analytic_report.csv
```

**Verify user segmentation results**:
```bash
docker exec big-data-mini-project-postgres-1 psql -U airflow -d airflow -c \
  "SELECT segment, COUNT(*) as count FROM user_segments GROUP BY segment;"
```

**View top 5 products**:
```bash
docker exec big-data-mini-project-postgres-1 psql -U airflow -d airflow -c \
  "SELECT * FROM top_products ORDER BY total_views DESC LIMIT 5;"
```

## 🛠 Troubleshooting

-   **Producer image build fails**: Ensure the `docker-compose.yml` uses `build` context instead of trying to pull `local/producer` image.
-   **Spark job fails to start**: Create the `/home/spark/.ivy2/cache` directory with proper permissions (see Step 2 above).
-   **Spark can't download packages**: Ensure the Ivy cache directory exists and has write permissions for the `spark` user.
-   **Airflow DAG fails with connection error**: Create the `postgres_default` connection using the command in Step 3.
-   **activity_logs table doesn't exist**: The Spark streaming job must be running to create and populate this table. Check Spark job logs.
-   **Spark fails to connect to Kafka**: Ensure the Spark job is using the internal Docker network address for Kafka (`kafka:29092`), not localhost.

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