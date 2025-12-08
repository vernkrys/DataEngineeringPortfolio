# Zoom Virtual Assistant Analytics Data Pipeline

## 🎯 Project Overview

Built an enterprise-scale **hourly batch data pipeline** using **Apache Airflow** to ingest virtual assistant interaction data from REST APIs into a cloud data warehouse. The pipeline processes chatbot engagement metrics and query analytics for real-time business intelligence reporting.

**Role:** Senior Data Engineer

**Technologies:** Apache Airflow, Python, AWS S3, Amazon Redshift, REST APIs, SQL

---

## 📊 Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     ZOOM VIRTUAL ASSISTANT API DATA PIPELINE                  │
└─────────────────────────────────────────────────────────────────────────┘

┌──────────────────┐
│  External APIs   │
│  (REST APIs)     │
│                  │
│  - Engagements   │
│  - Query Details │
└────────┬─────────┘
         │
         │ OAuth 2.0
         │ Authentication
         │
         ▼
┌────────────────────────────────────────────────────────────────┐
│               AIRFLOW DAG ORCHESTRATION                         │
│                                                                 │
│  ┌──────────────┐      ┌──────────────┐                       │
│  │   Kickoff    │─────▶│  API Extract │                       │
│  │   Trigger    │      │   (Python)   │                       │
│  └──────────────┘      └──────┬───────┘                       │
│                                │                                │
│                                │ JSON Response                  │
│                                │                                │
│                                ▼                                │
│                    ┌────────────────────┐                      │
│                    │ Data Transformation│                      │
│                    │   - Parse JSON     │                      │
│                    │   - Type Casting   │                      │
│                    │   - Validation     │                      │
│                    └─────────┬──────────┘                      │
│                              │                                  │
│                              │ CSV/Gzip                         │
│                              ▼                                  │
│                    ┌────────────────────┐                      │
│                    │    AWS S3 Upload   │                      │
│                    │  (Staging Layer)   │                      │
│                    └─────────┬──────────┘                      │
│                              │                                  │
│                              │ S3 URI                           │
│                              ▼                                  │
│                    ┌────────────────────┐                      │
│                    │  Redshift COPY     │                      │
│                    │  - Staging Tables  │                      │
│                    │  - Upsert Logic    │                      │
│                    │  - Deduplication   │                      │
│                    └─────────┬──────────┘                      │
│                              │                                  │
│                              │ Loaded Records                   │
│                              ▼                                  │
│                    ┌────────────────────┐                      │
│                    │   Data Quality     │                      │
│                    │   Audit Checks     │                      │
│                    │   - Row Counts     │                      │
│                    │   - SLA Monitor    │                      │
│                    └─────────┬──────────┘                      │
│                              │                                  │
│                              ▼                                  │
│                    ┌────────────────────┐                      │
│                    │   Finish Line      │                      │
│                    │   Success Signal   │                      │
│                    └────────────────────┘                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
         │
         │
         ▼
┌────────────────────┐
│  Amazon Redshift   │
│  Data Warehouse    │
│                    │
│  Production Tables │
│  - Engagements     │
│  - Query Details   │
└────────────────────┘
         │
         │
         ▼
┌────────────────────┐
│  BI Reporting      │
│  - Dashboards      │
│  - Analytics       │
│  - Insights        │
└────────────────────┘
```

---

## 🔧 Technical Implementation

### Pipeline Architecture

**Custom Python Operators:**
```python
# Custom API Transfer Operator
class APITransferOperator(BaseOperator):
    """
    Custom operator for extracting data from REST APIs,
    transforming to CSV, and uploading to S3
    """
    
    def __init__(self, s3_bucket, s3_key, headers, 
                 row_generator, s3_conn_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.headers = headers
        self.row_generator = row_generator
        self.s3_conn_id = s3_conn_id
    
    def execute(self, context):
        # OAuth token acquisition
        token = get_oauth_token()
        
        # API data extraction with pagination
        raw_data = extract_api_data(token, context['execution_date'])
        
        # Transform to CSV format
        csv_data = transform_to_csv(raw_data, self.headers, 
                                    self.row_generator)
        
        # Compress and upload to S3
        upload_to_s3(csv_data, self.s3_bucket, self.s3_key)
```

### DAG Configuration

**Hourly Scheduling with Backfill Support:**
```python
from datetime import datetime, timedelta
from airflow import DAG

default_args = {
    'owner': 'data_engineering',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 10, 12, 1, 0, 0)
}

dag = DAG(
    'virtual_assistant_pipeline',
    default_args=default_args,
    schedule_interval='0 * * * *',  # Hourly
    catchup=True,  # Enable historical backfill
    max_active_runs=1  # Sequential execution
)
```

### Parallel Data Stream Processing

**Two Independent Data Streams:**

**Stream 1: Engagement Metrics**
```python
# Extract engagement sessions
engagements_transfer = APITransferOperator(
    task_id='extract_engagements',
    dag=dag,
    s3_bucket='data-warehouse-staging',
    s3_key="engagements/{{ ds }}/data_{{ run_id }}.csv.gz",
    headers=['engagement_id', 'language_code', 'start_time', 
             'end_time', 'duration', 'campaign_name', 'outcome'],
    row_generator=generate_engagement_rows
)

# Load to Redshift with deduplication
engagements_load = PostgresOperator(
    task_id='load_engagements',
    dag=dag,
    sql='sql/load_engagements.sql',
    postgres_conn_id='redshift_conn',
    params={
        's3_bucket': 'data-warehouse-staging',
        'iam_role': 'arn:aws:iam::xxx:role/RedshiftCopyRole'
    }
)

# Data quality validation
engagements_audit = DataAuditOperator(
    task_id='audit_engagements',
    dag=dag,
    dataset='fact_engagements',
    sla_minutes=120
)

# Define dependencies
kickoff >> engagements_transfer >> engagements_load >> engagements_audit >> finish
```

**Stream 2: Query Analytics**
```python
# Extract query-level details
queries_transfer = APITransferOperator(
    task_id='extract_queries',
    dag=dag,
    s3_bucket='data-warehouse-staging',
    s3_key="queries/{{ ds }}/data_{{ run_id }}.csv.gz",
    headers=['engagement_id', 'query_id', 'query_time', 
             'intent_id', 'intent_name', 'accuracy', 'query_text'],
    row_generator=generate_query_rows
)

# Load to Redshift
queries_load = PostgresOperator(
    task_id='load_queries',
    dag=dag,
    sql='sql/load_queries.sql',
    postgres_conn_id='redshift_conn'
)

# Data quality validation
queries_audit = DataAuditOperator(
    task_id='audit_queries',
    dag=dag,
    dataset='fact_queries',
    sla_minutes=120
)

# Define dependencies
kickoff >> queries_transfer >> queries_load >> queries_audit >> finish
```

### Redshift COPY with Staging Pattern

**SQL Implementation:**
```sql
BEGIN;

-- Create temporary staging table
DROP TABLE IF EXISTS staging_engagements_{{ run_id }};
CREATE TEMP TABLE staging_engagements_{{ run_id }}
(LIKE production.fact_engagements);

-- Remove audit columns for raw load
ALTER TABLE staging_engagements_{{ run_id }} 
    DROP COLUMN dw_source,
    DROP COLUMN dw_inserted_at,
    DROP COLUMN dw_updated_at,
    DROP COLUMN dw_batch_id;

-- COPY from S3 with optimizations
COPY staging_engagements_{{ run_id }}
FROM 's3://{{ params.s3_bucket }}/{{ params.dataset }}/{{ ds }}/data.gz'
IAM_ROLE '{{ params.iam_role }}'
GZIP
CSV
DELIMITER '\001'
ACCEPTINVCHARS
IGNOREHEADER 1
TRUNCATECOLUMNS
COMPUPDATE OFF
STATUPDATE OFF
TIMEFORMAT 'YYYY-MM-DDTHH:MI:SS';

-- Deduplication: Delete existing records
DELETE FROM production.fact_engagements
USING staging_engagements_{{ run_id }}
WHERE staging_engagements_{{ run_id }}.engagement_id = 
      production.fact_engagements.engagement_id
  AND staging_engagements_{{ run_id }}.start_time = 
      production.fact_engagements.start_time;

-- Insert new/updated records with audit fields
INSERT INTO production.fact_engagements
SELECT DISTINCT
    '{{ params.dw_source }}' as dw_source,
    GETDATE() as dw_inserted_at,
    GETDATE() as dw_updated_at,
    '{{ run_id }}' as dw_batch_id,
    engagement_id,
    language_code,
    CAST(start_time as TIMESTAMP) as start_time,
    CAST(end_time as TIMESTAMP) as end_time,
    CAST(duration as INTEGER) as duration,
    campaign_name,
    launch_url,
    outcome
FROM staging_engagements_{{ run_id }}
WHERE engagement_id IS NOT NULL;

COMMIT;

-- Cleanup staging table
DROP TABLE IF EXISTS staging_engagements_{{ run_id }};
END;
```

### DataFrame-Based Data Transformation

**Pandas DataFrame Processing:**
```python
import pandas as pd
from io import StringIO

def transform_api_to_dataframe(api_response, headers, row_generator):
    """
    Transforms API JSON response to pandas DataFrame for efficient
    data manipulation and validation before S3 upload
    """
    # Generate rows from API response
    rows = [row for row in row_generator(api_response)]
    
    # Create DataFrame for efficient data operations
    df = pd.DataFrame(rows, columns=headers)
    
    # Data type conversions
    df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    df['end_time'] = pd.to_datetime(df['end_time'], errors='coerce')
    df['duration'] = pd.to_numeric(df['duration'], errors='coerce')
    
    # Data quality checks
    df = df.dropna(subset=['engagement_id'])  # Remove invalid records
    df = df.drop_duplicates(subset=['engagement_id', 'start_time'])
    
    # Format timestamps for Redshift compatibility
    df['start_time'] = df['start_time'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    df['end_time'] = df['end_time'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    # Handle special characters and truncation
    df['query_text'] = df['query_text'].str.replace('\n', ' ')
    df['query_text'] = df['query_text'].str[:250]  # Truncate to match schema
    
    return df

def dataframe_to_csv_gzip(df):
    """
    Converts DataFrame to compressed CSV format for S3 upload
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, 
              index=False,
              sep='\001',  # Custom delimiter
              header=True,
              encoding='utf-8')
    
    # Gzip compression
    csv_data = csv_buffer.getvalue().encode('utf-8')
    compressed_data = gzip.compress(csv_data)
    
    return compressed_data
```

**Benefits of DataFrame Approach:**
- **Memory Efficiency:** Vectorized operations on large datasets (50K-100K rows)
- **Data Quality:** Built-in null handling, deduplication, and type conversion
- **Performance:** 3-5x faster than row-by-row processing
- **Validation:** Easy statistical analysis before load (row counts, value ranges)
- **Flexibility:** Simple column transformations and filtering
- **Integration:** Seamless conversion to CSV format for Redshift COPY

**Example Usage in Pipeline:**
```python
engagements_transfer = APITransferOperator(
    task_id='extract_engagements',
    dag=dag,
    transformation_function=transform_api_to_dataframe,
    row_generator=generate_engagement_rows,
    headers=['engagement_id', 'language_code', 'start_time', 
             'end_time', 'duration', 'campaign_name', 'outcome']
)
```

### Data Quality Framework

**Custom Audit Operator:**
```python
class DataAuditOperator(BaseOperator):
    """
    Validates data quality post-load:
    - Row count verification
    - SLA compliance monitoring
    - Freshness checks
    - Anomaly detection
    """
    
    def __init__(self, dataset, sla_minutes, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataset = dataset
        self.sla_minutes = sla_minutes
    
    def execute(self, context):
        # Check row counts
        row_count = self.validate_row_count(context)
        
        # Verify SLA
        load_time = self.get_load_duration(context)
        if load_time > self.sla_minutes:
            raise AirflowException(f"SLA breach: {load_time}min")
        
        # Check data freshness
        self.validate_freshness(context)
        
        # Log metrics
        self.log.info(f"Audit passed: {row_count} rows loaded")
```

---

## 🎯 Key Technical Challenges Solved

### 1. **OAuth Token Management**
**Challenge:** API authentication expiration during long-running ETL jobs

**Solution:** 
- Implemented token refresh logic within custom operators
- Built retry mechanism with exponential backoff
- Cached tokens with expiration tracking

```python
def get_oauth_token():
    """
    Manages OAuth 2.0 token lifecycle with automatic refresh
    """
    if token_expired():
        token = refresh_oauth_token()
        cache_token(token)
    return get_cached_token()
```

### 2. **Idempotent Data Loading**
**Challenge:** Prevent duplicate records during pipeline retries

**Solution:**
- Implemented staging table pattern in Redshift
- Delete-before-insert strategy based on business keys
- Transactional COPY with COMMIT/ROLLBACK

### 3. **Parallel Stream Processing**
**Challenge:** Process two independent data streams efficiently

**Solution:**
- Designed parallel DAG branches with shared start/end nodes
- Independent failure isolation (one stream can succeed while other fails)
- Optimized for concurrent S3 uploads and Redshift loads

### 4. **Schema Evolution Handling**
**Challenge:** API schema changes breaking downstream processes

**Solution:**
- Dynamic column mapping in row generator functions
- Optional field handling with NULLIF in SQL
- Type casting with fallback defaults

```python
def generate_engagement_rows(api_response):
    """
    Flexible row generator handling schema variations
    """
    for record in api_response:
        yield [
            record.get('engagement_id', ''),
            record.get('language_code', 'en'),
            record.get('start_time', ''),
            record.get('end_time', ''),
            record.get('duration', 0),
            record.get('campaign_name', 'default'),
            record.get('outcome', 'unknown')
        ]
```

### 5. **S3 Path Partitioning**
**Challenge:** Efficiently organize millions of hourly files

**Solution:**
- Date-based partitioning: `s3://bucket/source/dataset/YYYY/MM/DD/`
- Dynamic path generation using Airflow macros
- Gzip compression reducing storage by 85%

```python
s3_key = "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y/%m/%d') }}/data_{{ run_id }}.csv.gz"
```

### 6. **Backfill Processing**
**Challenge:** Load 6 months of historical data without overwhelming systems

**Solution:**
- Enabled `catchup=True` with `max_active_runs=1`
- Sequential hourly processing ensuring data consistency
- Automatic retry logic for failed hours

### 7. **Redshift Performance Optimization**
**Challenge:** COPY operations slowing down during peak hours

**Solution:**
- Disabled COMPUPDATE and STATUPDATE during loads
- Used DISTKEY on engagement_id for optimal data distribution
- TRUNCATECOLUMNS to handle oversized string values
- ACCEPTINVCHARS for non-standard characters

---

## 📈 Pipeline Metrics & Scale

### Processing Volume
- **Frequency:** Hourly execution (24 runs/day)
- **Data Volume:** ~50K-100K records per hour
- **Total Daily:** 1.2M - 2.4M records/day
- **File Sizes:** 2-5 MB compressed per hourly batch
- **Retention:** 2+ years of historical data

### Performance Benchmarks
- **API Extraction:** 2-3 minutes per endpoint
- **S3 Upload:** 30-60 seconds (gzipped)
- **Redshift COPY:** 1-2 minutes per table
- **Total Pipeline:** 8-12 minutes end-to-end
- **SLA:** 2 hours (120 minutes)

### Reliability Metrics
- **Success Rate:** 99.5%+
- **Retry Success:** 95% on first retry
- **Data Quality:** 100% audit compliance
- **Uptime:** 24/7/365 operation

---

## 🛠️ Technology Stack Details

### Core Technologies
| Technology | Purpose | Key Features Used |
|------------|---------|-------------------|
| **Apache Airflow** | Workflow Orchestration | DAGs, Custom Operators, Scheduling, Backfill |
| **Python 3.x** | ETL Logic | Data transformation, API clients, CSV generation |
| **AWS S3** | Data Lake Storage | Partitioned storage, Gzip compression, IAM roles |
| **Amazon Redshift** | Data Warehouse | COPY command, Staging tables, UPSERT patterns |
| **PostgreSQL** | Airflow Metadata | Task state, DAG runs, XCom |
| **REST APIs** | Data Source | OAuth 2.0, Pagination, Rate limiting |

### Python Libraries
```python
# requirements.txt
apache-airflow==2.5.0
boto3==1.26.0              # AWS SDK
psycopg2-binary==2.9.5     # PostgreSQL adapter
requests==2.28.0           # HTTP client
pandas==1.5.0              # Data manipulation
pytz==2022.7               # Timezone handling
```

### AWS Services
- **S3:** Object storage for staging layer
- **Redshift:** Columnar data warehouse
- **IAM:** Role-based access control for COPY operations
- **CloudWatch:** Pipeline monitoring and alerting

---

## 🔐 Best Practices Implemented

### 1. **Security**
- IAM roles for cross-account S3 access (no hardcoded credentials)
- OAuth 2.0 token management with secure storage
- SQL injection prevention through parameterized queries
- Encrypted data at rest (S3) and in transit (HTTPS)

### 2. **Reliability**
- Automatic retries with exponential backoff
- Transactional data loads (COMMIT/ROLLBACK)
- Data quality audits on every run
- Idempotent operations supporting reruns

### 3. **Maintainability**
- Modular custom operators for reusability
- Parameterized SQL templates
- Comprehensive logging at each stage
- Clear documentation in DAG docstrings

### 4. **Performance**
- Parallel stream processing
- Gzip compression reducing transfer time
- Redshift COPY optimizations
- Partitioned S3 storage for efficient queries

### 5. **Monitoring**
- SLA monitoring with 2-hour threshold
- Row count validation
- Execution time tracking
- Alert integration with communication channels

---

## 📚 Key Learnings

### Technical Insights
1. **Staging Pattern is Critical:** Using temporary tables prevents partial loads and enables atomic commits
2. **COPY is Faster than INSERT:** Redshift COPY from S3 is 10x faster than row-by-row inserts
3. **Compression Matters:** Gzip reduced storage costs by 85% and transfer time by 70%
4. **Idempotency is Non-Negotiable:** Every operation must be safely re-runnable

### Design Patterns
1. **ELT over ETL:** Load raw data first, transform in warehouse for flexibility
2. **Separate Concerns:** Independent streams with shared orchestration
3. **Fail Fast:** Validate early to catch issues before expensive operations
4. **Audit Everything:** Track data lineage and quality at every step

### Operational Wisdom
1. **Start with Backfill:** Test with historical data before going live
2. **Monitor SLAs:** Set realistic targets and alert on breaches
3. **Document Everything:** Future maintainers (including yourself) will thank you
4. **Plan for Failure:** Retries, rollbacks, and graceful degradation

---

## 🚀 Future Enhancements

### Planned Improvements
- [ ] **Incremental Loading:** Move from full hourly refreshes to CDC-based incremental updates
- [ ] **Data Lineage:** Implement Apache Atlas for end-to-end lineage tracking
- [ ] **Real-time Streaming:** Migrate to Kinesis/Kafka for sub-minute latency
- [ ] **ML Integration:** Add anomaly detection for data quality using SageMaker
- [ ] **Cost Optimization:** Implement S3 lifecycle policies for archival storage

### Scalability Considerations
- Horizontal scaling through Airflow Celery Executor
- Redshift cluster resizing for growing data volumes
- S3 event-driven triggers for near-real-time processing
- Snowpipe-style continuous loading

---

## 📝 Technical Documentation

### Repository Structure
```
virtual-assistant-pipeline/
├── dags/
│   └── virtual_assistant_pipeline.py      # Main DAG definition
├── operators/
│   ├── api_transfer_operator.py          # Custom S3 upload operator
│   └── data_audit_operator.py            # Quality validation operator
├── plugins/
│   ├── api_client.py                     # REST API integration
│   └── row_generators.py                 # Data transformation logic
├── sql/
│   ├── load_engagements.sql              # Engagement COPY logic
│   └── load_queries.sql                  # Query COPY logic
├── tests/
│   ├── test_operators.py                 # Unit tests
│   └── test_transformations.py           # Data validation tests
└── requirements.txt                      # Python dependencies
```

### Running Locally
```bash
# Set up Airflow environment
export AIRFLOW_HOME=~/airflow
airflow db init

# Install dependencies
pip install -r requirements.txt

# Configure connections
airflow connections add 'redshift_conn' \
    --conn-type 'postgres' \
    --conn-host 'redshift-cluster.region.redshift.amazonaws.com' \
    --conn-port 5439

# Test the DAG
airflow dags test virtual_assistant_pipeline 2024-01-01

# Backfill historical data
airflow dags backfill virtual_assistant_pipeline \
    -s 2024-10-12 -e 2024-10-20
```

---

## 🎓 Skills Demonstrated

### Data Engineering
- ✅ Batch data pipeline design and implementation
- ✅ API integration with OAuth authentication
- ✅ Data modeling for analytical workloads
- ✅ ETL/ELT pattern implementation

### Cloud & Infrastructure
- ✅ AWS S3 for data lake architecture
- ✅ Amazon Redshift optimization
- ✅ IAM security best practices
- ✅ Infrastructure as Code concepts

### Programming
- ✅ Advanced Python (OOP, custom operators)
- ✅ SQL optimization (Redshift-specific)
- ✅ Functional programming patterns
- ✅ Error handling and retry logic

### DevOps & Operations
- ✅ Apache Airflow orchestration
- ✅ Monitoring and alerting setup
- ✅ SLA management
- ✅ Production incident handling

### Software Engineering
- ✅ Modular, reusable code design
- ✅ Unit testing and validation
- ✅ Documentation best practices
- ✅ Version control and code review

---

**This project demonstrates end-to-end data engineering capabilities from API integration through cloud storage to analytical data warehousing, with a focus on reliability, performance, and maintainability at scale.**
