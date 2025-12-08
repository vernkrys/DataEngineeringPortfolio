# Contact Center Analytics Batch Pipeline

## Project Overview

Built and maintained a production-scale batch data orchestration system for a contact center analytics platform, processing millions of customer interaction events daily. The system leveraged Apache Airflow to orchestrate distributed PySpark jobs on AWS EMR, transforming raw JSON event streams into structured analytical datasets for business intelligence reporting.

**Role:** Senior Data Engineer  

---

## Architecture

### Technology Stack
- **Orchestration**: Apache Airflow with custom YAML-based DAG configuration framework
- **Processing**: AWS EMR 7.x with PySpark on Graviton instances (ARM architecture)
- **Storage**: Amazon S3 (data lake) → Amazon Redshift (data warehouse)
- **Data Format**: JSON → Parquet → Redshift tables
- **Infrastructure**: Multi-environment deployment (beta/prod) with IAM role-based security

### Data Flow

```
Contact Center Events (JSON)
    ↓
S3 Raw Data Lake (Hourly Partitions)
    ↓
S3 Sensor (Airflow) - Detects New Data
    ↓
EMR Spark Cluster (Auto-provisioned)
    ↓
Schema Enforcement & Type Casting
    ↓
S3 Parquet Files (Compressed & Optimized)
    ↓
Redshift COPY Command (IAM-secured)
    ↓
Business Intelligence Tables
    ↓
Data Quality Checks & Volume Monitoring
```

## Scale of Data

### Volume Metrics
- **Processing Frequency**: Hourly batch runs (20 minutes past each hour)
- **Data Streams**: 2 primary event types (Contact Trace Records + Agent Events)
- **Partition Strategy**: Year/Month/Day/Hour directory structure
- **Cluster Configuration**: 
  - 1 Master node (M5.4xlarge Graviton)
  - 2 Core nodes (M5.4xlarge Graviton)
  - 356 Spark executors with 19GB memory each
  - 5 cores per executor
- **Data Retention**: Multi-year historical data across production S3 buckets

### Complex Schema Management
- **Nested JSON Structures**: 4-5 levels deep with arrays and structs
- **Fields per Dataset**: 70+ flattened columns from nested JSON
- **Dynamic Attributes**: Flexible key-value pair handling for custom contact attributes
- **Schema Evolution**: Managed backward compatibility across multiple data versions

## Technical Challenges Solved

### 1. **Schema Complexity & Type Safety**
**Challenge**: Contact center systems produce deeply nested JSON with inconsistent structure and complex hierarchies (agent status snapshots, routing profiles, contact queues).

**Solution**: 
- Implemented explicit PySpark schema definitions with proper type casting
- Flattened nested structures while preserving relationship context
- Applied DROPMALFORMED mode to handle malformed records gracefully
- Converted timestamps to proper TimestampType for accurate time-series analysis

### 2. **Data Quality & Monitoring**
**Challenge**: Ensuring data completeness and detecting pipeline failures or data volume anomalies.

**Solution**:
- Built custom Airflow audit tasks with SLA monitoring (120-minute threshold)
- Implemented volume check integrity tasks comparing current vs. historical data
- Created percentage-based anomaly detection (95th percentile lower bound)
- Configured environment-specific Slack alerting for production issues

### 3. **Multi-Environment Configuration Management**
**Challenge**: Managing identical workflows across beta and production with different S3 buckets, Redshift schemas, IAM roles, and alert channels.

**Solution**:
- Created YAML-based configuration framework with environment inheritance
- Used dynamic parameter resolution patterns for environment-specific values
- Implemented loop-based task generation for processing multiple entity types
- Maintained single DAG definition serving both environments

### 4. **Resource Optimization**
**Challenge**: Processing large volumes cost-effectively while maintaining performance SLAs.

**Solution**:
- Adopted ARM-based Graviton instances for 20% cost reduction
- Configured auto-terminating EMR clusters (terminate on completion)
- Optimized Spark partitioning (20 shuffle partitions) for hourly data volumes
- Used S3 `_SUCCESS` markers for reliable completion detection

### 5. **Dependency Management & Orchestration**
**Challenge**: Complex multi-step workflow with S3 sensors, EMR provisioning, Spark jobs, Redshift loads, and validation checks.

**Solution**:
- Implemented sequential task dependencies with proper error handling
- Used S3 sensors with wildcard matching for flexible file detection
- Configured bootstrap actions for EMR environment preparation
- Set 3 automatic retries with exponential backoff for transient failures

### 6. **Data Warehouse Integration**
**Challenge**: Efficiently loading transformed data into Redshift while maintaining security and access controls.

**Solution**:
- Used Redshift COPY command with IAM role authentication
- Implemented parameterized SQL templates for dynamic schema/table references
- Maintained separate development and production environments
- Applied proper access control with cross-account IAM role assumption

## Operational Excellence

### Monitoring & Alerting
- Real-time Slack notifications for pipeline failures
- SLA-based alerting (120-minute threshold per batch)
- Data volume anomaly detection with historical comparison
- Audit trail tracking for every data load

### Security & Compliance
- IAM role-based authentication (no hardcoded credentials)
- Data classification tagging (critical data assets)
- Resource ownership tracking via internal governance systems
- Cross-account access controls for multi-account architecture

### Performance Characteristics
- **Latency**: ~30-45 minutes from raw data arrival to Redshift availability
- **Reliability**: 3 automatic retries with failure isolation
- **Scalability**: Horizontally scalable with EMR fleet configuration
- **Cost Optimization**: Auto-terminating clusters + Graviton architecture

## Business Impact

This pipeline enabled:
- Real-time contact center performance dashboards
- Agent productivity analytics and workforce optimization
- Customer experience metrics and SLA tracking
- Historical trend analysis for capacity planning
- Data-driven decision making for support operations

## Key Takeaways

**What I Learned**:
- Designing resilient batch workflows with proper error handling and monitoring
- Managing schema evolution in production data pipelines
- Balancing cost optimization with performance requirements
- Implementing environment-agnostic configuration patterns
- Building observable systems with comprehensive logging and alerting

**Technologies Demonstrated**:
- Apache Airflow (custom DAG frameworks, sensors, operators)
- PySpark (schema enforcement, transformations, optimizations)
- AWS Services (EMR, S3, Redshift, IAM)
- Infrastructure as Code (YAML-based configuration)
- Data Engineering Best Practices (partitioning, monitoring, security)
