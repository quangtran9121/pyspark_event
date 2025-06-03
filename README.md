# PySpark Data Pipeline Project

![Xgame](https://media.licdn.com/dms/image/v2/D560BAQEcDLnSnoSYag/company-logo_200_200/company-logo_200_200/0/1711092343789/xgame_studio_logo?e=2147483647&v=beta&t=gN4wVJof6PTLcYCTnGGXhNzpCaidlIhzPgM_NxpSsaM)

This project demonstrates a data pipeline that reads data from Kafka, processes and transforms it using PySpark, and then saves:

- Raw data to ClickHouse,
- Dimension/Fact data to PostgreSQL.

Using Alembic and SQLAlchemy to manage database schema migrations and connections. Below is a comprehensive guide to setting up and running the project.

## Table of Contents

- [PySpark Data Pipeline Project](#pyspark-data-pipeline-project)
  - [Table of Contents](#table-of-contents)
  - [Architecture Overview](#architecture-overview)
  - [Tech Stack](#tech-stack)
  - [Prerequisites](#prerequisites)
  - [Project Structure](#project-structure)
  - [Installation \& Setup](#installation--setup)
    - [Clone the repository:](#clone-the-repository)
    - [Create a virtual environment (optional but recommended):](#create-a-virtual-environment-optional-but-recommended)
    - [Install dependencies:](#install-dependencies)
    - [Configure environment variables (Update conf/config.yaml and conf/.env):](#configure-environment-variables-update-confconfigyaml-and-confenv)
  - [Usage](#usage)
    - [First setup:](#first-setup)
    - [Running the PySpark Job:](#running-the-pyspark-job)
      - [Local mode:](#local-mode)
      - [Cluster mode:](#cluster-mode)
    - [Reading from Kafka:](#reading-from-kafka)
  - [Database Migrations](#database-migrations)
    - [Generate a new migration:](#generate-a-new-migration)
    - [Apply migrations:](#apply-migrations)
  - [Create table Clickhouse:](#create-table-clickhouse)

## Architecture Overview

```bash
Kafka Topic --> PySpark Stream --> Data Transformations -->
    +-- Raw data --> ClickHouse
    |
    +-- Processed Dims/Facts --> PostgreSQL
```

- Kafka: The pipeline reads messages from a Kafka topic.
- PySpark: Processes streaming data (or batch data) in a distributed manner.
- ClickHouse: Stores raw data for auditing and high-performance analytics.
- PostgreSQL: Stores curated data (dimensions & facts).
- Alembic & SQLAlchemy: Handles schema migrations and database interactions.

## Tech Stack

- **Language:** Python 3.10
- **Framework:** PySpark (Apache Spark)
- **Messaging:** Apache Kafka
- **Databases:**
  - **ClickHouse:** for raw data storage
  - **PostgreSQL:** for dimensional and fact tables
- **Database Tools:**
  - **Alembic:** for migrations
  - **SQLAlchemy:** for ORM and connections
- **Operating System:** Linux
- **Java:** OpenJDK 8 (for Spark)

## Prerequisites

Make sure environment meets the following requirements:

- Miniconda
- Python 3.10 installed on Linux
- Java 8 (OpenJDK 8 or Oracle JDK 8)
- Apache Spark 3+ (with PySpark)
- Kafka cluster accessible
- ClickHouse instance running and accessible
- PostgreSQL instance running and accessible

## Project Structure

- **common/:** Contains utility functions, shared classes, and logging configuration. This is where you can place helper methods that are reused across the project, along with any setup for loggers or other cross-cutting concerns.
- **conf/:** Configuration files (e.g., database URIs, Kafka topics).
- **events/:** Defines the schema for Spark DataFrames and other event-related structures. This folder can include case classes or schema definitions that standardize how events are processed in the Spark pipeline.
- **logs/:** Dedicated to logging output. This folder might hold log files generated during runtime, making it easier to debug issues or monitor the application’s behavior.
- **migrations/:** Contains Alembic configuration files and migration scripts used for managing PostgreSQL schema changes. It ensures that database schema changes are version-controlled and applied consistently across environments.
- **models/:** Houses data models and representations that application uses.
  - **game_specific/:** Contains models or classes specific to game-related data, encapsulating logic or structures unique to that domain. Each game has its own schema.
  - **common_dim.py:** Likely defines common dimensions or shared model attributes used across multiple datasets or tables.
- **processors/:** Contains the code responsible for processing data. This includes functions and classes that perform data transformations, aggregations, or any other processing logic within the Spark application. Each class processes one or multiple event types.
- **resources/:** Holds ancillary resources needed by the project.
  - **checkpoint/:** Used to store Spark checkpoint data, which helps in recovering state in case of failures during streaming or batch processing.
  - **driver/:** Additional resource files that support the main Spark driver program.
- **scripts/:** Contains auxiliary scripts that might be used for tasks such as data ingestion, job scheduling, or other automation tasks. These scripts can be executed as standalone programs for initial execution.
- **tests/:** Contains unit and integration tests to ensure code correctness and to validate that the application behaves as expected. This folder helps in maintaining code quality through automated testing.
- **requirements.txt:** Lists all the Python dependencies required by the project. This file is used to recreate the project’s environment consistently across different setups.

## Installation & Setup

### Clone the repository:

```bash
git clone https://github.com/your-organization/pyspark-event.git
cd pyspark-event
```

### Create a virtual environment (optional but recommended):

```bash
conda create -n <env_name> python==3.10.10
conda activate <env_name>
```

### Install dependencies:

```bash
pip install -r requirements.txt
```

If you have a local Spark installation, ensure pyspark matches the version of Spark installed.

### Configure environment variables (Update conf/config.yaml and conf/.env):

Example:

```bash
event:
  app_name: GameEventsETL
  kafka_bootstrap_servers: 207.148.76.77:9092
  kafka_topic_pattern: topic_game*
  num_process: 1000000
  num_partition: 10
  batch_duration: 10
```

## Usage

### First setup:

```bash
python scripts/insert_dim_date.py
python scripts/initial_data.py
```

### Running the PySpark Job:

#### Local mode:

```bash
spark-submit --master local[*] main.py
```

#### Cluster mode:

```bash
spark-submit \
  --master spark://<spark-master-url>:7077 \
  --deploy-mode cluster \
  main.py

```

- Replace <spark-master-url> with Spark master node IP or hostname.

### Reading from Kafka:

Depending on main.py configuration, the script will:

- Read data from the Kafka subscribe pattern topic configured (kafka_topic_pattern in config.yaml).
- Transform data (filtering, aggregations, etc.).
- Write raw data to ClickHouse.
- Write dimension/fact tables to PostgreSQL.

## Database Migrations

Using Alembic for PostgreSQL schema migrations. This ensures that dimension/fact tables are created and updated in a controlled manner.

When adding a new game or app for data processing, you must create a new schema in PostgreSQL for that game or app.
After creating a new game or app, add a new line to models/**init**.py

```bash
from .game_specific.game_... import *
```

### Generate a new migration:

```bash
alembic revision --autogenerate -m "<commit_migration>"
```

This will create a new file in migrations/versions/.

### Apply migrations:

```bash
alembic upgrade head
```

## Create table Clickhouse:

```bash
CREATE TABLE com_... (
    sequence_id UInt64,
    event_name String,
    event_data Nullable(String),
    user_properties Nullable(String),
    user_id String,
    app_version Nullable(String),
    _ts UInt64,
    batch_id UInt64,

    ts DateTime64(3) MATERIALIZED toDateTime64(_ts / 1000, 3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (user_id, sequence_id, _ts)
--PRIMARY KEY (sequenceId, _ts)
SETTINGS index_granularity = 8192;
```
