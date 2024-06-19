# BEES
The goal of this test is to assess your skills in consuming data from an API, transforming and persisting it  into a data lake following the medallion architecture with three layers: raw data, curated data  partitioned by location, and an analytical aggregated layer.


Airflow Data Pipeline with Docker
This project sets up a data pipeline using Apache Airflow, Docker, and the Celery Executor. The pipeline extracts data from an API, transforms it, and saves it into different storage layers (bronze, silver, and gold).

## Project Structure
- dags/: Contains Airflow's Directed Acyclic Graphs (DAGs).
- logs/: Directory for storing Airflow logs.
- plugins/: Directory for custom Airflow plugins.
- bronze/: Directory for raw data.
- silver/: Directory for transformed data.
- gold/: Directory for aggregated data.
- docker-compose.yml: Docker Compose configuration file.
- Dockerfile: Docker configuration file for building the Airflow image.

## Prerequisites
- Docker and Docker Compose installed.
- requirements.txt


## Configuration

### 1. Clone the Repository

Clone this repository to your local machine:
```bash
git clone https://github.com/seu-usuario/airflow-docker.git
cd airflow-docker
```
### 2. Install Requirements

Configure SSH Keys
Ensure that your SSH keys are configured correctly to allow push and pull on GitHub.

### 3. Configure Environment Variables
Add your environment variables to the .env file if necessary. Here's an example:


AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
AIRFLOW__CORE__FERNET_KEY=<fernet_key>

### 4.Build and Start Services

Build and start Docker containers:

```bash
docker-compose up --build
```

### 5.Initialize Airflow
Initialize the Airflow database and create the admin user:

```bash
docker-compose run --rm airflow-init
```

### 6.Access Airflow Web Interface
Access the Airflow web interface at http://localhost:8080 and log in with the following credentials:

Username: admin
Password: admin

### DAG Structure

The data pipeline is defined in the DAG data_pipeline and consists of the following tasks:

get_data_to_bronze: Extracts data from an API and saves it in the bronze directory.
transform_2_silver: Transforms raw data and saves it in the silver directory.
gold_view: Aggregates transformed data and saves it in the gold directory.


Contributions are welcome! Feel free to open issues and pull requests.








