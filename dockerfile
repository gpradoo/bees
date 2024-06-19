# Use the official Airflow image from Apache
FROM apache/airflow:2.6.3

# Install any dependencies you need, including a compatible version of pyarrow
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       default-libmysqlclient-dev \
       libpq-dev \
       apt-utils \
       libsasl2-dev \
       libsasl2-modules \
       libldap2-dev \
       python3-dev \
       gcc \
       unixodbc-dev \
       git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements.txt to the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code and tests to the container
COPY dags/ dags/
COPY tests/ tests/

# Define the entrypoint to run the tests
CMD ["python", "-m", "unittest", "discover", "-s", "tests"]

