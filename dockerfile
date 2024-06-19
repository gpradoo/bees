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

# Install pyarrow with the correct version
RUN pip install --upgrade pip
RUN pip install pyarrow==10.0.1
RUN pip install apache-airflow
RUN pip install datetime
RUN pip install requests
RUN pip install pandas


# Any other dependencies your project requires
# RUN pip install <other-dependencies>

