FROM apache/airflow:2.8.1-python3.11

# Switch to root to install system packages
USER root

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        wget \
        curl \
        default-jdk \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Copy custom configurations
COPY --chown=airflow:root airflow.cfg /opt/airflow/airflow.cfg

# Create necessary directories
RUN mkdir -p /opt/airflow/dags \
    && mkdir -p /opt/airflow/logs \
    && mkdir -p /opt/airflow/plugins \
    && mkdir -p /opt/airflow/scripts

# Copy DAGs and plugins
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root plugins/ /opt/airflow/plugins/
COPY --chown=airflow:root scripts/ /opt/airflow/scripts/

# Set the working directory
WORKDIR /opt/airflow

# Expose the webserver port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=5 \
    CMD curl -f http://localhost:8080/health || exit 1