# Custom Airflow image for the ETRM platform.
# Adds Java 17 (needed by Spark 4), the project's Python dependencies, and a
# matching PySpark client so SparkSubmitOperator can submit jobs to the cluster.
FROM apache/airflow:3.0.4

# --- System packages (Java) must be installed as root ---
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# JAVA_HOME for Debian Bookworm (base image of apache/airflow:3.0.4).
# If the build fails here, run `update-alternatives --list java` in the image
# to confirm the exact path and adjust it.
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# --- Python dependencies must be installed as the airflow user ---
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Point spark-submit at the pip-installed PySpark (matches the Spark 4 cluster).
ENV SPARK_HOME=/home/airflow/.local/lib/python3.12/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
