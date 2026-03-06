# Start with the official Apache Airflow Linux image
FROM apache/airflow:2.8.1-python3.10

# Switch to the airflow user to safely install Python packages
USER airflow

# Copy your strict 4-line requirements file into the Linux container
COPY requirements.txt /requirements.txt

# Install your Data Engineering libraries cleanly
RUN pip install --no-cache-dir -r /requirements.txt