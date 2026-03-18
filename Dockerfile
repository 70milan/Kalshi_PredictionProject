# Use the official Apache Spark image
FROM apache/spark:3.5.1

USER root

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.txt

# Create project directories and handle permissions
WORKDIR /app
RUN mkdir -p data/bronze data/silver data/gold && \
    mkdir -p /opt/spark/work-dir/.ivy2 && \
    chown -R 185:185 /app && \
    chown -R 185:185 /opt/spark/work-dir

# Set Ivy cache to the writable directory
ENV IVY_PACKAGE_DIR=/opt/spark/work-dir/.ivy2

# Switch back to non-root user (Spark default is 185)
USER 185
