# Use the official Apache Spark image
FROM apache/spark:3.5.1

USER root

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download Delta Lake JARs at build time so Ivy resolution
# never happens at runtime (fixes JAVA_GATEWAY_EXITED crashes)
COPY cache_delta_jars.py /tmp/cache_delta_jars.py
RUN mkdir -p /opt/spark/work-dir/.ivy2 && python3 /tmp/cache_delta_jars.py && rm /tmp/cache_delta_jars.py

# Create project directories and handle permissions
WORKDIR /app
RUN mkdir -p data/bronze data/silver data/gold && \
    chown -R 185:185 /app && \
    chown -R 185:185 /opt/spark/work-dir

# Ivy cache is pre-populated — set env so runtime Spark finds it
ENV IVY_PACKAGE_DIR=/opt/spark/work-dir/.ivy2

# Stay as root for volume-mounted writes on Windows/local dev
