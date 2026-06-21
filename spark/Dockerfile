# Base image ships PySpark + Hadoop 3.3.x.
FROM apache/spark:3.5.1

USER root
# The GCS connector. MUST match Hadoop's major version (hadoop3) or you get
# ClassNotFound/NoSuchMethod at runtime. The "-shaded" jar bundles its own
# deps so they can't conflict with Spark's. Canonical public source:
ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.22.jar \
    /opt/spark/jars/gcs-connector.jar
# ADD from a URL lands the file as 0600 root-only; the container runs as the
# non-root `spark` user, which then can't read it (→ "No FileSystem for scheme
# gs"). Make it world-readable so Spark loads it off the classpath.
RUN chmod 644 /opt/spark/jars/gcs-connector.jar

# Single-node tuning (driver heap) — read automatically by spark-submit.
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# pytest for the silver-transform unit tests. This image doubles as the local
# dev/test environment (the only place with Java + PySpark), so the test runner
# lives here; CI provides its own in Phase 3.
RUN python3 -m pip install --no-cache-dir pytest

USER spark