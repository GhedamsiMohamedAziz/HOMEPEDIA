# One image for every Python service (spark master/worker, jupyter, streamlit, cli) so the
# driver and executors share the exact same Python + PySpark version as local dev (3.11).
FROM python:3.11-slim

ENV PYTHONPATH=/opt/homepedia PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 \
    SPARK_HOME=/opt/spark SPARK_NO_DAEMONIZE=true

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless curl procps \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 1000 homepedia

COPY infrastructure/docker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && ln -s "$(python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')" /opt/spark

WORKDIR /opt/homepedia
COPY --chown=homepedia:homepedia . /opt/homepedia
USER homepedia
