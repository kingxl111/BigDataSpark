FROM bitnami/spark:3.5
USER root

RUN apt-get update && apt-get install -y netcat-openbsd python3-pip
RUN pip3 install psycopg2-binary

RUN mkdir -p /app && chmod -R 777 /app

RUN groupadd -r spark -g 1001 && \
    useradd -u 1001 -r -g spark -m -d /opt/bitnami/spark -s /sbin/nologin spark && \
    chown -R spark:spark /app

USER 1001