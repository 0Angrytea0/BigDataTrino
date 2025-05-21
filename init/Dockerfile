FROM clickhouse/clickhouse-server:latest
COPY ./init/ /docker-entrypoint-initdb.d/
RUN chmod +x /docker-entrypoint-initdb.d/*.sh

ENV CLICKHOUSE_DB=mydb \
    CLICKHOUSE_USER=default
