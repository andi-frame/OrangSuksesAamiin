FROM postgres:16-bookworm
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-venv \
    && python3 -m venv /opt/patroni \
    && /opt/patroni/bin/pip install --no-cache-dir 'patroni[kubernetes,psycopg3]==4.1.5' 'psycopg-binary>=3.2,<4' \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
COPY services/canonical-shard/start.py /usr/local/bin/start-shard
COPY services/canonical-shard/init.sh /usr/local/bin/init-shard
ENV PATH=/opt/patroni/bin:$PATH
ENTRYPOINT ["start-shard"]
USER postgres
