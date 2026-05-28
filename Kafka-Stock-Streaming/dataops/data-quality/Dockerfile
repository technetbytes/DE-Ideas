FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    soda-core-postgres==3.3.3 \
    soda-core-scientific==3.3.3

COPY soda-configuration.yml /app/
COPY soda-checks.yml /app/
COPY run-scan.sh /app/

RUN chmod +x /app/run-scan.sh

CMD ["/app/run-scan.sh"]
