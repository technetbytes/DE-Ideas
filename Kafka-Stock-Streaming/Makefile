.PHONY: up down restart logs status producer-logs consumer-logs clean health db-shell kafka-shell topics

# ─── Core Commands ────────────────────────────────────────────────────────────
up:
	docker compose up -d --build
	@echo "✓ Stack is starting..."
	@echo "  Kafka UI:    http://localhost:8080"
	@echo "  Grafana:     http://localhost:3000 (admin/admin)"
	@echo "  Prometheus:  http://localhost:9090"
	@echo "  PostgreSQL:  localhost:5432"

down:
	docker compose down

restart:
	docker compose restart

clean:
	docker compose down -v --remove-orphans
	docker system prune -f

# ─── Logs ─────────────────────────────────────────────────────────────────────
logs:
	docker compose logs -f

producer-logs:
	docker compose logs -f producer

consumer-logs:
	docker compose logs -f consumer

# ─── Status & Health ──────────────────────────────────────────────────────────
status:
	docker compose ps

health:
	@echo "── Kafka ──"
	@docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1 && echo "OK" || echo "FAIL"
	@echo "── PostgreSQL ──"
	@docker exec postgres pg_isready -U stockuser -d stockdata > /dev/null 2>&1 && echo "OK" || echo "FAIL"
	@echo "── Producer ──"
	@curl -sf http://localhost:8000/metrics > /dev/null 2>&1 && echo "OK" || echo "FAIL"
	@echo "── Consumer ──"
	@curl -sf http://localhost:8001/metrics > /dev/null 2>&1 && echo "OK" || echo "FAIL"
	@echo "── Prometheus ──"
	@curl -sf http://localhost:9090/-/healthy > /dev/null 2>&1 && echo "OK" || echo "FAIL"
	@echo "── Grafana ──"
	@curl -sf http://localhost:3000/api/health > /dev/null 2>&1 && echo "OK" || echo "FAIL"

# ─── Shell Access ─────────────────────────────────────────────────────────────
db-shell:
	docker exec -it postgres psql -U stockuser -d stockdata

kafka-shell:
	docker exec -it kafka bash

# ─── Kafka Operations ─────────────────────────────────────────────────────────
topics:
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

describe-topic:
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic stock-ticks

consumer-groups:
	docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list

consumer-lag:
	docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group stock-consumer-group

# ─── Database Queries ─────────────────────────────────────────────────────────
db-count:
	docker exec postgres psql -U stockuser -d stockdata -c "SELECT symbol, COUNT(*) as ticks FROM stock_ticks GROUP BY symbol ORDER BY ticks DESC;"

db-latest:
	docker exec postgres psql -U stockuser -d stockdata -c "SELECT symbol, price, bid, ask, volume, timestamp FROM stock_ticks ORDER BY timestamp DESC LIMIT 20;"

db-refresh-views:
	docker exec postgres psql -U stockuser -d stockdata -c "SELECT refresh_ohlcv_views();"

# ─── Development ──────────────────────────────────────────────────────────────
lint:
	flake8 producer/ consumer/ --max-line-length=120
	black --check --line-length=120 producer/ consumer/

format:
	black --line-length=120 producer/ consumer/
