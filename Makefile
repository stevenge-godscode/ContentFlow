# Genesis Connector - Makefile

.PHONY: help build up down logs ps restart clean setup init check test

# Default target
help:
	@echo "Genesis Connector - Content Processing System"
	@echo ""
	@echo "Available commands:"
	@echo "  setup     - Initial project setup"
	@echo "  init      - Initialize data directories and configuration"
	@echo "  build     - Build all Docker images"
	@echo "  up        - Start all services"
	@echo "  down      - Stop all services"
	@echo "  restart   - Restart all services"
	@echo "  logs      - Show service logs"
	@echo "  ps        - Show service status"
	@echo "  check     - Health check all services"
	@echo "  clean     - Clean up containers and volumes"
	@echo "  test      - Run tests"

# Project setup
setup:
	@echo "Setting up Genesis Connector..."
	@cp .env.example .env
	@echo "Please edit .env file with your configuration"
	@make init

# Initialize data directories
init:
	@echo "Initializing data directories..."
	@mkdir -p data/{html,content,images,logs,backups}
	@mkdir -p data/html/{temp}
	@mkdir -p data/content/{metadata}
	@chmod 755 data
	@chmod 755 data/*
	@echo "Data directories created"

# Build Docker images
build:
	@echo "Building Docker images..."
	docker-compose build

# Start services
up:
	@echo "Starting services..."
	docker-compose up -d
	@echo "Services started. Access:"
	@echo "  WeWe RSS: http://localhost:4000"
	@echo "  Web Interface: http://localhost:8080"
	@echo "  File Server: http://localhost:8081"

# Stop services
down:
	@echo "Stopping services..."
	docker-compose down

# Restart services
restart:
	@echo "Restarting services..."
	docker-compose restart

# Show logs
logs:
	docker-compose logs -f

# Show service status
ps:
	docker-compose ps

# Health check
check:
	@echo "=== Service Status ==="
	@docker-compose ps
	@echo ""
	@echo "=== Redis Queue Status ==="
	@docker-compose exec -T redis redis-cli llen download_tasks 2>/dev/null || echo "Redis not ready"
	@docker-compose exec -T redis redis-cli llen parse_tasks 2>/dev/null || echo "Redis not ready"
	@echo ""
	@echo "=== Database Status ==="
	@docker-compose exec -T postgres pg_isready -U user -d content_db 2>/dev/null || echo "PostgreSQL not ready"
	@echo ""
	@echo "=== Processing Statistics ==="
	@docker-compose exec -T postgres psql -U user -d content_db -c "SELECT download_status, COUNT(*) as count FROM articles_status GROUP BY download_status;" 2>/dev/null || echo "Database not ready"

# Clean up
clean:
	@echo "Cleaning up..."
	docker-compose down -v
	docker system prune -f

# Run tests
test:
	@echo "Running tests..."
	docker-compose -f docker-compose.yml -f docker-compose.test.yml up --build --abort-on-container-exit
	docker-compose -f docker-compose.yml -f docker-compose.test.yml down

# Development helpers
dev-logs:
	docker-compose logs -f content-discovery content-download content-parser content-web

dev-shell:
	docker-compose exec content-discovery /bin/bash

dev-redis:
	docker-compose exec redis redis-cli

dev-db:
	docker-compose exec postgres psql -U user -d content_db

# Production deployment
prod-up:
	docker-compose -f docker-compose.yml -f deployment/production/docker-compose.prod.yml up -d

prod-down:
	docker-compose -f docker-compose.yml -f deployment/production/docker-compose.prod.yml down

# Backup and restore
backup:
	@echo "Creating backup..."
	@mkdir -p data/backups
	@docker-compose exec postgres pg_dump -U user content_db > data/backups/db-backup-$(shell date +%Y-%m-%d-%H%M%S).sql
	@tar -czf data/backups/content-backup-$(shell date +%Y-%m-%d-%H%M%S).tar.gz data/content/
	@echo "Backup completed"

restore:
	@echo "Restore requires manual intervention. Check data/backups/ directory"