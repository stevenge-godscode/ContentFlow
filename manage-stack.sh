#!/bin/bash

# Genesis Connector Stack Management Script

case "$1" in
    "start")
        echo "🚀 Starting Genesis Stack with original data..."

        # Create network
        docker network create genesis-complete-network 2>/dev/null || echo "Network exists"

        # Start databases with original data
        echo "📦 Starting databases..."
        docker run -d --name mysql --network genesis-complete-network -p 3306:3306 \
          -e MYSQL_ROOT_PASSWORD=123456 -e TZ='Asia/Shanghai' -e MYSQL_DATABASE='wewe-rss' \
          -v af2387f83fdb6104ac54efc9e7b62a9ac27cced340a32250d00100b7094cd0d7:/var/lib/mysql \
          --restart unless-stopped \
          mysql:8.3.0 --mysql-native-password=ON 2>/dev/null || echo "MySQL already running"

        docker run -d --name redis --network genesis-complete-network \
          -v 71b038910c1ac45f9b38e50dac5cb5e1c03cb92672358545c980949956e5658e:/data \
          --restart unless-stopped \
          redis:7-alpine redis-server --save 60 1 --loglevel warning 2>/dev/null || echo "Redis already running"

        docker run -d --name postgres --network genesis-complete-network \
          -e POSTGRES_DB=content_db -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password \
          -e POSTGRES_INITDB_ARGS="--encoding=UTF-8 --lc-collate=C --lc-ctype=C" \
          -v af0339824b9f09e11313c105d2991e1b069fcd7bc50fdb4b8570fe179a4c1d45:/var/lib/postgresql/data \
          --restart unless-stopped \
          postgres:15-alpine 2>/dev/null || echo "PostgreSQL already running"

        sleep 10

        # Start WeWe RSS (if not already running)
        if ! docker ps | grep -q wewe-rss; then
            docker run -d --name wewe-rss --network genesis-complete-network -p 4000:4000 \
              -e DATABASE_URL=mysql://root:123456@mysql:3306/wewe-rss?schema=public \
              -e AUTH_CODE=123567 \
              --restart unless-stopped \
              cooderl/wewe-rss:latest
        fi

        # Create content volume
        docker volume create genesis_content_data 2>/dev/null || echo "Volume exists"

        # Start Genesis services (internal only)
        echo "🎯 Starting Genesis services..."
        docker run -d --name content-download-1 --network genesis-complete-network \
          -e REDIS_URL=redis://redis:6379 \
          -e POSTGRES_URL=postgresql://user:password@postgres:5432/content_db \
          -e WEWE_RSS_URL=http://wewe-rss:4000 \
          -e SERVICE_PORT=5003 \
          -e LOG_LEVEL=INFO \
          -v genesis_content_data:/tmp/genesis-content \
          --restart unless-stopped \
          genesis-download 2>/dev/null || echo "Download-1 already running"

        docker run -d --name content-download-2 --network genesis-complete-network \
          -e REDIS_URL=redis://redis:6379 \
          -e POSTGRES_URL=postgresql://user:password@postgres:5432/content_db \
          -e WEWE_RSS_URL=http://wewe-rss:4000 \
          -e SERVICE_PORT=5004 \
          -e LOG_LEVEL=INFO \
          -v genesis_content_data:/tmp/genesis-content \
          --restart unless-stopped \
          genesis-download 2>/dev/null || echo "Download-2 already running"

        docker run -d --name content-parser --network genesis-complete-network \
          -e REDIS_URL=redis://redis:6379 \
          -e POSTGRES_URL=postgresql://user:password@postgres:5432/content_db \
          -e TEXT_EXTRACTION_PORT=5006 \
          -e LOG_LEVEL=INFO \
          -v genesis_content_data:/tmp/genesis-content \
          --restart unless-stopped \
          genesis-extraction 2>/dev/null || echo "Parser already running"

        echo "✅ All services started!"
        ;;

    "stop")
        echo "🛑 Stopping Genesis Stack..."
        docker stop content-parser content-download-2 content-download-1 wewe-rss postgres redis mysql 2>/dev/null || true
        echo "✅ All services stopped!"
        ;;

    "status")
        echo "📊 Genesis Stack Status:"
        docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "(content-|wewe|redis|mysql|postgres|genesis)"
        echo ""
        echo "🔍 Data Status:"
        docker exec mysql mysql -uroot -p123456 wewe-rss -e "SELECT COUNT(*) as accounts FROM accounts; SELECT COUNT(*) as articles FROM articles;" 2>/dev/null | grep -v "Warning" || echo "MySQL not accessible"
        ;;

    "logs")
        echo "📋 Recent logs for all Genesis services:"
        for service in content-download-1 content-download-2 content-parser wewe-rss; do
            echo "=== $service ==="
            docker logs --tail 5 $service 2>/dev/null || echo "Service not running"
            echo ""
        done
        ;;

    "clean")
        echo "🧹 Cleaning stopped containers..."
        docker container prune -f
        echo "✅ Cleanup complete!"
        ;;

    *)
        echo "Genesis Connector Stack Manager"
        echo ""
        echo "Usage: $0 {start|stop|status|logs|clean}"
        echo ""
        echo "Commands:"
        echo "  start  - Start all services with original data"
        echo "  stop   - Stop all services"
        echo "  status - Show service and data status"
        echo "  logs   - Show recent logs"
        echo "  clean  - Remove stopped containers"
        echo ""
        echo "External Access:"
        echo "  WeWe RSS: http://localhost:4000"
        echo "  MySQL:    localhost:3306"
        echo ""
        echo "Internal Services (Docker network only):"
        echo "  content-download-1: 5003"
        echo "  content-download-2: 5004"
        echo "  content-parser:     5006"
        ;;
esac