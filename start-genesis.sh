#!/bin/bash

echo "🚀 Starting Genesis Connector Services..."

# 创建网络和卷
docker network create genesis-network 2>/dev/null || echo "Network already exists"
docker volume create genesis_content_data 2>/dev/null || echo "Volume already exists"

# 停止现有服务
echo "📦 Stopping existing containers..."
docker stop genesis-redis genesis-postgres genesis-download-1 genesis-download-2 genesis-extraction 2>/dev/null || true
docker rm genesis-redis genesis-postgres genesis-download-1 genesis-download-2 genesis-extraction 2>/dev/null || true

# 启动基础服务
echo "🔧 Starting Redis and PostgreSQL..."
docker run -d --name genesis-redis --network genesis-network -p 6379:6379 \
  --restart unless-stopped \
  redis:7-alpine redis-server --save 60 1 --loglevel warning

docker run -d --name genesis-postgres --network genesis-network -p 5433:5432 \
  --restart unless-stopped \
  -e POSTGRES_DB=content_db -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password \
  postgres:15-alpine

# 等待数据库启动
echo "⏳ Waiting for databases to start..."
sleep 8

# 启动Genesis服务
echo "🎯 Starting Genesis services..."
docker run -d --name genesis-download-1 --network genesis-network -p 5003:5003 \
  --restart unless-stopped \
  -e REDIS_URL=redis://genesis-redis:6379 \
  -e POSTGRES_URL=postgresql://user:password@genesis-postgres:5432/content_db \
  -e WEWE_RSS_URL=http://localhost:4000 \
  -e SERVICE_PORT=5003 \
  -v genesis_content_data:/tmp/genesis-content \
  genesis-download

docker run -d --name genesis-download-2 --network genesis-network -p 5004:5004 \
  --restart unless-stopped \
  -e REDIS_URL=redis://genesis-redis:6379 \
  -e POSTGRES_URL=postgresql://user:password@genesis-postgres:5432/content_db \
  -e WEWE_RSS_URL=http://localhost:4000 \
  -e SERVICE_PORT=5004 \
  -v genesis_content_data:/tmp/genesis-content \
  genesis-download

docker run -d --name genesis-extraction --network genesis-network -p 5006:5006 \
  --restart unless-stopped \
  -e REDIS_URL=redis://genesis-redis:6379 \
  -e POSTGRES_URL=postgresql://user:password@genesis-postgres:5432/content_db \
  -e TEXT_EXTRACTION_PORT=5006 \
  -v genesis_content_data:/tmp/genesis-content \
  genesis-extraction

# 等待服务启动
echo "⏳ Waiting for services to start..."
sleep 10

# 检查服务健康状态
echo "🔍 Checking service health..."
echo "Download Service 1:"
curl -s http://localhost:5003/health | jq . 2>/dev/null || echo "Service not ready yet"

echo -e "\nDownload Service 2:"
curl -s http://localhost:5004/health | jq . 2>/dev/null || echo "Service not ready yet"

echo -e "\nText Extraction Service:"
curl -s http://localhost:5006/health | jq . 2>/dev/null || echo "Service not ready yet"

echo -e "\n✅ Genesis Connector services started!"
echo "📊 Service status:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep genesis

echo -e "\n📋 Management commands:"
echo "  docker ps | grep genesis                    # View status"
echo "  docker logs genesis-download-1              # View logs"
echo "  docker stop genesis-download-1 genesis-extraction  # Stop services"
echo "  docker restart genesis-download-1           # Restart service"