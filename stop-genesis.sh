#!/bin/bash

echo "🛑 Stopping Genesis Connector Services..."

# 停止所有Genesis容器
echo "📦 Stopping containers..."
docker stop genesis-download-1 genesis-download-2 genesis-extraction genesis-redis genesis-postgres 2>/dev/null || true

echo "🗑️  Removing containers..."
docker rm genesis-download-1 genesis-download-2 genesis-extraction genesis-redis genesis-postgres 2>/dev/null || true

echo "✅ All Genesis services stopped and removed!"

# 可选：清理网络和卷
read -p "🔧 Do you want to remove network and data volumes? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🗑️  Removing network and volumes..."
    docker network rm genesis-network 2>/dev/null || true
    docker volume rm genesis_content_data 2>/dev/null || true
    echo "✅ Network and volumes removed!"
else
    echo "📁 Network and volumes preserved for next startup"
fi