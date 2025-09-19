FROM python:3.11-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . /app/src
WORKDIR /app/src

# 创建日志目录
RUN mkdir -p /app/data/logs

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:5001/health || exit 1

# 设置环境变量
ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

# 暴露端口
EXPOSE 5001

# 运行应用
CMD ["python", "-u", "app.py"]