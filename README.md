# ContentFlow

基于微服务架构的RSS内容自动化处理系统

## 项目概述

ContentFlow 是一个高性能的内容处理流水线，集成 WeWe RSS 系统，实现从内容发现、下载到文本提取的全自动化处理。

## 核心功能

- 🔍 **内容发现**: 自动从 WeWe RSS 发现新文章
- 📥 **并行下载**: 双worker并行下载HTML内容
- 📝 **文本提取**: 使用 trafilatura 库提取纯文本
- 🔄 **消息队列**: Redis驱动的任务队列系统
- 📊 **状态监控**: 实时监控处理流水线状态
- 🐳 **容器化**: 完整的Docker Compose部署

## 系统架构

```
WeWe RSS → Content Discovery → Redis Queue → Download Workers → Text Extraction → Output
    ↓              ↓                ↓              ↓                    ↓
  MySQL        PostgreSQL        Message Queue    File Storage      Text Files
```

## 服务组件

### 核心服务
- **WeWe RSS** (端口 4000): RSS聚合和管理
- **Download Service** (端口 5003): 下载服务API
- **Text Extraction Service** (端口 5006): 文本提取API

### 基础设施
- **Redis** (端口 6379): 消息队列和缓存
- **PostgreSQL** (端口 5433): 内容处理状态数据库
- **MySQL** (端口 3306): WeWe RSS数据库

## 快速开始

### 启动系统
```bash
docker-compose up -d
```

### 检查状态
```bash
# 全局状态
./status

# 详细流水线状态
./status --detailed
```

### 手动操作
```bash
# 运行下载批次
curl -X POST http://localhost:5003/download-batch

# 运行文本提取批次
curl -X POST http://localhost:5006/extract-batch

# 清理重复队列任务
python clean_queue.py
```

## 技术栈

- **后端**: Python + Flask
- **队列**: Redis
- **数据库**: PostgreSQL + MySQL
- **容器**: Docker + Docker Compose
- **文本提取**: trafilatura
- **监控**: 自定义状态监控系统

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

MIT License
