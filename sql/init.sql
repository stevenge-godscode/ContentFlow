-- Genesis Connector Database Schema
-- 内容处理状态管理数据库

-- 创建数据库（如果不存在）
-- CREATE DATABASE IF NOT EXISTS content_db;

-- 文章处理状态表
CREATE TABLE IF NOT EXISTS articles_status (
    id VARCHAR(255) PRIMARY KEY,
    url VARCHAR(1024) NOT NULL,
    title VARCHAR(512),
    mp_name VARCHAR(256),
    mp_id VARCHAR(255),
    publish_time BIGINT,

    -- 处理状态
    discovery_status VARCHAR(20) DEFAULT 'pending', -- pending, processing, completed, failed
    download_status VARCHAR(20) DEFAULT 'pending',
    parse_status VARCHAR(20) DEFAULT 'pending',
    storage_status VARCHAR(20) DEFAULT 'pending',

    -- 文件路径
    html_file_path VARCHAR(1024),
    content_file_path VARCHAR(1024),
    metadata_file_path VARCHAR(1024),
    images_dir_path VARCHAR(1024),

    -- 处理结果
    content_length INTEGER,
    word_count INTEGER,
    image_count INTEGER DEFAULT 0,

    -- 错误信息
    error_message TEXT,
    error_details JSONB,
    retry_count INTEGER DEFAULT 0,
    last_retry_at TIMESTAMP,

    -- 时间戳
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    discovered_at TIMESTAMP,
    downloaded_at TIMESTAMP,
    parsed_at TIMESTAMP,
    stored_at TIMESTAMP
);

-- 处理统计表
CREATE TABLE IF NOT EXISTS processing_stats (
    date DATE PRIMARY KEY,
    discovered_count INTEGER DEFAULT 0,
    downloaded_count INTEGER DEFAULT 0,
    parsed_count INTEGER DEFAULT 0,
    stored_count INTEGER DEFAULT 0,
    failed_count INTEGER DEFAULT 0,
    total_content_size BIGINT DEFAULT 0,
    total_word_count BIGINT DEFAULT 0,

    -- 处理时间统计
    avg_download_time DECIMAL(10,2),
    avg_parse_time DECIMAL(10,2),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 公众号信息表
CREATE TABLE IF NOT EXISTS mp_accounts (
    mp_id VARCHAR(255) PRIMARY KEY,
    mp_name VARCHAR(256) NOT NULL,
    mp_nickname VARCHAR(256),
    avatar_url VARCHAR(1024),
    description TEXT,

    -- 统计信息
    total_articles INTEGER DEFAULT 0,
    processed_articles INTEGER DEFAULT 0,
    last_article_time BIGINT,

    -- 配置信息
    is_active BOOLEAN DEFAULT true,
    priority INTEGER DEFAULT 0, -- 处理优先级
    custom_selectors JSONB, -- 自定义解析规则

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 任务队列表（用于持久化重要任务）
CREATE TABLE IF NOT EXISTS task_queue (
    id SERIAL PRIMARY KEY,
    task_type VARCHAR(50) NOT NULL, -- discovery, download, parse, storage
    task_data JSONB NOT NULL,
    priority INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    retry_count INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'pending', -- pending, processing, completed, failed
    error_message TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scheduled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

-- 系统配置表
CREATE TABLE IF NOT EXISTS system_config (
    key VARCHAR(255) PRIMARY KEY,
    value TEXT NOT NULL,
    description TEXT,
    config_type VARCHAR(50) DEFAULT 'string', -- string, number, boolean, json
    is_sensitive BOOLEAN DEFAULT false,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_articles_status_mp_id ON articles_status(mp_id);
CREATE INDEX IF NOT EXISTS idx_articles_status_publish_time ON articles_status(publish_time);
CREATE INDEX IF NOT EXISTS idx_articles_status_discovery_status ON articles_status(discovery_status);
CREATE INDEX IF NOT EXISTS idx_articles_status_download_status ON articles_status(download_status);
CREATE INDEX IF NOT EXISTS idx_articles_status_parse_status ON articles_status(parse_status);
CREATE INDEX IF NOT EXISTS idx_articles_status_created_at ON articles_status(created_at);

CREATE INDEX IF NOT EXISTS idx_processing_stats_date ON processing_stats(date);

CREATE INDEX IF NOT EXISTS idx_mp_accounts_is_active ON mp_accounts(is_active);
CREATE INDEX IF NOT EXISTS idx_mp_accounts_priority ON mp_accounts(priority);

CREATE INDEX IF NOT EXISTS idx_task_queue_status ON task_queue(status);
CREATE INDEX IF NOT EXISTS idx_task_queue_task_type ON task_queue(task_type);
CREATE INDEX IF NOT EXISTS idx_task_queue_scheduled_at ON task_queue(scheduled_at);
CREATE INDEX IF NOT EXISTS idx_task_queue_priority ON task_queue(priority);

-- 创建更新时间触发器函数
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 为需要的表创建更新时间触发器
CREATE TRIGGER update_articles_status_updated_at BEFORE UPDATE
    ON articles_status FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_processing_stats_updated_at BEFORE UPDATE
    ON processing_stats FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_mp_accounts_updated_at BEFORE UPDATE
    ON mp_accounts FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_task_queue_updated_at BEFORE UPDATE
    ON task_queue FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_system_config_updated_at BEFORE UPDATE
    ON system_config FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 插入默认系统配置
INSERT INTO system_config (key, value, description) VALUES
    ('discovery_interval', '300', '内容发现间隔（秒）'),
    ('download_timeout', '30', '下载超时时间（秒）'),
    ('concurrent_downloads', '5', '并发下载数'),
    ('parse_batch_size', '10', '解析批处理大小'),
    ('cleanup_temp_files', 'true', '是否清理临时文件'),
    ('system_maintenance_mode', 'false', '系统维护模式')
ON CONFLICT (key) DO NOTHING;