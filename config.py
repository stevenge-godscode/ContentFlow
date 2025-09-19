"""
Genesis Connector Configuration
"""
import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class Config:
    """应用配置类"""

    # WeWe RSS服务配置
    WEWE_RSS_URL: str = "http://localhost:4000"
    WEWE_RSS_TIMEOUT: int = 30

    # Redis配置
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_EXPIRE_TIME: int = 86400  # 24小时

    # PostgreSQL配置
    POSTGRES_URL: str = "postgresql://user:password@localhost:5433/content_db"

    # Discovery服务配置
    DISCOVERY_INTERVAL: int = 300  # 5分钟
    BATCH_SIZE: int = 100
    MAX_RETRIES: int = 3

    # 日志配置
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    @classmethod
    def from_env(cls) -> 'Config':
        """从环境变量加载配置"""
        return cls(
            WEWE_RSS_URL=os.getenv('WEWE_RSS_URL', cls.WEWE_RSS_URL),
            WEWE_RSS_TIMEOUT=int(os.getenv('WEWE_RSS_TIMEOUT', cls.WEWE_RSS_TIMEOUT)),

            REDIS_URL=os.getenv('REDIS_URL', cls.REDIS_URL),
            REDIS_EXPIRE_TIME=int(os.getenv('REDIS_EXPIRE_TIME', cls.REDIS_EXPIRE_TIME)),

            POSTGRES_URL=os.getenv('POSTGRES_URL', cls.POSTGRES_URL),

            DISCOVERY_INTERVAL=int(os.getenv('DISCOVERY_INTERVAL', cls.DISCOVERY_INTERVAL)),
            BATCH_SIZE=int(os.getenv('BATCH_SIZE', cls.BATCH_SIZE)),
            MAX_RETRIES=int(os.getenv('MAX_RETRIES', cls.MAX_RETRIES)),

            LOG_LEVEL=os.getenv('LOG_LEVEL', cls.LOG_LEVEL),
            LOG_FORMAT=os.getenv('LOG_FORMAT', cls.LOG_FORMAT)
        )