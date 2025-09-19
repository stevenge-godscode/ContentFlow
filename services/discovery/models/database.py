from sqlalchemy import create_engine, Column, String, Integer, BigInteger, Text, DateTime, Boolean, JSON, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql import func
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()

class ArticleStatus(Base):
    """文章处理状态表"""
    __tablename__ = 'articles_status'

    id = Column(String(255), primary_key=True)
    url = Column(String(1024), nullable=False)
    title = Column(String(512))
    mp_name = Column(String(256))
    mp_id = Column(String(255))
    publish_time = Column(BigInteger)

    # 处理状态
    discovery_status = Column(String(20), default='pending')
    download_status = Column(String(20), default='pending')
    parse_status = Column(String(20), default='pending')
    storage_status = Column(String(20), default='pending')

    # 文件路径
    html_file_path = Column(String(1024))
    content_file_path = Column(String(1024))
    metadata_file_path = Column(String(1024))
    images_dir_path = Column(String(1024))

    # 处理结果
    content_length = Column(Integer)
    word_count = Column(Integer)
    image_count = Column(Integer, default=0)

    # 错误信息
    error_message = Column(Text)
    error_details = Column(JSON)
    retry_count = Column(Integer, default=0)
    last_retry_at = Column(DateTime)

    # 时间戳
    created_at = Column(DateTime, default=func.current_timestamp())
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp())
    discovered_at = Column(DateTime)
    downloaded_at = Column(DateTime)
    parsed_at = Column(DateTime)
    stored_at = Column(DateTime)

class ProcessingStats(Base):
    """处理统计表"""
    __tablename__ = 'processing_stats'

    date = Column(String(10), primary_key=True)  # YYYY-MM-DD format
    discovered_count = Column(Integer, default=0)
    downloaded_count = Column(Integer, default=0)
    parsed_count = Column(Integer, default=0)
    stored_count = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)
    total_content_size = Column(BigInteger, default=0)
    total_word_count = Column(BigInteger, default=0)

    # 处理时间统计
    avg_download_time = Column(Integer)  # 秒
    avg_parse_time = Column(Integer)     # 秒

    created_at = Column(DateTime, default=func.current_timestamp())
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp())

class MPAccount(Base):
    """公众号信息表"""
    __tablename__ = 'mp_accounts'

    mp_id = Column(String(255), primary_key=True)
    mp_name = Column(String(256), nullable=False)
    mp_nickname = Column(String(256))
    avatar_url = Column(String(1024))
    description = Column(Text)

    # 统计信息
    total_articles = Column(Integer, default=0)
    processed_articles = Column(Integer, default=0)
    last_article_time = Column(BigInteger)

    # 配置信息
    is_active = Column(Boolean, default=True)
    priority = Column(Integer, default=0)
    custom_selectors = Column(JSON)

    created_at = Column(DateTime, default=func.current_timestamp())
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp())

class TaskQueue(Base):
    """任务队列表"""
    __tablename__ = 'task_queue'

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_type = Column(String(50), nullable=False)
    task_data = Column(JSON, nullable=False)
    priority = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    retry_count = Column(Integer, default=0)
    status = Column(String(20), default='pending')
    error_message = Column(Text)

    created_at = Column(DateTime, default=func.current_timestamp())
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp())
    scheduled_at = Column(DateTime, default=func.current_timestamp())
    started_at = Column(DateTime)
    completed_at = Column(DateTime)

class SystemConfig(Base):
    """系统配置表"""
    __tablename__ = 'system_config'

    key = Column(String(255), primary_key=True)
    value = Column(Text, nullable=False)
    description = Column(Text)
    config_type = Column(String(50), default='string')
    is_sensitive = Column(Boolean, default=False)

    created_at = Column(DateTime, default=func.current_timestamp())
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp())

class DatabaseManager:
    """数据库管理器"""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = None
        self.SessionLocal = None
        self._initialize()

    def _initialize(self):
        """初始化数据库连接"""
        try:
            self.engine = create_engine(
                self.database_url,
                pool_pre_ping=True,
                pool_recycle=300,
                echo=False
            )
            self.SessionLocal = sessionmaker(bind=self.engine)
            logger.info("Database connection initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    def get_session(self) -> Session:
        """获取数据库会话"""
        return self.SessionLocal()

    def create_tables(self):
        """创建表（如果不存在）"""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database tables created/verified")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    def health_check(self) -> bool:
        """数据库健康检查"""
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
                return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

    def get_article_status(self, article_id: str) -> ArticleStatus:
        """获取文章状态"""
        with self.get_session() as session:
            return session.query(ArticleStatus).filter(ArticleStatus.id == article_id).first()

    def create_or_update_article(self, article_data: dict) -> ArticleStatus:
        """创建或更新文章状态"""
        with self.get_session() as session:
            try:
                article_id = article_data['id']
                existing = session.query(ArticleStatus).filter(ArticleStatus.id == article_id).first()

                if existing:
                    # 更新现有记录
                    for key, value in article_data.items():
                        if hasattr(existing, key):
                            setattr(existing, key, value)
                    article = existing
                else:
                    # 创建新记录
                    article = ArticleStatus(**article_data)
                    session.add(article)

                session.commit()
                logger.debug(f"Article {article_id} saved to database")
                return article

            except Exception as e:
                session.rollback()
                logger.error(f"Failed to save article {article_data.get('id')}: {e}")
                raise

    def update_article_status(self, article_id: str, status_field: str, status_value: str,
                            error_message: str = None) -> bool:
        """更新文章处理状态"""
        with self.get_session() as session:
            try:
                article = session.query(ArticleStatus).filter(ArticleStatus.id == article_id).first()
                if not article:
                    logger.warning(f"Article {article_id} not found for status update")
                    return False

                setattr(article, status_field, status_value)

                if error_message:
                    article.error_message = error_message
                    article.retry_count = (article.retry_count or 0) + 1
                    article.last_retry_at = datetime.utcnow()

                # 设置对应的时间戳
                if status_value == 'completed':
                    if status_field == 'discovery_status':
                        article.discovered_at = datetime.utcnow()
                    elif status_field == 'download_status':
                        article.downloaded_at = datetime.utcnow()
                    elif status_field == 'parse_status':
                        article.parsed_at = datetime.utcnow()
                    elif status_field == 'storage_status':
                        article.stored_at = datetime.utcnow()

                session.commit()
                logger.debug(f"Article {article_id} status updated: {status_field}={status_value}")
                return True

            except Exception as e:
                session.rollback()
                logger.error(f"Failed to update article status: {e}")
                return False

    def get_pending_articles(self, status_field: str, limit: int = 100) -> list:
        """获取待处理的文章"""
        with self.get_session() as session:
            return (session.query(ArticleStatus)
                   .filter(getattr(ArticleStatus, status_field) == 'pending')
                   .limit(limit)
                   .all())

    def update_processing_stats(self, date: str, stats: dict) -> bool:
        """更新处理统计"""
        with self.get_session() as session:
            try:
                existing = session.query(ProcessingStats).filter(ProcessingStats.date == date).first()

                if existing:
                    for key, value in stats.items():
                        if hasattr(existing, key):
                            setattr(existing, key, value)
                else:
                    stats['date'] = date
                    stat_record = ProcessingStats(**stats)
                    session.add(stat_record)

                session.commit()
                return True

            except Exception as e:
                session.rollback()
                logger.error(f"Failed to update processing stats: {e}")
                return False

    def get_or_create_mp_account(self, mp_data: dict) -> MPAccount:
        """获取或创建公众号信息"""
        with self.get_session() as session:
            try:
                mp_id = mp_data['mp_id']
                existing = session.query(MPAccount).filter(MPAccount.mp_id == mp_id).first()

                if existing:
                    # 更新信息
                    for key, value in mp_data.items():
                        if hasattr(existing, key) and value:
                            setattr(existing, key, value)
                    mp_account = existing
                else:
                    # 创建新记录
                    mp_account = MPAccount(**mp_data)
                    session.add(mp_account)

                session.commit()
                return mp_account

            except Exception as e:
                session.rollback()
                logger.error(f"Failed to save MP account: {e}")
                raise

    def update_article_paths(self, article_id: str, paths_data: dict) -> bool:
        """更新文章文件路径信息"""
        with self.get_session() as session:
            try:
                article = session.query(ArticleStatus).filter(ArticleStatus.id == article_id).first()
                if article:
                    # 更新路径信息
                    for key, value in paths_data.items():
                        if hasattr(article, key):
                            setattr(article, key, value)

                    article.updated_at = datetime.utcnow()
                    session.commit()
                    return True
                return False

            except Exception as e:
                session.rollback()
                logger.error(f"Failed to update article paths for {article_id}: {e}")
                return False