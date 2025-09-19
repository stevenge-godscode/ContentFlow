import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from .wewe_client import WeWeRSSClient
from .queue_manager import QueueManager
from ..models.database import DatabaseManager, ArticleStatus

logger = logging.getLogger(__name__)

class DiscoveryEngine:
    """内容发现引擎"""

    def __init__(self, config):
        self.config = config
        self.wewe_client = WeWeRSSClient(
            base_url=config.WEWE_RSS_URL,
            timeout=config.WEWE_RSS_TIMEOUT
        )
        self.queue_manager = QueueManager(config.REDIS_URL)
        self.db_manager = DatabaseManager(config.POSTGRES_URL)

        # 统计信息
        self.stats = {
            'discovered': 0,
            'new_articles': 0,
            'duplicates': 0,
            'errors': 0,
            'last_run': None,
            'total_articles': 0
        }

    def run_discovery(self) -> Dict:
        """执行发现任务"""
        start_time = time.time()
        run_stats = {
            'discovered': 0,
            'new_articles': 0,
            'duplicates': 0,
            'errors': 0,
            'start_time': datetime.utcnow().isoformat(),
            'duration': 0
        }

        logger.info("Starting content discovery...")

        try:
            # 1. 健康检查
            if not self._health_check():
                raise Exception("Health check failed")

            # 2. 获取文章
            articles = self._fetch_articles()
            run_stats['discovered'] = len(articles)
            logger.info(f"Discovered {len(articles)} articles from WeWe RSS")

            # 3. 处理每篇文章
            for article in articles:
                try:
                    result = self._process_article(article)
                    if result == 'new':
                        run_stats['new_articles'] += 1
                    elif result == 'duplicate':
                        run_stats['duplicates'] += 1

                except Exception as e:
                    logger.error(f"Error processing article {article.get('id', 'unknown')}: {e}")
                    run_stats['errors'] += 1

            # 4. 更新统计
            run_stats['duration'] = time.time() - start_time
            self._update_stats(run_stats)

            logger.info(f"Discovery completed: {run_stats['new_articles']} new, "
                       f"{run_stats['duplicates']} duplicates, "
                       f"{run_stats['errors']} errors in {run_stats['duration']:.2f}s")

        except Exception as e:
            logger.error(f"Discovery failed: {e}")
            run_stats['errors'] += 1
            run_stats['duration'] = time.time() - start_time

        self.stats['last_run'] = run_stats
        return run_stats

    def _health_check(self) -> bool:
        """健康检查"""
        try:
            # 检查WeWe RSS连接
            if not self.wewe_client.health_check():
                logger.error("WeWe RSS health check failed")
                return False

            # 检查Redis连接
            if not self.queue_manager.health_check():
                logger.error("Redis health check failed")
                return False

            # 检查数据库连接
            if not self.db_manager.health_check():
                logger.error("Database health check failed")
                return False

            logger.debug("All services healthy")
            return True

        except Exception as e:
            logger.error(f"Health check error: {e}")
            return False

    def _fetch_articles(self) -> List[Dict]:
        """从WeWe RSS获取文章"""
        try:
            # 尝试获取最近24小时的文章
            recent_articles = self.wewe_client.get_recent_articles(hours=24, limit=1000)

            if recent_articles:
                logger.info(f"Retrieved {len(recent_articles)} recent articles")
                return recent_articles

            # 如果没有最近文章接口，获取所有文章
            logger.info("Falling back to get all articles")
            all_articles = self.wewe_client.get_all_articles(limit=1000)

            # 过滤最近24小时的文章
            cutoff_time = int(time.time() - 24 * 3600)
            filtered_articles = []

            for article in all_articles:
                publish_time = self._extract_publish_time(article)
                if publish_time and publish_time > cutoff_time:
                    filtered_articles.append(article)

            logger.info(f"Filtered to {len(filtered_articles)} recent articles")
            return filtered_articles

        except Exception as e:
            logger.error(f"Error fetching articles: {e}")
            return []

    def _process_article(self, raw_article: Dict) -> str:
        """处理单篇文章"""
        try:
            # 1. 提取文章信息
            article_info = self.wewe_client.extract_article_info(raw_article)

            if not article_info or not article_info.get('id') or not article_info.get('url'):
                logger.warning(f"Invalid article data: {raw_article}")
                return 'error'

            article_id = article_info['id']
            article_url = article_info['url']

            # 2. 检查重复
            if self.queue_manager.is_duplicate(article_id, article_url):
                logger.debug(f"Duplicate article: {article_id}")
                return 'duplicate'

            # 3. 检查数据库中是否已存在
            existing = self.db_manager.get_article_status(article_id)
            if existing:
                logger.debug(f"Article already in database: {article_id}")
                return 'duplicate'

            # 4. 创建数据库记录
            article_data = self._prepare_article_data(article_info)
            self.db_manager.create_or_update_article(article_data)

            # 5. 添加到下载队列
            if self.queue_manager.push_download_task(article_info):
                # 6. 更新处理状态
                self.db_manager.update_article_status(
                    article_id, 'discovery_status', 'completed'
                )

                # 7. 设置Redis处理状态
                self.queue_manager.set_processing_status(
                    article_id, 'queued_for_download',
                    {'discovered_at': datetime.utcnow().isoformat()}
                )

                logger.debug(f"New article processed: {article_id}")
                return 'new'
            else:
                logger.error(f"Failed to queue article for download: {article_id}")
                self.db_manager.update_article_status(
                    article_id, 'discovery_status', 'failed',
                    'Failed to add to download queue'
                )
                return 'error'

        except Exception as e:
            logger.error(f"Error processing article: {e}")
            return 'error'

    def _prepare_article_data(self, article_info: Dict) -> Dict:
        """准备数据库文章数据"""
        return {
            'id': article_info['id'],
            'url': article_info['url'],
            'title': article_info.get('title', '')[:512],  # 限制长度
            'mp_name': article_info.get('mp_name', '')[:256],
            'mp_id': article_info.get('mp_id', '')[:255],
            'publish_time': article_info.get('publish_time'),
            'discovery_status': 'processing',
            'discovered_at': datetime.utcnow()
        }

    def _extract_publish_time(self, article: Dict) -> Optional[int]:
        """提取发布时间"""
        return self.wewe_client._parse_publish_time(article)

    def _update_stats(self, run_stats: Dict) -> None:
        """更新统计信息"""
        try:
            # 更新内存统计
            self.stats['discovered'] += run_stats['discovered']
            self.stats['new_articles'] += run_stats['new_articles']
            self.stats['duplicates'] += run_stats['duplicates']
            self.stats['errors'] += run_stats['errors']

            # 更新数据库统计
            today = datetime.utcnow().date().isoformat()
            stats_data = {
                'discovered_count': run_stats['new_articles']  # 只统计新发现的
            }
            self.db_manager.update_processing_stats(today, stats_data)

            logger.debug(f"Stats updated: {stats_data}")

        except Exception as e:
            logger.error(f"Error updating stats: {e}")

    def get_discovery_status(self) -> Dict:
        """获取发现服务状态"""
        try:
            # 基本统计
            status = {
                'service': 'discovery',
                'status': 'running',
                'uptime': time.time(),
                'stats': self.stats.copy(),
                'config': {
                    'interval': self.config.DISCOVERY_INTERVAL,
                    'batch_size': self.config.BATCH_SIZE,
                    'wewe_rss_url': self.config.WEWE_RSS_URL
                }
            }

            # 队列状态
            queue_stats = self.queue_manager.get_queue_stats()
            status['queue_stats'] = queue_stats

            # 健康状态
            health = {
                'wewe_rss': self.wewe_client.health_check(),
                'redis': self.queue_manager.health_check(),
                'database': self.db_manager.health_check()
            }
            status['health'] = health
            status['healthy'] = all(health.values())

            return status

        except Exception as e:
            logger.error(f"Error getting discovery status: {e}")
            return {
                'service': 'discovery',
                'status': 'error',
                'error': str(e)
            }

    def force_discovery(self, hours: int = 24) -> Dict:
        """强制发现指定时间范围内的文章"""
        logger.info(f"Force discovery for last {hours} hours")

        try:
            # 获取指定时间范围的文章
            articles = self.wewe_client.get_recent_articles(hours=hours, limit=2000)

            processed = 0
            new_count = 0

            for article in articles:
                try:
                    result = self._process_article(article)
                    if result == 'new':
                        new_count += 1
                    processed += 1

                    # 每100篇记录一次进度
                    if processed % 100 == 0:
                        logger.info(f"Force discovery progress: {processed}/{len(articles)}")

                except Exception as e:
                    logger.error(f"Error in force discovery: {e}")
                    continue

            result = {
                'total_articles': len(articles),
                'processed': processed,
                'new_articles': new_count,
                'hours': hours
            }

            logger.info(f"Force discovery completed: {result}")
            return result

        except Exception as e:
            logger.error(f"Force discovery failed: {e}")
            return {'error': str(e)}

    def cleanup_old_data(self, days: int = 30) -> Dict:
        """清理旧数据"""
        try:
            # 清理Redis中的过期任务
            expired_tasks = self.queue_manager.clear_expired_tasks()

            # 这里可以添加清理数据库中旧记录的逻辑
            # 例如删除30天前的处理记录等

            result = {
                'expired_tasks_cleared': expired_tasks,
                'cleanup_date': datetime.utcnow().isoformat()
            }

            logger.info(f"Cleanup completed: {result}")
            return result

        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
            return {'error': str(e)}