import redis
import json
import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class QueueManager:
    """Redis队列管理器"""

    # 队列名称
    DOWNLOAD_QUEUE = 'download_tasks'
    PARSE_QUEUE = 'parse_tasks'
    STORAGE_QUEUE = 'storage_tasks'
    FAILED_QUEUE = 'failed_tasks'

    # 状态和统计缓存
    PROCESSING_STATUS = 'processing_status'
    DUPLICATE_CHECK = 'duplicate_check'
    STATS_PREFIX = 'stats:'

    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.redis_client = None
        self._initialize()

    def _initialize(self):
        """初始化Redis连接"""
        try:
            self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
            # 测试连接
            self.redis_client.ping()
            logger.info("Redis connection initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Redis: {e}")
            raise

    def health_check(self) -> bool:
        """Redis健康检查"""
        try:
            return self.redis_client.ping()
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False

    def push_download_task(self, article_data: Dict) -> bool:
        """添加下载任务"""
        try:
            task = {
                'id': article_data['id'],
                'url': article_data['url'],
                'title': article_data.get('title', ''),
                'mp_name': article_data.get('mp_name', ''),
                'mp_id': article_data.get('mp_id', ''),
                'priority': article_data.get('priority', 0),
                'retry_count': 0,
                'created_at': datetime.utcnow().isoformat(),
                'source': 'discovery'
            }

            # 使用优先级队列（Redis sorted set）
            score = time.time() - task['priority'] * 1000  # 优先级越高，score越小
            result = self.redis_client.zadd(self.DOWNLOAD_QUEUE, {json.dumps(task): score})

            if result:
                logger.debug(f"Download task added for article {article_data['id']}")
                self._update_queue_stats(self.DOWNLOAD_QUEUE, 'added')
                return True
            else:
                logger.warning(f"Failed to add download task for article {article_data['id']}")
                return False

        except Exception as e:
            logger.error(f"Error adding download task: {e}")
            return False

    def pop_download_task(self, timeout: int = 10) -> Optional[Dict]:
        """获取下载任务"""
        try:
            # 使用BZPOPMIN获取最高优先级的任务
            result = self.redis_client.bzpopmin(self.DOWNLOAD_QUEUE, timeout)
            if result:
                queue_name, task_json, score = result
                task = json.loads(task_json)
                logger.debug(f"Download task retrieved for article {task['id']}")
                self._update_queue_stats(self.DOWNLOAD_QUEUE, 'processed')
                return task
            return None

        except Exception as e:
            logger.error(f"Error retrieving download task: {e}")
            return None

    def push_parse_task(self, task_data: Dict) -> bool:
        """添加解析任务"""
        try:
            task = {
                'id': task_data['id'],
                'html_file_path': task_data['html_file_path'],
                'title': task_data.get('title', ''),
                'mp_name': task_data.get('mp_name', ''),
                'mp_id': task_data.get('mp_id', ''),
                'priority': task_data.get('priority', 0),
                'retry_count': 0,
                'created_at': datetime.utcnow().isoformat(),
                'source': 'download'
            }

            score = time.time() - task['priority'] * 1000
            result = self.redis_client.zadd(self.PARSE_QUEUE, {json.dumps(task): score})

            if result:
                logger.debug(f"Parse task added for article {task_data['id']}")
                self._update_queue_stats(self.PARSE_QUEUE, 'added')
                return True
            else:
                logger.warning(f"Failed to add parse task for article {task_data['id']}")
                return False

        except Exception as e:
            logger.error(f"Error adding parse task: {e}")
            return False

    def pop_parse_task(self, timeout: int = 10) -> Optional[Dict]:
        """获取解析任务"""
        try:
            result = self.redis_client.bzpopmin(self.PARSE_QUEUE, timeout)
            if result:
                queue_name, task_json, score = result
                task = json.loads(task_json)
                logger.debug(f"Parse task retrieved for article {task['id']}")
                self._update_queue_stats(self.PARSE_QUEUE, 'processed')
                return task
            return None

        except Exception as e:
            logger.error(f"Error retrieving parse task: {e}")
            return None

    def push_failed_task(self, task_data: Dict, error_message: str) -> bool:
        """添加失败任务"""
        try:
            failed_task = {
                **task_data,
                'error_message': error_message,
                'failed_at': datetime.utcnow().isoformat(),
                'retry_count': task_data.get('retry_count', 0) + 1
            }

            result = self.redis_client.lpush(self.FAILED_QUEUE, json.dumps(failed_task))
            if result:
                logger.debug(f"Failed task added: {task_data.get('id')}")
                self._update_queue_stats(self.FAILED_QUEUE, 'added')

                # 如果重试次数未超过限制，重新加入对应队列
                max_retries = task_data.get('max_retries', 3)
                if failed_task['retry_count'] <= max_retries:
                    self._retry_task(failed_task)

                return True
            return False

        except Exception as e:
            logger.error(f"Error adding failed task: {e}")
            return False

    def _retry_task(self, task_data: Dict) -> bool:
        """重试失败的任务"""
        try:
            # 延迟重试（指数退避）
            delay = min(60 * (2 ** task_data['retry_count']), 3600)  # 最大延迟1小时
            retry_time = time.time() + delay

            task_data['retry_at'] = retry_time
            task_data['updated_at'] = datetime.utcnow().isoformat()

            # 根据任务来源决定重新加入哪个队列
            if task_data.get('source') == 'discovery':
                queue_name = self.DOWNLOAD_QUEUE
            elif task_data.get('source') == 'download':
                queue_name = self.PARSE_QUEUE
            else:
                logger.warning(f"Unknown task source: {task_data.get('source')}")
                return False

            # 使用延迟时间作为score
            result = self.redis_client.zadd(queue_name, {json.dumps(task_data): retry_time})

            if result:
                logger.info(f"Task {task_data['id']} scheduled for retry in {delay} seconds")
                return True
            return False

        except Exception as e:
            logger.error(f"Error retrying task: {e}")
            return False

    def is_duplicate(self, article_id: str, url: str) -> bool:
        """检查是否重复"""
        try:
            # 使用ID和URL的哈希作为去重键
            import hashlib
            dup_key = hashlib.md5(f"{article_id}:{url}".encode()).hexdigest()

            # 检查是否已存在
            exists = self.redis_client.sismember(self.DUPLICATE_CHECK, dup_key)

            if not exists:
                # 添加到去重集合，设置过期时间（30天）
                self.redis_client.sadd(self.DUPLICATE_CHECK, dup_key)
                self.redis_client.expire(self.DUPLICATE_CHECK, 30 * 24 * 3600)
                return False

            return True

        except Exception as e:
            logger.error(f"Error checking duplicate: {e}")
            return False

    def set_processing_status(self, article_id: str, status: str, details: Dict = None) -> bool:
        """设置处理状态"""
        try:
            status_data = {
                'status': status,
                'updated_at': datetime.utcnow().isoformat(),
                'details': details or {}
            }

            key = f"{self.PROCESSING_STATUS}:{article_id}"
            result = self.redis_client.setex(key, 24 * 3600, json.dumps(status_data))  # 24小时过期

            if result:
                logger.debug(f"Processing status set for {article_id}: {status}")
            return result

        except Exception as e:
            logger.error(f"Error setting processing status: {e}")
            return False

    def get_processing_status(self, article_id: str) -> Optional[Dict]:
        """获取处理状态"""
        try:
            key = f"{self.PROCESSING_STATUS}:{article_id}"
            data = self.redis_client.get(key)

            if data:
                return json.loads(data)
            return None

        except Exception as e:
            logger.error(f"Error getting processing status: {e}")
            return None

    def get_queue_stats(self) -> Dict:
        """获取队列统计信息"""
        try:
            stats = {}

            # 获取队列长度
            queues = [self.DOWNLOAD_QUEUE, self.PARSE_QUEUE, self.STORAGE_QUEUE, self.FAILED_QUEUE]
            for queue in queues:
                if queue == self.FAILED_QUEUE:
                    # 失败队列使用list
                    length = self.redis_client.llen(queue)
                else:
                    # 其他队列使用sorted set
                    length = self.redis_client.zcard(queue)
                stats[f"{queue}_length"] = length

            # 获取处理统计
            for action in ['added', 'processed', 'failed']:
                for queue in queues:
                    key = f"{self.STATS_PREFIX}{queue}:{action}"
                    count = self.redis_client.get(key) or 0
                    stats[f"{queue}_{action}"] = int(count)

            # 获取当前处理状态数量
            status_keys = self.redis_client.keys(f"{self.PROCESSING_STATUS}:*")
            stats['current_processing'] = len(status_keys)

            return stats

        except Exception as e:
            logger.error(f"Error getting queue stats: {e}")
            return {}

    def _update_queue_stats(self, queue_name: str, action: str) -> None:
        """更新队列统计"""
        try:
            key = f"{self.STATS_PREFIX}{queue_name}:{action}"
            self.redis_client.incr(key)
            # 设置过期时间（7天）
            self.redis_client.expire(key, 7 * 24 * 3600)
        except Exception as e:
            logger.error(f"Error updating queue stats: {e}")

    def clear_expired_tasks(self) -> int:
        """清理过期任务"""
        try:
            current_time = time.time()
            expired_count = 0

            # 清理各个队列中的过期重试任务
            for queue_name in [self.DOWNLOAD_QUEUE, self.PARSE_QUEUE, self.STORAGE_QUEUE]:
                # 获取score小于当前时间的任务（即该重试的任务）
                expired_tasks = self.redis_client.zrangebyscore(
                    queue_name, '-inf', current_time, withscores=True
                )

                # 这些实际上是可以处理的任务，不应该清理
                # 这里只清理非常老的任务（超过24小时）
                very_old_time = current_time - 24 * 3600
                very_old_tasks = self.redis_client.zrangebyscore(
                    queue_name, '-inf', very_old_time
                )

                if very_old_tasks:
                    # 移动到失败队列而不是直接删除
                    for task_json in very_old_tasks:
                        task = json.loads(task_json)
                        self.push_failed_task(task, "Task expired (>24h in queue)")
                        self.redis_client.zrem(queue_name, task_json)
                        expired_count += 1

            logger.info(f"Cleared {expired_count} expired tasks")
            return expired_count

        except Exception as e:
            logger.error(f"Error clearing expired tasks: {e}")
            return 0

    def get_queue_sample(self, queue_name: str, count: int = 5) -> List[Dict]:
        """获取队列样本（用于调试）"""
        try:
            if queue_name == self.FAILED_QUEUE:
                # 失败队列使用list
                samples = self.redis_client.lrange(queue_name, 0, count - 1)
            else:
                # 其他队列使用sorted set
                samples = self.redis_client.zrange(queue_name, 0, count - 1)

            result = []
            for sample in samples:
                try:
                    task = json.loads(sample)
                    result.append(task)
                except json.JSONDecodeError:
                    continue

            return result

        except Exception as e:
            logger.error(f"Error getting queue sample: {e}")
            return []