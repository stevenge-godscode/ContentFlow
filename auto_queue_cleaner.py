#!/usr/bin/env python3
"""
自动队列清理工具 - 定期清理重复和已完成的任务
"""

import sys
import os
import json
import redis
import glob
import time
import logging
from pathlib import Path
from datetime import datetime

# 添加路径
sys.path.append('/home/azureuser/repository/genesis-connector')
from config import Config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AutoQueueCleaner:
    """自动队列清理器"""

    def __init__(self):
        self.config = Config.from_env()
        self.redis_client = None
        self.html_dir = '/tmp/genesis-content/html'
        self.last_cleanup = None

    def connect_redis(self):
        """连接Redis"""
        try:
            self.redis_client = redis.from_url(self.config.REDIS_URL, decode_responses=True)
            self.redis_client.ping()
            logger.info("Redis连接成功")
            return True
        except Exception as e:
            logger.error(f"Redis连接失败: {e}")
            return False

    def get_downloaded_files(self):
        """获取已下载的文件ID集合"""
        try:
            html_files = glob.glob(os.path.join(self.html_dir, "*.html"))
            downloaded_ids = set()

            for html_file in html_files:
                article_id = Path(html_file).stem
                downloaded_ids.add(article_id)

            logger.info(f"已下载文件数量: {len(downloaded_ids)}")
            return downloaded_ids
        except Exception as e:
            logger.error(f"获取已下载文件失败: {e}")
            return set()

    def clean_download_queue(self):
        """清理下载队列"""
        queue_name = 'download_tasks'

        try:
            # 获取队列长度
            queue_length = self.redis_client.zcard(queue_name)
            if queue_length == 0:
                logger.info("下载队列已为空")
                return 0

            logger.info(f"开始清理下载队列，当前任务数: {queue_length}")

            # 获取已下载的文件
            downloaded_ids = self.get_downloaded_files()

            # 获取队列中所有任务
            all_tasks = self.redis_client.zrange(queue_name, 0, -1, withscores=True)
            removed_count = 0

            for task_json, score in all_tasks:
                try:
                    task = json.loads(task_json)
                    article_id = task.get('id')

                    if article_id and article_id in downloaded_ids:
                        self.redis_client.zrem(queue_name, task_json)
                        removed_count += 1

                except (json.JSONDecodeError, KeyError):
                    # 移除无效任务
                    self.redis_client.zrem(queue_name, task_json)
                    removed_count += 1
                    continue

            final_length = self.redis_client.zcard(queue_name)
            logger.info(f"清理完成: 移除{removed_count}个任务, 队列长度: {queue_length} -> {final_length}")
            return removed_count

        except Exception as e:
            logger.error(f"清理下载队列失败: {e}")
            return 0

    def clean_failed_tasks(self):
        """清理失败任务队列"""
        failed_queue = 'failed_tasks'

        try:
            failed_count = self.redis_client.zcard(failed_queue)
            if failed_count > 0:
                # 清理超过24小时的失败任务
                cutoff_time = time.time() - 24 * 3600
                removed = self.redis_client.zremrangebyscore(failed_queue, 0, cutoff_time)
                logger.info(f"清理失败任务: 移除{removed}个超过24小时的失败任务")
                return removed
            return 0
        except Exception as e:
            logger.error(f"清理失败任务失败: {e}")
            return 0

    def run_cleanup(self):
        """执行清理操作"""
        if not self.connect_redis():
            return False

        logger.info("=== 开始自动队列清理 ===")

        # 清理下载队列
        download_removed = self.clean_download_queue()

        # 清理失败任务
        failed_removed = self.clean_failed_tasks()

        self.last_cleanup = datetime.now()

        logger.info(f"清理完成: 下载队列移除{download_removed}个, 失败队列移除{failed_removed}个")
        return True

    def should_run_cleanup(self):
        """判断是否需要运行清理"""
        if not self.last_cleanup:
            return True

        # 每30分钟运行一次清理
        time_since_last = (datetime.now() - self.last_cleanup).total_seconds()
        return time_since_last > 1800  # 30分钟

    def daemon_mode(self, interval=300):
        """守护进程模式，定期清理"""
        logger.info(f"启动守护进程模式，检查间隔: {interval}秒")

        while True:
            try:
                if self.should_run_cleanup():
                    self.run_cleanup()

                time.sleep(interval)

            except KeyboardInterrupt:
                logger.info("收到中断信号，退出守护进程")
                break
            except Exception as e:
                logger.error(f"守护进程错误: {e}")
                time.sleep(60)  # 出错后等待1分钟再继续

def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='自动队列清理工具')
    parser.add_argument('--daemon', action='store_true', help='以守护进程模式运行')
    parser.add_argument('--interval', type=int, default=300, help='守护进程检查间隔(秒)')
    args = parser.parse_args()

    cleaner = AutoQueueCleaner()

    if args.daemon:
        cleaner.daemon_mode(args.interval)
    else:
        cleaner.run_cleanup()

if __name__ == '__main__':
    main()