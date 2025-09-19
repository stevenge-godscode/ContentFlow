import schedule
import threading
import time
import logging
from datetime import datetime
from .discovery_engine import DiscoveryEngine

logger = logging.getLogger(__name__)

class DiscoveryScheduler:
    """发现服务调度器"""

    def __init__(self, config):
        self.config = config
        self.discovery_engine = DiscoveryEngine(config)
        self.is_running = False
        self.scheduler_thread = None
        self.last_run = None
        self.next_run = None

    def start(self):
        """启动调度器"""
        if self.is_running:
            logger.warning("Scheduler is already running")
            return

        logger.info(f"Starting discovery scheduler with {self.config.DISCOVERY_INTERVAL}s interval")

        # 设置定时任务
        schedule.every(self.config.DISCOVERY_INTERVAL).seconds.do(self._run_discovery_job)

        # 启动调度线程
        self.is_running = True
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()

        # 立即执行一次发现任务
        self._run_discovery_job()

        logger.info("Discovery scheduler started")

    def stop(self):
        """停止调度器"""
        if not self.is_running:
            return

        logger.info("Stopping discovery scheduler")
        self.is_running = False

        # 清除所有任务
        schedule.clear()

        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)

        logger.info("Discovery scheduler stopped")

    def _scheduler_loop(self):
        """调度器主循环"""
        while self.is_running:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")
                time.sleep(5)

    def _run_discovery_job(self):
        """执行发现任务"""
        try:
            logger.info("Running scheduled discovery job")
            self.last_run = datetime.utcnow()

            # 执行发现
            result = self.discovery_engine.run_discovery()

            # 计算下次运行时间
            self.next_run = datetime.utcnow().timestamp() + self.config.DISCOVERY_INTERVAL

            logger.info(f"Discovery job completed: {result}")

        except Exception as e:
            logger.error(f"Discovery job failed: {e}")

    def get_status(self) -> dict:
        """获取调度器状态"""
        return {
            'is_running': self.is_running,
            'last_run': self.last_run.isoformat() if self.last_run else None,
            'next_run': datetime.fromtimestamp(self.next_run).isoformat() if self.next_run else None,
            'interval': self.config.DISCOVERY_INTERVAL,
            'scheduled_jobs': len(schedule.jobs)
        }

    def force_run(self) -> dict:
        """强制执行发现任务"""
        logger.info("Force running discovery job")
        return self.discovery_engine.run_discovery()

    def get_discovery_engine(self) -> DiscoveryEngine:
        """获取发现引擎实例"""
        return self.discovery_engine