#!/usr/bin/env python3
"""
Text Extraction Engine
文本提取引擎 - 从队列读取任务并处理
"""

import logging
import time
import os
import json
import trafilatura
from datetime import datetime
from typing import Dict, Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)

class TextExtractionEngine:
    """文本提取引擎"""

    def __init__(self, config):
        self.config = config

        # 导入队列管理器和数据库管理器
        import sys
        sys.path.append('/home/azureuser/repository/genesis-connector/services/discovery')
        from utils.queue_manager import QueueManager
        from models.database import DatabaseManager

        self.queue_manager = QueueManager(config.REDIS_URL)
        self.db_manager = DatabaseManager(config.POSTGRES_URL)

        # 存储路径
        self.html_dir = getattr(config, 'HTML_DIR', '/tmp/genesis-content/html')
        self.output_dir = getattr(config, 'TEXT_OUTPUT_DIR', '/tmp/genesis-content/text')
        self._ensure_output_dir()

        # 统计信息
        self.stats = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'last_run': None
        }

    def _ensure_output_dir(self):
        """确保输出目录存在"""
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

    def extract_text_from_html_content(self, html_content: str) -> Optional[str]:
        """从HTML内容提取文本"""
        try:
            text = trafilatura.extract(html_content)
            return text
        except Exception as e:
            logger.error(f"Failed to extract text from HTML content: {e}")
            return None

    def extract_text_from_file(self, html_file_path: str) -> Optional[str]:
        """从HTML文件提取文本"""
        try:
            with open(html_file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            return self.extract_text_from_html_content(html_content)
        except Exception as e:
            logger.error(f"Failed to extract text from {html_file_path}: {e}")
            return None

    def save_text_to_file(self, text: str, output_file_path: str) -> bool:
        """保存文本到文件"""
        try:
            with open(output_file_path, 'w', encoding='utf-8') as f:
                f.write(text)
            return True
        except Exception as e:
            logger.error(f"Failed to save text to {output_file_path}: {e}")
            return False

    def process_single_task(self, task_data: Dict) -> Dict:
        """处理单个提取任务"""
        try:
            article_id = task_data.get('id')
            if not article_id:
                return {'success': False, 'error': 'Missing article ID'}

            # 构建HTML文件路径
            html_file_path = os.path.join(self.html_dir, f"{article_id}.html")

            if not os.path.exists(html_file_path):
                logger.warning(f"HTML file not found: {html_file_path}")
                return {'success': False, 'error': 'HTML file not found'}

            # 提取文本
            text = self.extract_text_from_file(html_file_path)
            if not text:
                return {'success': False, 'error': 'No text extracted'}

            # 生成输出文件路径
            output_file_path = os.path.join(self.output_dir, f"{article_id}.txt")

            # 保存文本
            if self.save_text_to_file(text, output_file_path):
                # 更新数据库状态
                try:
                    self.db_manager.update_article_status(
                        article_id, 'parse_status', 'completed'
                    )
                    # 可以添加更多元数据
                    metadata = {
                        'text_length': len(text),
                        'extracted_at': datetime.utcnow().isoformat(),
                        'output_file': output_file_path
                    }
                    self.db_manager.update_article_metadata(article_id, 'text_extraction', metadata)
                except Exception as e:
                    logger.warning(f"Failed to update database for {article_id}: {e}")

                return {
                    'success': True,
                    'article_id': article_id,
                    'input_file': html_file_path,
                    'output_file': output_file_path,
                    'text_length': len(text)
                }
            else:
                return {'success': False, 'error': 'Failed to save text'}

        except Exception as e:
            logger.error(f"Error processing task {task_data}: {e}")
            return {'success': False, 'error': str(e)}

    def run_extraction_worker(self, max_tasks: int = 10) -> Dict:
        """运行文本提取工作者"""
        start_time = time.time()
        run_stats = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'start_time': datetime.utcnow().isoformat(),
            'duration': 0
        }

        logger.info(f"开始提取工作者，最大任务数: {max_tasks}")

        try:
            # 从下载完成队列或解析队列获取任务
            tasks_processed = 0

            while tasks_processed < max_tasks:
                # 尝试从解析队列获取任务
                task = self.queue_manager.pop_parse_task()

                if not task:
                    # 如果解析队列为空，尝试从已下载的文件创建任务
                    task = self._create_task_from_downloaded_files(tasks_processed)

                if not task:
                    logger.info("没有更多任务可处理")
                    break

                # 处理任务
                result = self.process_single_task(task)
                run_stats['processed'] += 1
                tasks_processed += 1

                if result['success']:
                    run_stats['successful'] += 1
                    self.stats['successful'] += 1
                    logger.debug(f"成功提取文本: {result.get('article_id')}")
                else:
                    run_stats['failed'] += 1
                    self.stats['failed'] += 1
                    logger.warning(f"提取失败: {result.get('error')}")

                self.stats['processed'] += 1

                # 短暂延迟避免过度消耗资源
                time.sleep(0.01)

        except Exception as e:
            logger.error(f"提取工作者运行错误: {e}")

        run_stats['duration'] = time.time() - start_time
        self.stats['last_run'] = run_stats

        logger.info(f"提取工作者完成: 处理 {run_stats['processed']}, 成功 {run_stats['successful']}, 失败 {run_stats['failed']}")
        return run_stats

    def _create_task_from_downloaded_files(self, offset: int = 0) -> Optional[Dict]:
        """从已下载的文件创建任务"""
        try:
            import glob
            html_files = glob.glob(os.path.join(self.html_dir, "*.html"))

            if offset >= len(html_files):
                return None

            html_file = html_files[offset]
            article_id = Path(html_file).stem

            # 检查是否已经提取过
            output_file = os.path.join(self.output_dir, f"{article_id}.txt")
            if os.path.exists(output_file):
                return None  # 已经提取过，跳过

            return {
                'id': article_id,
                'source': 'file_discovery'
            }

        except Exception as e:
            logger.error(f"创建文件任务失败: {e}")
            return None

    def get_extraction_status(self) -> Dict:
        """获取提取状态"""
        try:
            import glob
            html_files = glob.glob(os.path.join(self.html_dir, "*.html"))
            text_files = glob.glob(os.path.join(self.output_dir, "*.txt"))

            queue_stats = self.queue_manager.get_queue_stats()

            return {
                'healthy': True,
                'html_files_count': len(html_files),
                'text_files_count': len(text_files),
                'remaining_to_process': len(html_files) - len(text_files),
                'queue_stats': queue_stats,
                'worker_stats': self.stats,
                'html_dir': self.html_dir,
                'output_dir': self.output_dir
            }

        except Exception as e:
            logger.error(f"获取提取状态失败: {e}")
            return {
                'healthy': False,
                'error': str(e)
            }

    def cleanup_old_files(self, days: int = 30) -> Dict:
        """清理旧文件"""
        try:
            cutoff_time = datetime.utcnow() - timedelta(days=days)

            import glob
            text_files = glob.glob(os.path.join(self.output_dir, "*.txt"))

            removed_count = 0
            for file_path in text_files:
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                if file_time < cutoff_time:
                    try:
                        os.remove(file_path)
                        removed_count += 1
                    except Exception as e:
                        logger.warning(f"删除文件失败 {file_path}: {e}")

            logger.info(f"清理完成，删除了 {removed_count} 个旧文件")
            return {
                'success': True,
                'removed_count': removed_count,
                'cutoff_days': days
            }

        except Exception as e:
            logger.error(f"清理文件失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }