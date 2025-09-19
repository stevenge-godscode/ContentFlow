import logging
import time
import os
import requests
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from urllib.parse import urljoin, urlparse
from pathlib import Path
import json

logger = logging.getLogger(__name__)

class DownloadEngine:
    """文章下载引擎"""

    def __init__(self, config):
        self.config = config

        # 导入队列管理器和数据库管理器
        import sys
        sys.path.append('/home/azureuser/repository/genesis-connector/services/discovery')
        from utils.queue_manager import QueueManager
        from models.database import DatabaseManager

        self.queue_manager = QueueManager(config.REDIS_URL)
        self.db_manager = DatabaseManager(config.POSTGRES_URL)

        # 配置HTTP会话
        self.session = self._create_session()

        # 存储路径
        self.storage_base = getattr(config, 'STORAGE_BASE_PATH', '/tmp/genesis-content')
        self._ensure_storage_paths()

        # 统计信息
        self.stats = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'last_run': None
        }

    def _create_session(self) -> requests.Session:
        """创建HTTP会话"""
        session = requests.Session()

        # 设置请求头
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })

        # 设置超时
        session.timeout = getattr(self.config, 'DOWNLOAD_TIMEOUT', 30)

        return session

    def _ensure_storage_paths(self):
        """确保存储路径存在"""
        paths = [
            self.storage_base,
            os.path.join(self.storage_base, 'html'),
            os.path.join(self.storage_base, 'images'),
            os.path.join(self.storage_base, 'metadata')
        ]

        for path in paths:
            Path(path).mkdir(parents=True, exist_ok=True)

    def run_download_worker(self, max_tasks: int = 10) -> Dict:
        """运行下载工作者"""
        start_time = time.time()
        run_stats = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'start_time': datetime.utcnow().isoformat(),
            'duration': 0
        }

        logger.info(f"Starting download worker, max_tasks: {max_tasks}")

        try:
            for i in range(max_tasks):
                # 从队列获取任务
                task = self.queue_manager.pop_download_task(timeout=5)
                if not task:
                    logger.info("No more download tasks available")
                    break

                run_stats['processed'] += 1

                try:
                    result = self._process_download_task(task)
                    if result['success']:
                        run_stats['successful'] += 1
                        # 标记任务完成
                        self.queue_manager.complete_download_task(task['id'], result)
                        # 更新数据库状态
                        self.db_manager.update_article_status(
                            task['id'], 'download_status', 'completed'
                        )
                    else:
                        run_stats['failed'] += 1
                        self._handle_download_failure(task, result['error'])

                except Exception as e:
                    logger.error(f"Error processing download task {task.get('id', 'unknown')}: {e}")
                    run_stats['failed'] += 1
                    self._handle_download_failure(task, str(e))

            # 更新统计
            run_stats['duration'] = time.time() - start_time
            self._update_stats(run_stats)

            logger.info(f"Download worker completed: {run_stats['successful']} successful, "
                       f"{run_stats['failed']} failed, {run_stats['skipped']} skipped "
                       f"in {run_stats['duration']:.2f}s")

        except Exception as e:
            logger.error(f"Download worker failed: {e}")
            run_stats['duration'] = time.time() - start_time

        return run_stats

    def _process_download_task(self, task: Dict) -> Dict:
        """处理单个下载任务"""
        article_id = task.get('id')
        article_url = task.get('url')
        article_title = task.get('title', 'Unknown')

        logger.info(f"Downloading article: {article_id} - {article_title[:50]}...")

        try:
            # 1. 下载HTML内容
            html_result = self._download_html(article_url, article_id)
            if not html_result['success']:
                return html_result

            # 2. 提取和下载图片
            images_result = self._download_images(html_result['content'], article_id)

            # 3. 保存元数据
            metadata = self._create_metadata(task, html_result, images_result)
            metadata_result = self._save_metadata(article_id, metadata)

            # 4. 更新数据库
            self._update_database_after_download(article_id, {
                'html_file_path': html_result.get('file_path'),
                'images_dir_path': images_result.get('images_dir'),
                'metadata_file_path': metadata_result.get('file_path'),
                'content_length': len(html_result.get('content', '')),
                'image_count': len(images_result.get('downloaded_images', []))
            })

            return {
                'success': True,
                'html_file': html_result.get('file_path'),
                'images_dir': images_result.get('images_dir'),
                'image_count': len(images_result.get('downloaded_images', [])),
                'metadata_file': metadata_result.get('file_path')
            }

        except Exception as e:
            logger.error(f"Failed to download article {article_id}: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def _download_html(self, url: str, article_id: str) -> Dict:
        """下载HTML内容"""
        try:
            logger.debug(f"Downloading HTML from: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            # 检测编码
            content = response.content
            encoding = response.encoding or 'utf-8'
            if encoding.lower() == 'iso-8859-1':  # 默认编码，通常不正确
                encoding = 'utf-8'

            try:
                html_content = content.decode(encoding)
            except UnicodeDecodeError:
                html_content = content.decode('utf-8', errors='ignore')

            # 保存HTML文件
            file_path = os.path.join(self.storage_base, 'html', f"{article_id}.html")
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(html_content)

            return {
                'success': True,
                'content': html_content,
                'file_path': file_path,
                'encoding': encoding,
                'status_code': response.status_code,
                'content_length': len(html_content)
            }

        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error downloading {url}: {e}")
            return {
                'success': False,
                'error': f"HTTP error: {e}"
            }
        except Exception as e:
            logger.error(f"Error downloading HTML {url}: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def _download_images(self, html_content: str, article_id: str) -> Dict:
        """从HTML中提取并下载图片"""
        try:
            import re
            from urllib.parse import urljoin, urlparse

            images_dir = os.path.join(self.storage_base, 'images', article_id)
            Path(images_dir).mkdir(parents=True, exist_ok=True)

            # 提取图片URL
            img_pattern = r'<img[^>]+src=["\']([^"\']+)["\'][^>]*>'
            img_matches = re.findall(img_pattern, html_content, re.IGNORECASE)

            downloaded_images = []
            failed_images = []

            for i, img_url in enumerate(img_matches[:10]):  # 限制最多10张图片
                try:
                    # 处理相对URL
                    if img_url.startswith('//'):
                        img_url = 'https:' + img_url
                    elif img_url.startswith('/'):
                        continue  # 跳过相对路径，因为我们没有base URL
                    elif not img_url.startswith(('http://', 'https://')):
                        continue  # 跳过其他类型的URL

                    # 生成文件名
                    parsed = urlparse(img_url)
                    ext = os.path.splitext(parsed.path)[1]
                    if not ext:
                        ext = '.jpg'  # 默认扩展名

                    filename = f"image_{i:02d}{ext}"
                    file_path = os.path.join(images_dir, filename)

                    # 下载图片
                    response = self.session.get(img_url, timeout=15, stream=True)
                    response.raise_for_status()

                    with open(file_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)

                    downloaded_images.append({
                        'url': img_url,
                        'file_path': file_path,
                        'filename': filename,
                        'size': os.path.getsize(file_path)
                    })

                    logger.debug(f"Downloaded image: {filename}")

                except Exception as e:
                    logger.warning(f"Failed to download image {img_url}: {e}")
                    failed_images.append({
                        'url': img_url,
                        'error': str(e)
                    })

            return {
                'success': True,
                'images_dir': images_dir,
                'downloaded_images': downloaded_images,
                'failed_images': failed_images,
                'total_found': len(img_matches),
                'total_downloaded': len(downloaded_images)
            }

        except Exception as e:
            logger.error(f"Error downloading images for {article_id}: {e}")
            return {
                'success': False,
                'error': str(e),
                'images_dir': images_dir if 'images_dir' in locals() else None,
                'downloaded_images': [],
                'failed_images': []
            }

    def _create_metadata(self, task: Dict, html_result: Dict, images_result: Dict) -> Dict:
        """创建元数据"""
        return {
            'article_id': task.get('id'),
            'title': task.get('title'),
            'url': task.get('url'),
            'mp_name': task.get('mp_name'),
            'mp_id': task.get('mp_id'),
            'publish_time': task.get('publish_time'),
            'download_info': {
                'downloaded_at': datetime.utcnow().isoformat(),
                'html_file': html_result.get('file_path'),
                'html_size': html_result.get('content_length', 0),
                'html_encoding': html_result.get('encoding'),
                'images_dir': images_result.get('images_dir'),
                'image_count': len(images_result.get('downloaded_images', [])),
                'images_failed': len(images_result.get('failed_images', [])),
                'download_duration': time.time() - time.time()  # 这里应该传入开始时间
            },
            'images': images_result.get('downloaded_images', []),
            'failed_images': images_result.get('failed_images', [])
        }

    def _save_metadata(self, article_id: str, metadata: Dict) -> Dict:
        """保存元数据"""
        try:
            file_path = os.path.join(self.storage_base, 'metadata', f"{article_id}.json")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)

            return {
                'success': True,
                'file_path': file_path
            }

        except Exception as e:
            logger.error(f"Error saving metadata for {article_id}: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def _update_database_after_download(self, article_id: str, download_data: Dict):
        """下载完成后更新数据库"""
        try:
            # 更新文章记录
            self.db_manager.update_article_paths(article_id, {
                'html_file_path': download_data.get('html_file_path'),
                'images_dir_path': download_data.get('images_dir_path'),
                'metadata_file_path': download_data.get('metadata_file_path'),
                'content_length': download_data.get('content_length'),
                'image_count': download_data.get('image_count'),
                'downloaded_at': datetime.utcnow()
            })

        except Exception as e:
            logger.error(f"Error updating database after download {article_id}: {e}")

    def _handle_download_failure(self, task: Dict, error: str):
        """处理下载失败"""
        article_id = task.get('id')
        retry_count = task.get('retry_count', 0)
        max_retries = getattr(self.config, 'MAX_DOWNLOAD_RETRIES', 3)

        try:
            if retry_count < max_retries:
                # 重新入队，增加重试次数
                task['retry_count'] = retry_count + 1
                task['last_error'] = error
                task['last_retry_at'] = datetime.utcnow().isoformat()

                self.queue_manager.push_download_task(task)
                logger.info(f"Re-queued failed download {article_id} (retry {retry_count + 1}/{max_retries})")
            else:
                # 达到最大重试次数，标记为失败
                self.db_manager.update_article_status(
                    article_id, 'download_status', 'failed', error
                )
                logger.error(f"Download permanently failed for {article_id}: {error}")

        except Exception as e:
            logger.error(f"Error handling download failure for {article_id}: {e}")

    def _update_stats(self, run_stats: Dict):
        """更新统计信息"""
        try:
            # 更新内存统计
            self.stats['processed'] += run_stats['processed']
            self.stats['successful'] += run_stats['successful']
            self.stats['failed'] += run_stats['failed']
            self.stats['skipped'] += run_stats['skipped']
            self.stats['last_run'] = run_stats

            # 更新数据库统计
            today = datetime.utcnow().date().isoformat()
            stats_data = {
                'downloaded_count': run_stats['successful']
            }
            self.db_manager.update_processing_stats(today, stats_data)

        except Exception as e:
            logger.error(f"Error updating stats: {e}")

    def get_download_status(self) -> Dict:
        """获取下载服务状态"""
        try:
            status = {
                'service': 'download',
                'status': 'running',
                'uptime': time.time(),
                'stats': self.stats.copy(),
                'config': {
                    'timeout': getattr(self.config, 'DOWNLOAD_TIMEOUT', 30),
                    'max_retries': getattr(self.config, 'MAX_DOWNLOAD_RETRIES', 3),
                    'storage_base': self.storage_base
                }
            }

            # 队列状态
            queue_stats = self.queue_manager.get_queue_stats()
            status['queue_stats'] = queue_stats

            # 健康状态
            health = {
                'redis': self.queue_manager.health_check(),
                'database': self.db_manager.health_check(),
                'storage': os.path.exists(self.storage_base)
            }
            status['health'] = health
            status['healthy'] = all(health.values())

            return status

        except Exception as e:
            logger.error(f"Error getting download status: {e}")
            return {
                'service': 'download',
                'status': 'error',
                'error': str(e)
            }

    def cleanup_old_files(self, days: int = 30) -> Dict:
        """清理旧文件"""
        try:
            cutoff_time = time.time() - (days * 24 * 3600)
            cleaned_files = 0

            for root, dirs, files in os.walk(self.storage_base):
                for file in files:
                    file_path = os.path.join(root, file)
                    if os.path.getmtime(file_path) < cutoff_time:
                        try:
                            os.remove(file_path)
                            cleaned_files += 1
                        except Exception as e:
                            logger.warning(f"Failed to remove old file {file_path}: {e}")

            result = {
                'cleaned_files': cleaned_files,
                'cleanup_date': datetime.utcnow().isoformat()
            }

            logger.info(f"Cleanup completed: {result}")
            return result

        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
            return {'error': str(e)}