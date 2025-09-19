#!/usr/bin/env python3
"""
Download Service Launcher
文章下载服务启动器
"""

import sys
import os
import time
import threading
import logging
from datetime import datetime
from flask import Flask, jsonify, request

# 添加路径
sys.path.append('/home/azureuser/repository/genesis-connector')

from config import Config
from services.download.utils.download_engine import DownloadEngine

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('download-service')

class DownloadService:
    """下载服务"""

    def __init__(self):
        self.config = Config.from_env()
        # 添加下载特定配置
        self.config.STORAGE_BASE_PATH = getattr(self.config, 'STORAGE_BASE_PATH', '/tmp/genesis-content')
        self.config.DOWNLOAD_TIMEOUT = getattr(self.config, 'DOWNLOAD_TIMEOUT', 30)
        self.config.MAX_DOWNLOAD_RETRIES = getattr(self.config, 'MAX_DOWNLOAD_RETRIES', 3)

        self.download_engine = DownloadEngine(self.config)
        self.is_running = False
        self.worker_thread = None

    def run_download_batch(self, max_tasks=10):
        """运行一批下载任务"""
        logger.info(f"运行下载批次，最大任务数: {max_tasks}")
        try:
            result = self.download_engine.run_download_worker(max_tasks=max_tasks)
            logger.info(f"下载批次完成: {result}")
            return result
        except Exception as e:
            logger.error(f"下载批次执行失败: {e}")
            return {'error': str(e)}

    def start_worker(self, batch_size=10, interval=60):
        """启动下载工作者"""
        if self.is_running:
            return {'message': '下载工作者已在运行'}

        def worker_loop():
            logger.info(f"启动下载工作者，批次大小: {batch_size}, 间隔: {interval}秒")
            while self.is_running:
                try:
                    result = self.run_download_batch(max_tasks=batch_size)
                    # 如果没有处理任何任务，等待更长时间
                    if result.get('processed', 0) == 0:
                        time.sleep(interval * 2)  # 双倍等待时间
                    else:
                        time.sleep(interval)
                except Exception as e:
                    logger.error(f"下载工作者循环失败: {e}")
                    time.sleep(interval)

        self.is_running = True
        self.worker_thread = threading.Thread(target=worker_loop, daemon=True)
        self.worker_thread.start()
        logger.info("下载工作者已启动")
        return {'message': '下载工作者已启动'}

    def stop_worker(self):
        """停止下载工作者"""
        self.is_running = False
        logger.info("下载工作者已停止")
        return {'message': '下载工作者已停止'}

    def get_status(self):
        """获取服务状态"""
        try:
            status = self.download_engine.get_download_status()
            status['worker_running'] = self.is_running
            return status
        except Exception as e:
            logger.error(f"获取状态失败: {e}")
            return {
                'service': 'download',
                'status': 'error',
                'error': str(e)
            }

    def cleanup_old_files(self, days=30):
        """清理旧文件"""
        try:
            return self.download_engine.cleanup_old_files(days=days)
        except Exception as e:
            logger.error(f"清理文件失败: {e}")
            return {'error': str(e)}

    def download_single_article(self, article_data):
        """下载单篇文章（用于测试）"""
        try:
            # 手动创建任务格式
            task = {
                'id': article_data.get('id'),
                'url': article_data.get('url'),
                'title': article_data.get('title', ''),
                'mp_name': article_data.get('mp_name', ''),
                'mp_id': article_data.get('mp_id', ''),
                'publish_time': article_data.get('publish_time'),
                'created_at': datetime.utcnow().isoformat(),
                'retry_count': 0
            }

            result = self.download_engine._process_download_task(task)
            return result

        except Exception as e:
            logger.error(f"单篇下载失败: {e}")
            return {'error': str(e)}

# 创建Flask应用
app = Flask(__name__)
download_service = DownloadService()

@app.route('/health')
def health():
    """健康检查"""
    return jsonify({
        'status': 'healthy',
        'service': 'download',
        'timestamp': datetime.utcnow().isoformat()
    })

@app.route('/status')
def status():
    """服务状态"""
    return jsonify(download_service.get_status())

@app.route('/download-batch', methods=['POST'])
def download_batch():
    """运行下载批次"""
    max_tasks = request.json.get('max_tasks', 10) if request.json else 10
    result = download_service.run_download_batch(max_tasks=max_tasks)
    return jsonify(result)

@app.route('/start-worker', methods=['POST'])
def start_worker():
    """启动下载工作者"""
    data = request.json or {}
    batch_size = data.get('batch_size', 10)
    interval = data.get('interval', 60)
    result = download_service.start_worker(batch_size=batch_size, interval=interval)
    return jsonify(result)

@app.route('/stop-worker', methods=['POST'])
def stop_worker():
    """停止下载工作者"""
    result = download_service.stop_worker()
    return jsonify(result)

@app.route('/cleanup', methods=['POST'])
def cleanup():
    """清理旧文件"""
    days = request.json.get('days', 30) if request.json else 30
    result = download_service.cleanup_old_files(days=days)
    return jsonify(result)

@app.route('/download-single', methods=['POST'])
def download_single():
    """下载单篇文章（测试用）"""
    if not request.json:
        return jsonify({'error': '需要文章数据'}), 400

    result = download_service.download_single_article(request.json)
    return jsonify(result)

@app.route('/queue-stats')
def queue_stats():
    """队列统计"""
    try:
        stats = download_service.download_engine.queue_manager.get_queue_stats()
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("=== Genesis Connector Download Service ===")
    print("启动文章下载服务...")
    print("")
    print("可用端点:")
    print("  GET  /health           - 健康检查")
    print("  GET  /status           - 服务状态")
    print("  GET  /queue-stats      - 队列统计")
    print("  POST /download-batch   - 运行下载批次")
    print("  POST /start-worker     - 启动下载工作者")
    print("  POST /stop-worker      - 停止下载工作者")
    print("  POST /download-single  - 下载单篇文章")
    print("  POST /cleanup          - 清理旧文件")
    print("")
    print("服务地址: http://localhost:5003")
    print("")

    # 启动服务
    app.run(host='0.0.0.0', port=5003, debug=False)