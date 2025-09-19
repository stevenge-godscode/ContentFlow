#!/usr/bin/env python3
"""
Text Extraction Service Launcher
文本提取服务启动器 - 基于消息队列
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
from services.parser.utils.text_extraction_engine import TextExtractionEngine

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('text-extraction-service')

class TextExtractionService:
    """文本提取服务"""

    def __init__(self):
        self.config = Config.from_env()
        # 添加提取特定配置
        self.config.HTML_DIR = getattr(self.config, 'HTML_DIR', '/tmp/genesis-content/html')
        self.config.TEXT_OUTPUT_DIR = getattr(self.config, 'TEXT_OUTPUT_DIR', '/tmp/genesis-content/text')
        self.config.EXTRACTION_TIMEOUT = getattr(self.config, 'EXTRACTION_TIMEOUT', 30)
        self.config.MAX_EXTRACTION_RETRIES = getattr(self.config, 'MAX_EXTRACTION_RETRIES', 3)

        self.extraction_engine = TextExtractionEngine(self.config)
        self.is_running = False
        self.worker_thread = None

    def run_extraction_batch(self, max_tasks=10):
        """运行一批提取任务"""
        logger.info(f"运行提取批次，最大任务数: {max_tasks}")
        try:
            result = self.extraction_engine.run_extraction_worker(max_tasks=max_tasks)
            logger.info(f"提取批次完成: {result}")
            return result
        except Exception as e:
            logger.error(f"提取批次执行失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'processed': 0,
                'successful': 0,
                'failed': 0
            }

    def start_worker(self):
        """启动工作者线程"""
        if self.is_running:
            logger.warning("工作者已在运行")
            return {'success': False, 'message': 'Worker already running'}

        self.is_running = True
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
        logger.info("提取工作者已启动")
        return {'success': True, 'message': 'Worker started'}

    def stop_worker(self):
        """停止工作者线程"""
        if not self.is_running:
            logger.warning("工作者未在运行")
            return {'success': False, 'message': 'Worker not running'}

        self.is_running = False
        if self.worker_thread:
            self.worker_thread.join(timeout=5)
        logger.info("提取工作者已停止")
        return {'success': True, 'message': 'Worker stopped'}

    def _worker_loop(self):
        """工作者循环"""
        logger.info("工作者循环开始")

        while self.is_running:
            try:
                # 运行一批提取任务
                result = self.run_extraction_batch(max_tasks=10)

                # 如果没有处理任何任务，稍作等待
                if result['processed'] == 0:
                    time.sleep(5)
                else:
                    # 处理了任务，短暂休息后继续
                    time.sleep(1)

            except Exception as e:
                logger.error(f"工作者循环错误: {e}")
                time.sleep(10)  # 错误时等待更长时间

        logger.info("工作者循环结束")

    def get_status(self):
        """获取服务状态"""
        try:
            extraction_status = self.extraction_engine.get_extraction_status()
            queue_stats = self.extraction_engine.queue_manager.get_queue_stats()

            return {
                'service': 'text-extraction',
                'status': 'running',
                'worker_running': self.is_running,
                'healthy': extraction_status.get('healthy', False),
                'extraction_status': extraction_status,
                'queue_stats': queue_stats,
                'stats': self.extraction_engine.stats,
                'config': {
                    'html_dir': self.config.HTML_DIR,
                    'output_dir': self.config.TEXT_OUTPUT_DIR,
                    'timeout': self.config.EXTRACTION_TIMEOUT,
                    'max_retries': self.config.MAX_EXTRACTION_RETRIES
                },
                'uptime': time.time()
            }

        except Exception as e:
            logger.error(f"获取状态失败: {e}")
            return {
                'service': 'text-extraction',
                'status': 'error',
                'error': str(e)
            }

# 创建Flask应用和服务实例
app = Flask(__name__)
extraction_service = TextExtractionService()

# API路由
@app.route('/health', methods=['GET'])
def health_check():
    """健康检查"""
    try:
        status = extraction_service.get_status()
        return jsonify({
            'service': 'text-extraction',
            'status': 'healthy' if status.get('healthy', False) else 'unhealthy',
            'timestamp': datetime.utcnow().isoformat(),
            'details': status
        }), 200 if status.get('healthy', False) else 503

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'service': 'text-extraction',
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/status', methods=['GET'])
def get_status():
    """获取服务状态"""
    try:
        status = extraction_service.get_status()
        return jsonify(status)
    except Exception as e:
        logger.error(f"Status check failed: {e}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/queue-stats', methods=['GET'])
def get_queue_stats():
    """获取队列统计"""
    try:
        queue_stats = extraction_service.extraction_engine.queue_manager.get_queue_stats()
        return jsonify({
            'queue_stats': queue_stats,
            'timestamp': datetime.utcnow().isoformat()
        })
    except Exception as e:
        logger.error(f"Queue stats failed: {e}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/extract-batch', methods=['POST'])
def extract_batch():
    """运行提取批次"""
    try:
        data = request.get_json() or {}
        max_tasks = data.get('max_tasks', 10)

        logger.info(f"手动提取批次请求: max_tasks={max_tasks}")
        result = extraction_service.run_extraction_batch(max_tasks)

        return jsonify({
            'status': 'completed',
            'result': result,
            'timestamp': datetime.utcnow().isoformat()
        })

    except Exception as e:
        logger.error(f"Extract batch failed: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/start-worker', methods=['POST'])
def start_worker():
    """启动提取工作者"""
    try:
        logger.info("启动工作者请求")
        result = extraction_service.start_worker()

        return jsonify({
            'status': 'completed',
            'result': result,
            'timestamp': datetime.utcnow().isoformat()
        })

    except Exception as e:
        logger.error(f"Start worker failed: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/stop-worker', methods=['POST'])
def stop_worker():
    """停止提取工作者"""
    try:
        logger.info("停止工作者请求")
        result = extraction_service.stop_worker()

        return jsonify({
            'status': 'completed',
            'result': result,
            'timestamp': datetime.utcnow().isoformat()
        })

    except Exception as e:
        logger.error(f"Stop worker failed: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/cleanup', methods=['POST'])
def cleanup_old_files():
    """清理旧文件"""
    try:
        data = request.get_json() or {}
        days = data.get('days', 30)

        if days <= 0 or days > 365:
            return jsonify({'error': 'Invalid days parameter (1-365)'}), 400

        logger.info(f"清理请求: days={days}")
        result = extraction_service.extraction_engine.cleanup_old_files(days)

        return jsonify({
            'status': 'completed',
            'result': result,
            'timestamp': datetime.utcnow().isoformat()
        })

    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

# 错误处理
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'error': 'Not found',
        'message': 'The requested endpoint does not exist',
        'timestamp': datetime.utcnow().isoformat()
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'error': 'Internal server error',
        'message': 'An unexpected error occurred',
        'timestamp': datetime.utcnow().isoformat()
    }), 500

def main():
    """主函数"""
    print("=== Genesis Connector Text Extraction Service ===")
    print("启动文本提取服务...")
    print()
    print("可用端点:")
    print("  GET  /health           - 健康检查")
    print("  GET  /status           - 服务状态")
    print("  GET  /queue-stats      - 队列统计")
    print("  POST /extract-batch    - 运行提取批次")
    print("  POST /start-worker     - 启动提取工作者")
    print("  POST /stop-worker      - 停止提取工作者")
    print("  POST /cleanup          - 清理旧文件")
    print()

    try:
        # 获取初始状态
        status = extraction_service.get_status()
        print(f"HTML文件目录: {status['config']['html_dir']}")
        print(f"输出目录: {status['config']['output_dir']}")

        if status.get('extraction_status'):
            ext_status = status['extraction_status']
            print(f"HTML文件: {ext_status.get('html_files_count', 0)}")
            print(f"已提取文本文件: {ext_status.get('text_files_count', 0)}")
            print(f"待处理: {ext_status.get('remaining_to_process', 0)}")

        print()

        # 启动Flask应用
        host = os.getenv('SERVICE_HOST', '0.0.0.0')
        port = int(os.getenv('TEXT_EXTRACTION_PORT', 5006))
        debug = os.getenv('DEBUG', 'False').lower() == 'true'

        print(f"服务地址: http://localhost:{port}")
        print()
        logger.info(f"Starting Flask app on {host}:{port}")

        app.run(host=host, port=port, debug=debug)

    except Exception as e:
        logger.error(f"Failed to start service: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()