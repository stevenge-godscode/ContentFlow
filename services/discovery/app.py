#!/usr/bin/env python3
"""
Genesis Connector - Discovery Service
内容发现和任务调度服务
"""

import os
import sys
import logging
import signal
import yaml
from flask import Flask, jsonify, request
from datetime import datetime

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import get_config
from utils.scheduler import DiscoveryScheduler
from utils.discovery_engine import DiscoveryEngine

# 配置日志
def setup_logging(config):
    """设置日志配置"""
    # 从YAML文件加载日志配置
    log_config_file = os.path.join(config.CONFIG_DIR, 'logging.yaml')
    if os.path.exists(log_config_file):
        with open(log_config_file, 'r') as f:
            log_config = yaml.safe_load(f)
        logging.config.dictConfig(log_config)
    else:
        # 默认日志配置
        logging.basicConfig(
            level=getattr(logging, config.LOG_LEVEL),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    return logging.getLogger('discovery')

# 创建Flask应用
def create_app():
    app = Flask(__name__)

    # 加载配置
    config = get_config()
    app.config.from_object(config)

    # 设置日志
    logger = setup_logging(config)

    # 创建调度器
    scheduler = DiscoveryScheduler(config)

    # 存储在app中以便在路由中访问
    app.scheduler = scheduler
    app.logger = logger

    return app

app = create_app()
logger = app.logger

# API路由
@app.route('/health', methods=['GET'])
def health_check():
    """健康检查"""
    try:
        discovery_engine = app.scheduler.get_discovery_engine()
        status = discovery_engine.get_discovery_status()

        return jsonify({
            'service': 'discovery',
            'status': 'healthy' if status.get('healthy', False) else 'unhealthy',
            'timestamp': datetime.utcnow().isoformat(),
            'details': status
        }), 200 if status.get('healthy', False) else 503

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'service': 'discovery',
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/status', methods=['GET'])
def get_status():
    """获取服务状态"""
    try:
        scheduler_status = app.scheduler.get_status()
        discovery_engine = app.scheduler.get_discovery_engine()
        discovery_status = discovery_engine.get_discovery_status()

        return jsonify({
            'service': 'discovery',
            'scheduler': scheduler_status,
            'discovery': discovery_status,
            'timestamp': datetime.utcnow().isoformat()
        })

    except Exception as e:
        logger.error(f"Status check failed: {e}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/discover', methods=['POST'])
def force_discovery():
    """强制执行发现任务"""
    try:
        hours = request.json.get('hours', 24) if request.is_json else 24

        if hours <= 0 or hours > 168:  # 最多一周
            return jsonify({'error': 'Invalid hours parameter (1-168)'}), 400

        logger.info(f"Force discovery requested for {hours} hours")

        discovery_engine = app.scheduler.get_discovery_engine()
        result = discovery_engine.force_discovery(hours)

        return jsonify({
            'status': 'completed',
            'result': result,
            'timestamp': datetime.utcnow().isoformat()
        })

    except Exception as e:
        logger.error(f"Force discovery failed: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/discover/run', methods=['POST'])
def run_discovery_now():
    """立即运行一次发现任务"""
    try:
        logger.info("Manual discovery run requested")
        result = app.scheduler.force_run()

        return jsonify({
            'status': 'completed',
            'result': result,
            'timestamp': datetime.utcnow().isoformat()
        })

    except Exception as e:
        logger.error(f"Manual discovery run failed: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/queue/stats', methods=['GET'])
def get_queue_stats():
    """获取队列统计"""
    try:
        discovery_engine = app.scheduler.get_discovery_engine()
        queue_stats = discovery_engine.queue_manager.get_queue_stats()

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

@app.route('/cleanup', methods=['POST'])
def cleanup_old_data():
    """清理旧数据"""
    try:
        days = request.json.get('days', 30) if request.is_json else 30

        if days <= 0 or days > 365:
            return jsonify({'error': 'Invalid days parameter (1-365)'}), 400

        logger.info(f"Cleanup requested for data older than {days} days")

        discovery_engine = app.scheduler.get_discovery_engine()
        result = discovery_engine.cleanup_old_data(days)

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

@app.route('/config', methods=['GET'])
def get_config_info():
    """获取配置信息"""
    try:
        config = app.config

        config_info = {
            'wewe_rss_url': config.get('WEWE_RSS_URL'),
            'discovery_interval': config.get('DISCOVERY_INTERVAL'),
            'batch_size': config.get('BATCH_SIZE'),
            'max_retries': config.get('MAX_RETRIES'),
            'service_port': config.get('SERVICE_PORT'),
            'log_level': config.get('LOG_LEVEL')
        }

        return jsonify({
            'config': config_info,
            'timestamp': datetime.utcnow().isoformat()
        })

    except Exception as e:
        logger.error(f"Config info failed: {e}")
        return jsonify({
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

# 信号处理
def signal_handler(sig, frame):
    """处理退出信号"""
    logger.info(f"Received signal {sig}, shutting down...")

    if hasattr(app, 'scheduler'):
        app.scheduler.stop()

    sys.exit(0)

# 主函数
def main():
    """主入口函数"""
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting Genesis Connector Discovery Service")

    try:
        # 启动调度器
        app.scheduler.start()

        # 启动Flask应用
        host = app.config.get('SERVICE_HOST', '0.0.0.0')
        port = app.config.get('SERVICE_PORT', 5001)
        debug = app.config.get('DEBUG', False)

        logger.info(f"Starting Flask app on {host}:{port}")
        app.run(host=host, port=port, debug=debug)

    except Exception as e:
        logger.error(f"Failed to start service: {e}")
        sys.exit(1)
    finally:
        # 确保调度器停止
        if hasattr(app, 'scheduler'):
            app.scheduler.stop()

if __name__ == '__main__':
    main()