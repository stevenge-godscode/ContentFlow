#!/usr/bin/env python3
"""
Genesis Connector - Parser Service
HTML解析和内容提取服务
"""

import os
import sys
import logging
import signal
from flask import Flask, jsonify, request
from datetime import datetime

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def create_app():
    app = Flask(__name__)

    # 基础配置
    app.config['DEBUG'] = os.getenv('DEBUG', 'False').lower() == 'true'

    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger = logging.getLogger('parser')
    app.logger = logger

    return app

app = create_app()
logger = app.logger

# API路由
@app.route('/health', methods=['GET'])
def health_check():
    """健康检查"""
    try:
        return jsonify({
            'service': 'parser',
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'service': 'parser',
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/status', methods=['GET'])
def get_status():
    """获取服务状态"""
    try:
        return jsonify({
            'service': 'parser',
            'status': 'running',
            'timestamp': datetime.utcnow().isoformat()
        })
    except Exception as e:
        logger.error(f"Status check failed: {e}")
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
    sys.exit(0)

# 主函数
def main():
    """主入口函数"""
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting Genesis Connector Parser Service")

    try:
        # 启动Flask应用
        host = os.getenv('SERVICE_HOST', '0.0.0.0')
        port = int(os.getenv('PARSER_PORT', 5003))
        debug = os.getenv('DEBUG', 'False').lower() == 'true'

        logger.info(f"Starting Flask app on {host}:{port}")
        app.run(host=host, port=port, debug=debug)

    except Exception as e:
        logger.error(f"Failed to start service: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()