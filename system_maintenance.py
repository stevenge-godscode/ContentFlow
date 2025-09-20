#!/usr/bin/env python3
"""
系统维护脚本 - 综合性维护工具
"""

import sys
import os
import subprocess
import logging
from datetime import datetime
from pathlib import Path

# 添加路径
sys.path.append('/home/azureuser/repository/genesis-connector')
from config import Config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SystemMaintenance:
    """系统维护工具"""

    def __init__(self):
        self.config = Config.from_env()
        self.html_dir = '/tmp/genesis-content/html'
        self.text_dir = '/tmp/genesis-content/text'

    def check_file_consistency(self):
        """检查文件一致性"""
        logger.info("=== 检查文件一致性 ===")

        try:
            import glob

            html_files = glob.glob(os.path.join(self.html_dir, "*.html"))
            text_files = glob.glob(os.path.join(self.text_dir, "*.txt"))

            html_ids = {Path(f).stem for f in html_files}
            text_ids = {Path(f).stem for f in text_files}

            missing_text = html_ids - text_ids
            orphaned_text = text_ids - html_ids

            logger.info(f"HTML文件: {len(html_files)}")
            logger.info(f"文本文件: {len(text_files)}")
            logger.info(f"缺失文本文件: {len(missing_text)}")
            logger.info(f"孤立文本文件: {len(orphaned_text)}")

            if missing_text:
                logger.warning(f"缺失文本文件: {list(missing_text)[:10]}...")  # 只显示前10个

            return {
                'html_count': len(html_files),
                'text_count': len(text_files),
                'missing_text': list(missing_text),
                'orphaned_text': list(orphaned_text),
                'consistent': len(missing_text) == 0
            }

        except Exception as e:
            logger.error(f"检查文件一致性失败: {e}")
            return {'error': str(e)}

    def fix_missing_text_files(self):
        """修复缺失的文本文件"""
        logger.info("=== 修复缺失的文本文件 ===")

        consistency = self.check_file_consistency()
        missing_text = consistency.get('missing_text', [])

        if not missing_text:
            logger.info("没有缺失的文本文件")
            return 0

        fixed_count = 0
        try:
            import trafilatura

            for file_id in missing_text[:50]:  # 限制一次处理50个
                html_path = os.path.join(self.html_dir, f"{file_id}.html")
                text_path = os.path.join(self.text_dir, f"{file_id}.txt")

                if not os.path.exists(html_path):
                    continue

                try:
                    with open(html_path, 'r', encoding='utf-8') as f:
                        html_content = f.read()

                    text = trafilatura.extract(html_content)
                    if text:
                        with open(text_path, 'w', encoding='utf-8') as f:
                            f.write(text)
                        fixed_count += 1
                        logger.info(f"修复: {file_id}")

                except Exception as e:
                    logger.error(f"修复失败 {file_id}: {e}")

            logger.info(f"修复完成: {fixed_count}/{len(missing_text)}")
            return fixed_count

        except Exception as e:
            logger.error(f"修复过程失败: {e}")
            return 0

    def clean_queues(self):
        """清理队列"""
        logger.info("=== 清理队列 ===")

        try:
            # 运行队列清理脚本
            result = subprocess.run([
                sys.executable, 'clean_queue.py'
            ], capture_output=True, text=True, timeout=60)

            if result.returncode == 0:
                logger.info("队列清理成功")
                return True
            else:
                logger.error(f"队列清理失败: {result.stderr}")
                return False

        except Exception as e:
            logger.error(f"运行队列清理失败: {e}")
            return False

    def restart_workers(self):
        """重启工作者"""
        logger.info("=== 重启工作者 ===")

        try:
            import requests

            # 停止下载工作者
            response = requests.post('http://localhost:5003/stop-worker', timeout=10)
            if response.status_code == 200:
                logger.info("下载工作者已停止")
            else:
                logger.warning("停止下载工作者失败")

            # 停止文本提取工作者
            response = requests.post('http://localhost:5006/stop-worker', timeout=10)
            if response.status_code == 200:
                logger.info("文本提取工作者已停止")
            else:
                logger.warning("停止文本提取工作者失败")

            # 等待一下
            import time
            time.sleep(2)

            # 启动下载工作者
            response = requests.post('http://localhost:5003/start-worker', timeout=10)
            if response.status_code == 200:
                logger.info("下载工作者已启动")
            else:
                logger.warning("启动下载工作者失败")

            # 启动文本提取工作者
            response = requests.post('http://localhost:5006/start-worker', timeout=10)
            if response.status_code == 200:
                logger.info("文本提取工作者已启动")
            else:
                logger.warning("启动文本提取工作者失败")

            return True

        except Exception as e:
            logger.error(f"重启工作者失败: {e}")
            return False

    def run_full_maintenance(self):
        """运行完整维护"""
        logger.info("=== 开始系统维护 ===")

        results = {
            'start_time': datetime.now().isoformat(),
            'consistency_check': None,
            'fixed_files': 0,
            'queue_cleaned': False,
            'workers_restarted': False
        }

        # 1. 检查文件一致性
        results['consistency_check'] = self.check_file_consistency()

        # 2. 修复缺失文件
        if not results['consistency_check'].get('consistent', False):
            results['fixed_files'] = self.fix_missing_text_files()

        # 3. 清理队列
        results['queue_cleaned'] = self.clean_queues()

        # 4. 重启工作者
        results['workers_restarted'] = self.restart_workers()

        results['end_time'] = datetime.now().isoformat()

        logger.info("=== 系统维护完成 ===")
        logger.info(f"修复文件: {results['fixed_files']}")
        logger.info(f"队列清理: {'成功' if results['queue_cleaned'] else '失败'}")
        logger.info(f"工作者重启: {'成功' if results['workers_restarted'] else '失败'}")

        return results

def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='系统维护工具')
    parser.add_argument('--check', action='store_true', help='只检查一致性')
    parser.add_argument('--fix', action='store_true', help='修复缺失文件')
    parser.add_argument('--clean', action='store_true', help='清理队列')
    parser.add_argument('--restart', action='store_true', help='重启工作者')
    parser.add_argument('--full', action='store_true', help='完整维护')
    args = parser.parse_args()

    maintenance = SystemMaintenance()

    if args.check:
        maintenance.check_file_consistency()
    elif args.fix:
        maintenance.fix_missing_text_files()
    elif args.clean:
        maintenance.clean_queues()
    elif args.restart:
        maintenance.restart_workers()
    elif args.full:
        maintenance.run_full_maintenance()
    else:
        # 默认运行完整维护
        maintenance.run_full_maintenance()

if __name__ == '__main__':
    main()