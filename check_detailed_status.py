#!/usr/bin/env python3
"""
Genesis Connector 详细状态监控工具
分别显示：待发现、待下载、待提取、已完成等独立状态
"""

import requests
import json
import time
import os
import glob
from datetime import datetime
import subprocess
import sys

def get_service_status(url, service_name):
    """获取服务状态"""
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            return {'status': 'online', 'data': response.json()}
        else:
            return {'status': 'error', 'error': f'HTTP {response.status_code}'}
    except requests.exceptions.ConnectionError:
        return {'status': 'offline', 'error': 'Connection refused'}
    except Exception as e:
        return {'status': 'error', 'error': str(e)}

def get_database_stats():
    """获取数据库状态统计"""
    try:
        result = subprocess.run([
            'docker-compose', 'exec', '-T', 'postgres', 'psql',
            '-U', 'user', '-d', 'content_db', '-c',
            'SELECT download_status, parse_status, COUNT(*) as count FROM articles_status GROUP BY download_status, parse_status;'
        ], capture_output=True, text=True, timeout=10)

        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')[2:-1]  # 跳过标题和总计行
            stats = {}
            for line in lines:
                if '|' in line:
                    parts = [p.strip() for p in line.split('|')]
                    if len(parts) >= 3:
                        download_status = parts[0] if parts[0] else 'null'
                        parse_status = parts[1] if parts[1] else 'null'
                        count = int(parts[2])
                        key = f"{download_status}_{parse_status}"
                        stats[key] = count
            return stats
        else:
            return {}
    except Exception as e:
        return {}

def get_file_stats():
    """获取文件系统统计"""
    try:
        html_dir = '/tmp/genesis-content/html'
        text_dir = '/tmp/genesis-content/text'
        metadata_dir = '/tmp/genesis-content/metadata'

        html_files = glob.glob(os.path.join(html_dir, "*.html"))
        text_files = glob.glob(os.path.join(text_dir, "*.txt"))
        metadata_files = glob.glob(os.path.join(metadata_dir, "*.json"))

        return {
            'html_count': len(html_files),
            'text_count': len(text_files),
            'metadata_count': len(metadata_files)
        }
    except Exception as e:
        return {'html_count': 0, 'text_count': 0, 'metadata_count': 0}

def analyze_processing_pipeline(services_info, db_stats, file_stats):
    """分析处理流水线状态"""
    # 从服务获取队列信息
    download_service = services_info.get('Download Service', {})
    extraction_service = services_info.get('Text Extraction Service', {})

    pipeline_stats = {
        'discovered_pending': 0,      # 已发现待下载
        'downloading': 0,             # 下载中
        'download_completed': 0,      # 下载完成待提取
        'extracting': 0,              # 提取中
        'completed': 0,               # 全部完成
        'failed_download': 0,         # 下载失败
        'failed_extraction': 0        # 提取失败
    }

    # 从数据库状态分析
    for status_key, count in db_stats.items():
        download_status, parse_status = status_key.split('_')

        if download_status == 'pending' and parse_status == 'null':
            pipeline_stats['discovered_pending'] = count
        elif download_status == 'processing':
            pipeline_stats['downloading'] = count
        elif download_status == 'completed' and parse_status in ['null', 'pending']:
            pipeline_stats['download_completed'] = count
        elif parse_status == 'processing':
            pipeline_stats['extracting'] = count
        elif download_status == 'completed' and parse_status == 'completed':
            pipeline_stats['completed'] = count
        elif download_status == 'failed':
            pipeline_stats['failed_download'] = count
        elif parse_status == 'failed':
            pipeline_stats['failed_extraction'] = count

    # 从文件系统获取实际完成数量
    actual_completed = min(file_stats['html_count'], file_stats['text_count'])
    pipeline_stats['actual_completed'] = actual_completed

    # 从队列获取实时信息
    if download_service.get('status') == 'online':
        queue_stats = download_service.get('data', {}).get('queue_stats', {})
        pipeline_stats['queue_download_pending'] = queue_stats.get('download_tasks_length', 0)
        pipeline_stats['queue_parse_pending'] = queue_stats.get('parse_tasks_length', 0)

    return pipeline_stats

def format_number(num):
    """格式化数字"""
    if num >= 1000:
        return f"{num/1000:.1f}K"
    return str(num)

def print_header():
    """打印标题"""
    print("=" * 80)
    print("🔍 Genesis Connector 详细状态监控")
    print(f"⏰ 检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

def print_pipeline_status(pipeline_stats):
    """打印处理流水线状态"""
    print(f"\n🚀 内容处理流水线")
    print("-" * 50)

    # 计算总数
    total_articles = (
        pipeline_stats['discovered_pending'] +
        pipeline_stats['downloading'] +
        pipeline_stats['download_completed'] +
        pipeline_stats['extracting'] +
        pipeline_stats['completed'] +
        pipeline_stats['failed_download'] +
        pipeline_stats['failed_extraction']
    )

    actual_completed = pipeline_stats['actual_completed']

    print(f"📊 总文章数: {format_number(total_articles)}")
    print(f"✅ 实际完成: {format_number(actual_completed)}")
    print()

    # 各阶段状态
    print("📋 各阶段状态:")

    # 1. 待下载
    pending = pipeline_stats['discovered_pending']
    if pending > 0:
        print(f"   🔵 已发现待下载: {format_number(pending)}")

    # 2. 下载中
    downloading = pipeline_stats['downloading']
    if downloading > 0:
        print(f"   🟡 下载中: {format_number(downloading)}")

    # 3. 待提取
    download_completed = pipeline_stats['download_completed']
    if download_completed > 0:
        print(f"   🟠 下载完成待提取: {format_number(download_completed)}")

    # 4. 提取中
    extracting = pipeline_stats['extracting']
    if extracting > 0:
        print(f"   🟣 提取中: {format_number(extracting)}")

    # 5. 全部完成
    completed = pipeline_stats['completed']
    print(f"   🟢 全部完成: {format_number(completed)}")

    # 6. 失败状态
    failed_download = pipeline_stats['failed_download']
    failed_extraction = pipeline_stats['failed_extraction']
    if failed_download > 0:
        print(f"   🔴 下载失败: {format_number(failed_download)}")
    if failed_extraction > 0:
        print(f"   🔴 提取失败: {format_number(failed_extraction)}")

    # 实际文件状态（文件系统真实情况）
    print()
    print("📁 实际文件状态:")
    html_count = pipeline_stats.get('html_count', 0)
    text_count = pipeline_stats.get('text_count', 0)

    print(f"   📄 HTML文件: {format_number(html_count)}")
    print(f"   📝 文本文件: {format_number(text_count)}")

    if html_count > 0 and text_count > 0:
        completion_rate = min(text_count / html_count * 100, 100)
        print(f"   📈 提取完成率: {completion_rate:.1f}%")

def print_queue_details(pipeline_stats):
    """打印队列详情"""
    print(f"\n🔄 队列状态详情")
    print("-" * 50)

    download_queue = pipeline_stats.get('queue_download_pending', 0)
    parse_queue = pipeline_stats.get('queue_parse_pending', 0)

    print(f"📥 下载队列: {format_number(download_queue)} 待处理")
    print(f"📝 解析队列: {format_number(parse_queue)} 待处理")

    if download_queue == 0 and parse_queue == 0:
        print("   ✨ 所有队列已清空")
    elif download_queue > 0:
        print("   ℹ️  下载队列中的任务可能是:")
        print("      - 新发现的文章")
        print("      - 下载失败需重试的文章")
        print("      - 状态同步延迟的已完成文章")

def print_worker_status(services_info):
    """打印工作者状态"""
    print(f"\n👷 工作者状态")
    print("-" * 50)

    # 下载服务工作者
    download_service = services_info.get('Download Service', {})
    if download_service.get('status') == 'online':
        data = download_service.get('data', {})
        worker_running = data.get('worker_running', False)
        stats = data.get('stats', {})

        status_icon = "🟢" if worker_running else "🔴"
        print(f"{status_icon} 下载工作者: {'运行中' if worker_running else '已停止'}")

        last_run = stats.get('last_run')
        if last_run:
            processed = last_run.get('processed', 0)
            successful = last_run.get('successful', 0)
            failed = last_run.get('failed', 0)
            print(f"   最近批次: 处理{processed}, 成功{successful}, 失败{failed}")

    # 文本提取服务工作者
    extraction_service = services_info.get('Text Extraction Service', {})
    if extraction_service.get('status') == 'online':
        data = extraction_service.get('data', {})
        worker_running = data.get('worker_running', False)
        stats = data.get('stats', {})

        status_icon = "🟢" if worker_running else "🔴"
        print(f"{status_icon} 文本提取工作者: {'运行中' if worker_running else '已停止'}")

        last_run = stats.get('last_run')
        if last_run:
            processed = last_run.get('processed', 0)
            successful = last_run.get('successful', 0)
            failed = last_run.get('failed', 0)
            print(f"   最近批次: 处理{processed}, 成功{successful}, 失败{failed}")

def print_summary(pipeline_stats, services_info):
    """打印总结"""
    print(f"\n📋 系统总结")
    print("-" * 50)

    actual_completed = pipeline_stats['actual_completed']

    if actual_completed > 1000:
        print(f"🎉 已成功处理 {format_number(actual_completed)} 篇文章！")
    elif actual_completed > 0:
        print(f"✅ 已处理 {format_number(actual_completed)} 篇文章")
    else:
        print("⏳ 处理进程刚开始")

    # 检查自动化状态
    download_running = False
    extraction_running = False

    download_service = services_info.get('Download Service', {})
    if download_service.get('status') == 'online':
        download_running = download_service.get('data', {}).get('worker_running', False)

    extraction_service = services_info.get('Text Extraction Service', {})
    if extraction_service.get('status') == 'online':
        extraction_running = extraction_service.get('data', {}).get('worker_running', False)

    if download_running and extraction_running:
        print("🤖 自动化流程: 全部激活，新文章将自动处理")
    elif download_running or extraction_running:
        print("🤖 自动化流程: 部分激活")
    else:
        print("🤖 自动化流程: 未激活")

def main():
    """主函数"""
    print_header()

    # 定义要检查的服务
    services = {
        'Download Service': 'http://localhost:5003/status',
        'Text Extraction Service': 'http://localhost:5006/status'
    }

    # 获取服务状态
    services_info = {}
    for service_name, url in services.items():
        services_info[service_name] = get_service_status(url, service_name)

    # 获取数据库统计
    db_stats = get_database_stats()

    # 获取文件系统统计
    file_stats = get_file_stats()

    # 分析处理流水线
    pipeline_stats = analyze_processing_pipeline(services_info, db_stats, file_stats)
    pipeline_stats.update(file_stats)  # 添加文件统计

    # 打印各部分
    print_pipeline_status(pipeline_stats)
    print_queue_details(pipeline_stats)
    print_worker_status(services_info)
    print_summary(pipeline_stats, services_info)

    print("\n" + "=" * 80)
    print("💡 提示:")
    print("  - 重新检查: python check_detailed_status.py")
    print("  - 简单状态: ./status")
    print("  - 文件位置: /tmp/genesis-content/text/")
    print("  - 下载状态: curl -s http://localhost:5003/status")
    print("  - 提取状态: curl -s http://localhost:5006/status")

if __name__ == "__main__":
    main()