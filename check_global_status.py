#!/usr/bin/env python3
"""
Genesis Connector 全局状态监控工具
"""

import requests
import json
import time
from datetime import datetime
import subprocess
import sys

def get_service_status(url, service_name):
    """获取服务状态"""
    try:
        # 添加缓存防止头部以确保获取最新数据
        headers = {
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0'
        }
        response = requests.get(url, timeout=5, headers=headers)
        if response.status_code == 200:
            # 对于WeWe RSS等非JSON服务，只检查HTTP状态
            if 'WeWe RSS' in service_name:
                return {'status': 'online', 'data': {'type': 'html_service'}}

            # 对于API服务，尝试解析JSON
            try:
                data = response.json()
                return {'status': 'online', 'data': data}
            except (ValueError, TypeError):
                # 如果不是JSON但HTTP状态正常，仍然认为在线
                return {'status': 'online', 'data': {'type': 'non_json_service'}}
        else:
            return {'status': 'error', 'error': f'HTTP {response.status_code}'}
    except requests.exceptions.ConnectionError:
        return {'status': 'offline', 'error': 'Connection refused'}
    except Exception as e:
        return {'status': 'error', 'error': str(e)}

def check_docker_services():
    """检查Docker服务状态"""
    try:
        result = subprocess.run(['docker-compose', 'ps'],
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            services = {}

            # 找到分隔线，跳过标题
            data_start = 0
            for i, line in enumerate(lines):
                if '---' in line:
                    data_start = i + 1
                    break

            # 解析服务状态
            for line in lines[data_start:]:
                if line.strip():
                    # 使用正则表达式更准确地解析状态
                    import re
                    # 匹配服务名和状态字段
                    match = re.search(r'^(\S+)\s+.*?\s+(Up\s*(?:\([^)]+\))?|Exited?\s*(?:\([^)]+\))?|Exit\s*\d+)', line)
                    if match:
                        name = match.group(1)
                        state = match.group(2).strip()
                        services[name] = state
                    else:
                        # 兜底方案：简单按空格分割
                        parts = line.split()
                        if len(parts) >= 3:
                            name = parts[0]
                            # 寻找状态字段（通常包含Up或Exit）
                            for part in parts[2:]:
                                if 'Up' in part or 'Exit' in part:
                                    state = part
                                    break
                            else:
                                state = 'Unknown'
                            services[name] = state
            return services
        else:
            return {}
    except Exception as e:
        return {}

def format_number(num):
    """格式化数字"""
    if num >= 1000:
        return f"{num/1000:.1f}K"
    return str(num)

def print_header():
    """打印标题"""
    print("=" * 80)
    print("🚀 Genesis Connector 全局状态监控")
    print(f"⏰ 检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

def print_service_section(title, services_info):
    """打印服务段落"""
    print(f"\n📊 {title}")
    print("-" * 50)

    for service_name, info in services_info.items():
        status = info.get('status', 'unknown')

        if status == 'online':
            print(f"✅ {service_name:<20} ONLINE")
        elif status == 'offline':
            print(f"❌ {service_name:<20} OFFLINE")
        else:
            print(f"⚠️  {service_name:<20} ERROR: {info.get('error', 'Unknown')}")

def print_queue_stats(services_info):
    """打印队列统计"""
    print(f"\n🔄 队列状态")
    print("-" * 50)

    # 从下载服务获取队列信息
    download_service = services_info.get('Download Service', {})
    if download_service.get('status') == 'online':
        data = download_service.get('data', {})
        queue_stats = data.get('queue_stats', {})

        print(f"📥 下载队列:")
        print(f"   待处理: {format_number(queue_stats.get('download_tasks_length', 0))}")
        print(f"   已处理: {format_number(queue_stats.get('download_tasks_processed', 0))}")
        print(f"   总添加: {format_number(queue_stats.get('download_tasks_added', 0))}")

        print(f"📝 解析队列:")
        print(f"   待处理: {format_number(queue_stats.get('parse_tasks_length', 0))}")
        print(f"   已处理: {format_number(queue_stats.get('parse_tasks_processed', 0))}")
    else:
        print("⚠️  无法获取队列状态")

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
        print(f"   成功: {format_number(stats.get('successful', 0))}")
        print(f"   失败: {format_number(stats.get('failed', 0))}")

    # 文本提取服务工作者
    extraction_service = services_info.get('Text Extraction Service', {})
    if extraction_service.get('status') == 'online':
        data = extraction_service.get('data', {})
        worker_running = data.get('worker_running', False)
        stats = data.get('stats', {})

        status_icon = "🟢" if worker_running else "🔴"
        print(f"{status_icon} 文本提取工作者: {'运行中' if worker_running else '已停止'}")
        print(f"   成功: {format_number(stats.get('successful', 0))}")
        print(f"   失败: {format_number(stats.get('failed', 0))}")

def print_file_stats(services_info):
    """打印文件统计"""
    print(f"\n📁 文件统计")
    print("-" * 50)

    # 从文本提取服务获取文件统计
    extraction_service = services_info.get('Text Extraction Service', {})
    if extraction_service.get('status') == 'online':
        data = extraction_service.get('data', {})
        ext_status = data.get('extraction_status', {})

        html_count = ext_status.get('html_files_count', 0)
        text_count = ext_status.get('text_files_count', 0)
        remaining = ext_status.get('remaining_to_process', 0)

        progress = (text_count / html_count * 100) if html_count > 0 else 0

        print(f"📄 HTML文件: {format_number(html_count)}")
        print(f"📝 文本文件: {format_number(text_count)}")

        # 只在有待处理文件时显示进度信息
        if remaining > 0:
            print(f"⏳ 待处理: {format_number(remaining)}")
            print(f"📈 完成度: {progress:.1f}%")

            # 进度条
            bar_length = 30
            filled_length = int(bar_length * progress / 100)
            bar = '█' * filled_length + '░' * (bar_length - filled_length)
            print(f"   [{bar}] {progress:.1f}%")
        elif html_count > 0 and text_count == html_count:
            print(f"✅ 全部处理完成")
    else:
        print("⚠️  无法获取文件统计")

def print_docker_status(docker_services):
    """打印Docker服务状态"""
    print(f"\n🐳 Docker 容器状态")
    print("-" * 50)

    if docker_services:
        for name, state in docker_services.items():
            if 'Up' in state:
                print(f"🟢 {name:<30} {state}")
            else:
                print(f"🔴 {name:<30} {state}")
    else:
        print("⚠️  无法获取Docker状态")

def print_summary(services_info):
    """打印总结"""
    print(f"\n📋 系统总结")
    print("-" * 50)

    online_count = sum(1 for info in services_info.values() if info.get('status') == 'online')
    total_count = len(services_info)

    if online_count == total_count:
        print("🎉 所有服务正常运行")
    elif online_count > 0:
        print(f"⚠️  {online_count}/{total_count} 服务在线")
    else:
        print("🚨 所有服务离线")

    # 工作者状态检查
    download_running = False
    extraction_running = False

    download_service = services_info.get('Download Service', {})
    if download_service.get('status') == 'online':
        download_running = download_service.get('data', {}).get('worker_running', False)

    extraction_service = services_info.get('Text Extraction Service', {})
    if extraction_service.get('status') == 'online':
        extraction_running = extraction_service.get('data', {}).get('worker_running', False)

    if download_running and extraction_running:
        print("🤖 自动化流程: 全部激活")
    elif download_running or extraction_running:
        print("🤖 自动化流程: 部分激活")
    else:
        print("🤖 自动化流程: 未激活")

def main():
    """主函数"""
    print_header()

    # 定义要检查的服务
    services = {
        'WeWe RSS': 'http://localhost:4000',
        'Download Service': 'http://localhost:5003/status',
        'Text Extraction Service': 'http://localhost:5006/status',
        'Web Interface': 'http://localhost:8080',
        'File Server': 'http://localhost:8081'
    }

    # 获取服务状态
    services_info = {}
    for service_name, url in services.items():
        services_info[service_name] = get_service_status(url, service_name)

    # 获取Docker状态
    docker_services = check_docker_services()

    # 打印各部分
    print_service_section("核心服务状态", services_info)
    print_queue_stats(services_info)
    print_worker_status(services_info)
    print_file_stats(services_info)
    print_docker_status(docker_services)
    print_summary(services_info)

    print("\n" + "=" * 80)
    print("💡 提示:")
    print("  - 重新运行: python check_global_status.py")
    print("  - 启动服务: docker-compose up -d")
    print("  - 查看日志: make logs")
    print("  - 下载状态: curl -s http://localhost:5003/status")
    print("  - 提取状态: curl -s http://localhost:5006/status")

if __name__ == "__main__":
    main()