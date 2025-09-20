#!/usr/bin/env python3
"""
Genesis Connector Docker容器化状态监控工具
"""

import requests
import json
import time
from datetime import datetime
import subprocess
import sys

def check_docker_container_status(container_name):
    """检查Docker容器状态"""
    try:
        result = subprocess.run(['docker', 'inspect', container_name, '--format={{.State.Status}}'],
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            status = result.stdout.strip()
            return {'status': 'online' if status == 'running' else 'offline', 'container_status': status}
        else:
            return {'status': 'offline', 'error': 'Container not found'}
    except Exception as e:
        return {'status': 'error', 'error': str(e)}

def get_service_status(url, service_name):
    """获取服务状态"""
    try:
        headers = {
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0'
        }
        response = requests.get(url, timeout=5, headers=headers)
        if response.status_code == 200:
            if 'WeWe RSS' in service_name:
                return {'status': 'online', 'data': {'type': 'html_service'}}
            try:
                data = response.json()
                return {'status': 'online', 'data': data}
            except (ValueError, TypeError):
                return {'status': 'online', 'data': {'type': 'non_json_service'}}
        else:
            return {'status': 'error', 'error': f'HTTP {response.status_code}'}
    except requests.exceptions.ConnectionError:
        return {'status': 'offline', 'error': 'Connection refused'}
    except Exception as e:
        return {'status': 'error', 'error': str(e)}

def get_mysql_data_count():
    """获取MySQL数据统计"""
    try:
        result = subprocess.run([
            'docker', 'exec', 'mysql', 'mysql', '-uroot', '-p123456',
            'wewe-rss', '-e', 'SELECT COUNT(*) as accounts FROM accounts; SELECT COUNT(*) as articles FROM articles;'
        ], capture_output=True, text=True, timeout=10)

        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            accounts = None
            articles = None

            for i, line in enumerate(lines):
                if line.strip() == 'accounts' and i + 1 < len(lines):
                    accounts = lines[i + 1].strip()
                elif line.strip() == 'articles' and i + 1 < len(lines):
                    articles = lines[i + 1].strip()

            return {'accounts': accounts, 'articles': articles}
        else:
            return {'error': 'Cannot access MySQL'}
    except Exception as e:
        return {'error': str(e)}

def print_header():
    """打印标题"""
    print("=" * 80)
    print("🚀 Genesis Connector Docker容器化状态监控")
    print(f"⏰ 检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

def print_service_section(title, services_info):
    """打印服务段落"""
    print(f"\n📊 {title}")
    print("-" * 50)

    for service_name, info in services_info.items():
        status = info.get('status', 'unknown')

        if status == 'online':
            print(f"✅ {service_name:<25} ONLINE")
        elif status == 'offline':
            print(f"❌ {service_name:<25} OFFLINE")
        else:
            print(f"⚠️  {service_name:<25} ERROR: {info.get('error', 'Unknown')}")

def print_data_section():
    """打印数据统计"""
    print(f"\n📊 数据统计")
    print("-" * 50)

    data = get_mysql_data_count()
    if 'error' in data:
        print(f"⚠️  无法获取数据统计: {data['error']}")
    else:
        print(f"👥 WeWe RSS 账号: {data.get('accounts', 'N/A')}")
        print(f"📄 WeWe RSS 文章: {data.get('articles', 'N/A')}")

def print_network_section():
    """打印网络配置"""
    print(f"\n🔗 网络配置")
    print("-" * 50)
    print("🌐 外部访问:")
    print("   • WeWe RSS Web: http://localhost:4000")
    print("   • MySQL数据库: localhost:3306")
    print("")
    print("🔒 内部服务 (Docker网络内部):")
    print("   • Download Service 1: content-download-1:5003")
    print("   • Download Service 2: content-download-2:5004")
    print("   • Text Extraction: content-parser:5006")
    print("   • Redis: redis:6379")
    print("   • PostgreSQL: postgres:5432")

def print_management_section():
    """打印管理命令"""
    print(f"\n🛠️  管理命令")
    print("-" * 50)
    print("📋 状态管理:")
    print("   ./manage-stack.sh status   # 查看详细状态")
    print("   ./manage-stack.sh logs     # 查看服务日志")
    print("")
    print("🚀 服务管理:")
    print("   ./manage-stack.sh start    # 启动所有服务")
    print("   ./manage-stack.sh stop     # 停止所有服务")
    print("   ./manage-stack.sh clean    # 清理停止的容器")

def main():
    """主函数"""
    print_header()

    # 定义要检查的Docker容器服务
    docker_services = {
        'WeWe RSS': 'genesis-connector_wewe-rss_1',
        'Download Service 1': 'content-download-1',
        'Download Service 2': 'content-download-2',
        'Text Extraction Service': 'content-parser',
        'MySQL Database': 'mysql',
        'Redis Cache': 'redis',
        'PostgreSQL Database': 'postgres'
    }

    # 获取服务状态
    services_info = {}
    for service_name, container_name in docker_services.items():
        services_info[service_name] = check_docker_container_status(container_name)

    # 对WeWe RSS做额外的HTTP可达性检查
    if services_info.get('WeWe RSS', {}).get('status') == 'online':
        wewe_http_status = get_service_status('http://localhost:4000', 'WeWe RSS')
        if wewe_http_status['status'] != 'online':
            services_info['WeWe RSS']['http_status'] = 'unreachable'

    # 打印各部分
    print_service_section("Docker容器状态", services_info)
    print_data_section()
    print_network_section()
    print_management_section()

    # 统计在线服务
    online_count = sum(1 for info in services_info.values() if info.get('status') == 'online')
    total_count = len(services_info)

    print(f"\n📋 系统总结")
    print("-" * 50)
    if online_count == total_count:
        print(f"✅ 所有服务运行正常 ({online_count}/{total_count})")
        print("🚀 系统状态: 健康")
    else:
        print(f"⚠️  {online_count}/{total_count} 服务在线")
        print("🚨 系统状态: 需要注意")

    print("\n" + "=" * 80)

if __name__ == "__main__":
    main()