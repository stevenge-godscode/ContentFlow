#!/usr/bin/env python3
"""
清理下载队列中的重复和已完成任务
"""

import sys
import os
import json
import redis
import glob
from pathlib import Path

# 添加路径
sys.path.append('/home/azureuser/repository/genesis-connector')
from config import Config

def main():
    """主函数"""
    print("=== Genesis Connector 队列清理工具 ===")

    # 获取配置
    config = Config.from_env()

    # 连接Redis
    try:
        redis_client = redis.from_url(config.REDIS_URL, decode_responses=True)
        redis_client.ping()
        print("✅ Redis连接成功")
    except Exception as e:
        print(f"❌ Redis连接失败: {e}")
        return

    # 获取已下载的文件ID列表
    html_dir = '/tmp/genesis-content/html'
    html_files = glob.glob(os.path.join(html_dir, "*.html"))
    downloaded_ids = set()

    for html_file in html_files:
        article_id = Path(html_file).stem
        downloaded_ids.add(article_id)

    print(f"📄 已下载文件数量: {len(downloaded_ids)}")

    # 检查下载队列
    queue_name = 'download_tasks'
    queue_length = redis_client.zcard(queue_name)
    print(f"📥 队列中任务数量: {queue_length}")

    if queue_length == 0:
        print("✅ 队列已为空")
        return

    # 获取队列中所有任务
    all_tasks = redis_client.zrange(queue_name, 0, -1, withscores=True)
    print(f"🔍 分析队列中的 {len(all_tasks)} 个任务...")

    removed_count = 0
    skipped_count = 0

    for task_json, score in all_tasks:
        try:
            task = json.loads(task_json)
            article_id = task.get('id')

            if not article_id:
                continue

            # 如果文件已下载，从队列中移除
            if article_id in downloaded_ids:
                redis_client.zrem(queue_name, task_json)
                removed_count += 1
                if removed_count % 100 == 0:
                    print(f"   已清理 {removed_count} 个重复任务...")
            else:
                skipped_count += 1

        except (json.JSONDecodeError, KeyError) as e:
            print(f"   ⚠️ 跳过无效任务: {e}")
            continue

    print(f"\n🧹 清理完成:")
    print(f"   ✅ 移除重复任务: {removed_count}")
    print(f"   ⏳ 保留待处理: {skipped_count}")

    # 最终统计
    final_queue_length = redis_client.zcard(queue_name)
    print(f"   📥 清理后队列长度: {final_queue_length}")

    if final_queue_length == 0:
        print("🎉 队列已完全清空！")
    elif final_queue_length < queue_length:
        print(f"✅ 队列已优化，减少了 {queue_length - final_queue_length} 个任务")
    else:
        print("ℹ️ 队列中仍有待处理的新任务")

if __name__ == '__main__':
    main()