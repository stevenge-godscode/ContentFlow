#!/usr/bin/env python3
"""
测试单个公众号功能
"""

import sys
import os
sys.path.append('/home/azureuser/repository/genesis-connector')

from config import Config
from services.discovery.utils.discovery_engine import DiscoveryEngine

def test_individual_feed():
    """测试单个公众号功能"""
    print("=== Genesis Connector 单个公众号功能测试 ===")

    # 获取配置
    config = Config.from_env()

    # 创建发现引擎
    discovery_engine = DiscoveryEngine(config)

    print("\n1. 测试获取公众号列表...")
    feeds = discovery_engine.get_feed_list()
    print(f"✅ 获取到 {len(feeds)} 个公众号")

    if feeds:
        # 显示前几个公众号
        print("\n📋 公众号列表:")
        for i, feed in enumerate(feeds[:5]):
            feed_id = feed.get('id', 'Unknown')
            title = feed.get('title', '未知标题')
            print(f"   {i+1}. {feed_id} - {title}")

        # 选择第一个公众号进行测试
        test_feed = feeds[0]
        test_feed_id = test_feed.get('id')

        if test_feed_id:
            print(f"\n2. 测试获取公众号信息: {test_feed_id}")
            feed_info = discovery_engine.get_feed_info(test_feed_id)
            if feed_info:
                print(f"✅ 获取到公众号信息:")
                print(f"   标题: {feed_info.get('title', 'Unknown')}")
                print(f"   ID: {feed_info.get('id', 'Unknown')}")
                print(f"   同步时间: {feed_info.get('syncTime', 'Unknown')}")
            else:
                print("❌ 获取公众号信息失败")

            print(f"\n3. 测试单个公众号发现: {test_feed_id}")
            result = discovery_engine.run_single_feed_discovery(test_feed_id)
            print(f"✅ 单个公众号发现完成:")
            print(f"   公众号ID: {result.get('feed_id')}")
            print(f"   发现文章: {result.get('discovered')}")
            print(f"   新文章: {result.get('new_articles')}")
            print(f"   重复文章: {result.get('duplicates')}")
            print(f"   错误数: {result.get('errors')}")
            print(f"   更新触发成功: {result.get('update_triggered')}")
            print(f"   耗时: {result.get('duration', 0):.2f}秒")
        else:
            print("❌ 找不到可测试的公众号ID")
    else:
        print("❌ 未获取到公众号列表")

if __name__ == '__main__':
    test_individual_feed()