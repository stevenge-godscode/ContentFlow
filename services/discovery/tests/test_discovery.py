#!/usr/bin/env python3
"""
Discovery Service 测试文件
"""

import unittest
import sys
import os

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import Mock, patch
from config import get_config
from utils.wewe_client import WeWeRSSClient
from utils.queue_manager import QueueManager
from utils.discovery_engine import DiscoveryEngine

class TestWeWeRSSClient(unittest.TestCase):
    """测试WeWe RSS客户端"""

    def setUp(self):
        self.client = WeWeRSSClient("http://test-wewe-rss:4000")

    def test_extract_article_info(self):
        """测试文章信息提取"""
        sample_article = {
            'id': 'test123',
            'title': 'Test Article',
            'link': 'https://example.com/article/123',
            'author': 'Test Author',
            'publish_time': 1640995200000,  # 2022-01-01 timestamp
            'description': 'Test description'
        }

        result = self.client.extract_article_info(sample_article)

        self.assertEqual(result['id'], 'test123')
        self.assertEqual(result['title'], 'Test Article')
        self.assertEqual(result['url'], 'https://example.com/article/123')
        self.assertEqual(result['mp_name'], 'Test Author')

    def test_parse_publish_time(self):
        """测试发布时间解析"""
        # 测试时间戳（毫秒）
        article1 = {'publish_time': 1640995200000}
        result1 = self.client._parse_publish_time(article1)
        self.assertEqual(result1, 1640995200)

        # 测试时间戳（秒）
        article2 = {'publish_time': 1640995200}
        result2 = self.client._parse_publish_time(article2)
        self.assertEqual(result2, 1640995200)

        # 测试无效时间
        article3 = {}
        result3 = self.client._parse_publish_time(article3)
        self.assertIsInstance(result3, int)  # 应该返回当前时间戳

class TestQueueManager(unittest.TestCase):
    """测试队列管理器"""

    def setUp(self):
        # 使用mock Redis
        self.mock_redis = Mock()
        with patch('redis.from_url', return_value=self.mock_redis):
            self.queue_manager = QueueManager("redis://test:6379")

    def test_push_download_task(self):
        """测试添加下载任务"""
        self.mock_redis.zadd.return_value = 1

        article_data = {
            'id': 'test123',
            'url': 'https://example.com/article/123',
            'title': 'Test Article'
        }

        result = self.queue_manager.push_download_task(article_data)
        self.assertTrue(result)
        self.mock_redis.zadd.assert_called_once()

    def test_is_duplicate(self):
        """测试重复检查"""
        # 测试不重复
        self.mock_redis.sismember.return_value = False
        result1 = self.queue_manager.is_duplicate('test123', 'https://example.com')
        self.assertFalse(result1)

        # 测试重复
        self.mock_redis.sismember.return_value = True
        result2 = self.queue_manager.is_duplicate('test123', 'https://example.com')
        self.assertTrue(result2)

class TestDiscoveryEngine(unittest.TestCase):
    """测试发现引擎"""

    def setUp(self):
        config = get_config()
        config.WEWE_RSS_URL = "http://test-wewe-rss:4000"
        config.REDIS_URL = "redis://test:6379"
        config.POSTGRES_URL = "postgresql://test:test@test:5432/test"

        with patch('utils.discovery_engine.WeWeRSSClient'), \
             patch('utils.discovery_engine.QueueManager'), \
             patch('utils.discovery_engine.DatabaseManager'):
            self.engine = DiscoveryEngine(config)

    def test_prepare_article_data(self):
        """测试文章数据准备"""
        article_info = {
            'id': 'test123',
            'url': 'https://example.com/article/123',
            'title': 'Test Article' * 50,  # 长标题测试
            'mp_name': 'Test Author',
            'mp_id': 'test_mp',
            'publish_time': 1640995200
        }

        result = self.engine._prepare_article_data(article_info)

        self.assertEqual(result['id'], 'test123')
        self.assertEqual(result['url'], 'https://example.com/article/123')
        self.assertEqual(len(result['title']), 512)  # 应该被截断
        self.assertEqual(result['discovery_status'], 'processing')

if __name__ == '__main__':
    # 运行测试
    unittest.main()