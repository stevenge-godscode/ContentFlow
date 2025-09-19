import requests
import time
import logging
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

class WeWeRSSClient:
    """WeWe RSS API客户端"""

    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """创建配置好的HTTP会话"""
        session = requests.Session()

        # 配置重试策略
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # 设置默认头部
        session.headers.update({
            'User-Agent': 'Genesis-Connector/1.0.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })

        return session

    def get_all_feeds(self, limit: int = 0) -> List[Dict]:
        """获取所有订阅源"""
        try:
            # 尝试RSS格式，支持limit参数
            rss_url = f"{self.base_url}/feeds/all.atom"

            # 如果limit为0或负数，获取所有文章
            if limit <= 0:
                params = {'limit': 10000}  # 设置一个很大的数字获取所有文章
                logger.info(f"Fetching ALL feeds from RSS: {rss_url} (no limit)")
            else:
                params = {'limit': limit}
                logger.info(f"Fetching feeds from RSS: {rss_url} (limit: {limit})")

            response = self.session.get(rss_url, params=params, timeout=self.timeout)
            response.raise_for_status()

            # 解析RSS/Atom feed
            feeds = self._parse_rss_feed(response.text)
            logger.info(f"Retrieved {len(feeds)} feeds from RSS")
            return feeds

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch feeds: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching feeds: {e}")
            raise

    def get_feed_articles(self, feed_id: str, limit: int = 100) -> List[Dict]:
        """获取指定订阅源的文章"""
        try:
            # 尝试不同的API端点
            endpoints = [
                f"/feeds/{feed_id}/articles.json",
                f"/feeds/{feed_id}.json",
                f"/api/feeds/{feed_id}/articles"
            ]

            for endpoint in endpoints:
                try:
                    url = f"{self.base_url}{endpoint}"
                    params = {'limit': limit}

                    logger.debug(f"Trying endpoint: {url}")
                    response = self.session.get(url, params=params, timeout=self.timeout)

                    if response.status_code == 200:
                        articles = response.json()
                        logger.info(f"Retrieved {len(articles)} articles from feed {feed_id}")
                        return articles
                    elif response.status_code == 404:
                        continue

                except requests.exceptions.RequestException:
                    continue

            logger.warning(f"No valid endpoint found for feed {feed_id}")
            return []

        except Exception as e:
            logger.error(f"Error fetching articles for feed {feed_id}: {e}")
            return []

    def get_recent_articles(self, hours: int = 24, limit: int = 1000) -> List[Dict]:
        """获取最近的文章"""
        try:
            # 计算时间戳
            since_timestamp = int(time.time() - (hours * 3600)) * 1000

            url = f"{self.base_url}/articles/recent.json"
            params = {
                'since': since_timestamp,
                'limit': limit
            }

            logger.info(f"Fetching recent articles since {hours} hours ago")
            response = self.session.get(url, params=params, timeout=self.timeout)

            if response.status_code == 200:
                articles = response.json()
                logger.info(f"Retrieved {len(articles)} recent articles")
                return articles
            else:
                logger.warning(f"Recent articles endpoint returned {response.status_code}")
                return []

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch recent articles: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching recent articles: {e}")
            return []

    def get_all_articles(self, limit: int = 0) -> List[Dict]:
        """获取所有文章（直接从RSS）"""
        try:
            # 直接从RSS获取所有文章，传递limit参数
            # limit=0 表示获取所有文章
            articles = self.get_all_feeds(limit=limit)  # 现在这个方法返回文章列表

            logger.info(f"Total articles retrieved: {len(articles)}")
            return articles

        except Exception as e:
            logger.error(f"Error fetching all articles: {e}")
            return []

    def health_check(self) -> bool:
        """检查WeWe RSS服务健康状态"""
        try:
            # 尝试访问根路径
            response = self.session.get(f"{self.base_url}/", timeout=10)
            return response.status_code in [200, 302]

        except Exception as e:
            logger.error(f"WeWe RSS health check failed: {e}")
            return False

    def get_article_detail(self, article_id: str) -> Optional[Dict]:
        """获取文章详情"""
        try:
            url = f"{self.base_url}/articles/{article_id}.json"
            response = self.session.get(url, timeout=self.timeout)

            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"Article {article_id} not found or error: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"Error fetching article {article_id}: {e}")
            return None

    def extract_article_info(self, article: Dict) -> Dict:
        """从WeWe RSS文章数据中提取标准化信息"""
        try:
            # 生成文章ID（如果没有的话）
            article_id = (
                article.get('id') or
                article.get('article_id') or
                article.get('link', '').split('/')[-1] or
                str(hash(article.get('link', '')))
            )

            # 提取基本信息
            extracted = {
                'id': str(article_id),
                'title': article.get('title', '').strip(),
                'url': article.get('link') or article.get('url', ''),
                'publish_time': self._parse_publish_time(article),

                # 公众号信息
                'mp_name': (
                    article.get('mp_name') or
                    article.get('feed_info', {}).get('mp_name') or
                    article.get('author', '')
                ),
                'mp_id': (
                    article.get('mp_id') or
                    article.get('feed_info', {}).get('mp_id') or
                    article.get('feed_id', '')
                ),

                # 其他信息
                'description': article.get('description', ''),
                'content_snippet': article.get('content', '')[:500] if article.get('content') else '',

                # 元数据
                'source_data': article  # 保留原始数据
            }

            return extracted

        except Exception as e:
            logger.error(f"Error extracting article info: {e}")
            return {}

    def _parse_publish_time(self, article: Dict) -> Optional[int]:
        """解析发布时间为时间戳"""
        try:
            # 尝试不同的时间字段
            time_fields = ['publish_time', 'pubDate', 'published', 'updated', 'date', 'timestamp']

            for field in time_fields:
                if field in article and article[field]:
                    time_value = article[field]

                    # 如果已经是时间戳
                    if isinstance(time_value, (int, float)):
                        # 如果是毫秒时间戳，转换为秒
                        if time_value > 1e10:
                            return int(time_value / 1000)
                        return int(time_value)

                    # 如果是字符串，尝试解析
                    if isinstance(time_value, str):
                        import dateutil.parser
                        try:
                            dt = dateutil.parser.parse(time_value)
                            return int(dt.timestamp())
                        except:
                            continue

            # 如果都没有，返回当前时间
            logger.debug(f"No valid publish time found in article {article.get('id', 'unknown')}, using current time")
            return int(time.time())

        except Exception as e:
            logger.error(f"Error parsing publish time: {e}")
            return int(time.time())

    def _parse_rss_feed(self, xml_content: str) -> List[Dict]:
        """解析RSS/Atom feed"""
        try:
            root = ET.fromstring(xml_content)
            articles = []

            # 检测是否是Atom格式
            if root.tag.endswith('}feed') or root.tag == 'feed':
                # Atom格式
                entries = root.findall('.//{http://www.w3.org/2005/Atom}entry')
                for entry in entries:
                    article = self._parse_atom_entry(entry)
                    if article:
                        articles.append(article)
            else:
                # RSS格式
                items = root.findall('.//item')
                for item in items:
                    article = self._parse_rss_item(item)
                    if article:
                        articles.append(article)

            logger.info(f"Parsed {len(articles)} articles from RSS feed")
            return articles

        except ET.ParseError as e:
            logger.error(f"XML parsing error: {e}")
            return []
        except Exception as e:
            logger.error(f"RSS parsing error: {e}")
            return []

    def _parse_atom_entry(self, entry) -> Optional[Dict]:
        """解析Atom entry"""
        try:
            # 提取基本信息
            title_elem = entry.find('.//{http://www.w3.org/2005/Atom}title')
            link_elem = entry.find('.//{http://www.w3.org/2005/Atom}link[@rel="alternate"]')
            if link_elem is None:
                link_elem = entry.find('.//{http://www.w3.org/2005/Atom}link')
            id_elem = entry.find('.//{http://www.w3.org/2005/Atom}id')
            published_elem = entry.find('.//{http://www.w3.org/2005/Atom}published')
            updated_elem = entry.find('.//{http://www.w3.org/2005/Atom}updated')
            summary_elem = entry.find('.//{http://www.w3.org/2005/Atom}summary')
            author_elem = entry.find('.//{http://www.w3.org/2005/Atom}author/{http://www.w3.org/2005/Atom}name')

            # 提取链接
            link = None
            if link_elem is not None:
                link = link_elem.get('href') or link_elem.text

            if not link:
                return None

            # 生成文章ID
            article_id = None
            if id_elem is not None:
                article_id = id_elem.text
            if not article_id:
                article_id = link.split('/')[-1] if '/' in link else str(hash(link))

            article = {
                'id': article_id,
                'title': title_elem.text if title_elem is not None else '',
                'link': link,
                'url': link,
                'author': author_elem.text if author_elem is not None else '',
                'description': summary_elem.text if summary_elem is not None else '',
                'published': published_elem.text if published_elem is not None else None,
                'updated': updated_elem.text if updated_elem is not None else None,
            }

            return article

        except Exception as e:
            logger.error(f"Error parsing Atom entry: {e}")
            return None

    def _parse_rss_item(self, item) -> Optional[Dict]:
        """解析RSS item"""
        try:
            # 提取基本信息
            title_elem = item.find('title')
            link_elem = item.find('link')
            guid_elem = item.find('guid')
            pub_date_elem = item.find('pubDate')
            description_elem = item.find('description')
            author_elem = item.find('author')

            link = link_elem.text if link_elem is not None else None
            if not link:
                return None

            # 生成文章ID
            article_id = None
            if guid_elem is not None:
                article_id = guid_elem.text
            if not article_id:
                article_id = link.split('/')[-1] if '/' in link else str(hash(link))

            article = {
                'id': article_id,
                'title': title_elem.text if title_elem is not None else '',
                'link': link,
                'url': link,
                'author': author_elem.text if author_elem is not None else '',
                'description': description_elem.text if description_elem is not None else '',
                'pubDate': pub_date_elem.text if pub_date_elem is not None else None,
            }

            return article

        except Exception as e:
            logger.error(f"Error parsing RSS item: {e}")
            return None