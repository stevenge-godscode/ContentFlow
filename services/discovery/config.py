import os
import yaml
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Discovery Service Configuration"""

    # Basic Flask Configuration
    SECRET_KEY = os.getenv('AUTH_SECRET_KEY', 'dev-secret-key')
    DEBUG = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'

    # WeWe RSS Connection
    WEWE_RSS_URL = os.getenv('WEWE_RSS_URL', 'http://wewe-rss:4000')
    WEWE_RSS_TIMEOUT = int(os.getenv('WEWE_RSS_TIMEOUT', '30'))

    # Database Configuration
    REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379')
    POSTGRES_URL = os.getenv('POSTGRES_URL', 'postgresql://user:password@postgres:5432/content_db')

    # Discovery Configuration
    DISCOVERY_INTERVAL = int(os.getenv('DISCOVERY_INTERVAL', '300'))  # 5 minutes
    BATCH_SIZE = int(os.getenv('DISCOVERY_BATCH_SIZE', '100'))
    MAX_RETRIES = int(os.getenv('DISCOVERY_MAX_RETRIES', '3'))

    # Service Configuration
    SERVICE_PORT = int(os.getenv('DISCOVERY_PORT', '5001'))
    SERVICE_HOST = os.getenv('DISCOVERY_HOST', '0.0.0.0')

    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

    # File Paths
    DATA_DIR = os.getenv('DATA_DIR', '/app/data')
    CONFIG_DIR = os.getenv('CONFIG_DIR', '/app/config')

    @classmethod
    def load_app_config(cls):
        """Load application-specific configuration from YAML"""
        config_file = os.path.join(cls.CONFIG_DIR, 'app-config.yaml')
        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                app_config = yaml.safe_load(f)

            # Update discovery settings
            discovery_config = app_config.get('discovery', {})
            cls.DISCOVERY_INTERVAL = discovery_config.get('interval', cls.DISCOVERY_INTERVAL)
            cls.BATCH_SIZE = discovery_config.get('batch_size', cls.BATCH_SIZE)
            cls.MAX_RETRIES = discovery_config.get('max_retries', cls.MAX_RETRIES)

            return app_config
        return {}

class DevelopmentConfig(Config):
    DEBUG = True
    LOG_LEVEL = 'DEBUG'

class ProductionConfig(Config):
    DEBUG = False
    LOG_LEVEL = 'INFO'

class TestingConfig(Config):
    TESTING = True
    DEBUG = True
    LOG_LEVEL = 'DEBUG'

# Configuration mapping
config_map = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}

def get_config():
    """Get configuration based on environment"""
    env = os.getenv('FLASK_ENV', 'development')
    return config_map.get(env, config_map['default'])