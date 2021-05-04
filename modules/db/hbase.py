import os

import socutils
from happybase.pool import ConnectionPool


def create_hbase_pool(settings_file: str = 'data/settings.yaml'):
    settings_file = os.getenv("APP_CONFIG_PATH", settings_file)
    settings = socutils.get_settings(settings_file)
    pool = ConnectionPool(5, **settings['hbase'])
    return pool


hbase_pool = create_hbase_pool()
