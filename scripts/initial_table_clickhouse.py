import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))

from sqlalchemy import create_engine

from conf.config import config_app
from models.clickhouse.raw_table import Base

def main() -> None:
    clickhose_user = config_app.env.CLICKHOUSE_USER
    clickhose_pw = config_app.env.CLICKHOUSE_PASSWORD
    clickhose_host = config_app.env.CLICKHOUSE_SERVER
    clickhose_port = config_app.env.CLICKHOUSE_PORT
    clickhose_db = config_app.env.CLICKHOUSE_PROD_DB
    
    clickhouse_url = f'clickhouse+http://{clickhose_user}:{clickhose_pw}@{clickhose_host}:{clickhose_port}/{clickhose_db}'
    
    engine = create_engine(clickhouse_url)
    Base.metadata.create_all(engine)


if __name__ == "__main__":
    main()