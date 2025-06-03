import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from conf.config import config_app
from scripts.init_dim_time import gen_dim_time
from scripts.init_dim_currency import gen_dim_currency
from models.common_dim import DimPlatform, DimEventType

def gen_dim_event_type(session):
    events = [
        "iap_show",
        "iap_click",
        "iap_purchase",
        "ad_load",
        "ad_show",
        "level_play",
        "level_end",
        "level_second_chance",
        "resource_sink",
        "resource_earn",
        "session_start",
        "player_change",
        "screen_play",
        "screen_end",
        "behavior",
        "first_open",
    ]
    for event in events:
        new_event_type = DimEventType(
            event_name=event
        )
        session.add(new_event_type)
    session.commit()    

def gen_dim_platform(session):
    
    platforms = [
        "ios",
        "android",
    ]
    for platform in platforms:
        new_platform = DimPlatform(
            platform_name=platform,
            platform_type='mobile'
        )
        session.add(new_platform)
    session.commit()       

if __name__ == '__main__':
    engine = create_engine(config_app.env.POSTGRES_URL_ENGINE)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    gen_dim_time(session)
    gen_dim_currency(session)
    gen_dim_event_type(session)
    gen_dim_platform(session)
    
    
    session.close()