import enum
from sqlalchemy import Column, Integer, String, Date, DateTime, Boolean, Float, Text, ForeignKey, Numeric, Enum as PgEnum
from sqlalchemy.orm import relationship

from models.base import Base

import pytz
utc_plus_7 = pytz.timezone("Asia/Bangkok")



class DimGameVersion(Base):
    __tablename__ = 'dim_game_version'
    __table_args__ = {'schema': 'game_screw_puzzle_story_schema'}

    version_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    game_id = Column(Integer, ForeignKey('common_dim_schema.dim_game.game_id'), nullable=False)
    version_number = Column(String(20), nullable=False) 
    start_date_id = Column(Integer, ForeignKey('common_dim_schema.dim_date.date_id'))

class DimItemType(Base):
    __tablename__ = 'dim_item_type'
    __table_args__ = {'schema': 'game_screw_puzzle_story_schema'}

    item_type_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    item_type_name = Column(String(100))

class DimItem(Base):
    __tablename__ = 'dim_item'
    __table_args__ = {'schema': 'game_screw_puzzle_story_schema'}

    item_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    item_name = Column(String(2500))
    item_type_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_item_type.item_type_id'))


class DimGamePosition(Base):
    __tablename__ = 'dim_game_position'
    __table_args__ = {'schema': 'game_screw_puzzle_story_schema'}

    game_position_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    position = Column(String(100))


class DimShowType(Base):
    __tablename__ = 'dim_show_type'
    __table_args__ = {'schema': 'game_screw_puzzle_story_schema'}

    show_type_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    show_type = Column(String(100))


class DimPackage(Base):
    __tablename__ = 'dim_package'
    __table_args__ = {'schema': 'game_screw_puzzle_story_schema'}

    package_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    package_name = Column(String(100))


class DimLevel(Base):
    __tablename__ = 'dim_level'
    __table_args__ = {'schema': 'game_screw_puzzle_story_schema'}

    level_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    level_number = Column(Integer)
    mode = Column(Integer)


class DimUser(Base):
    __tablename__ = 'dim_user'
    __table_args__ = {'schema': 'game_screw_puzzle_story_schema'}

    user_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_uuid = Column(String(50))
    device_id = Column(Integer, ForeignKey('common_dim_schema.dim_device.device_id'))
    registration_date_id = Column(Integer, ForeignKey('common_dim_schema.dim_date.date_id'))
    location_id = Column(Integer, ForeignKey('common_dim_schema.dim_location.location_id'))
    
    def __repr__(self):
        return super().__repr__()


class FactPurchaseAction(Base):
    __tablename__ = 'fact_purchase_action'
    __table_args__ = {'schema': 'game_screw_puzzle_story_schema'}

    purchase_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_user.user_id'))
    version_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_game_version.version_id'))
    purchase_date_id = Column(Integer, ForeignKey('common_dim_schema.dim_date.date_id'))
    purchase_time_id = Column(Integer, ForeignKey('common_dim_schema.dim_time.time_id'))
    package_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_package.package_id'))
    price = Column(Numeric(10, 2))
    currency_id = Column(Integer, ForeignKey('common_dim_schema.dim_currency.currency_id'))
    usd_price = Column(Numeric(10, 2))
    purchase_position_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_game_position.game_position_id'))
    sequence_id = Column(Integer)
    batch_id = Column(Integer)
    timestamp = Column(DateTime)


class FactIAP(Base):
    __tablename__ = 'fact_iap'
    __table_args__ = {'schema': 'game_screw_puzzle_story_schema'}

    iap_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_user.user_id'))
    version_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_game_version.version_id'))
    iap_date_id = Column(Integer, ForeignKey('common_dim_schema.dim_date.date_id'))
    iap_time_id = Column(Integer, ForeignKey('common_dim_schema.dim_time.time_id'))
    iap_position_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_game_position.game_position_id'))
    show_type_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_show_type.show_type_id'))
    package_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_package.package_id'))
    sequence_id = Column(Integer)
    batch_id = Column(Integer)
    timestamp = Column(DateTime)


class FactLevelEvent(Base):
    __tablename__ = 'fact_level_event'
    __table_args__ = {'schema': 'game_screw_puzzle_story_schema'}

    level_event_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_user.user_id'))
    version_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_game_version.version_id'))
    level_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_level.level_id'))
    event_type_id = Column(Integer, ForeignKey('common_dim_schema.dim_event_type.event_type_id'))
    event_date_id = Column(Integer, ForeignKey('common_dim_schema.dim_date.date_id'))
    time_id = Column(Integer, ForeignKey('common_dim_schema.dim_time.time_id'))
    require_time = Column(Integer)
    play_type = Column(String)
    play_index = Column(Integer)
    play_time = Column(Integer)
    result = Column(String)
    bonus_type = Column(String) 
    bonus_amount = Column(Integer) 
    sequence_id = Column(Integer)
    batch_id = Column(Integer)
    timestamp = Column(DateTime)


class FactItemTransaction(Base):
    __tablename__ = 'fact_item_transaction'
    __table_args__ = {'schema': 'game_screw_puzzle_story_schema'}

    transaction_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_user.user_id'))
    version_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_game_version.version_id'))
    level_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_level.level_id'))
    event_type_id = Column(Integer, ForeignKey('common_dim_schema.dim_event_type.event_type_id'))
    transaction_date_id = Column(Integer, ForeignKey('common_dim_schema.dim_date.date_id'))
    time_id = Column(Integer, ForeignKey('common_dim_schema.dim_time.time_id'))
    transaction_type = Column(String(20))
    item_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_item.item_id'))
    quantity = Column(Integer)
    game_position_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_game_position.game_position_id'))
    sequence_id = Column(Integer)
    batch_id = Column(Integer)
    timestamp = Column(DateTime)


class FactAdView(Base):
    __tablename__ = 'fact_ad_view'
    __table_args__ = {'schema': 'game_screw_puzzle_story_schema'}

    ad_view_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_user.user_id'))
    version_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_game_version.version_id'))
    level_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_level.level_id'))
    event_type_id = Column(Integer, ForeignKey('common_dim_schema.dim_event_type.event_type_id'))
    view_date_id = Column(Integer, ForeignKey('common_dim_schema.dim_date.date_id'))
    view_time_id = Column(Integer, ForeignKey('common_dim_schema.dim_time.time_id'))
    ad_position_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_game_position.game_position_id'))
    ad_id = Column(Integer, ForeignKey('common_dim_schema.dim_ad.ad_id'))
    is_success = Column(Boolean, default=True)
    load_time = Column(Integer)
    rewarded = Column(Integer)
    sequence_id = Column(Integer)
    batch_id = Column(Integer)
    timestamp = Column(DateTime)


class FactPlayerAction(Base):
    __tablename__ = 'fact_player_action'
    __table_args__ = {'schema': 'game_screw_puzzle_story_schema'}

    action_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_user.user_id'), nullable=False)
    version_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_game_version.version_id'))
    level_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_level.level_id'))
    action_date_id = Column(Integer, ForeignKey('common_dim_schema.dim_date.date_id'))
    time_id = Column(Integer, ForeignKey('common_dim_schema.dim_time.time_id'))
    action = Column(String(25))
    amount = Column(Integer)
    sequence_id = Column(Integer)
    batch_id = Column(Integer)
    timestamp = Column(DateTime)


class FactPlayerActivity(Base):
    __tablename__ = 'fact_player_activity'
    __table_args__ = {'schema': 'game_screw_puzzle_story_schema'}

    activity_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_user.user_id'), nullable=False)
    version_id = Column(Integer, ForeignKey('game_screw_puzzle_story_schema.dim_game_version.version_id'))
    date_id = Column(Integer, ForeignKey('common_dim_schema.dim_date.date_id'))
    is_active = Column(Integer)
    retention_day = Column(Integer)
    active_day = Column(Integer)
    batch_id = Column(Integer)

