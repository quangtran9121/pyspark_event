from sqlalchemy import (
    Column,
    Integer,
    String,
    Date,
    DateTime,
    Boolean,
    Float,
    Text,
    ForeignKey,
    Numeric,
    Enum as PgEnum,
)
from sqlalchemy.orm import relationship

from models.base import Base

import pytz

utc_plus_7 = pytz.timezone("Asia/Bangkok")


class DimGameVersion(Base):
    __tablename__ = "dim_game_version"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    version_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    game_id = Column(
        Integer, ForeignKey("common_dim_schema.dim_game.game_id"), nullable=False
    )
    version_number = Column(String(20), nullable=False)
    start_date_id = Column(Integer, ForeignKey("common_dim_schema.dim_date.date_id"))


class DimItemType(Base):
    __tablename__ = "dim_item_type"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    item_type_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    item_type_name = Column(String(100))


class DimItem(Base):
    __tablename__ = "dim_item"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    item_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    item_name = Column(String(2500))
    item_type_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_item_type.item_type_id")
    )


class DimGamePosition(Base):
    __tablename__ = "dim_game_position"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    game_position_id = Column(
        Integer, primary_key=True, autoincrement=True, unique=True
    )
    position = Column(String(100))


class DimShowType(Base):
    __tablename__ = "dim_show_type"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    show_type_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    show_type = Column(String(100))


class DimPackage(Base):
    __tablename__ = "dim_package"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    package_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    package_name = Column(String(250))


class DimLevel(Base):
    __tablename__ = "dim_level"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    level_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    level_number = Column(Integer)
    mode = Column(Integer)


class DimAPIType(Base):
    __tablename__ = "dim_api_type"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    api_type_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    api_type_name = Column(String)
    api_type_desc = Column(String)


class DimMovieType(Base):
    __tablename__ = "dim_movie_type"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    movie_type_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    movie_type_name = Column(String)


class DimMoviePlace(Base):
    __tablename__ = "dim_movie_place"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    movie_place_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    movie_place = Column(String)


class DimUser(Base):
    __tablename__ = "dim_user"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    user_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_uuid = Column(String(50))
    device_id = Column(Integer, ForeignKey("common_dim_schema.dim_device.device_id"))
    registration_date_id = Column(
        Integer, ForeignKey("common_dim_schema.dim_date.date_id")
    )
    location_id = Column(
        Integer, ForeignKey("common_dim_schema.dim_location.location_id")
    )
    language_id = Column(
        Integer, ForeignKey("common_dim_schema.dim_language.language_id")
    )

    def __repr__(self):
        return super().__repr__()

class DimEmail(Base):
    __tablename__ = "dim_email"
    __table_args__ = {"schema": "common_dim_schema"}
    email_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    email = user_uuid = Column(String(255))

class DimFeedback(Base):
    __tablename__ = "dim_feedback"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}
    feedback_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    feedback = user_uuid = Column(Text)

class FactPurchaseAction(Base):
    __tablename__ = "fact_purchase_action"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    purchase_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_user.user_id")
    )
    version_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_game_version.version_id")
    )
    purchase_date_id = Column(Integer, ForeignKey("common_dim_schema.dim_date.date_id"))
    purchase_time_id = Column(Integer, ForeignKey("common_dim_schema.dim_time.time_id"))
    package_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_package.package_id")
    )
    price = Column(Numeric(10, 2))
    currency_id = Column(
        Integer, ForeignKey("common_dim_schema.dim_currency.currency_id")
    )
    usd_price = Column(Numeric(10, 2))
    purchase_position_id = Column(
        Integer,
        ForeignKey("game_xgame_drama_max_schema.dim_game_position.game_position_id"),
    )
    sequence_id = Column(Integer)
    batch_id = Column(Integer)
    timestamp = Column(DateTime)


class FactCancelPurchaseAction(Base):
    __tablename__ = "fact_cancel_purchase_action"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    purchase_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_user.user_id")
    )
    version_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_game_version.version_id")
    )
    purchase_date_id = Column(Integer, ForeignKey("common_dim_schema.dim_date.date_id"))
    purchase_time_id = Column(Integer, ForeignKey("common_dim_schema.dim_time.time_id"))
    package_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_package.package_id")
    )
    price = Column(Numeric(10, 2))
    currency_id = Column(
        Integer, ForeignKey("common_dim_schema.dim_currency.currency_id")
    )
    usd_price = Column(Numeric(10, 2))
    purchase_position_id = Column(
        Integer,
        ForeignKey("game_xgame_drama_max_schema.dim_game_position.game_position_id"),
    )
    sequence_id = Column(Integer)
    batch_id = Column(Integer)
    timestamp = Column(DateTime)


class FactIAP(Base):
    __tablename__ = "fact_iap"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    iap_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_user.user_id")
    )
    version_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_game_version.version_id")
    )
    iap_date_id = Column(Integer, ForeignKey("common_dim_schema.dim_date.date_id"))
    iap_time_id = Column(Integer, ForeignKey("common_dim_schema.dim_time.time_id"))
    iap_position_id = Column(
        Integer,
        ForeignKey("game_xgame_drama_max_schema.dim_game_position.game_position_id"),
    )
    show_type_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_show_type.show_type_id")
    )
    package_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_package.package_id")
    )
    status = Column(Boolean)
    sequence_id = Column(Integer)
    batch_id = Column(Integer)
    timestamp = Column(DateTime)


class FactItemTransaction(Base):
    __tablename__ = "fact_item_transaction"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    transaction_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_user.user_id")
    )
    version_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_game_version.version_id")
    )
    level_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_level.level_id")
    )
    event_type_id = Column(
        Integer, ForeignKey("common_dim_schema.dim_event_type.event_type_id")
    )
    transaction_date_id = Column(
        Integer, ForeignKey("common_dim_schema.dim_date.date_id")
    )
    time_id = Column(Integer, ForeignKey("common_dim_schema.dim_time.time_id"))
    transaction_type = Column(String(20))
    item_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_item.item_id")
    )
    quantity = Column(Integer)
    game_position_id = Column(
        Integer,
        ForeignKey("game_xgame_drama_max_schema.dim_game_position.game_position_id"),
    )
    sequence_id = Column(Integer)
    batch_id = Column(Integer)
    timestamp = Column(DateTime)


class FactAdView(Base):
    __tablename__ = "fact_ad_view"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    ad_view_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_user.user_id")
    )
    version_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_game_version.version_id")
    )
    level_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_level.level_id")
    )
    event_type_id = Column(
        Integer, ForeignKey("common_dim_schema.dim_event_type.event_type_id")
    )
    view_date_id = Column(Integer, ForeignKey("common_dim_schema.dim_date.date_id"))
    view_time_id = Column(Integer, ForeignKey("common_dim_schema.dim_time.time_id"))
    ad_position_id = Column(
        Integer,
        ForeignKey("game_xgame_drama_max_schema.dim_game_position.game_position_id"),
    )
    ad_id = Column(Integer, ForeignKey("common_dim_schema.dim_ad.ad_id"))
    is_success = Column(Boolean, default=True)
    load_time = Column(Integer)
    rewarded = Column(Integer)
    sequence_id = Column(Integer)
    batch_id = Column(Integer)
    timestamp = Column(DateTime)


class FactScreen(Base):
    __tablename__ = "fact_screen"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    screen_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_user.user_id")
    )
    version_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_game_version.version_id")
    )
    event_type_id = Column(
        Integer, ForeignKey("common_dim_schema.dim_event_type.event_type_id")
    )
    date_id = Column(Integer, ForeignKey("common_dim_schema.dim_date.date_id"))
    time_id = Column(Integer, ForeignKey("common_dim_schema.dim_time.time_id"))
    screen_name = Column(Integer)
    id_item = Column(Integer)
    total_duration = Column(Integer)
    screen_session = Column(Integer)
    sequence_id = Column(Integer)
    batch_id = Column(Integer)


class FactPlayerActivity(Base):
    __tablename__ = "fact_player_activity"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    activity_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(
        Integer,
        ForeignKey("game_xgame_drama_max_schema.dim_user.user_id"),
        nullable=False,
    )
    version_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_game_version.version_id")
    )
    date_id = Column(Integer, ForeignKey("common_dim_schema.dim_date.date_id"))
    is_active = Column(Integer)
    retention_day = Column(Integer)
    active_day = Column(Integer)
    batch_id = Column(Integer)


class FactUserBehavior(Base):
    __tablename__ = "fact_user_behavior"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    behavior_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_user.user_id")
    )
    version_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_game_version.version_id")
    )
    behavior_date_id = Column(Integer, ForeignKey("common_dim_schema.dim_date.date_id"))
    time_id = Column(Integer, ForeignKey("common_dim_schema.dim_time.time_id"))
    screen_name = Column(Integer)
    id_item = Column(String)
    sequence_id = Column(Integer)
    batch_id = Column(Integer)


class FactMovieInfo(Base):
    __tablename__ = "fact_movie_info"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    movie_info_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(
        Integer,
        ForeignKey("game_xgame_drama_max_schema.dim_user.user_id"),
    )
    version_id = Column(
        Integer,
        ForeignKey("game_xgame_drama_max_schema.dim_game_version.version_id"),
    )
    movie_type_id = Column(
        Integer,
        ForeignKey("game_xgame_drama_max_schema.dim_movie_type.movie_type_id"),
    )
    language_id = Column(
        Integer,
        ForeignKey("common_dim_schema.dim_language.language_id"),
    )
    movie_date_id = Column(Integer, ForeignKey("common_dim_schema.dim_date.date_id"))
    time_id = Column(Integer, ForeignKey("common_dim_schema.dim_time.time_id"))
    movie_id = Column(Integer)
    movie_category = Column(Integer)
    ep_id = Column(Integer)
    time_watching = Column(Integer)
    sequence_id = Column(Integer)
    batch_id = Column(Integer)


class FactAPICall(Base):
    __tablename__ = "fact_api_call"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    api_call_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_user.user_id")
    )
    version_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_game_version.version_id")
    )
    event_type_id = Column(
        Integer, ForeignKey("common_dim_schema.dim_event_type.event_type_id")
    )
    date_id = Column(Integer, ForeignKey("common_dim_schema.dim_date.date_id"))
    time_id = Column(Integer, ForeignKey("common_dim_schema.dim_time.time_id"))
    api_type_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_api_type.api_type_id")
    )
    api_call_duration = Column(Integer)
    api_call_status = Column(Boolean)
    api_error_message = Column(String)
    sequence_id = Column(Integer)
    batch_id = Column(Integer)


class FactInitMovie(Base):
    __tablename__ = "fact_init_movie"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    init_movie_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_user.user_id")
    )
    version_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_game_version.version_id")
    )
    event_type_id = Column(
        Integer, ForeignKey("common_dim_schema.dim_event_type.event_type_id")
    )
    date_id = Column(Integer, ForeignKey("common_dim_schema.dim_date.date_id"))
    time_id = Column(Integer, ForeignKey("common_dim_schema.dim_time.time_id"))
    movie_place_id = Column(
        Integer,
        ForeignKey("game_xgame_drama_max_schema.dim_movie_place.movie_place_id"),
    )
    movie_type = Column(String)
    movie_id = Column(Integer)
    movie_ep_id = Column(Integer)
    movie_status = Column(Boolean)
    movie_duration = Column(Integer)
    sequence_id = Column(Integer)
    batch_id = Column(Integer)


class FactMovieClick(Base):
    __tablename__ = "fact_movie_click"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    movie_click_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_user.user_id")
    )
    version_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_game_version.version_id")
    )
    event_type_id = Column(
        Integer, ForeignKey("common_dim_schema.dim_event_type.event_type_id")
    )
    date_id = Column(Integer, ForeignKey("common_dim_schema.dim_date.date_id"))
    time_id = Column(Integer, ForeignKey("common_dim_schema.dim_time.time_id"))
    movie_id = Column(Integer)
    sequence_id = Column(Integer)
    batch_id = Column(Integer)


class FactPurchaseFailReport(Base):
    __tablename__ = "fact_purchase_fail_report"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    purchase_fail_id = Column(
        Integer, primary_key=True, autoincrement=True, unique=True
    )
    user_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_user.user_id")
    )
    version_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_game_version.version_id")
    )
    date_id = Column(Integer, ForeignKey("common_dim_schema.dim_date.date_id"))
    time_id = Column(Integer, ForeignKey("common_dim_schema.dim_time.time_id"))
    reason = Column(String)
    other_reason = Column(String)
    sequence_id = Column(Integer)
    batch_id = Column(Integer)


class FactUserLogin(Base):
    __tablename__ = "fact_user_login"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    user_login_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_user.user_id")
    )
    version_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_game_version.version_id")
    )
    date_id = Column(Integer, ForeignKey("common_dim_schema.dim_date.date_id"))
    time_id = Column(Integer, ForeignKey("common_dim_schema.dim_time.time_id"))
    mail = Column(String)
    status = Column(Boolean)
    sequence_id = Column(Integer)
    batch_id = Column(Integer)

class FactFeedback(Base):
    __tablename__ = "fact_feedback"
    __table_args__ = {"schema": "game_xgame_drama_max_schema"}

    fact_feedback_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    user_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_user.user_id")
    )
    version_id = Column(
        Integer, ForeignKey("game_xgame_drama_max_schema.dim_game_version.version_id")
    )
    date_id = Column(Integer, ForeignKey("common_dim_schema.dim_date.date_id"))
    time_id = Column(Integer, ForeignKey("common_dim_schema.dim_time.time_id"))
    email_id = Column(Integer, ForeignKey("common_dim_schema.dim_email.email_id"))
    feedback_id = Column(Integer, ForeignKey("game_xgame_drama_max_schema.dim_feedback.feedback_id"))
    sequence_id = Column(Integer)
    batch_id = Column(Integer)
