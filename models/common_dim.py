from sqlalchemy import Column, Integer, String, Date, ForeignKey
from models.base import Base


import pytz

utc_plus_7 = pytz.timezone("Asia/Bangkok")


class DimCurrency(Base):
    __tablename__ = "dim_currency"
    __table_args__ = {"schema": "common_dim_schema"}

    currency_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    currency_name = Column(String(100), nullable=False)
    currency_code = Column(String(50), nullable=False, unique=True)


class DimLocation(Base):
    __tablename__ = "dim_location"
    __table_args__ = {"schema": "common_dim_schema"}

    location_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    country = Column(String(100))
    country_code = Column(String(50))
    city = Column(String(50))


class DimPlatform(Base):
    __tablename__ = "dim_platform"
    __table_args__ = {"schema": "common_dim_schema"}

    platform_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    platform_name = Column(String(100))
    platform_type = Column(String(100))


class DimDevice(Base):
    __tablename__ = "dim_device"
    __table_args__ = {"schema": "common_dim_schema"}

    device_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    device_info = Column(String(100))
    platform = Column(String(100))
    # platform_id = Column(Integer, ForeignKey('common_dim_schema.dim_platform.platform_id'))


class DimTime(Base):
    __tablename__ = "dim_time"
    __table_args__ = {"schema": "common_dim_schema"}

    time_id = Column(Integer, primary_key=True, unique=True)
    hour = Column(Integer)
    minute = Column(Integer)
    second = Column(Integer)
    time_of_day = Column(String(20))

    def __init__(self, hour, minute, second):
        self.hour = hour
        self.minute = minute
        self.second = second
        self.time_id = hour * 3600 + minute * 60 + second
        self.time_of_day = self.determine_time_of_day(hour)

    def determine_time_of_day(self, hour):
        if 5 <= hour < 12:
            return "Morning"
        elif 12 <= hour < 17:
            return "Afternoon"
        elif 17 <= hour < 21:
            return "Evening"
        else:
            return "Night"


class DimDate(Base):
    __tablename__ = "dim_date"
    __table_args__ = {"schema": "common_dim_schema"}

    date_id = Column(Integer, primary_key=True, unique=True)
    full_date = Column(Date)
    day = Column(Integer)
    month = Column(Integer)
    quarter = Column(Integer)
    year = Column(Integer)
    weekday = Column(String(10))


class DimEventType(Base):
    __tablename__ = "dim_event_type"
    __table_args__ = {"schema": "common_dim_schema"}

    event_type_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    event_name = Column(String(100), nullable=False)
    event_type = Column(String(100))


class DimGenre(Base):
    __tablename__ = "dim_genre"
    __table_args__ = {"schema": "common_dim_schema"}

    genre_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    genre_name = Column(String(100))


class DimGame(Base):
    __tablename__ = "dim_game"
    __table_args__ = {"schema": "common_dim_schema"}

    game_id = Column(Integer, primary_key=True, unique=True, nullable=False)
    genre_id = Column(Integer, ForeignKey("common_dim_schema.dim_genre.genre_id"))
    platform_id = Column(
        Integer,
        ForeignKey("common_dim_schema.dim_platform.platform_id"),
        nullable=False,
    )
    package_name = Column(String(100))
    game_name = Column(String(100))
    start_date_id = Column(Integer, ForeignKey("common_dim_schema.dim_date.date_id"))


class DimAd(Base):
    __tablename__ = "dim_ad"
    __table_args__ = {"schema": "common_dim_schema"}

    ad_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    ad_format = Column(String(100))
    ad_platform = Column(String(100))
    ad_network = Column(String(100))


class DimLanguage(Base):
    __tablename__ = "dim_language"
    __table_args__ = {"schema": "common_dim_schema"}

    language_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    language_code = Column(String(3))
    language_name = Column(String(200))
