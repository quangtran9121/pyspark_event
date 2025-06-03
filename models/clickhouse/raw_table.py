from sqlalchemy import Column, text
from clickhouse_sqlalchemy import get_declarative_base, types, engines

Base = get_declarative_base()

# class ScrewPuzzleStory(Base):
#     __tablename__ = 'screw_puzzle_story'
    
#     sequence_id      = Column(types.UInt64, primary_key=True)
#     event_name       = Column(types.String)
#     event_data       = Column(types.String, nullable=True)
#     user_properties  = Column(types.String, nullable=True)
#     user_id          = Column(types.String)
#     app_version      = Column(types.String, nullable=True)
#     _ts              = Column(types.UInt64)
    
#     ts = Column(
#         types.DateTime64(3),
#         clickhouse_materialized=text("toDateTime64(_ts / 1000, 3)")
#     )
    
#     __table_args__ = (
#         engines.MergeTree(
#             partition_by=text("toYYYYMM(ts)"),
#             order_by=(user_id, sequence_id, _ts),
#             # primary_key=(sequence_id, _ts),
#             # settings={"index_granularity": 8192}
#         ),
#     )

class SupermarketJourney(Base):
    __tablename__ = 'com_supermarket_journey'
    
    sequence_id      = Column(types.UInt64, primary_key=True)
    event_name       = Column(types.String)
    event_data       = Column(types.String, nullable=True)
    user_properties  = Column(types.String, nullable=True)
    user_id          = Column(types.String)
    app_version      = Column(types.String, nullable=True)
    _ts              = Column(types.UInt64)
    
    ts = Column(
        types.DateTime64(3),
        clickhouse_materialized=text("toDateTime64(_ts / 1000, 3)")
    )
    
    __table_args__ = (
        engines.MergeTree(
            partition_by=text("toYYYYMM(ts)"),
            order_by=(user_id, sequence_id, _ts),
            # primary_key=(sequence_id, _ts),
            # settings={"index_granularity": 8192}
        ),
    )


class Statistics(Base):
    date = Column(types.Date, primary_key=True)
    sign = Column(types.Int8, nullable=True)
    grouping = Column(types.Int32, nullable=True)
    metric1 = Column(types.Int32, nullable=False)

    __table_args__ = (
        engines.CollapsingMergeTree(
            sign,
            partition_by=text("toYYYYMM(date)"),
            order_by=(date, grouping)
        ),
    )


# class CelebrityCheckChallengePrank(Base):
#     __tablename__ = 'celebrity_check_challenge_prank'
    
#     sequence_id      = Column(types.UInt64, primary_key=True)
#     event_name       = Column(types.String)
#     event_data       = Column(types.String, nullable=True)
#     user_properties  = Column(types.String, nullable=True)
#     user_id          = Column(types.String)
#     app_version      = Column(types.String, nullable=True)
#     _ts              = Column(types.UInt64)
    
#     ts = Column(
#         types.DateTime64(3),
#         clickhouse_materialized=text("toDateTime64(_ts / 1000, 3)")
#     )
    
#     __table_args__ = (
#         engines.MergeTree(
#             partition_by=text("toYYYYMM(ts)"),
#             order_by=(user_id, sequence_id, _ts),
#             # primary_key=(sequence_id, _ts),
#             # settings={"index_granularity": 8192}
#         ),
#     )
