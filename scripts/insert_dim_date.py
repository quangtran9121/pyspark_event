import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))

import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from conf.config import config_app
from models.common_dim import DimDate

def insert_dim_date():
    engine = create_engine(config_app.env.POSTGRES_URL_ENGINE)
    Session = sessionmaker(bind=engine)
    session = Session()

    today = datetime.date.today()
    # start_date = today
    start_date = today + datetime.timedelta(days=-3000)
    end_date = today + datetime.timedelta(days=3000) 

    current_date = start_date
    while current_date <= end_date:
        date_id = int(current_date.strftime("%Y%m%d"))
        
        existing_entry = session.query(DimDate).filter(DimDate.date_id == date_id).first()
        if existing_entry:
            current_date += datetime.timedelta(days=1)
            continue

        day = current_date.day
        month = current_date.month
        year = current_date.year
        quarter = (month - 1) // 3 + 1
        weekday = current_date.strftime('%A')

        new_row = DimDate(
            date_id=date_id,  
            full_date=current_date,
            day=day,
            month=month,
            quarter=quarter,
            year=year,
            weekday=weekday
        )

        session.add(new_row)

        current_date += datetime.timedelta(days=1)

    new_row = DimDate(
        date_id=19700101,  
        full_date=datetime.date(1970,1,1),
        day=1,
        month=1,
        quarter=1,
        year=1970,
    )
    session.add(new_row)
    

    session.commit()
    session.close()
    

if __name__ == '__main__':
    insert_dim_date()