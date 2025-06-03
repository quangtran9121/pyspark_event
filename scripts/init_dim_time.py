from models.common_dim import DimTime


def gen_dim_time(session):
    for hour in range(24):
        for minute in range(60):
            for second in range(60):
                dim_time_row = DimTime(hour, minute, second)

                session.add(dim_time_row)
                
    session.commit()

    
