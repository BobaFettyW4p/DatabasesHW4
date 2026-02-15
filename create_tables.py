from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import DeclarativeBase
import sqlite3


class Base(DeclarativeBase):
    pass

class dim_date(Base):
    __tablename__="dim_date"
    date_key = Column(String(8), primary_key = True, nullable = False)
    date = Column(String(10), nullable = False)
    year = Column(String(4), nullable = False)
    quarter = Column(String(1), nullable = False)
    month = Column(String(1), nullable = False)
    day_of_month = Column(String(1), nullable = False)
    day_of_week = Column(String(1), nullable = False)
    is_weekend = Column(Integer)

class dim_film(Base):
    __tablename__ = "dim_film"
    film_key = Column(Integer, primary_key = True,  nullable = False)
    film_id = Column(Integer,  nullable = False)
    title = Column(String(255),  nullable = False)
    rating = Column(String(5),  nullable = False)
    length = Column(Integer,  nullable = False)
    language = Column(String(20),  nullable = False)
    release_year = Column(String(4),  nullable = False)

class dim_actor(Base):
    __tablename__ = "dim_actor"
    actor_key = Column(Integer, primary_key = True, nullable = False)
    actor_id = Column(Integer,  nullable = False)
    first_name = Column(String(25),  nullable = False)
    last_name = Column(String(25),  nullable = False)
    last_update = Column(String(10),  nullable = False)
    

class dim_category(Base):
    __tablename__ = "dim_category"
    category_key = Column(Integer, primary_key = True,  nullable = False)
    category_id = Column(Integer,  nullable = False)
    name = Column(String(10),  nullable = False)
    last_update = Column(String(10),  nullable = False)

class dim_store(Base):
    __tablename__ = "dim_store"
    store_key = Column(Integer, primary_key = True,  nullable = False)
    store_id = Column(Integer,  nullable = False)
    city = Column(String(20),  nullable = False)
    country = Column(String(20),  nullable = False)
    last_update = Column(String(10),  nullable = False)

class dim_customer(Base):
    __tablename__ = "dim_customer"
    customer_key = Column(Integer, primary_key = True,  nullable = False)
    customer_id = Column(Integer,  nullable = False)
    first_name = Column(String(20),  nullable = False)
    last_name = Column(String(20),  nullable = False)

class bridge_film_actor(Base):
    __tablename__ = "bridge_film_actor"
    film_key = Column(Integer, primary_key = True, nullable = False)
    actor_key = Column(Integer,  nullable = False)

class bridge_film_category(Base):
    __tablename__ = "bridge_film_category"
    film_key = Column(Integer,  primary_key = True, nullable = False)
    category_key = Column(Integer,  nullable = False)


class fact_rental(Base):
    __tablename__ = "fact_rental"
    fact_rental_key = Column(Integer, primary_key = True,  nullable = False)
    rental_id = Column(Integer,  nullable = False)
    date_key_rented = Column(String(8),  nullable = False)
    date_key_returned = Column(String(8),  nullable = False)
    film_key = Column(Integer,  nullable = False)
    store_key = Column(Integer,  nullable = False)
    customer_key = Column(Integer, nullable = False)
    staff_id = Column(Integer,  nullable = False)
    rental_duration_days = Column(Integer,  nullable = False)
    
class fact_payment(Base):
    __tablename__ = "fact_payment"
    fact_payment_key = Column(Integer, primary_key = True, nullable = False)
    payment_id = Column(Integer, nullable = False)
    date_key_paid = Column(String(8),  nullable = False)
    customer_key = Column(Integer, nullable = False)
    store_key = Column(Integer, nullable = False)
    staff_id = Column(Integer, nullable = False)
    amount = Column(Float,  nullable = False)

if __name__ == "__main__":
    engine = create_engine("sqlite:///ivancicm.db", echo=True)

    try:
        engine.connect()
        print(f"database ivancicm has been created")
    except Exception as e:
        print(f"unable to create sqlite database ivancicm: {e}")
    
    try:
        Base.metadata.create_all(engine)
        print("All tables created successfully")
    except Exception as e:
        print(f"Tables not create successfully. Error message: {e}")
