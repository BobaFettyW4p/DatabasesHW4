from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, Float, Index
from typing import Optional

class Base(DeclarativeBase):
    pass

class dim_date(Base):
    __tablename__ = "dim_date"
    date_key: Mapped[str] = mapped_column(String(8), primary_key=True, nullable=False)
    date: Mapped[str] = mapped_column(String(10), nullable=False)
    year: Mapped[str] = mapped_column(String(4), nullable=False)
    quarter: Mapped[str] = mapped_column(String(1), nullable=False)
    month: Mapped[str] = mapped_column(String(1), nullable=False)
    day_of_month: Mapped[str] = mapped_column(String(1), nullable=False)
    day_of_week: Mapped[str] = mapped_column(String(1), nullable=False)
    is_weekend: Mapped[Optional[int]] = mapped_column(Integer)

    __table_args__ = (
        Index('index_dim_date_date', 'date'),
        Index('index_dim_date_day_of_week', 'day_of_week')
    )


class dim_film(Base):
    __tablename__ = "dim_film"
    film_key: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    film_id: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    rating: Mapped[str] = mapped_column(String(5), nullable=False)
    length: Mapped[int] = mapped_column(Integer, nullable=False)
    language: Mapped[str] = mapped_column(String(20), nullable=False)
    release_year: Mapped[str] = mapped_column(String(4), nullable=False)
    last_update: Mapped[str] = mapped_column(String(10), nullable=False)

    __table_args__ = (
        Index('index_dim_film_film_id', 'film_id'),
        Index('index_')
    )


class dim_actor(Base):
    __tablename__ = "dim_actor"
    actor_key: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    actor_id: Mapped[int] = mapped_column(Integer, nullable=False)
    first_name: Mapped[str] = mapped_column(String(25), nullable=False)
    last_name: Mapped[str] = mapped_column(String(25), nullable=False)
    last_update: Mapped[str] = mapped_column(String(10), nullable=False)


class dim_category(Base):
    __tablename__ = "dim_category"
    category_key: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    category_id: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(String(10), nullable=False)
    last_update: Mapped[str] = mapped_column(String(10), nullable=False)


class dim_store(Base):
    __tablename__ = "dim_store"
    store_key: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    store_id: Mapped[int] = mapped_column(Integer, nullable=False)
    city: Mapped[str] = mapped_column(String(20), nullable=False)
    country: Mapped[str] = mapped_column(String(20), nullable=False)
    last_update: Mapped[str] = mapped_column(String(10), nullable=False)


class dim_customer(Base):
    __tablename__ = "dim_customer"
    customer_key: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    customer_id: Mapped[int] = mapped_column(Integer, nullable=False)
    first_name: Mapped[str] = mapped_column(String(20), nullable=False)
    last_name: Mapped[str] = mapped_column(String(20), nullable=False)
    active: Mapped[int] = mapped_column(Integer, nullable=False)
    city: Mapped[str] = mapped_column(String(20), nullable=False)
    country: Mapped[str] = mapped_column(String(20), nullable=False)
    last_update: Mapped[str] = mapped_column(String(10), nullable=False)


class bridge_film_actor(Base):
    __tablename__ = "bridge_film_actor"
    film_key: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    actor_key: Mapped[int] = mapped_column(Integer, primary_key = True, nullable=False)


class bridge_film_category(Base):
    __tablename__ = "bridge_film_category"
    film_key: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    category_key: Mapped[int] = mapped_column(Integer, nullable=False)


class fact_rental(Base):
    __tablename__ = "fact_rental"
    fact_rental_key: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    rental_id: Mapped[int] = mapped_column(Integer, nullable=False)
    date_key_rented: Mapped[Optional[str]] = mapped_column(String(8))
    date_key_returned: Mapped[Optional[str]] = mapped_column(String(8))
    film_key: Mapped[int] = mapped_column(Integer, nullable=False)
    store_key: Mapped[int] = mapped_column(Integer, nullable=False)
    customer_key: Mapped[int] = mapped_column(Integer, nullable=False)
    staff_id: Mapped[int] = mapped_column(Integer, nullable=False)
    rental_duration_days: Mapped[Optional[int]] = mapped_column(Integer)


class fact_payment(Base):
    __tablename__ = "fact_payment"
    fact_payment_key: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    payment_id: Mapped[int] = mapped_column(Integer, nullable=False)
    date_key_paid: Mapped[str] = mapped_column(String(8), nullable=False)
    customer_key: Mapped[int] = mapped_column(Integer, nullable=False)
    store_key: Mapped[int] = mapped_column(Integer, nullable=False)
    staff_id: Mapped[int] = mapped_column(Integer, nullable=False)
    amount: Mapped[float] = mapped_column(Float, nullable=False)

class sync_state(Base):
    __tablename__ = "sync_state"
    table_name: Mapped[str] = mapped_column(String(30), primary_key=True, nullable=False)
    last_update: Mapped[str] = mapped_column(String(30), nullable=False)