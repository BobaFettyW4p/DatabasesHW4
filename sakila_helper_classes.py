from datetime import datetime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, Float, DateTime, Boolean


class SakilaBase(DeclarativeBase):
    pass

class Film(SakilaBase):
    __tablename__ = "film"
    film_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String(30))
    rating: Mapped[str] = mapped_column(String(5))
    length: Mapped[int] = mapped_column(Integer)
    release_year: Mapped[int] = mapped_column(Integer)
    last_update: Mapped[datetime] = mapped_column(DateTime)
    language_id: Mapped[int] = mapped_column(Integer)


class Language(SakilaBase):
    __tablename__ = "language"
    language_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(30))

class Actor(SakilaBase):
    __tablename__ = "actor"
    actor_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    first_name: Mapped[str] = mapped_column(String(30))
    last_name: Mapped[str] = mapped_column(String(30))
    last_update: Mapped[datetime] = mapped_column(DateTime)


class Category(SakilaBase):
    __tablename__ = "category"
    category_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    last_update: Mapped[datetime] = mapped_column(DateTime)

class FilmActor(SakilaBase):
    __tablename__ = "film_actor"
    actor_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    film_id: Mapped[int] = mapped_column(Integer, primary_key=True)


class FilmCategory(SakilaBase):
    __tablename__ = "film_category"
    film_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    category_id: Mapped[int] = mapped_column(Integer, primary_key=True)

class Inventory(SakilaBase):
    __tablename__ = "inventory"
    inventory_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    film_id: Mapped[int] = mapped_column(Integer)
    store_id: Mapped[int] = mapped_column(Integer)

class Rental(SakilaBase):
    __tablename__ = "rental"
    rental_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    rental_date: Mapped[datetime] = mapped_column(DateTime)
    return_date: Mapped[datetime] = mapped_column(DateTime)
    staff_id: Mapped[int] = mapped_column(Integer)
    inventory_id: Mapped[int] = mapped_column(Integer)
    customer_id: Mapped[int] = mapped_column(Integer)


class Payment(SakilaBase):
    __tablename__ = "payment"
    payment_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    payment_date: Mapped[datetime] = mapped_column(DateTime)
    customer_id: Mapped[int] = mapped_column(Integer)
    staff_id: Mapped[int] = mapped_column(Integer)
    amount: Mapped[float] = mapped_column(Float)
    rental_id: Mapped[int] = mapped_column(Integer)


class Store(SakilaBase):
    __tablename__ = "store"
    store_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    address_id: Mapped[int] = mapped_column(Integer)
    last_update: Mapped[datetime] = mapped_column(DateTime)


class Staff(SakilaBase):
    __tablename__ = "staff"
    staff_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    store_id: Mapped[int] = mapped_column(Integer)


class Customer(SakilaBase):
    __tablename__ = "customer"
    customer_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    first_name: Mapped[str] = mapped_column(String(30))
    last_name: Mapped[str] = mapped_column(String(30))
    active: Mapped[bool] = mapped_column(Boolean)
    address_id: Mapped[int] = mapped_column(Integer)
    last_update: Mapped[datetime] = mapped_column(DateTime)


class Address(SakilaBase):
    __tablename__ = "address"
    address_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    city_id: Mapped[int] = mapped_column(Integer)


class City(SakilaBase):
    __tablename__ = "city"
    city_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    city: Mapped[str] = mapped_column(String(50))
    country_id: Mapped[int] = mapped_column(Integer)


class Country(SakilaBase):
    __tablename__ = "country"
    country_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    country: Mapped[str] = mapped_column(String(50))

