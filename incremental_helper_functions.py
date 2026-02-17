from sakila_helper_classes import *
from sqlite_helper_classes import *
from sqlalchemy import func
from datetime import datetime, timedelta


def increment_dim_date(sqlite_session, mysql_session):
    current_max =sqlite_session.query(func.max(dim_date.date_key)).scalar()
    current_end = datetime.strptime(current_max, "%Y%m%d").date()

    new_dates = [
        mysql_session.query(func.max(Rental.rental_date)).scalar(),
        mysql_session.query(func.max(Rental.return_date)).scalar(),
        mysql_session.query(func.max(Payment.payment_date)).scalar(),
    ]

    valid_dates = [day.date() for day in new_dates if day]
    new_end = max(valid_dates)

    current = current_end + timedelta(days=1)
    while current <= new_end:
        sqlite_session.merge(dim_date(
            date_key=current.strftime("%Y%m%d"),
            date=current.strftime("%Y-%m-%d"),
            year=str(current.year),
            quarter=str((current.month - 1) // 3 + 1),
            month=str(current.month),
            day_of_month=str(current.day),
            day_of_week=str(current.isoweekday()),
            is_weekend=1 if current.isoweekday() >= 6 else 0
        ))
        current += timedelta(days=1)
    sqlite_session.commit()

def increment_dim_film(sqlite_session, mysql_session, last_sync):
    films = mysql_session.query(Film, Language).join(
        Language, Film.language_id == Language.language_id
    ).filter(Film.last_update > last_sync).all()
    for film, language in films:
        sqlite_session.merge(dim_film(
            film_key=film.film_id * 100 + 1,
            film_id=film.film_id,
            title=film.title,
            rating=film.rating,
            length=film.length,
            language=language.name,
            release_year=film.release_year,
            last_update=film.last_update.strftime("%Y-%m-%d")
        ))
    sqlite_session.commit()

def increment_dim_actor(sqlite_session, mysql_session, last_sync):
    actors = mysql_session.query(Actor).filter(Actor.last_update > last_sync).all()
    for actor in actors:
        sqlite_session.merge(dim_actor(
            actor_key=50000 + actor.actor_id,
            actor_id=actor.actor_id,
            first_name=actor.first_name,
            last_name=actor.last_name,
            last_update=actor.last_update.strftime("%Y-%m-%d")
        ))
    sqlite_session.commit()

def increment_dim_category(sqlite_session, mysql_session, last_sync):
    categories = mysql_session.query(Category).filter(Category.last_update > last_sync).all()
    for category in categories:
        sqlite_session.merge(dim_category(
            category_key = 30000 + category.category_id * 10 + 1,
            category_id = category.category_id,
            name = category.name,
            last_update = category.last_update.strftime("%Y-%m-%d")
        ))
    sqlite_session.commit()

def increment_dim_store(sqlite_session, mysql_session, last_sync):
    stores = mysql_session.query(Store, Address, City, Country).join(
        Address, Store.address_id == Address.address_id).join(
        City, Address.city_id == City.city_id).join(
        Country, City.country_id == Country.country_id
    ).filter(Store.last_update > last_sync).all()
    for store, address, city, country in stores:
        sqlite_session.merge(dim_store(
            store_key = 1000 + store.store_id,
            store_id = store.store_id,
            city = city.city,
            country = country.country,
            last_update = store.last_update.strftime("%Y-%m-%d")
        ))
    sqlite_session.commit()

def increment_dim_customer(sqlite_session, mysql_session, last_sync):
    customers = mysql_session.query(Customer, Address, City, Country).join(
        Address, Customer.address_id == Address.address_id).join(
        City, Address.city_id == City.city_id).join(
        Country, City.country_id == Country.country_id
    ).filter(Customer.last_update > last_sync).all()
    for customer, address, city, country in customers:
        sqlite_session.merge(dim_customer(
            customer_key = customer.customer_id * 100 + 1,
            customer_id = customer.customer_id,
            first_name = customer.first_name,
            last_name = customer.last_name,
            active = customer.active,
            city = city.city,
            country = country.country,
            last_update = customer.last_update.strftime("%Y-%m-%d")
        ))
    sqlite_session.commit()

def increment_bridge_film_actor(sqlite_session, mysql_session):
    '''
    we could find all records in MySQL that are not in SQLite, and update the table that way,
    which would not catch deleted rows.
    We could load both tables, and find all records that exist in only one table, and true up accordingly,
    but I think this would be quite slow. This just drops and rebuilds the table, which is simpler
    and similar performance-wise.
    '''
    sqlite_session.query(bridge_film_actor).delete()
    film_actors = mysql_session.query(FilmActor).all()
    for film_actor in film_actors:
        sqlite_session.merge(bridge_film_actor(
            film_key = film_actor.film_id * 100 + 1,
            actor_key=50000 + film_actor.actor_id
        ))
    sqlite_session.commit()


def increment_bridge_film_category(sqlite_session, mysql_session):
    '''
    everything said about the increment_bridge_film_actor table is also true here
    '''
    sqlite_session.query(bridge_film_category).delete()
    film_categories = mysql_session.query(FilmCategory).all()
    for film_category in film_categories:
        sqlite_session.merge(bridge_film_category(
            film_key=film_category.film_id * 100 + 1,
            category_key=30000 + film_category.category_id * 10 + 1
        ))
    sqlite_session.commit()

def increment_fact_rental(sqlite_session, mysql_session, last_sync):
    rentals = mysql_session.query(Rental, Inventory, Film).join(
        Inventory, Rental.inventory_id == Inventory.inventory_id).join(
        Film, Film.film_id == Inventory.film_id).filter(Rental.rental_date > last_sync).all()
    max_key = sqlite_session.query(func.max(fact_rental.fact_rental_key)).scalar() or 50000
    for i, (rental, inventory, film) in enumerate(rentals, start=1):
        sqlite_session.merge(fact_rental(
            fact_rental_key = max_key + i,
            rental_id = rental.rental_id,
            date_key_rented = rental.rental_date.strftime("%Y%m%d"),
            date_key_returned = rental.return_date.strftime("%Y%m%d") if rental.return_date is not None else None,
            film_key = film.film_id*100 + 1,
            store_key = 1000 + inventory.store_id,
            customer_key = rental.customer_id * 100 + 1,
            staff_id = rental.staff_id,
            rental_duration_days = (rental.return_date - rental.rental_date).days if rental.return_date is not None else None
        ))
    sqlite_session.commit()

def increment_fact_payment(sqlite_session, mysql_session, last_sync):
    payments = mysql_session.query(Payment, Staff).join(
        Staff, Payment.staff_id == Staff.staff_id
    ).filter(Payment.payment_date > last_sync).all()
    max_key = sqlite_session.query(func.max(fact_payment.fact_payment_key)).scalar() or 80000
    for i, (payment, staff) in enumerate(payments, start=1):
        sqlite_session.merge(fact_payment(
            fact_payment_key = max_key + i,
            payment_id = payment.payment_id,
            date_key_paid = payment.payment_date.strftime("%Y%m%d"),
            customer_key = payment.customer_id * 100 + 1,
            store_key = 1000 + staff.store_id,
            staff_id = payment.staff_id,
            amount = payment.amount
        ))
    sqlite_session.commit()