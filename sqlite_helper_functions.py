from sakila_helper_classes import *
from sqlite_helper_classes import *
from datetime import timedelta
from sqlalchemy import func


def create_dim_date(sqlite_session, mysql_session):
    '''
    I used generative AI to stratgegize how to write this function to populate the table

    https://claude.ai/share/2efca197-814a-4720-a949-bea2d58b7f5b

    overall, I think Claude's suggestions made sense, although it was initially perhaps too simple to account for all edge cases.
    I think this code is functional, even if it's not the most readable
    '''
    all_dates = [
        mysql_session.query(func.min(Rental.rental_date)).scalar(),
        mysql_session.query(func.max(Rental.rental_date)).scalar(),
        mysql_session.query(func.max(Rental.return_date)).scalar(),
        mysql_session.query(func.min(Payment.payment_date)).scalar(),
        mysql_session.query(func.max(Payment.payment_date)).scalar(),
        mysql_session.query(func.min(Film.last_update)).scalar(),
        mysql_session.query(func.max(Film.last_update)).scalar(),
        mysql_session.query(func.min(Actor.last_update)).scalar(),
        mysql_session.query(func.max(Actor.last_update)).scalar(),
        mysql_session.query(func.min(Category.last_update)).scalar(),
        mysql_session.query(func.max(Category.last_update)).scalar(),
        mysql_session.query(func.min(Store.last_update)).scalar(),
        mysql_session.query(func.max(Store.last_update)).scalar(),
        mysql_session.query(func.min(Customer.last_update)).scalar(),
        mysql_session.query(func.max(Customer.last_update)).scalar(),
    ]

    valid_dates = [d.date() if d else None for d in all_dates]
    valid_dates = [d for d in valid_dates if d is not None]

    start_date = min(valid_dates)
    end_date = max(valid_dates)
    current = start_date
    while current <= end_date:
        sqlite_session.merge(dim_date(
            date_key = current.strftime("%Y%m%d"),
            date = current.strftime("%Y-%m-%d"),
            year = str(current.year),
            quarter = str((current.month - 1) // 3 + 1),
            month = str(current.month),
            day_of_month = str(current.day),
            day_of_week = str(current.isoweekday()),
            is_weekend = 1 if current.isoweekday() >= 6 else 0
        ))
        current += timedelta(days=1)
    sqlite_session.commit()


def create_dim_film(sqlite_session, mysql_session):
    films = mysql_session.query(Film, Language).join(
        Language, Film.language_id == Language.language_id
    ).all()
    for film, language in films:
        sqlite_session.merge(dim_film(
            film_key = film.film_id*100 + 1,
            film_id = film.film_id,
            title = film.title,
            rating = film.rating,
            length = film.length,
            language = language.name,
            release_year = film.release_year,
            last_update = film.last_update.strftime("%Y-%m-%d")
        ))
    sqlite_session.commit()


def create_dim_actor(sqlite_session, mysql_session):
    actors = mysql_session.query(Actor).all()
    for actor in actors:
        sqlite_session.merge(dim_actor(
            actor_key = 50000 + actor.actor_id,
            actor_id = actor.actor_id,
            first_name = actor.first_name,
            last_name = actor.last_name,
            last_update = actor.last_update.strftime("%Y-%m-%d")
        ))
    sqlite_session.commit()


def create_dim_category(sqlite_session, mysql_session):
    categories = mysql_session.query(Category).all()
    for category in categories:
        sqlite_session.merge(dim_category(
            category_key = 30000 + category.category_id * 10 + 1,
            category_id = category.category_id,
            name = category.name,
            last_update = category.last_update.strftime("%Y-%m-%d")
        ))
    sqlite_session.commit()


def create_dim_store(sqlite_session, mysql_session):
    stores = mysql_session.query(Store, Address, City, Country).join(
        Address, Store.address_id == Address.address_id).join(
        City, Address.city_id == City.city_id).join(
        Country, City.country_id == Country.country_id
    ).all()
    for store, address, city, country in stores:
        sqlite_session.merge(dim_store(
            store_key = 1000 + store.store_id,
            store_id = store.store_id,
            city = city.city,
            country = country.country,
            last_update = store.last_update.strftime("%Y-%m-%d")
        ))
    sqlite_session.commit()

def create_dim_customer(sqlite_session, mysql_session):
    customers = mysql_session.query(Customer, Address, City, Country).join(
        Address, Customer.address_id == Address.address_id).join(
        City, Address.city_id == City.city_id).join(
        Country, City.country_id == Country.country_id
    ).all()
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



def create_bridge_film_actor(sqlite_session, mysql_session):
    film_actors = mysql_session.query(FilmActor).all()
    for film_actor in film_actors:
        sqlite_session.merge(bridge_film_actor(
            # these are the same formulas to determine the film_key from the dim_film table and the actor_key from dim_actor
            film_key = film_actor.film_id*100 + 1,
            actor_key = 50000 + film_actor.actor_id
        ))
    sqlite_session.commit()



def create_bridge_film_category(sqlite_session, mysql_session):
    film_categories = mysql_session.query(FilmCategory).all()
    for film_category in film_categories:
        sqlite_session.merge(bridge_film_category(
            film_key = film_category.film_id*100 + 1,
            category_key = 30000 + film_category.category_id * 10 + 1
        ))
    sqlite_session.commit()


def create_fact_rental(sqlite_session, mysql_session):
    rentals = mysql_session.query(Rental, Inventory, Film).join(
        Inventory, Rental.inventory_id == Inventory.inventory_id).join(
        Film, Film.film_id == Inventory.film_id).all()
    for i, (rental, inventory, film) in enumerate(rentals, start=1):
        sqlite_session.merge(fact_rental(
            fact_rental_key = 50000 + i,
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


def create_fact_payment(sqlite_session, mysql_session):
    payments = mysql_session.query(Payment, Staff).join(
        Staff, Payment.staff_id == Staff.staff_id
    ).all()
    for i, (payment, staff) in enumerate(payments, start=1):
        sqlite_session.merge(fact_payment(
            fact_payment_key = 80000 + i,
            payment_id = payment.payment_id,
            date_key_paid = payment.payment_date.strftime("%Y%m%d"),
            customer_key = payment.customer_id * 100 + 1,
            store_key = 1000 + staff.store_id,
            staff_id = payment.staff_id,
            amount = payment.amount
        ))
    sqlite_session.commit()

def create_sync_state(sqlite_session):
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tables = ["dim_film", "dim_actor", "dim_category", "dim_store",
              "dim_customer", "fact_rental", "fact_payment"]
    for table_name in tables:
        sqlite_session.merge(sync_state(
            table_name = table_name,
            last_update = current_time
        ))
    sqlite_session.commit()
    print("sync state initialized")

