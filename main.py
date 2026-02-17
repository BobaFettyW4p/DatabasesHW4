from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from sqlite_helper_classes import *
from sqlite_helper_functions import *
from sakila_helper_classes import *
from incremental_helper_functions import *
import argparse
from dotenv import load_dotenv
from pathlib import Path
import os

def configure_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["Init", "Full-load","Incremental","Validate"])
    return parser.parse_args()

def create_sqlite_engine(db_name):
    engine = create_engine(f"sqlite:///{db_name}.db", echo=False)

    try:
        engine.connect()
        print(f"database ivancicm has been created")
        return engine
    except Exception as e:
        print(f"unable to create sqlite database ivancicm: {e}")


def create_mysql_engine(mysql_username, mysql_password):
    engine = create_engine(f"mysql+pymysql://{mysql_username}:{mysql_password}@localhost/sakila", echo=False)

    try:
        engine.connect()
        print(f"connection to sakila database has been extablished")
        return engine
    except Exception as e:
        print(f"unable to connect to sakila database: {e}")


def create_sqlite_tables(engine):
    Base.metadata.create_all(engine)
    return "all tables have been created"

def retrieve_mysql_credentials():
    env_file = Path(".env")
    if env_file.exists():
        load_dotenv()
    else:
        raise Exception(".env file not populated. Please recreate and try again")
    return os.getenv("DB_USERNAME"), os.getenv("DB_PASSWORD")


def create_sqlite_session(engine):
    Session = sessionmaker(bind=engine)
    try:
        sqlite_session = Session()
        print(f"sqlite session established.")
        return sqlite_session
    except Exception as e:
        print(f"unable to establish sqlite session: {e}")

def create_mysql_session(engine):
    Session = sessionmaker(bind=engine)
    try:
        mysql_session = Session()
        print(f"mysql session established.")
        return mysql_session
    except Exception as e:
        print(f"unable to establish mysql session: {e}")

def populate_sqlite_tables(sqlite_session, mysql_session):
    print(f"beginning populating sqlite tables")
    create_dim_film(sqlite_session, mysql_session)
    create_dim_actor(sqlite_session, mysql_session)
    create_dim_category(sqlite_session, mysql_session)
    create_dim_store(sqlite_session, mysql_session)
    create_dim_customer(sqlite_session, mysql_session)
    create_bridge_film_actor(sqlite_session, mysql_session)
    create_bridge_film_category(sqlite_session, mysql_session)
    create_fact_rental(sqlite_session, mysql_session)
    create_fact_payment(sqlite_session, mysql_session)
    create_dim_date(sqlite_session, mysql_session)
    create_sync_state(sqlite_session)

def incremental_sync(sqlite_session, mysql_session):
    print(f"beginning incremental update")
    sync_config = [
        ("dim_film", Film, Film.last_update, increment_dim_film),
        ("dim_actor", Actor, Actor.last_update, increment_dim_actor),
        ("dim_category", Category, Category.last_update, increment_dim_category),
        ("dim_store", Store, Store.last_update, increment_dim_store),
        ("dim_customer", Customer, Customer.last_update, increment_dim_customer),
        ("fact_rental", Rental, Rental.rental_date, increment_fact_rental),
        ("fact_payment", Payment, Payment.payment_date, increment_fact_payment),
    ]

    for table_name, model, timestamp_column, incremental_function in sync_config:
        state = sqlite_session.query(sync_state).filter_by(table_name=table_name).first()
        last_sync = datetime.strptime(state.last_update, "%Y-%m-%d %H:%M:%S") if state else datetime.min

        incremental_function(sqlite_session, mysql_session, last_sync)

        max_ts = mysql_session.query(func.max(timestamp_column)).scalar()
        if max_ts:
            sqlite_session.merge(sync_state(
                table_name=table_name,
                last_update=max_ts.strftime("%Y-%m-%d %H:%M:%S"),
            ))
            sqlite_session.commit()
            print(f"sync_state updated: {table_name} -> {max_ts}")

    increment_bridge_film_actor(sqlite_session, mysql_session)
    increment_bridge_film_category(sqlite_session, mysql_session)
    increment_dim_date(sqlite_session, mysql_session)
    return "full sync complete!"

def validate_sqlite_database(tables_to_validate, sqlite_session, mysql_session):
    all_passed = True
    failed_tables = []
    for table, mysql_table, sqlite_table in tables_to_validate:
        table_status = validate_table(sqlite_session, sqlite_table, mysql_session, mysql_table)
        if table_status == False:
            all_passed = False
            failed_tables.append(table)
        else:
            continue
    if failed_tables:
        return False, failed_tables
    return True, None


def validate_table(sqlite_session, sqlite_table, mysql_session, mysql_table):
    source_count = mysql_session.query(mysql_table).count()
    target_count = sqlite_session.query(sqlite_table).count()
    validation_status = source_count == target_count
    return validation_status

def validate_payment_amounts(sqlite_session, mysql_session):
    mysql_total = mysql_session.query(func.sum(Payment.amount)).scalar()
    sqlite_total = sqlite_session.query(func.sum(fact_payment.amount)).scalar()
    if mysql_total == sqlite_total:
        return True, mysql_total, sqlite_total
    else:
        return False, mysql_total, sqlite_total


def main():
    db_name = "ivancicm"
    mysql_username, mysql_password = retrieve_mysql_credentials()
    args = configure_arguments()

    sqlite_engine = create_sqlite_engine(db_name)
    mysql_engine = create_mysql_engine(mysql_username, mysql_password)
    sqlite_session = create_sqlite_session(sqlite_engine)
    mysql_session = create_mysql_session(mysql_engine)

    if args.mode == "Init":
        create_sqlite_tables(sqlite_engine)
    elif args.mode == "Full-load":
        populate_sqlite_tables(sqlite_session, mysql_session)
    elif args.mode == "Incremental":
        incremental_sync(sqlite_session, mysql_session)

    elif args.mode == "Validate":
        tables_to_validate = [
        ("film", Film, dim_film),
        ("actor", Actor, dim_actor),
        ("category", Category, dim_category),
        ("store", Store, dim_store),
        ("customer", Customer, dim_customer),
        ("film_actor", FilmActor, bridge_film_actor),
        ("film_category", FilmCategory, bridge_film_category),
        ("rental", Rental, fact_rental),
        ("payment", Payment, fact_payment),
    ]
        validation_status, failed_tables = validate_sqlite_database(tables_to_validate, sqlite_session, mysql_session)
        payment_validation_status, mysql_amount, sqlite_amount = validate_payment_amounts(sqlite_session, mysql_session)
        if validation_status == True and payment_validation_status == True:
            print(f"tables successfully validated!")
        elif validation_status == False and payment_validation_status == True:
            print(f"tables not successfully validated: {failed_tables}")
        elif validation_status == True and payment_validation_status == False:
            print(f"payment table not validated. mysql value:{mysql_amount}, sqlite value:{sqlite_amount}")
        else:
            print(f"tables not successfully validated: {failed_tables} and amounts not validated: mysql value:{mysql_amount}, sqlite value:{sqlite_amount}")
    else:
        raise Exception("Invalid mode")



if __name__ == "__main__":
    main()