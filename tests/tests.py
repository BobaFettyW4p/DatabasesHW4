import pytest

from sqlalchemy import inspect
from main import *
from sakila_helper_classes import *

#declare path constants to allow the tests to run properly
PROJECT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_DIR / "ivancicm"


#fixtures to be used in testing
@pytest.fixture(scope="session")
def sqlite_engine():
    engine = create_engine(f"sqlite:///{DB_PATH}.db", echo=False)
    return engine


@pytest.fixture(scope="session")
def mysql_engine():
    original_dir = os.getcwd()
    os.chdir(PROJECT_DIR)
    username, password = retrieve_mysql_credentials()
    os.chdir(original_dir)
    engine = create_engine(
        f"mysql+pymysql://{username}:{password}@localhost/sakila", echo=False
    )
    return engine


@pytest.fixture(scope="session")
def sqlite_session(sqlite_engine):
    Session = sessionmaker(bind=sqlite_engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture(scope="session")
def mysql_session(mysql_engine):
    Session = sessionmaker(bind=mysql_engine)
    session = Session()
    yield session
    session.close()


#init tests
class TestInit:
    def test_sqlite_db_created(self):
        '''confirm the sqlite db is created in the correct location'''
        db_file = Path(f"{DB_PATH}.db")
        assert db_file.exists(), "SQLite database file should exist"

    def test_sqlite_db_not_empty(self):
        '''confirm sqlite db is populated with entries'''
        db_file = Path(f"{DB_PATH}.db")
        assert db_file.stat().st_size > 0, "SQLite database file should not be empty"

    def test_all_tables_exist(self, sqlite_engine):
        '''confirm all tables created successfully'''
        inspector = inspect(sqlite_engine)
        tables = inspector.get_table_names()
        expected_tables = [
            "dim_date", "dim_film", "dim_actor", "dim_category",
            "dim_store", "dim_customer", "bridge_film_actor",
            "bridge_film_category", "fact_rental", "fact_payment",
            "sync_state",
        ]
        for table in expected_tables:
            assert table in tables, f"Table '{table}' should exist in SQLite database"

    def test_dim_date_columns(self, sqlite_engine):
        '''confirm dim_date is populated with all of the correct columns'''
        inspector = inspect(sqlite_engine)
        columns = [col["name"] for col in inspector.get_columns("dim_date")]
        expected = ["date_key", "date", "year", "quarter", "month",
                    "day_of_month", "day_of_week", "is_weekend"]
        for col in expected:
            assert col in columns, f"dim_date should have column '{col}'"

    def test_dim_film_columns(self, sqlite_engine):
        '''confirm dim_film is populated with all of the correct columns'''
        inspector = inspect(sqlite_engine)
        columns = [col["name"] for col in inspector.get_columns("dim_film")]
        expected = ["film_key", "film_id", "title", "rating", "length",
                    "language", "release_year", "last_update"]
        for col in expected:
            assert col in columns, f"dim_film should have column '{col}'"

    def test_fact_rental_columns(self, sqlite_engine):
        '''confirm fact_rental is populated with all of the correct columns'''
        inspector = inspect(sqlite_engine)
        columns = [col["name"] for col in inspector.get_columns("fact_rental")]
        expected = ["fact_rental_key", "rental_id", "date_key_rented",
                    "date_key_returned", "film_key", "store_key",
                    "customer_key", "staff_id", "rental_duration_days"]
        for col in expected:
            assert col in columns, f"fact_rental should have column '{col}'"

    def test_fact_payment_columns(self, sqlite_engine):
        '''confirm fact_payment is populated with all of the correct columns'''
        inspector = inspect(sqlite_engine)
        columns = [col["name"] for col in inspector.get_columns("fact_payment")]
        expected = ["fact_payment_key", "payment_id", "date_key_paid",
                    "customer_key", "store_key", "staff_id", "amount"]
        for col in expected:
            assert col in columns, f"fact_payment should have column '{col}'"


#full-load test class
class TestFullLoad:
    #confirm mysql,sqlite engine is populated properly
    def test_sqlite_engine_exists(self, sqlite_engine):
        '''confirm sqlite engine is initialized'''
        assert sqlite_engine is not None

    def test_mysql_engine_exists(self, mysql_engine):
        '''confirm mysql engine is initialized'''
        assert mysql_engine is not None

    def test_sqlite_session_exists(self, sqlite_session):
        '''confirm sqlite session is initialized'''
        assert sqlite_session is not None

    def test_mysql_session_exists(self, mysql_session):
        '''confirm mysql session is initialized'''
        assert mysql_session is not None

    #confirm all tables are populated from the mysql database
    def test_dim_film_populated(self, sqlite_session):
        '''confirm dim_film has at least one row'''
        count = sqlite_session.query(dim_film).count()
        assert count > 0, "dim_film should have rows"

    def test_dim_actor_populated(self, sqlite_session):
        '''confirm dim_actor has at least one row'''
        count = sqlite_session.query(dim_actor).count()
        assert count > 0, "dim_actor should have rows"

    def test_dim_category_populated(self, sqlite_session):
        '''confirm dim_category has at least one row'''
        count = sqlite_session.query(dim_category).count()
        assert count > 0, "dim_category should have rows"

    def test_dim_store_populated(self, sqlite_session):
        '''confirm dim_store has at least one row'''
        count = sqlite_session.query(dim_store).count()
        assert count > 0, "dim_store should have rows"

    def test_dim_customer_populated(self, sqlite_session):
        '''confirm dim_customer has at least one row'''
        count = sqlite_session.query(dim_customer).count()
        assert count > 0, "dim_customer should have rows"

    def test_dim_date_populated(self, sqlite_session):
        '''confirm dim_date has at least one row'''
        count = sqlite_session.query(dim_date).count()
        assert count > 0, "dim_date should have rows"

    def test_bridge_film_actor_populated(self, sqlite_session):
        '''confirm bridge_film_actor has at least one row'''
        count = sqlite_session.query(bridge_film_actor).count()
        assert count > 0, "bridge_film_actor should have rows"

    def test_bridge_film_category_populated(self, sqlite_session):
        '''confirm bridge_film_category has at least one row'''
        count = sqlite_session.query(bridge_film_category).count()
        assert count > 0, "bridge_film_category should have rows"

    def test_fact_rental_populated(self, sqlite_session):
        '''confirm fact_rental has at least one row'''
        count = sqlite_session.query(fact_rental).count()
        assert count > 0, "fact_rental should have rows"

    def test_fact_payment_populated(self, sqlite_session):
        '''confirm fact_payment has at least one row'''
        count = sqlite_session.query(fact_payment).count()
        assert count > 0, "fact_payment should have rows"

    def test_sync_state_populated(self, sqlite_session):
        '''confirm sync_state has exactly 7 entries, one per synced table'''
        count = sqlite_session.query(sync_state).count()
        assert count == 7, "sync_state should have 7 entries (one per synced table)"


    #data quality tests
    def test_dim_date_key_format(self, sqlite_session):
        """date_key should be 8-char YYYYMMDD"""
        dates = sqlite_session.query(dim_date).all()
        for date in dates:
            assert len(date.date_key) == 8, f"date_key '{d.date_key}' should be 8 chars"

    def test_dim_date_date_format(self, sqlite_session):
        """date should be YYYY-MM-DD"""
        dates = sqlite_session.query(dim_date).all()
        for date in dates:
            assert len(date.date) == 10 and date.date[4] == "-" and date.date[7] == "-", \
                f"date '{date.date}' should be YYYY-MM-DD format"

    def test_dim_date_weekend_flag(self, sqlite_session):
        '''is_weekend should be 1 for Saturday/Sunday, 0 otherwise'''
        dates = sqlite_session.query(dim_date).all()
        for date in dates:
            parsed = datetime.strptime(date.date_key, "%Y%m%d")
            expected = 1 if parsed.isoweekday() >= 6 else 0
            assert date.is_weekend == expected, \
                f"is_weekend for {date.date} should be {expected}, got {date.is_weekend}"

    def test_dim_date_quarter_values(self, sqlite_session):
        '''quarter should be 1, 2, 3, or 4'''
        dates = sqlite_session.query(dim_date).all()
        for date in dates:
            assert date.quarter in ("1", "2", "3", "4"), \
                f"quarter should be 1-4, got {date.quarter}"

    #row counts match
    def test_film_count_matches_mysql(self, sqlite_session, mysql_session):
        '''confirm dim_film row count matches mysql film source'''
        sqlite_count = sqlite_session.query(dim_film).count()
        mysql_count = mysql_session.query(Film).count()
        assert sqlite_count == mysql_count, \
            f"Film counts differ: SQLite={sqlite_count}, MySQL={mysql_count}"

    def test_actor_count_matches_mysql(self, sqlite_session, mysql_session):
        '''confirm dim_actor row count matches mysql actor source'''
        sqlite_count = sqlite_session.query(dim_actor).count()
        mysql_count = mysql_session.query(Actor).count()
        assert sqlite_count == mysql_count

    def test_category_count_matches_mysql(self, sqlite_session, mysql_session):
        '''confirm dim_category row count matches mysql category source'''
        sqlite_count = sqlite_session.query(dim_category).count()
        mysql_count = mysql_session.query(Category).count()
        assert sqlite_count == mysql_count

    def test_store_count_matches_mysql(self, sqlite_session, mysql_session):
        '''confirm dim_store row count matches mysql store source'''
        sqlite_count = sqlite_session.query(dim_store).count()
        mysql_count = mysql_session.query(Store).count()
        assert sqlite_count == mysql_count

    def test_customer_count_matches_mysql(self, sqlite_session, mysql_session):
        '''confirm dim_customer row count matches mysql customer source'''
        sqlite_count = sqlite_session.query(dim_customer).count()
        mysql_count = mysql_session.query(Customer).count()
        assert sqlite_count == mysql_count

    def test_rental_count_matches_mysql(self, sqlite_session, mysql_session):
        '''confirm fact_rental row count matches mysql rental source'''
        sqlite_count = sqlite_session.query(fact_rental).count()
        mysql_count = mysql_session.query(Rental).count()
        assert sqlite_count == mysql_count

    def test_payment_count_matches_mysql(self, sqlite_session, mysql_session):
        '''confirm fact_payment row count matches mysql payment source'''
        sqlite_count = sqlite_session.query(fact_payment).count()
        mysql_count = mysql_session.query(Payment).count()
        assert sqlite_count == mysql_count

    def test_bridge_film_actor_count_matches_mysql(self, sqlite_session, mysql_session):
        '''confirm bridge_film_actor row count matches mysql film_actor source'''
        sqlite_count = sqlite_session.query(bridge_film_actor).count()
        mysql_count = mysql_session.query(FilmActor).count()
        assert sqlite_count == mysql_count

    def test_bridge_film_category_count_matches_mysql(self, sqlite_session, mysql_session):
        '''confirm bridge_film_category row count matches mysql film_category source'''
        sqlite_count = sqlite_session.query(bridge_film_category).count()
        mysql_count = mysql_session.query(FilmCategory).count()
        assert sqlite_count == mysql_count


#incremental mode testing
class TestIncremental:
    def test_sync_state_has_all_tables(self, sqlite_session):
        '''confirm sync_state has an entry for each synced table'''
        expected = ["dim_film", "dim_actor", "dim_category", "dim_store",
                    "dim_customer", "fact_rental", "fact_payment"]
        for table_name in expected:
            state = sqlite_session.query(sync_state).filter_by(
                table_name=table_name
            ).first()
            assert state is not None, \
                f"sync_state should have entry for {table_name}"

    def test_sync_state_timestamps_valid(self, sqlite_session):
        '''sync_state timestamps should be valid YYYY-MM-DD HH:MM:SS strings'''
        states = sqlite_session.query(sync_state).all()
        for state in states:
            try:
                datetime.strptime(state.last_update, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pytest.fail(
                    f"Invalid timestamp for {state.table_name}: {state.last_update}"
                )

    def test_dim_film_no_duplicate_keys(self, sqlite_session):
        '''confirm dim_film has no duplicate film_keys'''
        total = sqlite_session.query(dim_film).count()
        distinct = sqlite_session.query(dim_film.film_key).distinct().count()
        assert total == distinct, "dim_film should have no duplicate film_keys"

    def test_dim_actor_no_duplicate_keys(self, sqlite_session):
        '''confirm dim_actor has no duplicate actor_keys'''
        total = sqlite_session.query(dim_actor).count()
        distinct = sqlite_session.query(dim_actor.actor_key).distinct().count()
        assert total == distinct

    def test_fact_rental_no_duplicate_keys(self, sqlite_session):
        '''confirm fact_rental has no duplicate fact_rental_keys'''
        total = sqlite_session.query(fact_rental).count()
        distinct = sqlite_session.query(fact_rental.fact_rental_key).distinct().count()
        assert total == distinct

    def test_fact_payment_no_duplicate_keys(self, sqlite_session):
        '''confirm fact_payment has no duplicate fact_payment_keys'''
        total = sqlite_session.query(fact_payment).count()
        distinct = sqlite_session.query(fact_payment.fact_payment_key).distinct().count()
        assert total == distinct

    def test_bridge_film_actor_film_keys_valid(self, sqlite_session):
        '''film_keys in bridge_film_actor should all exist in dim_film'''
        bridge_keys = {r.film_key for r in sqlite_session.query(bridge_film_actor.film_key).distinct()}
        dim_keys = {r.film_key for r in sqlite_session.query(dim_film.film_key)}
        orphans = bridge_keys - dim_keys
        assert len(orphans) == 0, f"Orphan film_keys in bridge_film_actor: {orphans}"

    def test_bridge_film_actor_actor_keys_valid(self, sqlite_session):
        '''actor_keys in bridge_film_actor should all exist in dim_actor'''
        bridge_keys = {r.actor_key for r in sqlite_session.query(bridge_film_actor.actor_key).distinct()}
        dim_keys = {r.actor_key for r in sqlite_session.query(dim_actor.actor_key)}
        orphans = bridge_keys - dim_keys
        assert len(orphans) == 0, f"Orphan actor_keys in bridge_film_actor: {orphans}"

    def test_bridge_film_category_film_keys_valid(self, sqlite_session):
        '''film_keys in bridge_film_category should all exist in dim_film'''
        bridge_keys = {r.film_key for r in sqlite_session.query(bridge_film_category.film_key).distinct()}
        dim_keys = {r.film_key for r in sqlite_session.query(dim_film.film_key)}
        orphans = bridge_keys - dim_keys
        assert len(orphans) == 0

    def test_fact_rental_film_keys_valid(self, sqlite_session):
        '''film_keys in fact_rental should all exist in dim_film'''
        film_key = {rental.film_key for rental in sqlite_session.query(fact_rental.film_key).distinct()}
        dup_key = {rental.film_key for rental in sqlite_session.query(dim_film.film_key)}
        orphans = film_key - dup_key
        assert len(orphans) == 0, f"Orphan film_keys in fact_rental: {orphans}"

    def test_fact_rental_customer_keys_valid(self, sqlite_session):
        '''customer_keys in fact_rental should all exist in dim_customer'''
        film_key = {rental.customer_key for rental in sqlite_session.query(fact_rental.customer_key).distinct()}
        dup_key = {rental.customer_key for rental in sqlite_session.query(dim_customer.customer_key)}
        orphans = film_key - dup_key
        assert len(orphans) == 0, f"Orphan customer_keys in fact_rental: {orphans}"

    def test_fact_rental_store_keys_valid(self, sqlite_session):
        '''store_keys in fact_rental should all exist in dim_store'''
        film_key = {rental.store_key for rental in sqlite_session.query(fact_rental.store_key).distinct()}
        dup_key = {rental.store_key for rental in sqlite_session.query(dim_store.store_key)}
        orphans = film_key - dup_key
        assert len(orphans) == 0, f"Orphan store_keys in fact_rental: {orphans}"

    def test_fact_payment_customer_keys_valid(self, sqlite_session):
        '''customer_keys in fact_payment should all exist in dim_customer'''
        film_key = {rental.customer_key for rental in sqlite_session.query(fact_payment.customer_key).distinct()}
        dup_key = {rental.customer_key for rental in sqlite_session.query(dim_customer.customer_key)}
        orphans = film_key - dup_key
        assert len(orphans) == 0, f"Orphan customer_keys in fact_payment: {orphans}"

    def test_fact_payment_store_keys_valid(self, sqlite_session):
        '''store_keys in fact_payment should all exist in dim_store'''
        film_key = {rental.store_key for rental in sqlite_session.query(fact_payment.store_key).distinct()}
        dup_key = {rental.store_key for rental in sqlite_session.query(dim_store.store_key)}
        orphans = film_key - dup_key
        assert len(orphans) == 0, f"Orphan store_keys in fact_payment: {orphans}"

    def test_fact_rental_date_keys_valid(self, sqlite_session):
        '''date_key_rented in fact_rental should all exist in dim_date'''
        film_key = {rental.date_key_rented for rental in sqlite_session.query(fact_rental.date_key_rented).distinct() if r.date_key_rented}
        dup_key = {rental.date_key for rental in sqlite_session.query(dim_date.date_key)}
        orphans = film_key - dup_key
        assert len(orphans) == 0, f"Orphan date_key_rented in fact_rental: {orphans}"


#tests for the validation mode
class TestValidate:
    def test_validate_table_film(self, sqlite_session, mysql_session):
        '''validate_table should return True for film'''
        result = validate_table(sqlite_session, dim_film, mysql_session, Film)
        assert result is True, "Film row count validation should pass"

    def test_validate_table_actor(self, sqlite_session, mysql_session):
        '''validate_table should return True for actor'''
        result = validate_table(sqlite_session, dim_actor, mysql_session, Actor)
        assert result is True

    def test_validate_table_category(self, sqlite_session, mysql_session):
        '''validate_table should return True for category'''
        result = validate_table(sqlite_session, dim_category, mysql_session, Category)
        assert result is True

    def test_validate_table_store(self, sqlite_session, mysql_session):
        '''validate_table should return True for store'''
        result = validate_table(sqlite_session, dim_store, mysql_session, Store)
        assert result is True

    def test_validate_table_customer(self, sqlite_session, mysql_session):
        '''validate_table should return True for customer'''
        result = validate_table(sqlite_session, dim_customer, mysql_session, Customer)
        assert result is True

    def test_validate_table_rental(self, sqlite_session, mysql_session):
        '''validate_table should return True for rental'''
        result = validate_table(sqlite_session, fact_rental, mysql_session, Rental)
        assert result is True

    def test_validate_table_payment(self, sqlite_session, mysql_session):
        '''validate_table should return True for payment'''
        result = validate_table(sqlite_session, fact_payment, mysql_session, Payment)
        assert result is True

    def test_validate_table_bridge_film_actor(self, sqlite_session, mysql_session):
        '''validate_table should return True for bridge_film_actor'''
        result = validate_table(sqlite_session, bridge_film_actor, mysql_session, FilmActor)
        assert result is True

    def test_validate_table_bridge_film_category(self, sqlite_session, mysql_session):
        '''validate_table should return True for bridge_film_category'''
        result = validate_table(sqlite_session, bridge_film_category, mysql_session, FilmCategory)
        assert result is True

    def test_validate_payment_amounts(self, sqlite_session, mysql_session):
        '''validate_payment_amounts should return True when totals match'''
        status, mysql_total, sqlite_total = validate_payment_amounts(
            sqlite_session, mysql_session
        )
        assert status is True, \
            f"Payment amounts should match: MySQL={mysql_total}, SQLite={sqlite_total}"

    def test_payment_amounts_not_none(self, sqlite_session, mysql_session):
        '''validate_payment_amounts should return non-None totals'''
        _, mysql_total, sqlite_total = validate_payment_amounts(
            sqlite_session, mysql_session
        )
        assert mysql_total is not None
        assert sqlite_total is not None

    def test_payment_amounts_positive(self, sqlite_session, mysql_session):
        '''payment totals from both databases should be positive'''
        _, mysql_total, sqlite_total = validate_payment_amounts(
            sqlite_session, mysql_session
        )
        assert mysql_total > 0
        assert sqlite_total > 0

    def test_validate_sqlite_database_all_pass(self, sqlite_session, mysql_session):
        '''validate_sqlite_database should pass for all tables'''
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
        result, failed = validate_sqlite_database(
            tables_to_validate, sqlite_session, mysql_session
        )
        assert result is True, f"All tables should validate. Failed: {failed}"
        assert failed is None

    def test_validate_table_returns_false_on_mismatch(self, sqlite_session):
        """validate_table should return False when row counts differ"""
        # dim_film and dim_date have different row counts, so this should be False
        result = validate_table(sqlite_session, dim_film, sqlite_session, dim_date)
        assert result is False
