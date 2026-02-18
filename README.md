# DatabasesHW4

This project is written in python and uses sqlalchemy as the ORM used in this project.

In order to properly manage packages in python, I have chosen to use `uv`. For more information on installing this package manger on your system:

https://docs.astral.sh/uv/getting-started/installation/

in order to run the project:

1. Clone the repo

```
git clone https://github.com/BobaFettyW4p/DatabasesHW4.git
```

2. Sync uv for the project

```
uv sync
```

3. Create a .env file with the credentials to access to access the sakila database hosted locally

```
DB_USERNAME=your_database_username
DB_PASSWORD=super_secret_password
```
NOTE: your .env file will be blocked from version control by the .gitignore file

With this prep work complete, you are ready to use this project. Use the following commands...

4. To initialize the sqlite database

```
uv run main.py --mode Init
```

5. To perform a full load from mysql into sqlite

```
uv run main.py --mode Full-load
```

6. To perform an incremental update

```
uv run main.py --mode Incremental
```

7. To perform validation on your created sqlite database

```
uv run main.py --mode Validate
```
