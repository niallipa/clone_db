
import pandas as pd
from sqlalchemy import create_engine, MetaData, inspect
import time

# Параметры подключения к PostgreSQL
pg_user = "postgres"
pg_password = "37892afeff8e120d714cf5d7b39bdfea"
pg_host = "95.140.159.144"
pg_port = '5432'
pg_db = "energy_db"
pg_schema = "towers"

# Параметры подключения к SQLite
sqlite_db = "test.sqlite"

# Подключение к PostgreSQL
pg_connection_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
pg_engine = create_engine(pg_connection_string)

# Подключение к SQLite
sqlite_engine = create_engine(f"sqlite:///{sqlite_db}")

# Создание объекта Inspector для PostgreSQL
inspector = inspect(pg_engine)

# Получение списка всех таблиц из PostgreSQL
pg_tables = inspector.get_table_names(schema=pg_schema)

# Вывод списка таблиц
print("Список таблиц в PostgreSQL:")
for table in pg_tables:
    print(table, end=', ')

print(f'\nВсего таблиц: {len(pg_tables)}\n')
print('Клонирование:')
start_clone = time.time()
i = 1
# Клонирование таблиц
for table in pg_tables:
    print(f"{i}. Клонирование таблицы {table}...", end="", flush=True)
    start_clone_table = time.time()
    pg_df = pd.read_sql_table(table, pg_engine, schema=pg_schema)
    pg_df.to_sql(table, sqlite_engine, if_exists='replace', index=False)
    print(f"\r{i}. Таблица {table} склонирована за {time.time() - start_clone_table:.2f} сек")
    i += 1

# Закрытие соединений
pg_engine.dispose()
sqlite_engine.dispose()
print(f'Клонирование базы завершено за {time.time() - start_clone:.2f}')
