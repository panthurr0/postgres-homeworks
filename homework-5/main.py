import json
import sqlite3
import psycopg2

from config import config


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'

    db_name = 'my_new_db'

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur, suppliers)
                print(f"FOREIGN KEY успешно добавлены")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f'DROP DATABASE {db_name}')
    cur.execute(f'CREATE DATABASE {db_name}')

    cur.close()
    conn.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(script_file, 'r') as file:
        cur.execute(file.read())


def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    cur.execute(f'CREATE TABLE suppliers('
                f'supplier_id serial PRIMARY KEY,'
                f'company_name character varying(100) NOT NULL,'
                f'contact character varying(100),'
                f'address character varying(100),'
                f'phone character varying(100),'
                f'fax character varying(100),'
                f'homepage text'
                f')')


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    with open(json_file) as file:
        data_list = json.load(file)
    return data_list


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    for supplier in suppliers:
        cur.execute('INSERT INTO suppliers (company_name, contact, address, phone, fax, homepage) '
                    'VALUES (%s, %s, %s, %s, %s, %s)',
                    (supplier['company_name'], supplier['contact'], supplier['address'],
                     supplier['phone'], supplier['fax'], supplier['homepage']))


def add_foreign_keys(cur, suppliers) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    cur.execute('ALTER TABLE products '
                'ADD COLUMN supplier_id INTEGER REFERENCES suppliers(supplier_id)'
                )

    cur.execute('ALTER TABLE products '
                'ADD CONSTRAINT fk_products_suppliers '
                'FOREIGN KEY (supplier_id) REFERENCES suppliers;')

    for supplier in suppliers:
        cur.execute('SELECT supplier_id FROM suppliers '
                    'WHERE company_name = %s',
                    (supplier['company_name'],))
        supplier_id = cur.fetchone()[0]

        for product in supplier['products']:
            cur.execute('UPDATE products '
                        'SET supplier_id = %s '
                        'WHERE product_name = %s',
                        (supplier_id, product))


if __name__ == '__main__':
    main()
