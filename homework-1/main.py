"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import psycopg2
from config import N_EMPLOYEES_DATA, N_CUSTOMERS_DATA, N_ORDERS_DATA

try:
    conn = psycopg2.connect(
        host='localhost',
        database='north',
        user='postgres',
        password='1111'
    )
    with conn.cursor() as cur:
        with open(N_CUSTOMERS_DATA) as customers_file:
            data = csv.DictReader(customers_file)
            for record in data:
                cur.execute('INSERT INTO customers VALUES (%s,%s,%s)', (
                    record['customer_id'],
                    record['company_name'],
                    record['contact_name']
                ))
        with open(N_EMPLOYEES_DATA) as employees_file:
            data = csv.DictReader(employees_file)
            for record in data:
                cur.execute('INSERT INTO employees VALUES (%s,%s,%s,%s,%s,%s)', (
                    record['employee_id'],
                    record['first_name'],
                    record['last_name'],
                    record['title'],
                    record['birth_date'],
                    record['notes']
                ))
        with open(N_ORDERS_DATA) as orders_file:
            data = csv.DictReader(orders_file)
            for record in data:
                cur.execute('INSERT INTO orders VALUES (%s,%s,%s,%s,%s)', (
                    record['order_id'],
                    record['customer_id'],
                    record['employee_id'],
                    record['order_date'],
                    record['ship_city']
                ))
        cur.execute('SELECT * FROM employees')
        cur.execute('SELECT * FROM customers')
        cur.execute('SELECT * FROM orders')
finally:
    conn.commit()
    conn.close()
