"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import psycopg2
from config import N_EMPLOYEES_DATA, N_CUSTOMERS_DATA, N_ORDERS_DATA


def open_data_file(data):
    new_list = []

    with open(data, encoding='utf-8') as file:
        csv_file = csv.DictReader(file)
        for row in csv_file:
            if row.get('employee_id'):
                row['employee_id'] = int(row['employee_id'])
            new_list.append(row)
    return new_list


def add_data(data_file, table_name):
    with psycopg2.connect(host='localhost', database='north', user='postgres', password=1111) as conn:
        with conn.cursor() as cur:
            for data_info in data_file:
                count = '%s ' * len(data_info)
                value = tuple(data_info.values())
                cur.execute(f"INSERT INTO {table_name} VALUES ({', '.join(count.split())})", value)
    conn.close()


if __name__ == '__main__':
    data_employees = open_data_file(N_EMPLOYEES_DATA)
    add_data(data_employees, 'employees')

    data_customers = open_data_file(N_CUSTOMERS_DATA)
    add_data(data_customers, 'customers')

    data_orders = open_data_file(N_ORDERS_DATA)
    add_data(data_orders, 'orders')
