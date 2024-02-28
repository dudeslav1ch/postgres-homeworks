"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import psycopg2

# connect to db
conn = psycopg2.connect(
    host='localhost',
    database='north',
    user='postgres',
    password='136400'
)
try:
    with conn:
        with conn.cursor() as cur:
            with open('../homework-1/north_data/employees_data.csv') as file:
                csv_reader = csv.DictReader(file)
                for row in csv_reader:
                    cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                                (row['employee_id'], row['first_name'], row['last_name'],
                                 row['title'], row['birth_date'], row['notes']))

            with open('../homework-1/north_data/customers_data.csv') as file:
                csv_reader = csv.DictReader(file)
                for row in csv_reader:
                    cur.execute("INSERT INTO customers VALUES (%s, %s, %s)",
                                (row['customer_id'], row['company_name'], row['contact_name']))

            with open('../homework-1/north_data/orders_data.csv') as file:
                csv_reader = csv.DictReader(file)
                for row in csv_reader:
                    cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                                (row['order_id'], row['customer_id'], row['employee_id'],
                                 row['order_date'], row['ship_city']))

            cur.execute("SELECT * FROM employees")
            cur.execute("SELECT * FROM customers")
            cur.execute("SELECT * FROM orders")
finally:
    conn.close()
