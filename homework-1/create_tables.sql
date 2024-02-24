-- SQL-команды для создания таблиц
CREATE TABLE employees
(
	employee_id int PRIMARY KEY,
	first_name varchar(50) NOT NULL,
	last_name varchar(50) NOT NULL,
	title varchar(100) NOT NULL,
	birth_date date NOT NULL,
	notes text
);

CREATE TABLE customers
(
    customer_id varchar PRIMARY KEY,
	company_name varchar(100) NOT NULL,
	contact_name varchar(50) NOT NULL
);

CREATE TABLE orders
(
    order_id varchar PRIMARY KEY,
	customer_id varchar REFERENCES customers(customer_id) NOT NULL,
	employee_id int REFERENCES employees(employee_id) NOT NULL,
	order_date date NOT NULL,
	ship_city varchar(50) NOT NULL
);

SELECT * FROM employees
SELECT * FROM customers
SELECT * FROM orders