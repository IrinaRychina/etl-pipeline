﻿# etl-pipeline
Проект ETL (Extract, Transform, Load) для обработки и анализа данных.

В проекте используется данные TPC-H Benchmark:
https://www.kaggle.com/datasets/marcogorelli/tpc-h-parquet-s-1

TPC-H - это данные, сгенерированные для теста производительности СУБД на больших объемах данных.


Используя тестовые данные бенчмарка TPC-H с kaggle, в проекте строится pipeline данных:
* Extract - получить данные с сайта kaggle "marcogorelli/tpc-h-parquet-s-1"
* Transform - преобразовать данные для сохранения в базу данных
* Load - сохранение в бд, создание витрины данных

Предусмотрена реализация 5 отчетов:
- LineItems
- Orders
- Customers
- Suppliers
- Part

### Вид выполненного дага:

<img width="1545" height="742" alt="dag_graph_visualsitaion" src="https://github.com/user-attachments/assets/c5e72ef8-7bdb-4ca6-b2e9-fd824cc22fda" />

В каждом отчете выполняются преобразования, например, вычисление среднего, минимального, максимального значения, суммы, подсчет нужных значений.

### Логи задания, которое выполняет преобразования для отчета LineItems:
<img width="1682" height="701" alt="transform_lineitems" src="https://github.com/user-attachments/assets/41ee3c52-7707-4557-af85-c8e48dc6b737" />

### Логи задания для отчета Orders:
<img width="1671" height="957" alt="transform_orders" src="https://github.com/user-attachments/assets/03acdb29-d22e-48a9-a03d-b74c29865803" />

### Логи задания для отчета Customers:
<img width="1710" height="946" alt="transform_customers" src="https://github.com/user-attachments/assets/545043a1-0fd3-445e-906b-dedee4e9430c" />

### Логи задания для отчета Suppliers:
<img width="1669" height="939" alt="transform_suppliers" src="https://github.com/user-attachments/assets/f0373a64-4892-4a84-824e-948d6d6a9a21" />

### Логи задания для отчета Part:
<img width="1669" height="692" alt="transform_parts" src="https://github.com/user-attachments/assets/5ce05f2c-7ada-4f1f-8ef6-e0ee6a9ddc12" />


# Технологический стек
- Airflow - оркестрация ETL процессов
- PySpark - обработка данных
- Clickhouse - хранение данных 

# Инфраструктура
- Docker - контейнеризация всех компонентов

# Установка
Для сборки на своем локальном компьютере нужно скачать репозиторий и в директории проекта выполнить команду:

``` docker-compose up -d --build ```

После сборки airflow будет доступен по адресу http://localhost:8080.
На вкладке Dags будет доступен даг tpch_report.
