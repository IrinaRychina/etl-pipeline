# etl-pipeline
Проект ETL (Extract, Transform, Load) для обработки и анализа данных.
Используя тестовые данные бенчмарка TPC-H с kaggle, в проекте строится pipeline данных:
Extract - получить данные с сайта kaggle "marcogorelli/tpc-h-parquet-s-1"
Transform - очистить и преобразовать данные для сохранения в базу данных
Load - сохранение в бд, создание витрины данных

Предусмотрена реализация 5 отчетов:
- [ ] LineItems
- [ ] Orders
- [ ] Customers
- [ ] Suppliers
- [ ] Part

# Технологический стек
- [x] Airflow - оркестрация ETL процессов
- [x] PySpark - обработка данных
- [ ] Greenplum - хранение данных 

# Инфраструктура
* Docker - контейнеризация всех компонентов