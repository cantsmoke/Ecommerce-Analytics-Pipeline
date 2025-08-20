# Ecommerce Analytics Pipeline  

![Python](https://img.shields.io/badge/Python-3.9-blue?logo=python)  
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue?logo=postgresql)  
![Airflow](https://img.shields.io/badge/Apache-Airflow-darkblue?logo=apacheairflow)  
![Power BI](https://img.shields.io/badge/PowerBI-Dashboards-yellow?logo=powerbi)  
![Telegram](https://img.shields.io/badge/Telegram-Bot-26A5E4?logo=telegram)  

**End-to-end аналитическая платформа для онлайн-магазина.**  
Проект охватывает полный цикл работы с данными - от генерации и хранения до визуализации, алертов и автоматической отчетности.  

Схемы архитектуры (DFD и ER-диаграммы), скриншоты дашбордов, алертов, отчетов доступны в  [Аналитический pipeline (PDF)](./Аналитический%20pipeline.pdf).  

## Архитектура  

Основные компоненты:  
- **PostgreSQL** – хранилище данных  
- **ETL-скрипты** – генерация и загрузка данных  
- **Apache Airflow** – оркестрация процессов  
- **Power BI** – дашборды и аналитика  
- **Telegram Bot** – алерты и отчеты  

## Дашборды  

В Power BI реализованы:  
- DAU / WAU / MAU  
- Кол-во заказов и выручка  
- Конверсия просмотров в заказы  
- Популярность категорий и брендов  
- Выручка по городам  

## Система алертов  

- Мониторинг ключевых метрик (**DAU, Revenue, Orders, Conversion Rate**)  
- Пороговые значения и уведомления при аномалиях  
- Автоматическая отправка в **Telegram**  

## Автоматическая отчетность  

- Еженедельные **PDF-отчеты** (WAU, Revenue, динамика заказов/выручки, распределение по городам)  
- Генерация с помощью Airflow  
- Хранение по периодам  
- Запрос отчетов через **Telegram-бота**  

Примеры автоматически сформированных отчетов можно посмотреть в [Report Preview](./ReportPreview)
