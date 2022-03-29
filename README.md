# Training_Project_For_DE
Решение задачи поиска мошеннических операций

Начальная структура проекта:
1) archive - директория с отработанными файлами
2) py_scripts - директория с python библиотеками (модулями)
2.1) io.py - модуль в котором описаны функции работы с файлами и полный ETL-процесс
2.2) connection_db.py - модуль для функций взаимодействия с БД
3) sql_scripts - директория с SQL скриплами (запросами)
3.1) rep_fraud.sql - запрос для создания таблицы с отчетом
3.2) all_data.sql - запрос для создания таблицы с полной информацией для анализа
6) main.cron - исполняемый файл для утилиты CRON. Запуск регламентного задания
7) main.py - исполняемый файл программы

KharlamovFA.pdf - презентация проекта с подробным описанием действий

