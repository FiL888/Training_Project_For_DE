import py_scripts.io as io
import py_scripts.connection_db as db
import time

# формирование STG слоя. Закончит тогда, когда в папке закончатся файлы
while io.create_stg_layer(): 
    # формирование Dim слоя
    io.create_dim_layer()
    # формирование Fact слоя
    io.create_fact_layer() 
    # если создание отчета возможно,
    # а возможно оно бывает тогда, когда присутствуют все необходимые для этого таблицы
    if db.possible_create_rep():
        # Формирование отчета
        db.create_rep_table()
    # удаление таблиц STG слоя
    db.drop_tmp_tables()
    # удаление Fact таблиц
    db.drop_fact_tables()
    # задержка на 5 секунд
    time.sleep(5)

# Отключение от БД
db.close_connect()