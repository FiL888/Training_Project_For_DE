import py_scripts.connection_db as db
import pandas as pd
import os
import re

# формирование STG слоя
def create_stg_layer():
    # Получаем каталог, в котором содержится наш код, т.к. в нём содержатся и все нужные нам файлы
    dir_name = os.path.dirname(os.path.dirname(__file__))
    try:
        # Получаем список активных файлов
        ls_names_files = get_list_active_files(dir_name)
        if len(ls_names_files):
            for f_name in ls_names_files:
                full_path = dir_name+'/'+f_name
                f_name_backup = dir_name+'/'+'archive'+'/'+f_name+'.backup'
                # Читаем файлы и грузим в БД, затем перемещаем их в archive с расширением backup
                if is_true_file('passport_blacklist',f_name):
                    df = pd.read_excel(full_path, dtype={'date':str})
                    create_and_replace('passport_blacklist', f_name, f_name_backup, df)
                elif is_true_file('terminals',f_name):
                    df = pd.read_excel(full_path)
                    create_and_replace('terminals', f_name, f_name_backup, df)
                elif is_true_file('transactions',f_name, 'txt'):
                    df = pd.read_csv(full_path,sep = ';')
                    df['amount'] = pd.to_numeric(df['amount'].str.replace(',','.'))
                    create_and_replace('transactions', f_name, f_name_backup, df)
        else:
            return False

        db.init_stg_layer_from_existed_tables()
    except Exception as e:
        print('На этапе загрузки первичной информации произошла ошибка: {}'.format(e))
        return False

    return True

# формирование fact слоя
def create_fact_layer():

    try:
        db.create_fact_tables()
    except Exception as e:
        print('На этапе создания fact-слоя произошли ошибки: {}'.format(e))
        return False

    return True

# формирование Dim слоя
def create_dim_layer():

    try:
        db.create_dim_tables()
    except Exception as e:
        print('На этапе создания Dim-слоя произошли ошибки: {}'.format(e))
        return False

    return True

# Получаем список активных файлов
# Активными считаются такие файлы, которые пришли раньше всех
# dir_name - строка - полный путь к директории
# Возврат - список - имена активных файлов
def get_list_active_files(dir_name):
    ls_all = os.listdir(dir_name)
    ls_dict_files = [dict(), dict(), dict()]
    # Отбираем имена файлов из всего списка в директории
    for f_name in ls_all:
        if is_true_file('passport_blacklist',f_name):
            add_to_dict_of_files(ls_dict_files[0], f_name)
        elif is_true_file('terminals', f_name):
            add_to_dict_of_files(ls_dict_files[1], f_name)
        elif is_true_file('transactions',f_name,'txt'):
            add_to_dict_of_files(ls_dict_files[2], f_name)
    # сортируем по ключу, чтоб однозначно определить файл, пришедший раньше всех
    # затем отдаём первый элемент, который и будет самым ранним файлом
    answer_ls = []
    for i in range(len(ls_dict_files)):
        if len(ls_dict_files[i]):
            ks = list(ls_dict_files[i].keys())
            # это для оптимизации. Исключаем сортировку, если файл 1
            if len(ls_dict_files[i]) == 1:
                answer_ls.append(ls_dict_files[i][ks[0]])
            else:
                answer_ls.append(sorted(ls_dict_files[i].items())[0][1])

    return answer_ls

# Добавляем имя файла в словарь, при этом вычисляем цифровой идентификатор по дате
# где дата "01032022", превратившись в число будет выглядеть так 20220301
# это решает проблему сортировки строк с повышением десятичного разряда
# d - словарь - целевой словарь, куда поместится имя файла по идентификатору
# f_name - строка - имя файла
def add_to_dict_of_files(d, f_name):
    date_str = re.search(r'\d+',f_name).group(0)
    d[int(date_str[4:8]+date_str[2:4]+date_str[:2])] = f_name

# Даёт понимание, является ли файл тем, который нужен
# f_part_name - строка - часть имени файла
# f_name - строка - имя файла
# f_extension - строка - расширение файла
def is_true_file(f_part_name, f_name, f_extension = 'xlsx'):
    
    # ++ Не работает на python 3.4
    res = re.search(r''+f_part_name+'_\d+.'+f_extension,f_name)
    try:
        if res.group(0) == f_name:
    # --
    #res = f_name.find(f_part_name) >= 0
    #try:    
    #    if res:
            return True
        else:
            return False
    except:
        return False
        
# создаёт таблицу STG слоя, заполняет её, а затем переносит файл в указанную папку
# in_name - имя таблицы
# f_name - имя файла (полный путь)
# f_name_backup - имя файла для архива
# df - DataFrame с вносимой в таблицы БД информацией
def create_and_replace(in_name, f_name, f_name_backup, df):
    if in_name == 'passport_blacklist':
        sokr_name = 'pssprt_blcklst'
    else:
        sokr_name = in_name
    db.create_tmp_table(sokr_name)
    db.set_turples_in_tmp_table(df.values, sokr_name)
    os.replace(f_name, f_name_backup)
