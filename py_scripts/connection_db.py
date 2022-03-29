import jaydebeapi as cn
from sys import platform
import os

my_id = 'fil8' # собственный идентификатор
list_t = ['terminals','transactions','pssprt_blcklst'] # список постоянных имен таблиц
list_facts_tables = ['transactions','pssprt_blcklst'] # список таблиц фактов
d_facts_tables_fields = {'transactions':'trans_id,trans_date', \
                         'pssprt_blcklst': 'entry_dt,passport_num'}
list_dim_tables = ['terminals','cards','clients','accounts'] # список таблиц фактов
list_t_unchanged = ['cards', 'accounts', 'clients']
list_other_tables =  ['all_data']
# словарь {Таблица : Список колонок(строкой)}, где Список колонок указан в такой же последовательности, как и в файле, а имена, как в ER-диаграмме
d_columns = {'terminals' : 'terminal_id,terminal_type,terminal_city,terminal_address', \
            'transactions' : 'trans_id,trans_date,amt,card_num,oper_type,oper_result,terminal', \
            'pssprt_blcklst' : 'entry_dt,passport_num', \
            'cards' : 'card_num,account', \
            'clients' : 'client_id,last_name,first_name,patronymic,date_of_birth,passport_num,passport_valid_to,phone', \
            'accounts' : 'account,valid_to,client'}
# словарь идентификаторов таблиц
d_identificators = {'terminals' : 'terminal_id', \
                    'transactions' : 'trans_id,trans_date,amt', \
                    'pssprt_blcklst' : 'entry_dt,passport_num', \
                    'cards' : 'card_num', \
                    'clients' : 'client_id', \
                    'accounts' : 'account'}

path_jar = 'C:\sqldeveloper\jdbc\lib\ojdbc8.jar'
if platform.find('linux') != -1:
    path_jar = '/home/de3tn/ojdbc8.jar'

conn = cn.connect('oracle.jdbc.driver.OracleDriver',
                        'jdbc:oracle:thin:de3tn/farmermaggot@de-oracle.chronosavant.ru:1521/deoracle',
                        ['de3tn','farmermaggot'],
                        path_jar)

curs = conn.cursor()

# Закрытие подключения
def close_connect():
    curs.close()
    conn.close()

# Инициализация STG слоя из существующих таблиц
def init_stg_layer_from_existed_tables():
    for t_name in list_t_unchanged:
        create_tmp_table(t_name, True)
    
# Создание временныых таблиц
# t_name - имя таблицы
# from_existed - устанавливаем True, если временная таблица будет создана из существующей
def create_tmp_table(t_name, from_existed = False):
    
    try:
        curs.execute('''
        select * from {}_STG_{}
        WHERE ROWNUM = 1
        '''.format(my_id, t_name))
    except:
        if from_existed != True:
            fields = text_fields_for_create_tables(t_name)
            curs.execute('''
            create table {}_STG_{} ({})
            '''.format(my_id, t_name, fields))
        else:
            curs.execute('''
            create table {}_STG_{} as 
            (
                select * from bank.{}
            )
            '''.format(my_id, t_name, t_name))

# Создание таблиц для Fact слоя
def create_fact_tables():

    for t_name in list_facts_tables:
        create_fact_table(t_name)

# Создание таблицы для Fact слоя
# t_name - имя таблицы
def create_fact_table(t_name):

    # Проверяем, существует ли таблица в STG слое, если нет, то ничего не делаем
    if not exist_STG_table(t_name):
       return

    # если дошли до сюда, то стоит произвести операцию с таблицей факта
    try:
        # узнаем, существует ли таблица
        curs.execute('''
        select * from {}_dwh_fact_{}
        WHERE ROWNUM = 1
        '''.format(my_id,t_name))
        
        # Если мы сюда пришли, значит существует, и её необходимо дополнить
        fields = d_facts_tables_fields[t_name]
        curs.execute('''
        insert into {}_dwh_fact_{}
        select * from {}_STG_{}
        where ({}) not in (select {} 
                                from {}_dwh_fact_{})
        '''.format(my_id, t_name, my_id, t_name, fields, fields, my_id, t_name))
    except:
        curs.execute('''
        create table {}_dwh_fact_{} as 
        (
            select * from {}_STG_{}
        )
        '''.format(my_id, t_name, my_id, t_name))   

# Создание таблиц для Dim слоя
def init_dim_tables():
    for t_name in list_dim_tables:
        create_dim_table(t_name)

# Создание таблицы для Dim слоя
# t_name - имя таблицы
def create_dim_table(t_name):
    try:
        curs.execute('''
        select * from {}_dwh_dim_{}_hist
        WHERE ROWNUM = 1
        '''.format(my_id, t_name))
    except:
        fields = text_fields_for_create_tables(t_name)
        curs.execute('''
        create table {}_dwh_dim_{}_hist
        (effective_from date DEFAULT sysdate, 
        effective_to date DEFAULT TO_DATE('31-12-2999 23:59:59','DD-MM-YYYY HH24:Mi:SS'),
        deleted_flg numeric(1) default 0,
        {})'''.format(my_id, t_name, fields))

# Создание таблицы для инкремента
def init_tmp_tables_for_etl_process():
    for t_name in list_dim_tables:
        init_tmp_table_for_etl_process(t_name)

def init_tmp_table_for_etl_process(t_name):
    
    if exist_STG_table(t_name):
        fields = text_fields_for_create_tables(t_name)
        for i in range(3):
            curs.execute('''
            create table {}_stg_{}_0{}
            ({})'''.format(my_id, t_name, i, fields))

def update_New_rows_tables():
    for t_name in list_dim_tables:
        update_New_rows_table(t_name)

def update_New_rows_table(t_name):
    if not exist_STG_table(t_name):
        return
    name_t1 = '{}_stg_{}'.format(my_id, t_name)
    name_t2 = '{}_dwh_dim_{}_hist'.format(my_id, t_name)
    fields_pref = add_prefix('t1',d_columns[t_name])
    sql_tmpl = '''

        insert into {}_stg_{}_00
        select {}
        from {} t1
        left join (select {} from {}
                    where deleted_flg = 0) t2
                    on t1.{} = t2.{}
        where t2.{} is null
        
    '''.format(my_id, t_name, fields_pref, name_t1, d_columns[t_name], name_t2, d_identificators[t_name], d_identificators[t_name], d_identificators[t_name])
    curs.execute(sql_tmpl)

def update_Deleted_rows_tables():
    for t_name in list_dim_tables:
        update_Deleted_rows_table(t_name)

def update_Deleted_rows_table(t_name):
    if not exist_STG_table(t_name):
        return
    name_t1 = '{}_stg_{}'.format(my_id, t_name)
    name_t2 = '{}_dwh_dim_{}_hist'.format(my_id, t_name)
    fields_pref = add_prefix('t1',d_columns[t_name])
    curs.execute('''

        insert into {}_stg_{}_02
        select {}
        from (select {} from {}
                where deleted_flg = 0) t1
        left join {} t2
        on t1.{} = t2.{}
        where t2.{} is null
        
    '''.format(my_id, t_name, fields_pref, d_columns[t_name], name_t2, name_t1, d_identificators[t_name], d_identificators[t_name], d_identificators[t_name]))                

def update_Changed_rows_tables():
    for t_name in list_dim_tables:
        update_Changed_rows_table(t_name)

def update_Changed_rows_table(t_name):
    if not exist_STG_table(t_name):
        return
    name_t1 = '{}_stg_{}'.format(my_id,t_name)
    name_t2 = '{}_dwh_dim_{}_hist'.format(my_id, t_name)
    fields_pref = add_prefix('t1',d_columns[t_name])
    chk_fields = check_fields(d_columns[t_name], d_identificators[t_name])
    curs.execute('''

        insert into {}_stg_{}_01
        select
            {}
        from {} t1
        inner join (select {} from {}
                    where deleted_flg = 0) t2
        on t1.{} = t2.{}
        and (
                {}
            )

    '''.format(my_id, t_name, fields_pref, name_t1, d_columns[t_name], name_t2, d_identificators[t_name], d_identificators[t_name], chk_fields))

def update_Hist_rows_tables():
    for t_name in list_dim_tables:
        update_Hist_rows_table(t_name)

def update_Hist_rows_table(t_name):
    if not exist_STG_table(t_name):
        return

    name_t0 = '{}_stg_{}_00'.format(my_id, t_name)
    name_t1 = '{}_stg_{}_01'.format(my_id, t_name)
    name_t2 = '{}_stg_{}_02'.format(my_id, t_name)
    name_hist = '{}_dwh_dim_{}_hist'.format(my_id, t_name)
    
    # Изменяем дату окончания действия и ставим пометку удаления измененных и удаленных записей
    tmpl = '''
        update {}
        set effective_to = sysdate - interval '1' second, deleted_flg = 1
        where ({} in (select {} from {} union select {} from {}))
        and (effective_to = TO_DATE('31-12-2999 23:59:59','DD-MM-YYYY HH24:Mi:SS') or deleted_flg = 0)
        '''.format(name_hist, d_identificators[t_name], d_identificators[t_name], name_t1, d_identificators[t_name], name_t2)
    # print(tmpl)
    curs.execute(tmpl)

    # заносим в историческую таблицу новые и измененные записи
    tmpl = '''
        insert into {} ({})
        select {} from {} 
        union 
        select {} from {} 
        '''.format(name_hist, d_columns[t_name], d_columns[t_name], name_t0, d_columns[t_name], name_t1)
    # print(tmpl)
    curs.execute(tmpl)

# Главная функция инкремента. Создаёт таблицы и заполняет их
def create_dim_tables():
    init_dim_tables() # Создание Hist таблиц
    init_tmp_tables_for_etl_process() # Создание временных таблиц для новый, измененных и удаленных записей
    update_New_rows_tables() # Заполнение таблиц Новых записей
    update_Deleted_rows_tables() # Заполнение таблиц удаленных записей
    update_Changed_rows_tables() # Заполнение таблиц измененных записей
    update_Hist_rows_tables() # Обновление исторических таблиц

# Создание и заполнение таблицы отчета
# Здесь же создаётся таблица, куда будут собраны все объединенные записи из всех актуальных таблиц процесса
def create_rep_table():
    
    if not exist_table('STG_all_data'):
        text = get_query('all_data')
        curs.execute(text)
    
    text = get_query('rep_fraud')
    if exist_table('rep_fraud'):
        curs.execute('''insert into {}_rep_fraud
                        {}'''.format(my_id, text))
    else:
        curs.execute('''CREATE TABLE {}_rep_fraud as 
                        {}'''.format(my_id, text))
    

def drop_tmp_tables():
    for t_name in list_t:
        try:
            drop_tmp_table(t_name)
        except:
            continue

    for t_name in list_t_unchanged:
        try:
            drop_tmp_table(t_name)
        except:
            continue

    for t_name in list_other_tables:
        try:
            drop_tmp_table(t_name)
        except:
            continue

    for t_name in list_dim_tables:
        try:
            drop_tmp_increment_table(t_name)
        except:
            continue

def drop_tmp_increment_table(t_name):
    for i in range(3):
        curs.execute('''
            drop table {}_STG_{}_0{}
            '''.format(my_id, t_name, str(i)))
        
def drop_tmp_table(t_name):
    curs.execute('''
    drop table {}_STG_{}
    '''.format(my_id, t_name))

def drop_table(t_name):
    curs.execute('''
    drop table {}_{}
    '''.format(my_id, t_name))

def drop_fact_tables():
    for t_name in list_facts_tables:
        try:
            drop_fact_table(t_name)
        except:
            continue

def drop_fact_table(t_name):
    curs.execute('''
    drop table {}_dwh_fact_{}
    '''.format(my_id, t_name))

def drop_dim_tables():
     for t_name in list_dim_tables:
         try:
            drop_dim_table(t_name)
         except:
            continue

def drop_dim_table(t_name):
    curs.execute('''
    drop table {}_dwh_dim_{}_hist
    '''.format(my_id, t_name))

def drop_rep_table():
    if exist_table('rep_fraud'):
        curs.execute('''
        drop table {}_{}
        '''.format(my_id, 'rep_fraud'))

# Функция показывает возможность формирования таблицы отчета
def possible_create_rep():
    for t_name in list_facts_tables:
        if not exist_table('dwh_fact_{}'.format(t_name)):
            return False
    for t_name in list_dim_tables:
        if not exist_table('dwh_dim_{}_hist'.format(t_name)):
            return False
    return True

# Поля таблиц процесса для создания таблиц
def text_fields_for_create_tables(t_name):
    text = ''
    if t_name == 'transactions':
        text = '''trans_id varchar(11),
        trans_date date,
        card_num char(20),
        oper_type varchar(8),
        amt number(15,2),
        oper_result varchar(7),
        terminal varchar(5)
        '''
    elif t_name == 'terminals':
        text =  '''terminal_id varchar(5),
        terminal_type varchar(3),
        terminal_city varchar(70),
        terminal_address varchar(255)
        '''
    elif t_name == 'pssprt_blcklst':
        text = '''passport_num varchar(11),
        entry_dt date
        '''
    elif t_name == 'cards':
        text = '''card_num char(20),
        account char(20)
        '''
    elif t_name == 'clients':
        text = '''client_id varchar(20),
        last_name varchar(100),
        first_name varchar(100),
        patronymic varchar(100),
        date_of_birth date,
        passport_num varchar(15),
        passport_valid_to date,
        phone varchar(20)
        '''
    elif t_name == 'accounts':
        text = '''account char(20),
        valid_to date,
        client varchar(20)
        '''

    return text

# Вспомогательная функция
# Создаёт строку в виде '?,?,?...' по количеству элементов в списке ls
def create_arguments(ls):
    template_ls = []
    for el in ls.split(','):
        if el in ('trans_date','entry_dt'):
            template_ls.append("to_date(?,'YYYY-MM-DD HH24:Mi:SS')")
        else:
            template_ls.append("?")
    return ','.join(template_ls)

# Заполнение таблицы
# ls          - список - данные
# t_name    - строка - имя таблицы
# cmt           - булево - признак фиксации транзакции (по-умолчанию False)
def set_turples_in_tmp_table(ls, t_name, cmt = False):
    names_columns = d_columns[t_name]
    str_arguments = create_arguments(names_columns)
    input_ls = ls.tolist()
    
    templ = "insert into {}_STG_{} ({}) values ({})".format(my_id, t_name, names_columns, str_arguments)
    curs.executemany(templ, input_ls)
    if cmt:
        conn.commit()

def exist_STG_table(t_name):
    return exist_table("STG_{}".format(t_name))

# Определяет, существует ли таблица
def exist_table(t_name):
    # Проверяем, существует ли таблица в STG слое, если нет, то ничего не делаем
    try:
        # узнаем, существует ли таблица
        curs.execute('''
        select * from {}_{}
        WHERE ROWNUM = 1
        '''.format(my_id, t_name))
        return True
    except:
        return False

# Добавляет префикс к таблице
def add_prefix(prefix, ls):
    return ','.join([prefix + '.' + x for x in ls.split(',')])

# Создает строку полей поиска для измененных данных
def check_fields(ls_fields, field_except):
    ls_result = []
    for el in ls_fields.split(','):
        if el != field_except:
            ls_result.append('t1.{} <> t2.{}'.format(el, el))
    return ' or '.join(ls_result)

# Получает запрос из файла
def get_query(q_name):

    try:
        dir_name = os.path.dirname(os.path.dirname(__file__))
        f = open('{}/sql_scripts/{}.sql'.format(dir_name,q_name), 'r')
        text = f.read()
        f.close()
        return text
    except Exception as e:
        print('На этапе чтения скрипта из {} произошла ошибка: {}'.format(q_name, e))
        return ''
