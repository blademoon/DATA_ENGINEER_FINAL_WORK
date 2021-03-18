#!/usr/bin/python
import logging
import os
import re
import sys

import jaydebeapi
import pandas as pd

# Отладочный режим
# Значение True - включается автокомит после каждой транзакции (autocommit = true), выдается больше отладочной
# информации на экран.
# Значение False - Режим работы в продакшене. Автокоммит после каждой транзакции выключен. На экран работы ничего не
# выдается. Транзакция коммитится только в конце успешной загрузки всех данных.
# ВАЖНЫЙ МОМЕНТ! Сообщения в журнал работы (файл "main.log") пишутся в обоих режимах.
DEBUG = True


# Функция загружает указанный файл в pandas data frame
def file_to_df(directory_full_path, file_template):
    # Ищем нужный файл в указанной директории
    for filename in os.listdir(directory_full_path):

        if DEBUG:
            print("Current file in loop: {}".format(filename))

        # Если файл соответствует переданному шаблону, то обрабатываем его
        if re.match(file_template, filename):
            if DEBUG:
                print("File detected! {}".format(filename))
            # Если это xlsx файл c черным списком паспортов, то загружаем соответсвующим образом
            if (filename.lower().endswith(".xlsx")) and ("passport" in filename):
                # Считываем файл в dataframe
                func_result = pd.read_excel(filename, sheet_name="blacklist", header=0, index_col=None)
                # Переименовываем файл
                os.rename(filename, ".//archive//" + filename + ".backup")
                return func_result
            if (filename.lower().endswith(".xlsx")) and ("terminals" in filename):
                # Считываем файл в dataframe
                func_result = pd.read_excel(filename, sheet_name="terminals", header=0, index_col=None)

                # Получаем дату из имени файла
                temp_date = (re.search('\d{8}', filename)).group(0)
                date_str = temp_date[0:2] + "." + temp_date[2:4] + "." + temp_date[4:8]

                # Добавляем к полученному датафрейму серию (колонку) с датой файла
                func_result["date"] = date_str

                # Переименовываем файл
                os.rename(filename, ".//archive//" + filename + ".backup")
                return func_result
            if (filename.lower().endswith(".txt")):
                # Считываем файл в dataframe
                func_result = pd.read_csv(filename, sep=';', decimal=',', header=0)
                # Переименовываем файл
                os.rename(filename, ".//archive//" + filename + ".backup")
                return func_result

    # Если до текущего момента мы не вернули дата фрейм с первым найденным по шаблону файлом,
    # то файлов соответсвующих шаблону не существует. Кинем исключение.
    raise Exception("Can't find a data flat file that matches the pattern \"{}\"".format(file_template))


# Функция для подключения к серверу DWH
def connect_to_dwh(username, password, server, port, ojdbc8_jar_file_path):
    connection = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver',
                                    'jdbc:oracle:thin:{usr}/{passwd}@{serv}:{port}/deoracle'.format(usr=username,
                                                                                                    passwd=password,
                                                                                                    serv=server,
                                                                                                    port=port),
                                    [username, password],
                                    ojdbc8_jar_file_path)
    return connection


# Функция для очистки таблиц стейджинга
def clear_all_tables(stg_table_names, sql_curs) -> None:
    # Перебираем все таблицы стейджинга из списка и удаляем из них строки.
    for table_name in stg_table_names:
        # Формируем запрос.
        sql_req = "DELETE FROM {tbl_nm}".format(tbl_nm=table_name)

        # Выполняем запрос
        sql_curs.execute(sql_req)

    return None


# Небольшой обработчик выхода с закрытием соединения с сервером.
def exit_hadler(sql_server_curs, sql_server_connection):
    # Закрываем курсор
    sql_server_curs.close()

    # Если не включен отладочный режим, выполняем откат изменений сделанных в хранилище.
    # Сделано специально чтобы при работе в продакшене не повредить информацию уже хранящуюся в хранилище.
    if not DEBUG:
        # В случае завершения работы с ошибкой - откатываетм все изменения (транзакцию).
        sql_server_connection.rollback()

    # Закрываем соединение
    sql_server_connection.close()

    # Выходим из скрипта
    sys.exit()


# Функция выполняющая множественную вставку значений из Pandas DataFrame в SQL таблицу.
def load_flat_file_to_stg(user_name, table_name, table_fields, sql_curs, flat_file_dataframe) -> None:
    # Расчитаем кол-во значение передаваемых в запрос
    n = len(table_fields)

    # Соберем шаблон запроса.
    sql_req1 = """insert into {usr}.{tbl} ( {fields} ) values ( {val_cnt} ) """.format(usr=user_name,
                                                                                       tbl=table_name,
                                                                                       fields=(', '.join(table_fields)),
                                                                                       val_cnt=("?, " * (n - 1) + "?"))
    # Отладка
    if DEBUG:
        print(sql_req1)

    # Выполним запрос.
    sql_curs.executemany(sql_req1, flat_file_dataframe.values.tolist())

    # Возвращяем None
    return None

# Получаем текущий путь
cwd = os.getcwd()

# Получаем имя скрипта, для открытия одноимённого файла журнала
(script_name, ext) = os.path.splitext(os.path.basename(__file__))

try:
    logging.basicConfig(filename=(script_name + '.log'), filemode='a', level=logging.DEBUG, encoding='utf-8',
                        format='%(asctime)s %(levelname)s %(message)s', datefmt='%d-%m-%Y %H:%M:%S')
except Exception as exc:
    # Сообщаем об исключении и выходим.
    print("Can\'t create or open log file! Abnormal termination of script execution. \n{}".format(exc))
    exit()

# Сообщаем об успешном запуске скрипта.
logging.info('The script was launched successfully.')

# Проверим существование и возможность записи в архивную папку где храняться обранные плоские файлы.
ArchiveSubDir = "archive"
ArchiveFullPath = os.path.join(cwd, ArchiveSubDir)
# Если архивная директория не существует, создадим ее. Иначе проверим права доступа.
if not os.path.isdir(ArchiveFullPath):
    try:
        # Создаём необходимую директорию.
        os.mkdir(ArchiveFullPath)
        # Сообщаем об успехе, продолжаем.
        logging.info("Archive directory \"{}\" created successfully.".format(ArchiveFullPath))
    except Exception as exc:
        # Сообщаем об исключении и выходим.
        logging.error("Can't create archive directory! {} Abnormal termination of script execution.".format(exc))
        exit()
else:
    if not os.access(ArchiveFullPath, os.W_OK):
        # Если прав нет, то сообщаем об ощибке в журнал и выходим.
        logging.error("Can't write to archive directory. Abnormal termination of script execution.")
        exit()
    else:
        logging.info("Archive directory \"{}\" is exist and available for writing.".format(ArchiveFullPath))

# Подключимся к хранилищу данных DWH используя следующие параметры соединения.
stg_user_name = "DEMIPT"
password = "gandalfthegrey"
server = "de-oracle.chronosavant.ru"
port = "1521"
path = "/home/demipt/anbo/ojdbc8.jar"

try:
    conn = connect_to_dwh(stg_user_name, password, server, port, path)

    # Если не включен отладочный режим, то отключаем autocommit
    if not DEBUG:
        conn.jconn.setAutoCommit(False)

    curs = conn.cursor()

    # Сообщаем об успешном подключении к серверу.
    logging.info("Connection to the server \"{}\" was established successfully.".format(server))

except Exception as exc:
    logging.error(
        "Can't connect to DWH server. Abnormal termination of script execution. \n Detailed information: {}".format(
            exc))
    exit()

# --------------------------------------------- 1. Очистка данных из STG -----------------------------------------------

# SCD-2, этап 1. Очиста стейджинговых таблиц.
staging_table_names = ["ANBO_STG_PSSPRT_BLCKLST",
                       "ANBO_STG_TRANSACTIONS",
                       "ANBO_STG_TERMINALS",
                       "ANBO_STG_TERMINALS_DRFT",
                       "ANBO_STG_BANK_ACCOUNTS",
                       "ANBO_STG_BANK_CARDS",
                       "ANBO_STG_BANK_CLIENTS",
                       "ANBO_STG_BANK_ACCOUNTS_DEL",
                       "ANBO_STG_BANK_CARDS_DEL",
                       "ANBO_STG_BANK_CLIENTS_DEL",
                       "ANBO_STG_TERMINALS_DEL"]

# Выполняем очистку всех стейджинговых таблиц.
try:
    clear_all_tables(staging_table_names, curs)
    # Сообщаем об успехе, продолжаем.
    logging.info("SCD-2.1: All staging tables have been cleared successfully.".format(ArchiveFullPath))
except Exception as exc:
    # Сообщаем об исключении и выходим.
    logging.error(
        "SCD-2.1: Can't clear the staging tables. Abnormal termination of script execution. \n Detailed information: {}".format(
            exc))
    exit_hadler(curs, conn)

# ---------------------------------------- 2. Захват данных из источника в STG -----------------------------------------

# Загружаем данные из файлов в стейджинг
flat_files_templates = ["passport_blacklist_\d{8}.xlsx", "transactions_\d{8}.txt", "terminals_\d{8}.xlsx"]

# Список словарей содержащих информацию о стейджинг таблицах для загрузки плоских файлов.
staging_tables_descriptions = [{"ANBO_STG_PSSPRT_BLCKLST": ['ENTRY_DT', 'PASSPORT_NUM']},
                               {"ANBO_STG_TRANSACTIONS": ['TRANSACTION_ID', 'TRANSACTION_DATE', 'AMOUNT', 'CARD_NUM',
                                                          'OPER_TYPE', 'OPER_RESULT', 'TERMINAL']},
                               {"ANBO_STG_TERMINALS_DRFT": ['TERMINAL_ID', 'TERMINAL_TYPE', 'TERMINAL_CITY',
                                                            'TERMINAL_ADDRESS', 'UPLOAD_DT']}]

# Перебираем два списка одновременно.
for (file_name_template, current_table_desc) in zip(flat_files_templates, staging_tables_descriptions):

    # Загружаем данные из плоского файла в дата фрейм для текущего шаблона.
    try:
        df = file_to_df(cwd, file_name_template)

        # Для дата фрейма с черным списком паспортов, меняем тип колонки на string
        if "passport_blacklist_" in file_name_template:
            # Преобразуем тип timestamp в строковый тип. Чтобы не получить проблемы при вставке данных
            df['date'] = df['date'].astype(str)

        logging.info(
            "SCD-2.2: The file matching the pattern \"{}\" was read to dataframe successfully.".format(
                file_name_template))
    except Exception as exc:
        # Сообщаем об исключении и выходим.
        logging.error(
            "SCD-2.2: Can't load data from a flat file that matches the pattern \"{}\" into a data frame \n Detailed information: {}".format(
                file_name_template, exc))
        exit_hadler(curs, conn)

    # Получаем данные о имени стейджинг таблицы и ее полях.
    [[table_name, table_fields]] = current_table_desc.items()

    # Вставляем данные в соответствующую стейджинг таблицу.
    try:
        load_flat_file_to_stg("DEMIPT",
                              table_name,
                              table_fields,
                              curs,
                              df)
        logging.info(
            "SCD-2.2: The data from the flat file was successfully loaded into the \"{}\" table.".format(table_name))
    except Exception as exc:
        # Сообщаем об исключении и выходим.
        logging.error(
            "SCD-2.2: Can't load data into the staging table for a file with the \"{}\" template. \n Detailed information: {}".format(
                file_name_template, exc))
        exit_hadler(curs, conn)

# Загружаем данные из SQL источников в стейджинг таблицы.

# BANK.ACCOUNTS -> ANBO_STG_BANK_ACCOUNTS
try:
    sql_req = """
    insert into DEMIPT.ANBO_STG_BANK_ACCOUNTS( ACCOUNT, VALID_TO, CLIENT, CREATE_DT, UPDATE_DT )
    select ACCOUNT, VALID_TO, CLIENT, CREATE_DT, UPDATE_DT
    from BANK.ACCOUNTS
    where COALESCE(UPDATE_DT,CREATE_DT) > (select LAST_UPDATE from DEMIPT.ANBO_META_LOADING where DBNAME = 'DEMIPT' and TABLENAME = 'ANBO_DWH_DIM_ACCOUNTS_HIST')
    """

    curs.execute(sql_req)

    logging.info("SCD-2.2: BANK.ACCOUNTS -> ANBO_STG_BANK_ACCOUNTS OK.")
except Exception as exc:
    # Сообщаем об исключении и выходим.
    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.2: BANK.ACCOUNTS -> ANBO_STG_BANK_ACCOUNTS. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# BANK.CARDS -> ANBO_STG_BANK_CARDS
# Сразу уберем лишние пробелы из ключа таблицы пластиковых карт, чтобы потом небыло проблем.
try:
    sql_req = """
    insert into DEMIPT.ANBO_STG_BANK_CARDS( CARD_NUM, ACCOUNT, CREATE_DT, UPDATE_DT )
    select TRIM(CARD_NUM), ACCOUNT, CREATE_DT, UPDATE_DT
    from BANK.CARDS
    where COALESCE(UPDATE_DT,CREATE_DT) > (
    select LAST_UPDATE from DEMIPT.ANBO_META_LOADING where DBNAME = 'DEMIPT' and TABLENAME = 'ANBO_DWH_DIM_CARDS_HIST')
    """

    curs.execute(sql_req)

    logging.info("SCD-2.2: BANK.CARDS -> ANBO_STG_BANK_CARDS.")
except Exception as exc:
    # Сообщаем об исключении и выходим.
    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.2: BANK.CARDS -> ANBO_STG_BANK_CARDS. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# BANK.CLIENTS -> ANBO_STG_BANK_CLIENTS
try:
    sql_req = """
    insert into DEMIPT.ANBO_STG_BANK_CLIENTS ( CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, CREATE_DT, UPDATE_DT )
    select CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, CREATE_DT, UPDATE_DT
    from BANK.CLIENTS
    where COALESCE(UPDATE_DT,CREATE_DT) > ( select LAST_UPDATE from DEMIPT.ANBO_META_LOADING where DBNAME = 'DEMIPT' and TABLENAME = 'ANBO_DWH_DIM_CLIENTS_HIST')
    """

    curs.execute(sql_req)

    logging.info("SCD-2.2: BANK.CLIENTS -> ANBO_STG_BANK_CLIENTS OK.")
except Exception as exc:
    # Сообщаем об исключении и выходим.
    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.2: BANK.CLIENTS -> ANBO_STG_BANK_CLIENTS. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# ANBO_STG_TERMINALS_DRFT -> ANBO_STG_TERMINALS
# Изменившиеся строки.
try:

    sql_req = """
    INSERT INTO ANBO_STG_TERMINALS ( TERMINAL_ID, TERMINAL_TYPE, TERMINAL_CITY, TERMINAL_ADDRESS, CREATE_DT, UPDATE_DT )
    SELECT
        t1.TERMINAL_ID,
        t1.TERMINAL_TYPE,
        t1.TERMINAL_CITY,
        t1.TERMINAL_ADDRESS,
        MIN(t2.EFFECTIVE_FROM) OVER (PARTITION BY t2.TERMINAL_ID ORDER BY t2.EFFECTIVE_FROM) as CREATE_DT, 
        TO_DATE(t1.UPLOAD_DT,'DD.MM.YYYY')
    FROM ANBO_STG_TERMINALS_DRFT t1
    LEFT JOIN ANBO_DWH_DIM_TERMINALS_HIST t2
    ON t1.terminal_id = t2.TERMINAL_ID
    AND t2.EFFECTIVE_TO = TO_DATE('31.12.2999','DD.MM.YYYY')
    WHERE
        t1.terminal_type != t2.terminal_type
    OR
        t1.terminal_city != t2.terminal_city
    OR
        t1.terminal_type != t2.terminal_type
    OR
        t1.TERMINAL_ADDRESS != t2.terminal_address
    """
    curs.execute(sql_req)

    logging.info(
        "SCD-2.2: (CHANGED ROWS) ANBO_STG_TERMINALS_DRFT -> ANBO_STG_TERMINALS OK.")
except Exception as exc:
    # Сообщаем об исключении и выходим.
    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.2: (CHANGED ROWS) ANBO_STG_TERMINALS_DRFT -> ANBO_STG_TERMINALS. \n Detailed information: {}".format(
            exc))
    exit_hadler(curs, conn)

# ANBO_STG_TERMINALS_DRFT -> ANBO_STG_TERMINALS
# Новые строки.

try:
    sql_req = """
    INSERT INTO ANBO_STG_TERMINALS ( TERMINAL_ID, TERMINAL_TYPE, TERMINAL_CITY, TERMINAL_ADDRESS, CREATE_DT, UPDATE_DT )
    SELECT
        t1.TERMINAL_ID,
        t1.TERMINAL_TYPE,
        t1.TERMINAL_CITY,
        t1.TERMINAL_ADDRESS,
        TO_DATE(t1.UPLOAD_DT, 'DD.MM.YYYY'),
        NULL
    FROM  ANBO_STG_TERMINALS_DRFT t1
    LEFT JOIN ANBO_DWH_DIM_TERMINALS_HIST t2
    ON t1.terminal_id = t2.TERMINAL_ID
    WHERE
        t2.terminal_id IS NULL
    """
    curs.execute(sql_req)

    logging.info(
        "SCD-2.2: (NEW ROWS) ANBO_STG_TERMINALS_DRFT -> ANBO_STG_TERMINALS.")
except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)
    logging.error(
        "SCD-2.2: (NEW ROWS) ANBO_STG_TERMINALS_DRFT -> ANBO_STG_TERMINALS. \n Detailed information: {}".format(
            exc))
    exit_hadler(curs, conn)

# ------------------------------------ 3. Загрузка данных из стейджинга в хранилище ------------------------------------
# (FACT) ANBO_STG_TRANSACTIONS -> ANBO_DWH_FACT_TRANSACTIONS (INSERT)
try:
    sql_req = """
    INSERT INTO DEMIPT.ANBO_DWH_FACT_TRANSACTIONS( TRANS_ID, TRANS_DATE, CARD_NUM, OPER_TYPE, AMT, OPER_RESULT, TERMINAL)
    SELECT
        TRANSACTION_ID,
        TO_DATE(TRANSACTION_DATE,'YYYY-MM-DD HH24:MI:SS'),
        CARD_NUM,
        OPER_TYPE,
        AMOUNT,
        OPER_RESULT,
        TERMINAL
    FROM DEMIPT.ANBO_STG_TRANSACTIONS
    """
    curs.execute(sql_req)

    logging.info("SCD-2.3: (FACT) ANBO_STG_TRANSACTIONS -> ANBO_DWH_FACT_TRANSACTIONS.")
except Exception as exc:
    # Сообщаем об исключении и выходим.
    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.3: (FACT) ANBO_STG_TRANSACTIONS -> ANBO_DWH_FACT_TRANSACTIONS. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# (FACT) ANBO_STG_PSSPRT_BLCKLST -> ANBO_DWH_FACT_PSSPRT_BLCKLST (INSERT)
try:
    sql_req = """
    INSERT INTO ANBO_DWH_FACT_PSSPRT_BLCKLST( PASSPORT_NUM, ENTRY_DT )
    SELECT 
        PASSPORT_NUM,
        TO_DATE(ENTRY_DT, 'YYYY-MM-DD')
    FROM ANBO_STG_PSSPRT_BLCKLST
    WHERE TO_DATE(ENTRY_DT, 'YYYY-MM-DD') > (
        SELECT LAST_UPDATE FROM ANBO_META_LOADING WHERE DBNAME = 'DEMIPT' AND TABLENAME = 'ANBO_DWH_FACT_PSSPRT_BLCKLST'
    )
    """
    curs.execute(sql_req)

    logging.info(
        "SCD-2.3: (FACT) ANBO_STG_PSSPRT_BLCKLST -> ANBO_DWH_FACT_PSSPRT_BLCKLST. OK")
except Exception as exc:
    # Сообщаем об исключении и выходим.
    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.3: (FACT) ANBO_STG_PSSPRT_BLCKLST -> ANBO_DWH_FACT_PSSPRT_BLCKLST. \n Detailed information: {}".format(
            exc))
    exit_hadler(curs, conn)

# (DIM) ANBO_STG_TERMINALS -> ANBO_DWH_DIM_TERMINALS_HIST (INSERT)
try:
    sql_req = """
    INSERT INTO DEMIPT.ANBO_DWH_DIM_TERMINALS_HIST( TERMINAL_ID, TERMINAL_TYPE, TERMINAL_CITY, TERMINAL_ADDRESS, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG )
    SELECT 
        TERMINAL_ID,
        TERMINAL_TYPE, 
        TERMINAL_CITY, 
        TERMINAL_ADDRESS, 
        COALESCE( UPDATE_DT, CREATE_DT ), 
        TO_DATE( '2999-12-31', 'YYYY-MM-DD' ), 
        'N'
    FROM DEMIPT.ANBO_STG_TERMINALS
    """

    curs.execute(sql_req)

    logging.info("SCD-2.3: (DIM INSERT) ANBO_STG_TERMINALS -> ANBO_DWH_DIM_TERMINALS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.
    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.3: (DIM INSERT) ANBO_STG_TERMINALS -> ANBO_DWH_DIM_TERMINALS_HIST \n Detailed information: {}".format(
            exc))
    exit_hadler(curs, conn)

# (DIM) ANBO_STG_TERMINALS -> ANBO_DWH_DIM_TERMINALS_HIST (MERGE)
try:
    sql_req = """
    MERGE INTO DEMIPT.ANBO_DWH_DIM_TERMINALS_HIST tgt
    USING DEMIPT.ANBO_STG_TERMINALS src
    ON ( tgt.TERMINAL_ID = src.TERMINAL_ID and tgt.EFFECTIVE_FROM < COALESCE( src.UPDATE_DT, src.CREATE_DT ) )
    WHEN matched THEN UPDATE SET tgt.EFFECTIVE_TO = COALESCE( src.UPDATE_DT, src.CREATE_DT ) - interval '1' second
    WHERE tgt.EFFECTIVE_TO = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
    """

    curs.execute(sql_req)

    logging.info("SCD-2.3: (DIM MERGE) ANBO_STG_TERMINALS -> ANBO_DWH_DIM_TERMINALS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.
    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.3: (DIM MERGE) ANBO_STG_TERMINALS -> ANBO_DWH_DIM_TERMINALS_HIST \n Detailed information: {}".format(
            exc))
    exit_hadler(curs, conn)

# (DIM) ANBO_STG_BANK_ACCOUNTS -> ANBO_DWH_DIM_ACCOUNTS_HIST (INSERT)
try:
    sql_req = """
    INSERT INTO DEMIPT.ANBO_DWH_DIM_ACCOUNTS_HIST( ACCOUNT_NUM, VALID_TO, CLIENT, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG )
    SELECT ACCOUNT, VALID_TO, CLIENT, COALESCE( UPDATE_DT, CREATE_DT ), TO_DATE( '2999-12-31', 'YYYY-MM-DD' ), 'N'
    FROM DEMIPT.ANBO_STG_BANK_ACCOUNTS
    """

    curs.execute(sql_req)

    logging.info("SCD-2.3: (DIM INSERT) ANBO_STG_BANK_ACCOUNTS -> ANBO_DWH_DIM_ACCOUNTS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.
    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.3: (DIM INSERT) ANBO_STG_BANK_ACCOUNTS -> ANBO_DWH_DIM_ACCOUNTS_HIST \n Detailed information: {}".format(
            exc))
    exit_hadler(curs, conn)

# (DIM) ANBO_STG_BANK_ACCOUNTS -> ANBO_DWH_DIM_ACCOUNTS_HIST (MERGE)
try:
    sql_req = """
    MERGE INTO DEMIPT.ANBO_DWH_DIM_ACCOUNTS_HIST tgt
    USING DEMIPT.ANBO_STG_BANK_ACCOUNTS src
    ON ( tgt.ACCOUNT_NUM = src.ACCOUNT and tgt.EFFECTIVE_FROM < COALESCE( src.UPDATE_DT, src.CREATE_DT ) )
    WHEN matched THEN UPDATE SET tgt.EFFECTIVE_TO = COALESCE( src.UPDATE_DT, src.CREATE_DT ) - interval '1' second
    WHERE tgt.EFFECTIVE_TO = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
    """

    curs.execute(sql_req)

    logging.info("SCD-2.3: (DIM MERGE) ANBO_STG_BANK_ACCOUNTS -> ANBO_DWH_DIM_ACCOUNTS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.
    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.3: (DIM MERGE) ANBO_STG_BANK_ACCOUNTS -> ANBO_DWH_DIM_ACCOUNTS_HIST \n Detailed information: {}".format(
            exc))
    exit_hadler(curs, conn)

# (DIM) ANBO_STG_BANK_CARDS -> ANBO_DWH_DIM_CARDS_HIST (INSERT)
# TRIM для того чтобы убрать лишние пробелы с поля ключа.
try:
    sql_req = """
    INSERT INTO DEMIPT.ANBO_DWH_DIM_CARDS_HIST( CARD_NUM, ACCOUNT_NUM, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG )
    SELECT
    TRIM(CARD_NUM),
    ACCOUNT,
    COALESCE( UPDATE_DT, CREATE_DT ),
    TO_DATE( '2999-12-31', 'YYYY-MM-DD' ),
    'N'
    FROM DEMIPT.ANBO_STG_BANK_CARDS
    """

    curs.execute(sql_req)

    logging.info("SCD-2.3: (DIM INSERT TRIM) ANBO_STG_BANK_CARDS -> ANBO_DWH_DIM_CARDS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.3: (DIM INSERT) ANBO_STG_BANK_CARDS -> ANBO_DWH_DIM_CARDS_HIST \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# (DIM) ANBO_STG_BANK_CARDS -> ANBO_DWH_DIM_CARDS_HIST (MERGE)
# TRIM для того чтобы убрать лишние пробелы с поля ключа.
try:
    sql_req = """
    MERGE INTO DEMIPT.ANBO_DWH_DIM_CARDS_HIST tgt
    USING DEMIPT.ANBO_STG_BANK_CARDS src
    ON ( tgt.CARD_NUM = TRIM(src.CARD_NUM) and tgt.EFFECTIVE_FROM < COALESCE( src.UPDATE_DT, src.CREATE_DT ) )
    WHEN matched THEN UPDATE SET tgt.EFFECTIVE_TO = COALESCE( src.UPDATE_DT, src.CREATE_DT ) - interval '1' second
    WHERE tgt.EFFECTIVE_TO = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
    """

    curs.execute(sql_req)

    logging.info("SCD-2.3: (DIM MERGE TRIM) ANBO_STG_BANK_CARDS -> ANBO_DWH_DIM_CARDS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.
    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.3: (DIM MERGE TRIM) ANBO_STG_BANK_CARDS -> ANBO_DWH_DIM_CARDS_HIST \n Detailed information: {}".format(
            exc))
    exit_hadler(curs, conn)

# (DIM) DEMIPT.ANBO_STG_BANK_CLIENTS -> ANBO_DWH_DIM_CLIENTS_HIST (INSERT)
try:
    sql_req = """
    INSERT INTO DEMIPT.ANBO_DWH_DIM_CLIENTS_HIST( CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG )
    SELECT
    CLIENT_ID,
    LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE,
    COALESCE( UPDATE_DT, CREATE_DT ),
    TO_DATE( '2999-12-31', 'YYYY-MM-DD' ),
    'N'
    FROM DEMIPT.ANBO_STG_BANK_CLIENTS
    """

    curs.execute(sql_req)

    logging.info("SCD-2.3: (DIM INSERT) DEMIPT.ANBO_STG_BANK_CLIENTS -> ANBO_DWH_DIM_CLIENTS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.3: (DIM INSERT) DEMIPT.ANBO_STG_BANK_CLIENTS -> ANBO_DWH_DIM_CLIENTS_HIST \n Detailed information: {}".format(
            exc))
    exit_hadler(curs, conn)

# (DIM) DEMIPT.ANBO_STG_BANK_CLIENTS -> ANBO_DWH_DIM_CLIENTS_HIST (MERGE)
try:
    sql_req = """
    MERGE INTO DEMIPT.ANBO_DWH_DIM_CLIENTS_HIST tgt
    USING DEMIPT.ANBO_STG_BANK_CLIENTS src
    ON ( tgt.CLIENT_ID = src.CLIENT_ID and tgt.EFFECTIVE_FROM < COALESCE( src.UPDATE_DT, src.CREATE_DT ) )
    WHEN matched THEN UPDATE SET tgt.EFFECTIVE_TO = COALESCE( src.UPDATE_DT, src.CREATE_DT ) - interval '1' second
    WHERE tgt.EFFECTIVE_TO = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
    """

    curs.execute(sql_req)

    logging.info("SCD-2.3: (DIM MERGE) ANBO_STG_BANK_CLIENTS -> ANBO_DWH_DIM_CLIENTS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.
    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.3: (DIM MERGE) ANBO_STG_BANK_CLIENTS -> ANBO_DWH_DIM_CLIENTS_HIST \n Detailed information: {}".format(
            exc))
    exit_hadler(curs, conn)

# ------------------------------ 4. Захватываем ключи для проверки удалений (опционально) ------------------------------

# ANBO_STG_TERMINALS_DRFT -> ANBO_STG_TERMINALS_DEL
try:
    sql_req = """
    INSERT INTO DEMIPT.ANBO_STG_TERMINALS_DEL ( TERMINAL_ID )
    select TERMINAL_ID FROM DEMIPT.ANBO_STG_TERMINALS_DRFT
    """

    curs.execute(sql_req)

    logging.info("SCD-2.4: (DELETING, KEYS) ANBO_STG_TERMINALS_DRFT -> ANBO_STG_TERMINALS_DEL. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.4: (DELETING, KEYS) ANBO_STG_TERMINALS_DRFT -> ANBO_STG_TERMINALS_DEL. \n Detailed information: {}".format(
            exc))
    exit_hadler(curs, conn)

# BANK.ACCOUNTS -> ANBO_STG_BANK_ACCOUNTS_DEL
try:
    sql_req = """
    INSERT INTO DEMIPT.ANBO_STG_BANK_ACCOUNTS_DEL ( ACCOUNT )
    select ACCOUNT FROM BANK.ACCOUNTS
    """

    curs.execute(sql_req)

    logging.info("SCD-2.4: (DELETING, KEYS) BANK.ACCOUNTS -> ANBO_STG_BANK_ACCOUNTS_DEL. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.4: (DELETING, KEYS) BANK.ACCOUNTS -> ANBO_STG_BANK_ACCOUNTS_DEL. \n Detailed information: {}".format(
            exc))
    exit_hadler(curs, conn)

# BANK.CARDS -> ANBO_STG_BANK_CARDS_DEL
# TRIM для того чтобы убрать лишние пробелы с поля ключа.
# Дальше в проеверки удаленных строк будет LEFT JOIN с таблицей из DWH в которой TRIM уже сделан.
try:
    sql_req = """
    INSERT INTO DEMIPT.ANBO_STG_BANK_CARDS_DEL ( CARD_NUM )
    select TRIM(CARD_NUM) FROM BANK.CARDS
    """

    curs.execute(sql_req)

    logging.info("SCD-2.4: (DELETING, KEYS) BANK.CARDS -> ANBO_STG_BANK_CARDS_DEL. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.4: (DELETING, KEYS) BANK.CARDS -> ANBO_STG_BANK_CARDS_DEL. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# BANK.CLIENTS -> ANBO_STG_BANK_CLIENTS_DEL
try:
    sql_req = """
    INSERT INTO DEMIPT.ANBO_STG_BANK_CLIENTS_DEL ( CLIENT_ID )
    select CLIENT_ID FROM BANK.CLIENTS
    """

    curs.execute(sql_req)

    logging.info("SCD-2.4: (DELETING, KEYS) BANK.CARDS -> ANBO_STG_BANK_CARDS_DEL. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.4: (DELETING, KEYS) BANK.CARDS -> ANBO_STG_BANK_CARDS_DEL. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# ----------------------------------- 5. Удаляем удаленные записи в целевой таблице ------------------------------------

# ANBO_DWH_DIM_TERMINALS_HIST INSERT
try:
    sql_req = """
    INSERT INTO ANBO_DWH_DIM_TERMINALS_HIST( TERMINAL_ID, TERMINAL_TYPE, TERMINAL_CITY, TERMINAL_ADDRESS, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG )
    SELECT
        t.TERMINAL_ID, t.TERMINAL_TYPE, t.TERMINAL_CITY, t.TERMINAL_ADDRESS, 
        sysdate,
        TO_DATE( '2999-12-31', 'YYYY-MM-DD' ),
        'Y'
    FROM ANBO_DWH_DIM_TERMINALS_HIST t
    LEFT JOIN ANBO_STG_TERMINALS_DEL s
    ON t.TERMINAL_ID = s.TERMINAL_ID
    AND deleted_flg = 'N'
    WHERE s.TERMINAL_ID IS NULL
    AND EFFECTIVE_TO = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
    """

    curs.execute(sql_req)

    logging.info("SCD-2.5: (DELETING, INSERT, FLAG = 'Y') ANBO_DWH_DIM_TERMINALS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.5: (DELETING, INSERT, FLAG = 'Y') ANBO_DWH_DIM_TERMINALS_HIST. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# ANBO_DWH_DIM_TERMINALS_HIST UPDATE
try:
    sql_req = """
    UPDATE ANBO_DWH_DIM_TERMINALS_HIST
        SET EFFECTIVE_TO = sysdate - interval '1' second
    WHERE TERMINAL_ID in (
    SELECT 
        t.TERMINAL_ID
    FROM ANBO_DWH_DIM_TERMINALS_HIST t
    LEFT JOIN ANBO_STG_TERMINALS_DEL s
    ON t.TERMINAL_ID = s.TERMINAL_ID
    WHERE s.TERMINAL_ID IS NULL
    AND EFFECTIVE_TO = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
    AND deleted_flg = 'N')
    AND EFFECTIVE_TO = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
    AND EFFECTIVE_FROM < sysdate
    AND deleted_flg = 'N'
    """

    curs.execute(sql_req)

    logging.info("SCD-2.5: (DELETING, UPDATE) ANBO_DWH_DIM_TERMINALS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error("SCD-2.5: (DELETING, UPDATE) ANBO_DWH_DIM_TERMINALS_HIST. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# ANBO_DWH_DIM_ACCOUNTS_HIST INSERT
try:
    sql_req = """
    INSERT INTO ANBO_DWH_DIM_ACCOUNTS_HIST( ACCOUNT_NUM, VALID_TO, CLIENT, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG )
    SELECT
    t.ACCOUNT_NUM, t.VALID_TO, t.CLIENT, 
    sysdate,
    TO_DATE( '2999-12-31', 'YYYY-MM-DD' ),
    'Y'
    FROM ANBO_DWH_DIM_ACCOUNTS_HIST t
    LEFT JOIN ANBO_STG_BANK_ACCOUNTS_DEL s
    ON t.ACCOUNT_NUM = s.ACCOUNT
    AND deleted_flg = 'N'
    WHERE s.ACCOUNT IS NULL
    AND EFFECTIVE_TO = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
    """

    curs.execute(sql_req)

    logging.info("SCD-2.5: (DELETING, INSERT, FLAG = 'Y') ANBO_DWH_DIM_ACCOUNTS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.5: (DELETING, INSERT, FLAG = 'Y') ANBO_DWH_DIM_ACCOUNTS_HIST. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# ANBO_DWH_DIM_ACCOUNTS_HIST UPDATE
try:
    sql_req = """
    UPDATE ANBO_DWH_DIM_ACCOUNTS_HIST
    SET EFFECTIVE_TO = sysdate - interval '1' second
    WHERE ACCOUNT_NUM in (
    SELECT t.ACCOUNT_NUM
    FROM ANBO_DWH_DIM_ACCOUNTS_HIST t
    LEFT JOIN ANBO_STG_BANK_ACCOUNTS_DEL s
    ON t.ACCOUNT_NUM = s.ACCOUNT
    AND deleted_flg = 'N'
    WHERE s.ACCOUNT IS NULL
    AND EFFECTIVE_TO = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
    AND deleted_flg = 'N')
    AND EFFECTIVE_TO = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
    AND EFFECTIVE_FROM < sysdate
    """

    curs.execute(sql_req)

    logging.info("SCD-2.5: (DELETING, UPDATE) ANBO_DWH_DIM_ACCOUNTS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error("SCD-2.5: (DELETING, UPDATE) ANBO_DWH_DIM_ACCOUNTS_HIST. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# ANBO_DWH_DIM_CARDS_HIST INSERT
try:
    sql_req = """
    INSERT INTO ANBO_DWH_DIM_CARDS_HIST( CARD_NUM, ACCOUNT_NUM, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG )
    SELECT
    t.CARD_NUM, t.ACCOUNT_NUM, 
    sysdate,
    TO_DATE( '2999-12-31', 'YYYY-MM-DD' ),
    'Y'
    FROM ANBO_DWH_DIM_CARDS_HIST t
    LEFT JOIN ANBO_STG_BANK_CARDS_DEL s
    ON t.CARD_NUM = s.CARD_NUM
    AND deleted_flg = 'N'
    WHERE s.CARD_NUM IS NULL
    AND EFFECTIVE_TO = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
    """

    curs.execute(sql_req)

    logging.info("SCD-2.5: (DELETING, INSERT, FLAG = 'Y') ANBO_DWH_DIM_CARDS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.5: (DELETING, INSERT, FLAG = 'Y') ANBO_DWH_DIM_CARDS_HIST. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# ANBO_DWH_DIM_CARDS_HIST UPDATE
try:
    sql_req = """
    UPDATE ANBO_DWH_DIM_CARDS_HIST
    SET EFFECTIVE_TO = sysdate - interval '1' second
    WHERE CARD_NUM in (
    SELECT t.CARD_NUM
    FROM ANBO_DWH_DIM_CARDS_HIST t
    LEFT JOIN ANBO_STG_BANK_CARDS_DEL s
    ON t.CARD_NUM = s.CARD_NUM
    WHERE s.CARD_NUM IS NULL
    AND EFFECTIVE_TO = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
    AND deleted_flg = 'N')
    AND EFFECTIVE_TO = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
    AND EFFECTIVE_FROM < sysdate
    AND deleted_flg = 'N'
    """

    curs.execute(sql_req)

    logging.info("SCD-2.5: (DELETING, UPDATE) ANBO_DWH_DIM_CARDS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error("SCD-2.5: (DELETING, UPDATE) ANBO_DWH_DIM_CARDS_HIST. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# ANBO_DWH_DIM_CLIENTS_HIST INSERT
try:
    sql_req = """
    INSERT INTO ANBO_DWH_DIM_CLIENTS_HIST( CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG )
    SELECT
    t.CLIENT_ID, t.LAST_NAME, t.FIRST_NAME, t.PATRONYMIC, t.DATE_OF_BIRTH, t.PASSPORT_NUM, t.PASSPORT_VALID_TO, t.PHONE, 
    sysdate,
    TO_DATE( '2999-12-31', 'YYYY-MM-DD' ),
    'Y'
    FROM ANBO_DWH_DIM_CLIENTS_HIST t
    LEFT JOIN ANBO_STG_BANK_CLIENTS_DEL s
    ON t.CLIENT_ID = s.CLIENT_ID
    AND deleted_flg = 'N'
    WHERE s.CLIENT_ID IS NULL
    AND EFFECTIVE_TO = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
    """

    curs.execute(sql_req)

    logging.info("SCD-2.5: (DELETING, INSERT, FLAG = 'Y') ANBO_DWH_DIM_CARDS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error(
        "SCD-2.5: (DELETING, INSERT, FLAG = 'Y') ANBO_DWH_DIM_CLIENTS_HIST. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# ANBO_DWH_DIM_CLIENTS_HIST UPDATE
try:
    sql_req = """
    UPDATE ANBO_DWH_DIM_CLIENTS_HIST
    SET EFFECTIVE_TO = sysdate - interval '1' second
    WHERE CLIENT_ID in (
    SELECT t.CLIENT_ID
    FROM ANBO_DWH_DIM_CLIENTS_HIST t
    LEFT JOIN ANBO_STG_BANK_CLIENTS_DEL s
    ON t.CLIENT_ID = s.CLIENT_ID
    WHERE s.CLIENT_ID IS NULL
    AND EFFECTIVE_TO = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
    AND deleted_flg = 'N')
    AND EFFECTIVE_TO = TO_DATE( '2999-12-31', 'YYYY-MM-DD' )
    AND EFFECTIVE_FROM < sysdate
    AND deleted_flg = 'N'
    """

    curs.execute(sql_req)

    logging.info("SCD-2.5: (DELETING, UPDATE) ANBO_DWH_DIM_CARDS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error("SCD-2.5: (DELETING, UPDATE) ANBO_DWH_DIM_CLIENTS_HIST. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# ------------------------------- 6. Обновляем метаданные - дату максимальной загрузуки --------------------------------

# ANBO_DWH_DIM_TERMINALS_HIST
try:
    sql_req = """
    UPDATE
        ANBO_META_LOADING
    SET
        last_update = ( SELECT
                            MAX( TO_DATE(UPLOAD_DT, 'DD.MM.YYYY') )
                        FROM
                            DEMIPT.ANBO_STG_TERMINALS_DRFT )
    WHERE
        dbname = 'DEMIPT'
    AND tablename = 'ANBO_DWH_DIM_TERMINALS_HIST'
    AND ( SELECT MAX( TO_DATE(UPLOAD_DT, 'DD.MM.YYYY') )
    FROM DEMIPT.ANBO_STG_TERMINALS_DRFT ) IS NOT NULL
    """

    curs.execute(sql_req)

    logging.info("SCD-2.6: (METADATA UPDATE) ANBO_DWH_DIM_TERMINALS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error("SCD-2.6: (METADATA UPDATE) ANBO_DWH_DIM_TERMINALS_HIST. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)




# ANBO_DWH_FACT_PSSPRT_BLCKLST
try:

    sql_req = """
    UPDATE
        ANBO_META_LOADING
    SET
        last_update = ( SELECT
                            MAX( TO_DATE( ENTRY_DT, 'YYYY-MM-DD' ))
                        FROM
                            DEMIPT.ANBO_STG_PSSPRT_BLCKLST )
    WHERE
        dbname = 'DEMIPT'
    AND tablename = 'ANBO_DWH_FACT_PSSPRT_BLCKLST'
    AND (SELECT MAX( TO_DATE(ENTRY_DT, 'YYYY-MM-DD' ) )
        FROM DEMIPT.ANBO_STG_PSSPRT_BLCKLST ) IS NOT NULL
    """

    curs.execute(sql_req)

    logging.info("SCD-2.6: (METADATA UPDATE) ANBO_DWH_FACT_PSSPRT_BLCKLST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error("SCD-2.6: (METADATA UPDATE) ANBO_DWH_FACT_PSSPRT_BLCKLST. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# ANBO_DWH_DIM_ACCOUNTS_HIST
try:
    sql_req = """
    UPDATE 
        DEMIPT.ANBO_META_LOADING
    SET
    last_update = ( SELECT
                        MAX( COALESCE( UPDATE_DT, CREATE_DT ) )
                    FROM
                        DEMIPT.ANBO_STG_BANK_ACCOUNTS )
    WHERE
        dbname = 'DEMIPT'
    AND tablename = 'ANBO_DWH_DIM_ACCOUNTS_HIST'
    AND ( SELECT MAX( COALESCE( UPDATE_DT, CREATE_DT ) )
    FROM DEMIPT.ANBO_STG_BANK_ACCOUNTS ) IS NOT NULL
    """

    curs.execute(sql_req)

    logging.info("SCD-2.6: (METADATA UPDATE) ANBO_DWH_DIM_ACCOUNTS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error("SCD-2.6: (METADATA UPDATE) ANBO_DWH_DIM_ACCOUNTS_HIST. \n Detailed information: {}".format(exc))

# ANBO_DWH_DIM_CLIENTS_HIST
try:
    sql_req = """
    UPDATE 
        DEMIPT.ANBO_META_LOADING
    SET
        last_update = ( SELECT
                            MAX( COALESCE( UPDATE_DT, CREATE_DT ) )
                        FROM 
                            DEMIPT.ANBO_STG_BANK_CLIENTS )
    WHERE
        dbname = 'DEMIPT'
    AND tablename = 'ANBO_DWH_DIM_CLIENTS_HIST'
    AND ( SELECT MAX( COALESCE( UPDATE_DT, CREATE_DT ) )
    FROM DEMIPT.ANBO_STG_BANK_CLIENTS ) IS NOT NULL
    """

    curs.execute(sql_req)

    logging.info("SCD-2.6: (METADATA UPDATE) ANBO_DWH_DIM_CLIENTS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error("SCD-2.6: (METADATA UPDATE) ANBO_DWH_DIM_CLIENTS_HIST. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)


# ANBO_DWH_FACT_TRANSACTIONS
try:
    sql_req = """
    UPDATE 
            DEMIPT.ANBO_META_LOADING
        SET
            last_update = ( SELECT
                                MAX( TO_DATE(TRANSACTION_DATE,'YYYY-MM-DD HH24:MI:SS') )
                            FROM 
                                DEMIPT.ANBO_STG_TRANSACTIONS )
        WHERE
            dbname = 'DEMIPT'
        AND tablename = 'ANBO_DWH_FACT_TRANSACTIONS'
        AND ( SELECT MAX( TO_DATE(TRANSACTION_DATE,'YYYY-MM-DD HH24:MI:SS') )
        FROM DEMIPT.ANBO_STG_TRANSACTIONS ) IS NOT NULL
    """

    curs.execute(sql_req)

    logging.info("SCD-2.6: (METADATA UPDATE) ANBO_DWH_FACT_TRANSACTIONS. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error("SCD-2.6: (METADATA UPDATE) ANBO_DWH_FACT_TRANSACTIONS. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# ANBO_DWH_DIM_CARDS_HIST
try:
    sql_req = """
    UPDATE 
        DEMIPT.ANBO_META_LOADING
    SET
    last_update = ( SELECT
                        MAX( COALESCE( UPDATE_DT, CREATE_DT ) )
                    FROM
                        DEMIPT.ANBO_STG_BANK_CARDS )
    WHERE
        dbname = 'DEMIPT'
    AND tablename = 'ANBO_DWH_DIM_CARDS_HIST'
    AND ( SELECT MAX( COALESCE( UPDATE_DT, CREATE_DT ) )
    FROM DEMIPT.ANBO_STG_BANK_CARDS ) IS NOT NULL
    """

    curs.execute(sql_req)

    logging.info("SCD-2.6: (METADATA UPDATE) ANBO_DWH_DIM_CARDS_HIST. OK")

except Exception as exc:
    # Сообщаем об исключении и выходим.

    if DEBUG:
        print(sql_req)

    logging.error("SCD-2.6: (METADATA UPDATE) ANBO_DWH_DIM_CARDS_HIST. \n Detailed information: {}".format(exc))

# ---------------------------------------------- 7. Фиксируем транзакцию -----------------------------------------------
# Если не включен отладочный режим, то фиксируем изменения в базе
if not DEBUG:
    try:
        conn.commit()
        logging.info("SCD-2.7: All data loaded successfully. Transaction completed successfully. \n\n")
    except Exception as exc:
        logging.error(
            "SCD-2.7:An error occurred while committing the transaction. "
            "\n Detailed information: {EXCEPTION}".format(EXCEPTION=exc))
        exit_hadler(curs, conn)

if DEBUG:
    # Пока что фиксация транзации автоматическая, после выполнения каждого запроса.
    logging.info("SCD-2.7: All data loaded successfully. Transaction completed successfully. \n\n")

# Закрываем курсор и соединение.
curs.close()
conn.close()

stg_user_name = "DEMIPT"


