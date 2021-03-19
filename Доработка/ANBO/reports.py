#!/usr/bin/python
# Скрипт генерации отчетов
import jaydebeapi
import os
import logging
import sys

# Отладочный режим
# Значение True - включается автокомит после каждой транзакции, выдается больше отладочной информации на экран.
# Значение False - Режим работы в продакшене. На экран работы ничего не выдается.
# ВАЖНЫЙ МОМЕНТ! Сообщения в журнал работы (файл "main.log") пишутся в обоих режимах.
DEBUG = True

# Дата на которую генерируется отчет
REPORT_DATE = '03.03.2021 23:59:59'


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


# Получаем имя скрипта, для открытия одноимённого файла журнала
(script_name, ext) = os.path.splitext(os.path.basename(__file__))

try:
    logging.basicConfig(filename=(script_name + '.log'), filemode='a', level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s', datefmt='%d-%m-%Y %H:%M:%S')
except Exception as exc:
    # Сообщаем об исключении и выходим.
    print("Can\'t create or open log file! Abnormal termination of script execution. \n{}".format(exc))
    exit()

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

# Сгенерируем 1 отчёт
try:
    sql_req = """
    INSERT INTO DEMIPT.ANBO_REP_FRAUD ( EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT) 
    SELECT DISTINCT
        t1.TRANS_DATE,
        t4.PASSPORT_NUM,
        t4.LAST_NAME||' '||t4.FIRST_NAME||' '||t4.PATRONYMIC,
        t4.PHONE,
        '1',
        TO_DATE( '{GENERATION_DATE}', 'DD.MM.YYYY HH24:MI:SS' )
    FROM DEMIPT.ANBO_DWH_FACT_TRANSACTIONS T1
    INNER JOIN DEMIPT.ANBO_DWH_DIM_CARDS_HIST T2
    ON T1.CARD_NUM = T2.CARD_NUM
    AND t1.TRANS_DATE <= TO_DATE( '{GENERATION_DATE}', 'DD.MM.YYYY HH24:MI:SS' ) -- (1)
    AND TO_DATE( '{GENERATION_DATE}', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T2.EFFECTIVE_FROM AND T2.EFFECTIVE_TO --(2)
    AND T2.DELETED_FLG = 'N' -- (3)
    INNER JOIN DEMIPT.ANBO_DWH_DIM_ACCOUNTS_HIST T3
    ON T2.ACCOUNT_NUM = T3.ACCOUNT_NUM
    AND TO_DATE( '{GENERATION_DATE}', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T3.EFFECTIVE_FROM AND T3.EFFECTIVE_TO -- (2)
    AND T3.DELETED_FLG = 'N' -- (3)
    INNER JOIN DEMIPT.ANBO_DWH_DIM_CLIENTS_HIST T4
    ON T3.CLIENT = T4.CLIENT_ID
    AND TO_DATE( '{GENERATION_DATE}', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T4.EFFECTIVE_FROM AND T4.EFFECTIVE_TO -- (2)
    AND T4.DELETED_FLG = 'N' -- (3)
    WHERE 1=1
    AND T4.PASSPORT_NUM IN (SELECT PASSPORT_NUM FROM ANBO_DWH_FACT_PSSPRT_BLCKLST WHERE ENTRY_DT <= TO_DATE( '{GENERATION_DATE}', 'DD.MM.YYYY HH24:MI:SS' ) )
    OR T4.PASSPORT_VALID_TO < TO_DATE( '{GENERATION_DATE}', 'DD.MM.YYYY HH24:MI:SS' )
    """.format(GENERATION_DATE=REPORT_DATE)

    # Отладочное сообщение
    if DEBUG:
        print("Report number 1 has been generated successfully.")

    curs.execute(sql_req)

    logging.info("REPORT 1: Request completed successfully.")
except Exception as exc:

    # Сообщаем об исключении и выходим.
    if DEBUG:
        print(sql_req + "\n {}".format(exc))

    logging.error("REPORT 1: An error occurred while executing the request.. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# Сгенерируем 2 отчёт
try:
    sql_req = """
    INSERT INTO DEMIPT.ANBO_REP_FRAUD ( EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT) 
    SELECT DISTINCT
        t1.TRANS_DATE,
        t4.PASSPORT_NUM,
        t4.LAST_NAME||' '||t4.FIRST_NAME||' '||t4.PATRONYMIC,
        t4.PHONE,
        '2',
        TO_DATE( '{GENERATION_DATE}', 'DD.MM.YYYY HH24:MI:SS' )
    FROM DEMIPT.ANBO_DWH_FACT_TRANSACTIONS T1
    INNER JOIN DEMIPT.ANBO_DWH_DIM_CARDS_HIST T2
    ON T1.CARD_NUM = T2.CARD_NUM
    AND t1.TRANS_DATE <= TO_DATE( '{GENERATION_DATE}', 'DD.MM.YYYY HH24:MI:SS' ) -- (1)
    AND TO_DATE( '{GENERATION_DATE}', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T2.EFFECTIVE_FROM AND T2.EFFECTIVE_TO --(2)
    AND T2.DELETED_FLG = 'N' -- (3)
    INNER JOIN DEMIPT.ANBO_DWH_DIM_ACCOUNTS_HIST T3
    ON T2.ACCOUNT_NUM = T3.ACCOUNT_NUM
    AND TO_DATE( '{GENERATION_DATE}', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T3.EFFECTIVE_FROM AND T3.EFFECTIVE_TO -- (2)
    AND T3.DELETED_FLG = 'N' -- (3)
    INNER JOIN DEMIPT.ANBO_DWH_DIM_CLIENTS_HIST T4
    ON T3.CLIENT = T4.CLIENT_ID
    AND TO_DATE( '{GENERATION_DATE}', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T4.EFFECTIVE_FROM AND T4.EFFECTIVE_TO -- (2)
    AND T4.DELETED_FLG = 'N' -- (3)
    WHERE 1=1
    AND T3.VALID_TO < TO_DATE( '{GENERATION_DATE}', 'DD.MM.YYYY HH24:MI:SS' )
    """.format(GENERATION_DATE=REPORT_DATE)

    # Отладочное сообщение
    if DEBUG:
        print("Report number 1 has been generated successfully.")

    curs.execute(sql_req)

    logging.info("REPORT 2: Request completed successfully.")
except Exception as exc:

    # Сообщаем об исключении и выходим.
    if DEBUG:
        print(sql_req + "\n {}".format(exc))

    logging.error("REPORT 2: An error occurred while executing the request.. \n Detailed information: {}".format(exc))
    exit_hadler(curs, conn)

# ------------------------------------ Фиксируем транзакцию и закрываем соединение -------------------------------------
# Если не включен отладочный режим, то фиксируем изменения в базе
if not DEBUG:
    try:
        conn.commit()
        logging.info("Transaction completed successfully. \n\n")
    except Exception as exc:
        logging.error(
            "An error occurred while closing a transaction. "
            "\n Detailed information: {EXCEPTION}".format(EXCEPTION=exc))
        exit_hadler(curs, conn)

if DEBUG:
    # Пока что фиксация транзации автоматическая, после выполнения каждого запроса.
    logging.info("The script has finished running. \n\n")

# Закрываем курсор и соединение.
curs.close()
conn.close()
