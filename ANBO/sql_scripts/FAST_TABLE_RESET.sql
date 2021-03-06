-- Очищаем таблицы стейджинга и хранилища
DELETE FROM DEMIPT.ANBO_STG_PSSPRT_BLCKLST;
DELETE FROM DEMIPT.ANBO_STG_TRANSACTIONS;
DELETE FROM DEMIPT.ANBO_STG_TERMINALS;
DELETE FROM DEMIPT.ANBO_STG_TERMINALS_DRFT;
DELETE FROM DEMIPT.ANBO_STG_BANK_ACCOUNTS;
DELETE FROM DEMIPT.ANBO_STG_BANK_CARDS;
DELETE FROM DEMIPT.ANBO_STG_BANK_CLIENTS;
DELETE FROM DEMIPT.ANBO_STG_BANK_ACCOUNTS_DEL;
DELETE FROM DEMIPT.ANBO_STG_BANK_CARDS_DEL;
DELETE FROM DEMIPT.ANBO_STG_BANK_CLIENTS_DEL;
DELETE FROM DEMIPT.ANBO_STG_TERMINALS_DEL;

DELETE FROM DEMIPT.ANBO_DWH_FACT_TRANSACTIONS;
DELETE FROM DEMIPT.ANBO_DWH_FACT_PSSPRT_BLCKLST;
DELETE FROM DEMIPT.ANBO_DWH_DIM_TERMINALS_HIST;
DELETE FROM DEMIPT.ANBO_DWH_DIM_CARDS_HIST;
DELETE FROM DEMIPT.ANBO_DWH_DIM_ACCOUNTS_HIST;
DELETE FROM DEMIPT.ANBO_DWH_DIM_CLIENTS_HIST;

DELETE FROM DEMIPT.ANBO_REP_FRAUD;

-- Очищаем таблицу метаданных загрузки.
DELETE FROM ANBO_META_LOADING;

-- Заполняем ее начальным решением (пока данных не было, ставим заведомо минимальную дату - 1900 год)
-- Делаем это для всех таблиц измерений SDC-2
-- Вносим дату '1899-01-01' в качестве начального решения, так как минимальная дата в таблицах источниках '1900-01-01'.
INSERT INTO DEMIPT.ANBO_META_LOADING( DBNAME, TABLENAME, LAST_UPDATE ) 
VALUES ( 'DEMIPT', 'ANBO_DWH_DIM_TERMINALS_HIST', to_date( '1899-01-01', 'YYYY-MM-DD' ) );

INSERT INTO DEMIPT.ANBO_META_LOADING( DBNAME, TABLENAME, LAST_UPDATE ) 
VALUES ( 'DEMIPT', 'ANBO_DWH_DIM_CARDS_HIST', to_date( '1899-01-01', 'YYYY-MM-DD' ) );

INSERT INTO DEMIPT.ANBO_META_LOADING( DBNAME, TABLENAME, LAST_UPDATE ) 
VALUES ( 'DEMIPT', 'ANBO_DWH_DIM_ACCOUNTS_HIST', to_date( '1899-01-01', 'YYYY-MM-DD' ) );

INSERT INTO DEMIPT.ANBO_META_LOADING( DBNAME, TABLENAME, LAST_UPDATE ) 
VALUES ( 'DEMIPT', 'ANBO_DWH_DIM_CLIENTS_HIST', to_date( '1899-01-01', 'YYYY-MM-DD' ) );

INSERT INTO DEMIPT.ANBO_META_LOADING( DBNAME, TABLENAME, LAST_UPDATE ) 
VALUES ( 'DEMIPT', 'ANBO_DWH_FACT_PSSPRT_BLCKLST', to_date( '1899-01-01', 'YYYY-MM-DD' ) );

INSERT INTO DEMIPT.ANBO_META_LOADING( DBNAME, TABLENAME, LAST_UPDATE ) 
VALUES ( 'DEMIPT', 'ANBO_DWH_FACT_TRANSACTIONS', to_date( '1899-01-01', 'YYYY-MM-DD' ) );

-- Фиксируем изменения. Закрывааем транзакцию.
commit;



-- SELECT * FROM DEMIPT.ANBO_DWH_DIM_TERMINALS_HIST
-- WHERE TERMINAL_ID = 'TEST';

-- Каждый день (01.03.2021, 02.03.2021, 03.02.2021) меняет адрес.
-- SELECT * FROM DEMIPT.ANBO_DWH_DIM_TERMINALS_HIST
-- WHERE TERMINAL_ID = 'A8966';

-- После загрузки 3 файлов появляется на 02.03.2021 и удаляется 03.02.2021
-- SELECT * FROM DEMIPT.ANBO_DWH_DIM_TERMINALS_HIST
-- WHERE TERMINAL_ID = 'P9111';

-- SELECT * FROM ANBO_STG_TERMINALS_DRFT
-- WHERE TERMINAL_ID = 'A8966';

-- SELECT * FROM ANBO_STG_TERMINALS
-- WHERE TERMINAL_ID = 'A8966';

-- SELECT * FROM ANBO_STG_TERMINALS_DEL
-- WHERE TERMINAL_ID = 'A8966';