-- Создаём таблицу для хранения метаданных загрузки стейджинга

-- Сбрасываем все существующие таблицы
DROP TABLE DEMIPT.ANBO_META_LOADING;

CREATE TABLE ANBO_META_LOADING (
	DBNAME varchar2(30),
	TABLENAME varchar2(40),
	LAST_UPDATE date 
);

-- Очищаем таблицу метаданных загрузки.
--DELETE FROM ANBO_META_LOADING;

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

commit;



