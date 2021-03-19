-- Сбрасываем все существующие таблицы
DROP TABLE DEMIPT.ANBO_STG_PSSPRT_BLCKLST;
DROP TABLE DEMIPT.ANBO_STG_TRANSACTIONS;
DROP TABLE DEMIPT.ANBO_STG_TERMINALS;
DROP TABLE DEMIPT.ANBO_STG_TERMINALS_DRFT;
DROP TABLE DEMIPT.ANBO_STG_BANK_ACCOUNTS;
DROP TABLE DEMIPT.ANBO_STG_BANK_CARDS;
DROP TABLE DEMIPT.ANBO_STG_BANK_CLIENTS;
DROP TABLE DEMIPT.ANBO_STG_BANK_ACCOUNTS_DEL;
DROP TABLE DEMIPT.ANBO_STG_BANK_CARDS_DEL;
DROP TABLE DEMIPT.ANBO_STG_BANK_CLIENTS_DEL;
DROP TABLE DEMIPT.ANBO_STG_TERMINALS_DEL;

DROP TABLE DEMIPT.ANBO_DWH_FACT_TRANSACTIONS;
DROP TABLE DEMIPT.ANBO_DWH_FACT_PSSPRT_BLCKLST;
DROP TABLE DEMIPT.ANBO_DWH_DIM_TERMINALS_HIST;
DROP TABLE DEMIPT.ANBO_DWH_DIM_CARDS_HIST;
DROP TABLE DEMIPT.ANBO_DWH_DIM_ACCOUNTS_HIST;
DROP TABLE DEMIPT.ANBO_DWH_DIM_CLIENTS_HIST;

DROP TABLE DEMIPT.ANBO_META_LOADING;

DROP TABLE DEMIPT.ANBO_REP_FRAUD;


-------------------- Создаём стейджинг таблицы --------------------------------
-- Создание стейджинг таблиц. Максимально близко к оригинальному формату данных.

-- Стейджинг для черного списка паспортов (Факты).
CREATE TABLE ANBO_STG_PSSPRT_BLCKLST (
	ENTRY_DT varchar(12),
	PASSPORT_NUM varchar(14)
);

-- Стейджинг для списка транзакций за текущий день (Факты).

CREATE TABLE ANBO_STG_TRANSACTIONS (
	TRANSACTION_ID varchar(12),
	TRANSACTION_DATE varchar(20),
	AMOUNT varchar(12),									
	CARD_NUM varchar(20),
	OPER_TYPE varchar(10),
	OPER_RESULT varchar(10),
	TERMINAL varchar(10)
);

-- Стейджинг для списка терминалов (полным срезом)(измерения).
-- Грузим из STG в DWH как SCD-2 (используя META).
CREATE TABLE ANBO_STG_TERMINALS (
	TERMINAL_ID varchar(6),
	TERMINAL_TYPE varchar(5),
	TERMINAL_CITY varchar(50),
	TERMINAL_ADDRESS varchar(100),
	CREATE_DT date,
	UPDATE_DT date
);

-- Стейджинг черновой для списка терминалов (полным срезом)(измерения).
-- В эту таблицу загружается информация из файла, до обработки.
CREATE TABLE ANBO_STG_TERMINALS_DRFT (
	TERMINAL_ID varchar(6),
	TERMINAL_TYPE varchar(5),
	TERMINAL_CITY varchar(50),
	TERMINAL_ADDRESS varchar(100),
	UPLOAD_DT varchar(10)
);

-- Данные из СУДБ SQL BANK.* (измерения) SDC-2
-- Стейджинг таблица для BANK.ACCOUNTS
CREATE TABLE ANBO_STG_BANK_ACCOUNTS (
	ACCOUNT char(20),
	VALID_TO date,
	CLIENT varchar (20),
	CREATE_DT date,
	UPDATE_DT date
);

-- Стейджинг таблица для BANK.CARDS (измерения) SCD-2
CREATE TABLE ANBO_STG_BANK_CARDS (
	CARD_NUM char(20),
	ACCOUNT char(20),
	CREATE_DT date,
	UPDATE_DT date
);

-- Стейджинг таблица для BANK.CLIENTS (измерения) SCD-2
CREATE TABLE ANBO_STG_BANK_CLIENTS (
	CLIENT_ID varchar2(20),
	LAST_NAME varchar2(100),
	FIRST_NAME varchar2(100),
	PATRONYMIC varchar2(100),
	DATE_OF_BIRTH date,
	PASSPORT_NUM varchar2(15),
	PASSPORT_VALID_TO date,
	PHONE varchar2(20),
	CREATE_DT date,
	UPDATE_DT date
);

-- Таблицы для отслеживания удалений 
-- Стейджинг таблица для отслеживания удалений BANK.ACCOUNTS (измерения) SCD-2
create table ANBO_STG_BANK_ACCOUNTS_DEL (
	ACCOUNT char(20)
);

-- Стейджинг таблица для отслеживания удалений BANK.CARDS (измерения) SCD-2
create table ANBO_STG_BANK_CARDS_DEL (
	CARD_NUM char(20)
);

-- Стейджинг таблица для отслеживания удалений BANK.CLIENTS (измерения) SCD-2
create table ANBO_STG_BANK_CLIENTS_DEL (
	CLIENT_ID varchar2(20)
);

-- Стейджинг таблица для отслеживания удалений из плоского файла списка терминалов.
CREATE TABLE ANBO_STG_TERMINALS_DEL(
	TERMINAL_ID varchar(6)
);

-- Создаём таблицу для хранилища DWH
-- Таблицы для хранения фактов.
CREATE TABLE ANBO_DWH_FACT_TRANSACTIONS(
	TRANS_ID varchar(12),
	TRANS_DATE	date,
	CARD_NUM varchar2(20),
	OPER_TYPE varchar2(10),
	AMT	DECIMAL,
	OPER_RESULT varchar2(10),
	TERMINAL varchar2(10)
);

CREATE TABLE ANBO_DWH_FACT_PSSPRT_BLCKLST (
	PASSPORT_NUM varchar(14),
	ENTRY_DT date
);

-- Таблицы для хранения измерений SDC-2
CREATE TABLE ANBO_DWH_DIM_TERMINALS_HIST (
	TERMINAL_ID varchar2(6),
	TERMINAL_TYPE varchar2(5),
	TERMINAL_CITY varchar2(50),
	TERMINAL_ADDRESS varchar2(100),
	EFFECTIVE_FROM DATE, 
	EFFECTIVE_TO DATE,
	DELETED_FLG CHAR( 1 BYTE )
);

CREATE TABLE ANBO_DWH_DIM_CARDS_HIST (
	CARD_NUM varchar2(20),
	ACCOUNT_NUM varchar2(20),
	EFFECTIVE_FROM DATE, 
	EFFECTIVE_TO DATE,
	DELETED_FLG CHAR( 1 BYTE )
);

CREATE TABLE ANBO_DWH_DIM_ACCOUNTS_HIST (
	ACCOUNT_NUM varchar2(20),
	VALID_TO date,
	CLIENT varchar2(20),
	EFFECTIVE_FROM DATE, 
	EFFECTIVE_TO DATE,
	DELETED_FLG CHAR( 1 BYTE )
);

CREATE TABLE ANBO_DWH_DIM_CLIENTS_HIST (
	CLIENT_ID varchar2(20),
	LAST_NAME varchar2(100),
	FIRST_NAME varchar2(100),
	PATRONYMIC varchar2(100),
	DATE_OF_BIRTH date,
	PASSPORT_NUM varchar2(15),
	PASSPORT_VALID_TO date,
	PHONE varchar2(20),
	EFFECTIVE_FROM DATE, 
	EFFECTIVE_TO DATE,
	DELETED_FLG CHAR( 1 BYTE )
);

CREATE TABLE ANBO_META_LOADING (
	DBNAME varchar2(30),
	TABLENAME varchar2(40),
	LAST_UPDATE date 
);

-- Таблицы для хранения отчётов.
-- Длинна поля FIO определена как сумма длинны полей last_name (100), first_name(100), patronimic(100) 
-- таблицы BANK.CLIENTS. Да, расточительно, но гарантировано не должны получить проблемы с тем, 
-- что комбинация не влезет в нашу таблицу для хранения отчетов.
CREATE TABLE ANBO_REP_FRAUD(
	EVENT_ID DATE,
	PASSPORT VARCHAR2(15 BYTE),
	FIO varchar2(300 BYTE),
	PHONE varchar2(20),
	EVENT_TYPE CHAR(1 BYTE),
	REPORT_DT DATE
);

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