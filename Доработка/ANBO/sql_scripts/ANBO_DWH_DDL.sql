-- Создаём таблицу для хранилища DWH
-- В хранилище мы не создаём ключи, так как это накладно!

-- Сбрасываем все существующие таблицы
DROP TABLE DEMIPT.ANBO_DWH_FACT_TRANSACTIONS;
DROP TABLE DEMIPT.ANBO_DWH_FACT_PSSPRT_BLCKLST;
DROP TABLE DEMIPT.ANBO_DWH_DIM_TERMINALS_HIST;
DROP TABLE DEMIPT.ANBO_DWH_DIM_CARDS_HIST;
DROP TABLE DEMIPT.ANBO_DWH_DIM_ACCOUNTS_HIST;
DROP TABLE DEMIPT.ANBO_DWH_DIM_CLIENTS_HIST;

-- Очищаем таблицы.
--DELETE FROM DEMIPT.ANBO_DWH_FACT_TRANSACTIONS;
--DELETE FROM DEMIPT.ANBO_DWH_FACT_PSSPRT_BLCKLST;
--DELETE FROM DEMIPT.ANBO_DWH_DIM_TERMINALS_HIST;
--DELETE FROM DEMIPT.ANBO_DWH_DIM_CARDS_HIST;
--DELETE FROM DEMIPT.ANBO_DWH_DIM_ACCOUNTS_HIST;
--DELETE FROM DEMIPT.ANBO_DWH_DIM_CLIENTS_HIST;

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

-- Поля сверены с методическими указаниями.
-- Имя таблицы тоже.
CREATE TABLE ANBO_DWH_FACT_PSSPRT_BLCKLST (
	PASSPORT_NUM varchar(14),
	ENTRY_DT date
);

-- Таблицы для хранения измерений SDC-2
-- Поля сверены с методическими указаниями.
-- Имя таблицы тоже.
CREATE TABLE ANBO_DWH_DIM_TERMINALS_HIST (
	TERMINAL_ID varchar2(6),
	TERMINAL_TYPE varchar2(5),
	TERMINAL_CITY varchar2(50),
	TERMINAL_ADDRESS varchar2(100),
	EFFECTIVE_FROM DATE, 
	EFFECTIVE_TO DATE,
	DELETED_FLG CHAR( 1 BYTE )
);

-- Поля сверены с методическими указаниями.
-- Имя таблицы тоже.
CREATE TABLE ANBO_DWH_DIM_CARDS_HIST (
	CARD_NUM varchar2(20),
	ACCOUNT_NUM varchar2(20),
	EFFECTIVE_FROM DATE, 
	EFFECTIVE_TO DATE,
	DELETED_FLG CHAR( 1 BYTE )
);

-- Поля сверены с методическими указаниями.
-- Имя таблицы тоже.
CREATE TABLE ANBO_DWH_DIM_ACCOUNTS_HIST (
	ACCOUNT_NUM varchar2(20),
	VALID_TO date,
	CLIENT varchar2(20),
	EFFECTIVE_FROM DATE, 
	EFFECTIVE_TO DATE,
	DELETED_FLG CHAR( 1 BYTE )
);

-- Поля сверены с методическими указаниями.
-- Имя таблицы тоже.
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

commit;
