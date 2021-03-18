-- Финальный скрипт создания таблиц для хранилища данный
-- Будем использовать SDC-2

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

-- Очищаем таблицы.
--DELETE FROM ANBO_STG_PSSPRT_BLCKLST;
--DELETE FROM ANBO_STG_TRANSACTIONS;
--DELETE FROM ANBO_STG_TERMINALS;
--DELETE FROM ANBO_STG_TERMINALS_DRFT;
--DELETE FROM ANBO_STG_BANK_ACCOUNTS;
--DELETE FROM ANBO_STG_BANK_CARDS;
--DELETE FROM ANBO_STG_BANK_CLIENTS;
--DELETE FROM ANBO_STG_BANK_ACCOUNTS_DEL;
--DELETE FROM ANBO_STG_BANK_CARDS_DEL;
--DELETE FROM ANBO_STG_BANK_CLIENTS_DEL;
--DELETE FROM ANBO_STG_TERMINALS_DEL;



-------------------- Создаём стейджинг таблицы --------------------------------
-- Создание стейджинг таблиц. Максимально близко к оригинальному формату данных.
-- Используем символьный тип для предотвращения потерь данных.

-- Данные из плоских файлов.

-- Стейджинг для черного списка паспортов (Факты).
-- Загружаем в стейджинг все, но будем фильтровать данные по дате. Мужно 
-- обрабатывать через META. Связана с таблице ANBO_DWH_FACT_PSSPRT_BLCKLST,
-- колонки только поменять местами и привести к типу DATE.
CREATE TABLE ANBO_STG_PSSPRT_BLCKLST (
	ENTRY_DT varchar(12),
	PASSPORT_NUM varchar(14)
);

-- Стейджинг для списка транзакций за текущий день (Факты).
-- Загружаем сразу из STG в DWH с перестановкой порядка полей и явным 
-- преобразованием типов (если нужно). Дублей не должно быть, данные только 
-- за этот день. Самая крупная транзакция в мире $1 000 000 000, тогда полей
-- AMOUNT имеет тип varchar(12). Должно хватить.
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


-- Проверка стейджинговых таблиу
--SELECT * FROM DEMIPT.ANBO_STG_PSSPRT_BLCKLST;
--SELECT * FROM DEMIPT.ANBO_STG_TRANSACTIONS;
--SELECT * FROM DEMIPT.ANBO_STG_TERMINALS;