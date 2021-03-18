-- Сбрасываем все существующие таблицы
DROP TABLE DEMIPT.ANBO_REP_FRAUD;

-- Очищаем таблицу.
-- DELETE FROM DEMIPT.ANBO_REP_FRAUD;

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
