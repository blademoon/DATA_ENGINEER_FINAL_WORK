-- 1. Совершение операции при просроченном или заблокированном паспорте.
-- (1) Транзакции только за этот день и предыдущий, накоплением (реализованно через запуск процедуры формирования отчета за каждый день).
-- (2) Данные таблицы измерения актуальные только на этот момент времени.
-- (3) Не удаленные строки.
-- (4) Последний день срока действия паспорта - не преступление.
-- (5) Убираем строки полные дубли.

INSERT INTO DEMIPT.ANBO_REP_FRAUD ( EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT) 
SELECT DISTINCT
    t1.TRANS_DATE,
    t4.PASSPORT_NUM,
    t4.LAST_NAME||' '||t4.FIRST_NAME||' '||t4.PATRONYMIC,
    t4.PHONE,
    '1',
    TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' )
FROM DEMIPT.ANBO_DWH_FACT_TRANSACTIONS T1
INNER JOIN DEMIPT.ANBO_DWH_DIM_CARDS_HIST T2
ON T1.CARD_NUM = T2.CARD_NUM
AND t1.TRANS_DATE <= TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ) -- (1)
AND TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T2.EFFECTIVE_FROM AND T2.EFFECTIVE_TO --(2)
AND T2.DELETED_FLG = 'N' -- (3)
INNER JOIN DEMIPT.ANBO_DWH_DIM_ACCOUNTS_HIST T3
ON T2.ACCOUNT_NUM = T3.ACCOUNT_NUM
AND TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T3.EFFECTIVE_FROM AND T3.EFFECTIVE_TO -- (2)
AND T3.DELETED_FLG = 'N' -- (3)
INNER JOIN DEMIPT.ANBO_DWH_DIM_CLIENTS_HIST T4
ON T3.CLIENT = T4.CLIENT_ID
AND TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T4.EFFECTIVE_FROM AND T4.EFFECTIVE_TO -- (2)
AND T4.DELETED_FLG = 'N' -- (3)
WHERE 1=1
AND T4.PASSPORT_NUM IN (SELECT PASSPORT_NUM FROM ANBO_DWH_FACT_PSSPRT_BLCKLST WHERE ENTRY_DT <= TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ) )
OR T4.PASSPORT_VALID_TO < TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ); -- (4)

-- 2. Совершение операции при недействующем договоре

INSERT INTO DEMIPT.ANBO_REP_FRAUD ( EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT) 
SELECT DISTINCT
    t1.TRANS_DATE,
    t4.PASSPORT_NUM,
    t4.LAST_NAME||' '||t4.FIRST_NAME||' '||t4.PATRONYMIC,
    t4.PHONE,
    '2',
    TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' )
FROM DEMIPT.ANBO_DWH_FACT_TRANSACTIONS T1
INNER JOIN DEMIPT.ANBO_DWH_DIM_CARDS_HIST T2
ON T1.CARD_NUM = T2.CARD_NUM
AND t1.TRANS_DATE <= TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ) -- (1)
AND TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T2.EFFECTIVE_FROM AND T2.EFFECTIVE_TO --(2)
AND T2.DELETED_FLG = 'N' -- (3)
INNER JOIN DEMIPT.ANBO_DWH_DIM_ACCOUNTS_HIST T3
ON T2.ACCOUNT_NUM = T3.ACCOUNT_NUM
AND TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T3.EFFECTIVE_FROM AND T3.EFFECTIVE_TO -- (2)
AND T3.DELETED_FLG = 'N' -- (3)
INNER JOIN DEMIPT.ANBO_DWH_DIM_CLIENTS_HIST T4
ON T3.CLIENT = T4.CLIENT_ID
AND TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T4.EFFECTIVE_FROM AND T4.EFFECTIVE_TO -- (2)
AND T4.DELETED_FLG = 'N' -- (3)
WHERE 1=1
AND T3.VALID_TO < TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' );


-- 3. Совершение операций в разных городах в течении одного часа.
-- (1) Транзакции только за этот день и предыдущий, накоплением (реализованно через запуск процедуры формирования отчета за каждый день).
-- (2) Данные таблицы измерения актуальные только на этот момент времени.
-- (3) Не удаленные строки.
-- (4) Расчитаем время между парами соседних транзакций в днях, чтобы получить это время в часах просто умножим результат на 24 и округлим. 
INSERT INTO DEMIPT.ANBO_REP_FRAUD ( EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)
SELECT DISTINCT
    T3.CUR_TRANS_DATE,
    T6.PASSPORT_NUM,
    T6.LAST_NAME||' '||T6.FIRST_NAME||' '||T6.PATRONYMIC,
    T6.PHONE,
    '3',
     TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' )
FROM (
	SELECT
	-- Нужно выбрать только необходимые поля!
		CUR_CARD_NUM,
        CUR_TRANS_DATE,
        PREV_TRANS_DATE,
        DIFF_HOUR,
        CUR_TRANS_CITY,
        PREV_TRANS_CITY
	FROM(
		SELECT
			T1.CARD_NUM as CUR_CARD_NUM,
			t1.TRANS_DATE as CUR_TRANS_DATE,
			LAG(t1.trans_date) over ( PARTITION BY t1.card_num ORDER BY t1.trans_date) AS PREV_TRANS_DATE,
			ROUND((24 * ( t1.trans_date - (lag(t1.trans_date) over (partition by t1.card_num order by t1.trans_date) ) ) ) ) as DIFF_HOUR, -- (4)
			t2.TERMINAL_CITY as CUR_TRANS_CITY,
			LAG(t2.TERMINAL_CITY) over ( PARTITION BY t1.card_num ORDER BY t1.trans_date) AS PREV_TRANS_CITY
		FROM 
			DEMIPT.ANBO_DWH_FACT_TRANSACTIONS T1
		LEFT JOIN
			DEMIPT.anbo_dwh_dim_terminals_hist T2
		ON
			T1.TERMINAL = T2.TERMINAL_ID
		AND t1.TRANS_DATE <= TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ) -- (1)
		AND TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T2.EFFECTIVE_FROM AND T2.EFFECTIVE_TO --(2)
		AND T2.DELETED_FLG = 'N' -- (3)
	)
	WHERE 1=1
	AND	DIFF_HOUR <=1
	AND CUR_TRANS_CITY != PREV_TRANS_CITY
) T3
LEFT JOIN DEMIPT.ANBO_DWH_DIM_CARDS_HIST T4
ON T3.CUR_CARD_NUM = T4.CARD_NUM
AND TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T4.EFFECTIVE_FROM AND T4.EFFECTIVE_TO --(2)
AND T4.DELETED_FLG = 'N' -- (3)
LEFT JOIN DEMIPT.ANBO_DWH_DIM_ACCOUNTS_HIST T5
ON T4.ACCOUNT_NUM = T5.ACCOUNT_NUM
AND TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T5.EFFECTIVE_FROM AND T5.EFFECTIVE_TO -- (2)
AND T5.DELETED_FLG = 'N' -- (3)
LEFT JOIN DEMIPT.ANBO_DWH_DIM_CLIENTS_HIST T6
ON T5.CLIENT = T6.CLIENT_ID
AND TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T6.EFFECTIVE_FROM AND T6.EFFECTIVE_TO -- (2)
AND T6.DELETED_FLG = 'N'; -- (3)


-- 4. Попытка подбора суммы. В течении 20 минут проходит более 3х операций со следующим шаблоном - каждая последующая
-- меньше предыдущей, при этом отклонены все, кроме последней. Последняя операция (успешная) в такой цепочке считается мошеннической.
-- (5) - разница по времени между текущей транзакцией и предпреддыдущей транзакцией (в минутах) РАБОТАЕТ! ПРОВЕРИЛ!.
-- (6) - поля даты предыдущей и предпредидущей транзакций. Нужны юыли для отладки, в итоговом запросе не нужны.
-- Экономим ресурсы хранилища.
INSERT INTO DEMIPT.ANBO_REP_FRAUD ( EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)
SELECT DISTINCT
	T1.EVENT_DT,
    T4.PASSPORT_NUM,
    T4.LAST_NAME||' '||T4.FIRST_NAME||' '||T4.PATRONYMIC,
    T4.PHONE,
    '4',
    TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' )
FROM (
	SELECT
		CUR_TRANS_DATE as EVENT_DT,
		CUR_CARD_NUM as CARD_NUM
	FROM (
		SELECT
			CARD_NUM as CUR_CARD_NUM,
			TRANS_DATE as CUR_TRANS_DATE,
			LAG(trans_date,1) over ( PARTITION BY card_num ORDER BY trans_date) AS PRE_TRANS_DATE,   -- (6)
			LAG(trans_date,2) over ( PARTITION BY card_num ORDER BY trans_date) AS PRE_PRE_TRANS_DATE, -- (6)
			AMT as CUR_TRANS_AMT,
			LAG(AMT,1) over ( PARTITION BY card_num ORDER BY trans_date) AS PRE_TRANS_AMT,
			LAG(AMT,2) over ( PARTITION BY card_num ORDER BY trans_date) AS PRE_PRE_TRANS_AMT,
			OPER_RESULT as CUR_TRANS_OP_RES,
			LAG(OPER_RESULT,1) over ( PARTITION BY card_num ORDER BY trans_date) AS PRE_TRANS_OP_RES,
			LAG(OPER_RESULT,2) over ( PARTITION BY card_num ORDER BY trans_date) AS PRE_PRE_TRANS_OP_RES,
			ROUND((60 * 24 * ( trans_date - (lag(trans_date,2) over (partition by card_num order by trans_date) ) ) ) ) as DIFF_MIN_CUR_PRE_PRE_TR -- (5)
		FROM 
			DEMIPT.ANBO_DWH_FACT_TRANSACTIONS
        WHERE
            TRANS_DATE <= TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ) -- (1)
	)
	WHERE 1=1
	AND DIFF_MIN_CUR_PRE_PRE_TR <=20
	AND CUR_TRANS_OP_RES = 'SUCCESS'
	AND PRE_TRANS_OP_RES = 'REJECT'
	AND PRE_PRE_TRANS_OP_RES = 'REJECT' 
	AND CUR_TRANS_AMT < PRE_TRANS_AMT
	AND PRE_TRANS_AMT < PRE_PRE_TRANS_AMT
	AND PRE_TRANS_AMT IS NOT NULL
	AND PRE_PRE_TRANS_AMT IS NOT NULL
) T1
LEFT JOIN DEMIPT.ANBO_DWH_DIM_CARDS_HIST T2
ON T1.CARD_NUM = T2.CARD_NUM
AND TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T2.EFFECTIVE_FROM AND T2.EFFECTIVE_TO --(2)
AND T2.DELETED_FLG = 'N' -- (3)
LEFT JOIN DEMIPT.ANBO_DWH_DIM_ACCOUNTS_HIST T3
ON T2.ACCOUNT_NUM = T3.ACCOUNT_NUM
AND TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T3.EFFECTIVE_FROM AND T3.EFFECTIVE_TO -- (2)
AND T3.DELETED_FLG = 'N' -- (3)
LEFT JOIN DEMIPT.ANBO_DWH_DIM_CLIENTS_HIST T4
ON T3.CLIENT = T4.CLIENT_ID
AND TO_DATE( '03.03.2021 23:59:59', 'DD.MM.YYYY HH24:MI:SS' ) BETWEEN T4.EFFECTIVE_FROM AND T4.EFFECTIVE_TO -- (2)
AND T4.DELETED_FLG = 'N' -- (3)




	

	
	
	
	
	