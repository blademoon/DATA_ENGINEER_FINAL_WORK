INSERT INTO demipt.elph_rep_fraud
            (event_dt,
             passport,
             fio,
             phone,
             event_type,
             report_dt)
SELECT t1.trans_date,
       t4.passport_num,
       t4.last_name || ' ' || t4.first_name || ' ' ||t4.patronymic,
       t4.phone,
       '3',
       (SELECT Max(last_update)
        FROM   demipt.elph_meta_tables)
FROM   (SELECT t0.*,
               Round(( t0.trans_date - ( Lag(t0.trans_date)
                                           over (
                                             PARTITION BY t0.card_num
                                             ORDER BY t0.trans_date) ) ) * 1440)
               diff,
               t00.terminal_city,
               Lag(t00.terminal_city)
                 over (
                   PARTITION BY t0.card_num
                   ORDER BY t0.trans_date)
                      city_prev,
               Lead(t00.terminal_city)
                 over (
                   PARTITION BY t0.card_num
                   ORDER BY t0.trans_date)
                      city_next
        FROM   demipt.elph_dwh_fact_transactions t0
               inner join elph_dwh_dim_terminals_hist t00
                       ON t0.terminal = t00.terminal_id
                          AND Extract(day FROM t0.trans_date) = Extract(day FROM
                              (SELECT last_update
                                            FROM   demipt.elph_meta_tables
                                            WHERE  dbname = 'DEMIPT'
                                                   AND
                              tablename = 'ELPH_DWH_FACT_TRANSACTIONS'))) t1
       inner join demipt.elph_dwh_dim_cards_hist t2
               ON t1.card_num = t2.card_num
                  AND t2.effective_to_dt = To_date('2999-12-31', 'YYYY-MM-DD')
                  AND t2.deleted_flg = 'N'
       inner join demipt.elph_dwh_dim_accounts_hist t3
               ON t2.account_num = t3.account_num
                  AND t3.effective_to_dt = To_date('2999-12-31', 'YYYY-MM-DD')
                  AND t3.deleted_flg = 'N'
       inner join demipt.elph_dwh_dim_clients_hist t4
               ON t3.client = t4.client_id
                  AND t4.effective_to_dt = To_date('2999-12-31', 'YYYY-MM-DD')
                  AND t4.deleted_flg = 'N'
WHERE  1 = 1
       AND t1.diff BETWEEN 1 AND 60
       AND t1.city_prev != t1.terminal_city
       AND t1.city_next != t1.terminal_city; 