DELETE FROM "DM"."DM_ACCOUNT_BALANCE_F" WHERE on_date = '2017-12-31';


INSERT INTO "DM"."DM_ACCOUNT_BALANCE_F" (
    on_date,
    account_rk,
    balance_out,
    balance_out_rub
)
SELECT 
    '2017-12-31'::DATE AS on_date,
    fb.account_rk,
    fb.balance_out,
    fb.balance_out * COALESCE(er.reduced_cource, 1) as balance_out_rub
FROM 
    "DS"."FT_BALANCE_F" fb
LEFT JOIN 
    "DS"."MD_ACCOUNT_D" acc ON fb.account_rk = acc.account_rk
LEFT JOIN 
    "DS"."MD_EXCHANGE_RATE_D" er ON acc.currency_rk = er.currency_rk
    AND '2017-12-31'::DATE BETWEEN er.data_actual_date AND er.data_actual_end_date
WHERE 
    fb.on_date = '2017-12-31'::DATE