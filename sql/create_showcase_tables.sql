CREATE TABLE
    IF NOT EXISTS "DM"."DM_ACCOUNT_TURNOVER_F" (
        on_date DATE,
        account_rk NUMERIC,
        credit_amount NUMERIC(23, 8),
        credit_amount_rub NUMERIC(23, 8),
        debet_amount NUMERIC(23, 8),
        debet_amount_rub NUMERIC(23, 8)
    );

CREATE TABLE
    IF NOT EXISTS "DM"."DM_ACCOUNT_BALANCE_F" (
        on_date DATE,
        account_rk NUMERIC,
        balance_out NUMERIC(23, 8),
        balance_out_rub NUMERIC(23, 8)
    );