CREATE OR REPLACE PROCEDURE "DS".fill_account_turnover_f(i_OnDate DATE)
AS $$
DECLARE
  v_start TIMESTAMP = NOW();
  v_log INTEGER;
BEGIN
  -- logging start
  INSERT INTO "LOGS".procedure_logs(procedure_name, run_date, start_time)
    VALUES('fill_account_turnover_f', i_OnDate, v_start)
    RETURNING id INTO v_log;

  -- remove old data
  DELETE FROM "DM"."DM_ACCOUNT_TURNOVER_F" WHERE on_date = i_OnDate;

  -- filling new data
  WITH
    credit AS (
        SELECT credit_account_rk AS account_rk,
            SUM(credit_amount) AS credit_amt
        FROM "DS"."FT_POSTING_F"
        WHERE oper_date = i_OnDate
        GROUP BY credit_account_rk
        ),
    debet AS (
        SELECT debet_account_rk AS account_rk,
            SUM(debet_amount) AS debet_amt
        FROM "DS"."FT_POSTING_F"
        WHERE oper_date = i_OnDate
        GROUP BY debet_account_rk
        ),
    turnover AS (
        SELECT
            COALESCE(c.account_rk, d.account_rk) AS account_rk,
            COALESCE(c.credit_amt, 0) AS credit_amount,
            COALESCE(d.debet_amt, 0) AS debet_amount
        FROM credit c
        FULL JOIN debet d USING(account_rk)
    )

  INSERT INTO "DM"."DM_ACCOUNT_TURNOVER_F" (
    on_date,
    account_rk,
    credit_amount,
    credit_amount_rub,
    debet_amount,
    debet_amount_rub
  )
  SELECT
    i_OnDate,
    t.account_rk,
    t.credit_amount,
    t.credit_amount * COALESCE(er.reduced_cource, 1),
    t.debet_amount,
    t.debet_amount  * COALESCE(er.reduced_cource, 1)
  FROM turnover t
  LEFT JOIN "DS"."MD_ACCOUNT_D" acc
    ON acc.account_rk = t.account_rk
  LEFT JOIN "DS"."MD_EXCHANGE_RATE_D" er
    ON er.currency_rk = acc.currency_rk
   AND i_OnDate BETWEEN er.data_actual_date AND er.data_actual_end_date;

  -- logging end
  UPDATE "LOGS".procedure_logs
    SET end_time = NOW()
  WHERE id = v_log;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE "DS".fill_account_balance_f(i_OnDate DATE)
AS $$
DECLARE
    v_start TIMESTAMP;
    v_log INTEGER;
    rec_account RECORD;
    v_prev_balance_out NUMERIC(23,8);
    v_prev_balance_rub NUMERIC(23,8);
    v_debet_amount NUMERIC(23,8);
    v_credit_amount NUMERIC(23,8);
    v_debet_amount_rub NUMERIC(23,8);
    v_credit_amount_rub NUMERIC(23,8);
    v_new_balance_out NUMERIC(23,8);
    v_new_balance_rub NUMERIC(23,8);
BEGIN
    -- logging start
    v_start = NOW();
    INSERT INTO "LOGS".procedure_logs (procedure_name, run_date, start_time)
      VALUES ('fill_account_balance_f', i_OnDate, v_start)
      RETURNING id INTO v_log;
    
    -- Remove old data
    DELETE FROM "DM"."DM_ACCOUNT_BALANCE_F" WHERE on_date = i_OnDate;
    
    -- Filling new data
    FOR rec_account IN
        SELECT 
            account_rk,
            char_type,
            currency_rk
        FROM "DS"."MD_ACCOUNT_D"
        WHERE i_OnDate BETWEEN data_actual_date AND data_actual_end_date
    LOOP
        -- Get last day balance
        SELECT 
            COALESCE(balance_out, 0),
            COALESCE(balance_out_rub, 0)
        INTO 
            v_prev_balance_out,
            v_prev_balance_rub
        FROM "DM"."DM_ACCOUNT_BALANCE_F"
        WHERE account_rk = rec_account.account_rk
          AND on_date = i_OnDate - INTERVAL '1 day'
        LIMIT 1;
        
        -- If balance not found, set 0
        IF v_prev_balance_out IS NULL THEN
            v_prev_balance_out := 0;
        END IF;
        
        IF v_prev_balance_rub IS NULL THEN
            v_prev_balance_rub := 0;
        END IF;
        
        -- Get current day turnover
        SELECT 
            COALESCE(debet_amount, 0),
            COALESCE(credit_amount, 0),
            COALESCE(debet_amount_rub, 0),
            COALESCE(credit_amount_rub, 0)
        INTO 
            v_debet_amount,
            v_credit_amount,
            v_debet_amount_rub,
            v_credit_amount_rub
        FROM "DM"."DM_ACCOUNT_TURNOVER_F"
        WHERE account_rk = rec_account.account_rk
          AND on_date = i_OnDate
        LIMIT 1;
        
        -- Calculate new balance
        IF rec_account.char_type = 'А' THEN
            -- For active accounts
            v_new_balance_out := COALESCE(v_prev_balance_out + v_debet_amount - v_credit_amount, 0);
            v_new_balance_rub := COALESCE(v_prev_balance_rub + v_debet_amount_rub - v_credit_amount_rub, 0);
        ELSE
            -- For passive accounts
            v_new_balance_out := COALESCE(v_prev_balance_out - v_debet_amount + v_credit_amount, 0);
            v_new_balance_rub := COALESCE(v_prev_balance_rub - v_debet_amount_rub + v_credit_amount_rub, 0);
        END IF;
        
        -- Insert new balance
        INSERT INTO "DM"."DM_ACCOUNT_BALANCE_F" (
            on_date,
            account_rk,
            balance_out,
            balance_out_rub
        ) VALUES (
            i_OnDate,
            rec_account.account_rk,
            v_new_balance_out,
            v_new_balance_rub
        );
    END LOOP;
    
    -- Logging end
    UPDATE "LOGS".procedure_logs
    SET end_time = NOW()
    WHERE id = v_log;

END;
$$ LANGUAGE plpgsql;

