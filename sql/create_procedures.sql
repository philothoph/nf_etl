-- fill dm_account_turnover_f
CREATE OR REPLACE PROCEDURE "DS".fill_account_turnover_f(i_OnDate DATE)
AS $$
DECLARE
  v_start TIMESTAMP = CLOCK_TIMESTAMP();
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
    SET end_time = CLOCK_TIMESTAMP()
  WHERE id = v_log;
END;
$$ LANGUAGE plpgsql;

-- fill dm_account_balance_f
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
    v_start = CLOCK_TIMESTAMP();
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
    SET end_time = CLOCK_TIMESTAMP()
    WHERE id = v_log;

END;
$$ LANGUAGE plpgsql;

-- fill DM_F101_ROUND_F
CREATE OR REPLACE PROCEDURE "DM".fill_f101_round_f(i_OnDate DATE)
AS $$
DECLARE
  v_start TIMESTAMP;
  v_log INTEGER;
  v_first_day DATE;
  v_last_day DATE;
  v_balance_date DATE;
BEGIN
  -- logging start
  v_start := CLOCK_TIMESTAMP();
  INSERT INTO "LOGS".procedure_logs (procedure_name, run_date, start_time)
    VALUES ('fill_f101_round_f', i_OnDate, v_start)
    RETURNING id INTO v_log;

  -- calculate report dates
    v_first_day := i_OnDate - INTERVAL '1 month';
    v_last_day := i_OnDate - INTERVAL '1 day';
    v_balance_date := v_first_day - INTERVAL '1 day';

  -- remove old data
    DELETE FROM "DM"."DM_F101_ROUND_F" WHERE from_date = v_first_day AND to_date = v_last_day;

  -- insert new data
    WITH account_data AS (
        SELECT 
            acc.account_rk,
            SUBSTRING(acc.account_number, 1, 5) as ledger_account,
            acc.char_type as characteristic,
            acc.currency_code,
            las.chapter
        FROM "DS"."MD_ACCOUNT_D" acc
        LEFT JOIN "DS"."MD_LEDGER_ACCOUNT_S" las 
            ON las.ledger_account::text = SUBSTRING(acc.account_number, 1, 5)
            AND las.start_date <= v_last_day
            AND (las.end_date IS NULL OR las.end_date >= v_first_day)
        WHERE 
            acc.data_actual_date <= v_last_day
            AND (acc.data_actual_end_date IS NULL OR acc.data_actual_end_date >= v_first_day)
    ),
    
    turnover_data AS (
        SELECT 
            account_rk,
            SUM(debet_amount_rub) as debet_amount_rub,
            SUM(credit_amount_rub) as credit_amount_rub
        FROM "DM"."DM_ACCOUNT_TURNOVER_F"
        WHERE on_date >= v_first_day AND on_date <= v_last_day
        GROUP BY account_rk
    ),
    
    balance_in_data AS (
        SELECT 
            account_rk,
            balance_out_rub
        FROM "DM"."DM_ACCOUNT_BALANCE_F"
        WHERE on_date = v_balance_date
    ),
    
    balance_out_data AS (
        SELECT 
            account_rk,
            balance_out_rub
        FROM "DM"."DM_ACCOUNT_BALANCE_F"
        WHERE on_date = v_last_day
    )
    
    INSERT INTO "DM"."DM_F101_ROUND_F" (
        from_date,
        to_date,
        chapter,
        ledger_account,
        characteristic,
        balance_in_rub,
        balance_in_val,
        balance_in_total,
        turn_deb_rub,
        turn_deb_val,
        turn_deb_total,
        turn_cre_rub,
        turn_cre_val,
        turn_cre_total,
        balance_out_rub,
        balance_out_val,
        balance_out_total
    )
    SELECT 
        v_first_day,
        v_last_day,
        ad.chapter,
        ad.ledger_account,
        ad.characteristic,
        
        -- Incoming balances (day before reporting period)
        COALESCE(SUM(CASE WHEN ad.currency_code IN ('810', '643') 
                         THEN bi.balance_out_rub 
                         ELSE 0 END), 0) as balance_in_rub,
        COALESCE(SUM(CASE WHEN ad.currency_code NOT IN ('810', '643') 
                         THEN bi.balance_out_rub 
                         ELSE 0 END), 0) as balance_in_val,
        COALESCE(SUM(bi.balance_out_rub), 0) as balance_in_total,
        
        -- Debit turnovers for reporting period
        COALESCE(SUM(CASE WHEN ad.currency_code IN ('810', '643') 
                         THEN td.debet_amount_rub 
                         ELSE 0 END), 0) as turn_deb_rub,
        COALESCE(SUM(CASE WHEN ad.currency_code NOT IN ('810', '643') 
                         THEN td.debet_amount_rub 
                         ELSE 0 END), 0) as turn_deb_val,
        COALESCE(SUM(td.debet_amount_rub), 0) as turn_deb_total,
        
        -- Credit turnovers for reporting period
        COALESCE(SUM(CASE WHEN ad.currency_code IN ('810', '643') 
                         THEN td.credit_amount_rub 
                         ELSE 0 END), 0) as turn_cre_rub,
        COALESCE(SUM(CASE WHEN ad.currency_code NOT IN ('810', '643') 
                         THEN td.credit_amount_rub 
                         ELSE 0 END), 0) as turn_cre_val,
        COALESCE(SUM(td.credit_amount_rub), 0) as turn_cre_total,
        
        -- Outgoing balances (last day of reporting period)
        COALESCE(SUM(CASE WHEN ad.currency_code IN ('810', '643') 
                         THEN bo.balance_out_rub 
                         ELSE 0 END), 0) as balance_out_rub,
        COALESCE(SUM(CASE WHEN ad.currency_code NOT IN ('810', '643') 
                         THEN bo.balance_out_rub 
                         ELSE 0 END), 0) as balance_out_val,
        COALESCE(SUM(bo.balance_out_rub), 0) as balance_out_total
    
    FROM account_data ad
    LEFT JOIN balance_in_data bi ON bi.account_rk = ad.account_rk
    LEFT JOIN balance_out_data bo ON bo.account_rk = ad.account_rk
    LEFT JOIN turnover_data td ON td.account_rk = ad.account_rk
    
    WHERE 
        -- Only include accounts that have some activity or balances
        (bi.balance_out_rub IS NOT NULL 
         OR bo.balance_out_rub IS NOT NULL 
         OR td.debet_amount_rub IS NOT NULL 
         OR td.credit_amount_rub IS NOT NULL)
    
    GROUP BY 
        ad.chapter,
        ad.ledger_account,
        ad.characteristic
    
    HAVING 
        -- Only include records where at least one value is non-zero
        COALESCE(SUM(bi.balance_out_rub), 0) != 0
        OR COALESCE(SUM(bo.balance_out_rub), 0) != 0
        OR COALESCE(SUM(td.debet_amount_rub), 0) != 0
        OR COALESCE(SUM(td.credit_amount_rub), 0) != 0;

  -- logging end
  UPDATE "LOGS".procedure_logs
  SET end_time = CLOCK_TIMESTAMP()
  WHERE id = v_log;
END;
$$ LANGUAGE plpgsql;