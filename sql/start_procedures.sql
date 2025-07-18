-- 1) Turnover for Jan-2018
DO
$$
DECLARE
  dt    DATE := '2018-01-01';
  last  DATE := '2018-01-31';
BEGIN
  WHILE dt <= last LOOP
    RAISE NOTICE 'Calling ds.fill_account_turnover_f for %', dt;
    CALL "DS".fill_account_turnover_f(dt);
    dt := dt + INTERVAL '1 day';
  END LOOP;
END
$$ LANGUAGE plpgsql;


-- 2) Balances for Jan-2018
DO
$$
DECLARE
  dt    DATE := '2018-01-01';
  last  DATE := '2018-01-31';
BEGIN
  WHILE dt <= last LOOP
    RAISE NOTICE 'Calling ds.fill_account_balance_f for %', dt;
    CALL "DS".fill_account_balance_f(dt);
    dt := dt + INTERVAL '1 day';
  END LOOP;
END
$$ LANGUAGE plpgsql;


-- 3) F101 for Jan-2018
DO
$$
DECLARE
  dt    DATE := '2018-02-01';
BEGIN
  RAISE NOTICE 'Calling dm.fill_f101_round_f for %', dt;
  CALL "DM".fill_f101_round_f(dt);
END
$$ LANGUAGE plpgsql;