BEGIN
    -- Scheduler for scheduling job to run every night
    DBMS_SCHEDULER.create_job (
        job_name        => 'daily_stock_comparison_job',
        job_type        => 'PLSQL_BLOCK',
        job_action      => 'BEGIN stock_comparison_pkg.compare_stock_data; END;',
        start_date      => SYSTIMESTAMP,  -- Starts immediately; adjust as needed
        repeat_interval => 'FREQ=DAILY; BYHOUR=0; BYMINUTE=0; BYSECOND=0',  -- Runs daily at midnight; adjust as needed
        enabled         => TRUE,
        comments        => 'Daily job to compare stock data between exchange and depository using external tables'
    );
END;
/