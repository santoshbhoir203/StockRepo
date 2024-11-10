CREATE OR REPLACE NONEDITIONABLE PACKAGE BODY stock_comparison_pkg IS
/*
There are 3 approaches to load CSV file into tables that are as follows.
1) SQL loader - Using SQL loader requires creation of control files which is executed using sqlldr command.
2) DBMS_LOB.LOADCLOBFROMFILE - This is an external utility to load the file as CLOB and then distibute data into table.
3) External tables - External tables requires creation of directory path where the CSV file will be stored 
   and DB user needs to have Read/write to that directory. 

I have chosen external table approach because,
- Simplified Data Access: Unlike SQL*Loader, which requires defining control files and running the sqlldr command-line utility, 
external tables can be queried directly using standard SQL, simplifying data access and operations.

- Ease of Use: Loading data using DBMS_LOB.LOADCLOBFROMFILE requires converting the file content to a CLOB and then parsing 
it manually with PL/SQL logic, which can be complex and error-prone for large datasets or complex data structures.
*/

    -- It check for the existince of the CSV file by performing the select on external table.
    PROCEDURE check_corrupt_or_missing_file (
       p_external_table_file   IN VARCHAR2
    )
    IS
        file_missing EXCEPTION;
        PRAGMA exception_init(file_missing, -29913); -- ORA-29913: file not found exception
    BEGIN
        -- Attempt to read from external table
        DECLARE
            v_count NUMBER;
        BEGIN
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || p_external_table_file INTO v_count;
        EXCEPTION
            WHEN file_missing THEN
                DBMS_OUTPUT.PUT_LINE('Error: ' || p_external_table_file || ' data file not found or is inaccessible.');
                RAISE; -- Optional: re-raise or handle differently
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Unexpected error while accessing '|| p_external_table_file || ' data: ' || SQLERRM);
        END;
    END check_corrupt_or_missing_file;
    
    
    PROCEDURE compare_stock_data 
    IS
        -- Cursor to fetch and compare data from the external tables for exchange and depository stock data.
        -- The comparison is based on user_id and stock_id, and determines whether the stock counts match or mismatch.
        CURSOR c_compared_data 
        IS
        SELECT  e.user_id
              , e.stock_id
              , e.stock_count AS exchange_stock_count
              , d.stock_count AS depository_stock_count
              , CASE
                   WHEN e.stock_count = d.stock_count THEN 'MATCH'   -- Status is 'MATCH' if counts are equal
                   ELSE 'MISMATCH'                                   -- Status is 'MISMATCH' if counts differ
                END AS match_status
        FROM exchange_stock_data_ext e
        LEFT JOIN depository_stock_data_ext d    -- Left join to include all exchange data, even if there is no matching record in the depository data
        ON e.user_id = d.user_id AND e.stock_id = d.stock_id
        ORDER BY e.user_id, e.stock_id;
        
        -- Define a PL/SQL table type to hold the fetched comparison data.
        TYPE t_compared_data IS TABLE OF c_compared_data%ROWTYPE;
        l_compared_data t_compared_data := t_compared_data();
    BEGIN
        -- Check if the external files for exchange and depository data are corrupt or missing.
        check_corrupt_or_missing_file('exchange_stock_data_ext');
        check_corrupt_or_missing_file('depository_stock_data_ext');
    
        -- Delete any existing comparison data for the current date.
        -- This ensures no duplicate comparisons are logged if the procedure is run multiple times in a day.
        DELETE FROM stock_data_comparison_log
        WHERE TRUNC(LOG_DATE) = TRUNC(SYSDATE);
    
         -- Open the cursor to fetch data for comparison.
        OPEN c_compared_data;
        LOOP
            -- Fetch data in bulk (10000 rows at a time) into the PL/SQL table variable.
            FETCH c_compared_data BULK COLLECT INTO l_compared_data LIMIT 10000;
            
            BEGIN
                -- Perform bulk inserts using the FORALL statement to improve performance.
                -- This inserts comparison results (user_id, stock_id, stock counts, and match status) into the log table.
                FORALL I IN 1 .. l_compared_data.COUNT
                    -- Insert comparison results into a log table
                    INSERT INTO stock_data_comparison_log 
                        ( user_id 
                        , stock_id
                        , exchange_stock_count
                        , depository_stock_count
                        , match_status
                        )
                    VALUES
                        ( l_compared_data(I).user_id
                        , l_compared_data(I).stock_id
                        , l_compared_data(I).exchange_stock_count
                        , l_compared_data(I).depository_stock_count
                        , l_compared_data(I).match_status
                        );                        
            END;
            
            -- Exit the loop when all data has been processed.
            EXIT WHEN c_compared_data%NOTFOUND;
        END LOOP;
        -- Close the cursor after data processing is complete.
        CLOSE c_compared_data;

    END compare_stock_data;
    
END stock_comparison_pkg;
/
