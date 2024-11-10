/* Create a directory object (ensure the OS directory exists)
** Ensure to specify the your local path of your computer where CSV files will be stored.
*/
CREATE OR REPLACE DIRECTORY csv_dir AS 'C:\Users\SANTOSH\Desktop\TEST';

-- Grant necessary privileges
GRANT READ, WRITE ON DIRECTORY csv_dir TO system;

-- External table for exchange data
CREATE TABLE exchange_stock_data_ext (
    user_id      NUMBER,
    stock_id     VARCHAR2(20),
    stock_name   VARCHAR2(100),
    stock_count  NUMBER
)
ORGANIZATION EXTERNAL
(
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY csv_dir
    ACCESS PARAMETERS
    (
        RECORDS DELIMITED BY NEWLINE
        LOAD WHEN (user_id != BLANKS AND stock_id != BLANKS AND stock_name != BLANKS AND stock_count != BLANKS)
        SKIP 1
        BADFILE 'exchange_data_bad.bad'
        LOGFILE 'exchange_data_log.log'
        DISCARDFILE 'exchange_data_dsc.dsc'
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LTRIM
        MISSING FIELD VALUES ARE NULL
        REJECT ROWS WITH ALL NULL FIELDS
        (user_id,
         stock_id,
         stock_name,
         stock_count
        )
    )
    LOCATION ('exchange_data.csv')
)
PARALLEL 
REJECT LIMIT UNLIMITED;


-- External table for depository data
CREATE TABLE depository_stock_data_ext  (
    user_id      NUMBER,
    stock_id     VARCHAR2(20),
    stock_name   VARCHAR2(100),
    stock_count  NUMBER
)
ORGANIZATION EXTERNAL
(
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY csv_dir
    ACCESS PARAMETERS
    (
        RECORDS DELIMITED BY NEWLINE
        LOAD WHEN (user_id != BLANKS AND stock_id != BLANKS AND stock_name != BLANKS AND stock_count != BLANKS)
        SKIP 1
        BADFILE 'depositoty_data_bad.bad'
        LOGFILE 'depositoty_data_log.log'
        DISCARDFILE 'depositoty_data_dsc.dsc'
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
        MISSING FIELD VALUES ARE NULL
        REJECT ROWS WITH ALL NULL FIELDS
        (user_id,
         stock_id,
         stock_name,
         stock_count
        )
    )
    LOCATION ('depository_data.csv')
)
PARALLEL 
REJECT LIMIT UNLIMITED;


-- Table to log comparison outcomes
CREATE TABLE stock_data_comparison_log (
    log_id       NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    user_id      NUMBER,
    stock_id     VARCHAR2(20),
    exchange_stock_count NUMBER,
    depository_stock_count NUMBER,
    match_status VARCHAR2(20), -- 'MATCH' or 'MISMATCH'
    log_date     DATE DEFAULT SYSDATE
);
