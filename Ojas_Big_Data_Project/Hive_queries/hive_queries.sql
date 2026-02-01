CREATE DATABASE IF NOT EXISTS project;
USE project;


CREATE EXTERNAL TABLE IF NOT EXISTS train_cleaned (
    `date`            DATE,
    store_nbr       INT,
    family           STRING,
    sales            DOUBLE,
    onpromotion      INT,
    city             STRING,
    type_of_store    STRING,
    cluster          INT,
    dcoilwtico       DOUBLE,
    transactions     INT,
    n_holidays       INT
)
STORED AS PARQUET
LOCATION '/user/project/retail/cleaned/train';


CREATE EXTERNAL TABLE IF NOT EXISTS test_cleaned (
    `date`            DATE,
    store_nbr       INT,
    family           STRING,
    sales            DOUBLE,
    onpromotion      INT,
    city             STRING,
    type_of_store    STRING,
    cluster          INT,
    dcoilwtico       DOUBLE,
    transactions     INT,
    n_holidays       INT
)
STORED AS PARQUET
LOCATION '/user/project/retail/cleaned/test';


SELECT COUNT(*) FROM train_cleaned;
SELECT * FROM train_cleaned LIMIT 10;

SELECT COUNT(*) FROM test_cleaned;
SELECT * FROM test_cleaned LIMIT 10;


DROP MATERIALIZED VIEW mv_train_daily_sales;

CREATE MATERIALIZED VIEW mv_train_daily_sales
AS
SELECT
    `date`,
    store_nbr,
    family,
    SUM(sales)        AS total_sales,
    SUM(transactions) AS total_transactions,
    AVG(dcoilwtico)   AS avg_oil_price
FROM train_cleaned
GROUP BY
    `date`,
    store_nbr,
    family;



USE project;

DROP TABLE IF EXISTS train_features;

CREATE TABLE train_features
STORED AS PARQUET
AS
SELECT
    `date`,
    store_nbr,
    family,
    total_sales,
    total_transactions,
    avg_oil_price
FROM mv_train_daily_sales;
