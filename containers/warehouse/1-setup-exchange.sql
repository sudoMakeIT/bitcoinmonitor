DROP TABLE IF EXISTS bitcoin.price;
DROP SCHEMA IF EXISTS bitcoin;
CREATE SCHEMA bitcoin;
CREATE TABLE bitcoin.price (
    id VARCHAR(50),
    symbol VARCHAR(50),
    name VARCHAR(50),
    rank INT,
    supply NUMERIC,
    maxSupply NUMERIC,
    marketCapUsd NUMERIC,
    volumeUsd24Hr NUMERIC,
    priceUsd NUMERIC,
    changePercent24Hr NUMERIC,
    vwap24Hr NUMERIC,
    updated_utc TIMESTAMP
);