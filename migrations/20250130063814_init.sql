-- Add migration script here
-- Create the recent_trades table
CREATE TABLE IF NOT EXISTS recent_trades (
    tid VARCHAR PRIMARY KEY,
    pair VARCHAR NOT NULL,
    price VARCHAR NOT NULL,
    amount VARCHAR NOT NULL,
    side VARCHAR NOT NULL,
    timestamp BIGINT NOT NULL
);

-- Create the vbs table for storing volume base data (used in the Kline table)
CREATE TABLE IF NOT EXISTS vbs (
    id SERIAL PRIMARY KEY,
    buy_base DOUBLE PRECISION NOT NULL,
    sell_base DOUBLE PRECISION NOT NULL,
    buy_quote DOUBLE PRECISION NOT NULL,
    sell_quote DOUBLE PRECISION NOT NULL
);

-- Create the klines table
CREATE TABLE IF NOT EXISTS klines (
    pair VARCHAR NOT NULL,
    time_frame VARCHAR NOT NULL,
    o DOUBLE PRECISION NOT NULL,
    h DOUBLE PRECISION NOT NULL,
    l DOUBLE PRECISION NOT NULL,
    c DOUBLE PRECISION NOT NULL,
    utc_begin BIGINT NOT NULL,
    vbs_id INTEGER NOT NULL,
    FOREIGN KEY (vbs_id) REFERENCES vbs(id),
    PRIMARY KEY (pair, time_frame, utc_begin)
);