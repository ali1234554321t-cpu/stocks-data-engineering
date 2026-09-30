WITH realtime AS (

    SELECT
        SYMBOL,
        EVENT_TIME,
        PRICE,
        CHANGE,
        CHANGE_PERCENT,
        HIGH,
        LOW,
        OPEN,
        PREVIOUS_CLOSE,
        HIGH - LOW AS TRADING_RANGE,
        ABS(CHANGE) AS PRICE_CHANGE_ABS,
        CASE
            WHEN CHANGE > 0 THEN 'UP'
            WHEN CHANGE < 0 THEN 'DOWN'
            ELSE 'UNCHANGED'
        END AS PRICE_DIRECTION,
        SOURCE_TIMESTAMP,
        INGESTED_AT,
        NULL AS VOLUME,
        NULL AS DIVIDENDS,
        NULL AS STOCK_SPLITS,
        'REALTIME' AS DATA_SOURCE

    FROM {{ ref('stg_stock_prices') }}

),

historical AS (

    SELECT
        SYMBOL,
        EVENT_TIME,
        PRICE,
        CHANGE,
        CHANGE_PERCENT,
        HIGH,
        LOW,
        OPEN,
        PREVIOUS_CLOSE,
        HIGH - LOW AS TRADING_RANGE,
        ABS(CHANGE) AS PRICE_CHANGE_ABS,
        CASE
            WHEN CHANGE > 0 THEN 'UP'
            WHEN CHANGE < 0 THEN 'DOWN'
            ELSE 'UNCHANGED'
        END AS PRICE_DIRECTION,
        SOURCE_TIMESTAMP,
        INGESTED_AT,
        VOLUME,
        DIVIDENDS,
        STOCK_SPLITS,
        'HISTORICAL' AS DATA_SOURCE

    FROM {{ ref('stg_stock_prices_history') }}

)

SELECT * FROM realtime

UNION ALL

SELECT * FROM historical