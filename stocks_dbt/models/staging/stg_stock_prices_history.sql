WITH source_data AS (

    SELECT
        SYMBOL,
        TRADE_DATE,
        OPEN,
        HIGH,
        LOW,
        CLOSE,
        VOLUME,
        DIVIDENDS,
        STOCK_SPLITS,
        LOADED_AT
    FROM {{ source('stock_market', 'stock_prices_history') }}

),

with_previous_close AS (

    SELECT
        *,
        LAG(CLOSE) OVER (
            PARTITION BY SYMBOL
            ORDER BY TRADE_DATE
        ) AS PREVIOUS_CLOSE
    FROM source_data

)

SELECT
    SYMBOL,

    TRADE_DATE,

    CLOSE AS PRICE,

    CLOSE - PREVIOUS_CLOSE AS CHANGE,

    CASE
        WHEN PREVIOUS_CLOSE IS NULL OR PREVIOUS_CLOSE = 0
            THEN NULL
        ELSE ((CLOSE - PREVIOUS_CLOSE) / PREVIOUS_CLOSE) * 100
    END AS CHANGE_PERCENT,

    HIGH,
    LOW,
    OPEN,
    PREVIOUS_CLOSE,

    TRADE_DATE::TIMESTAMP AS EVENT_TIME,

    NULL AS SOURCE_TIMESTAMP,

    LOADED_AT AS INGESTED_AT,

    VOLUME,
    DIVIDENDS,
    STOCK_SPLITS

FROM with_previous_close