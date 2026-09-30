WITH source_data AS (

    SELECT
        SYMBOL,
        PRICE,
        CHANGE,
        CHANGE_PERCENT,
        HIGH,
        LOW,
        OPEN,
        PREVIOUS_CLOSE,
        TIMESTAMP AS SOURCE_TIMESTAMP,
        EVENT_TIME,
        INGESTED_AT
    FROM {{ source('stock_market', 'stock_prices') }}

),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY SYMBOL, EVENT_TIME
            ORDER BY INGESTED_AT DESC
        ) AS row_num
    FROM source_data

)

SELECT
    SYMBOL,
    PRICE,
    CHANGE,
    CHANGE_PERCENT,
    HIGH,
    LOW,
    OPEN,
    PREVIOUS_CLOSE,
    SOURCE_TIMESTAMP,
    EVENT_TIME,
    INGESTED_AT
FROM deduplicated
WHERE row_num = 1