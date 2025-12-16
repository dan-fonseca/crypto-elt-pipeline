WITH source AS (
    SELECT * FROM {{ source('airflow_db', 'raw_crypto_data') }}
),

cleaned AS (
    SELECT
        symbol,
        name AS coin_name,
        current_price AS price_usd,
        market_cap,
        volume_24h,
        CAST(ingested_at AS TIMESTAMP) AS captured_at
    FROM source
)

SELECT * FROM cleaned