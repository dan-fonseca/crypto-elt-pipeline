WITH staging AS (
    SELECT * FROM {{ ref('stg_crypto_prices') }}
)

SELECT
    symbol AS coin_id, -- FK to the dim table
    price_usd,
    market_cap,
    volume_24h,
    captured_at
FROM staging