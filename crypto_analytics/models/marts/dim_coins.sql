WITH staging AS (
    SELECT * FROM {{ ref('stg_crypto_prices') }}
)

SELECT DISTINCT
    symbol AS coin_id, --symbol == PK
    coin_name
FROM staging