query_to_update_historic_table = """
    MERGE crypto_raw.market_history AS target
    USING crypto_raw.temp_market_history AS source
    ON target.coin_id = source.coin_id
        AND target.event_timestamp = source.event_timestamp
    WHEN NOT MATCHED THEN
        INSERT ROW;
"""

query_to_calculate_diff_prices = """
    MERGE `crypto_analytics.market_history_analytics` AS target
    USING (
        WITH int_data_table AS (
            SELECT
                *
            FROM `crypto_raw.market_history`
                QUALIFY
                ROW_NUMBER() OVER (
                    PARTITION BY coin_id
                    ORDER BY event_timestamp DESC
                ) <= 2
        ),

        calculations AS (
            SELECT
                it.*,
                it.price_usd - LAG(it.price_usd) OVER (
                    PARTITION BY it.coin_id
                    ORDER BY it.event_timestamp
                ) AS price_change,
                SAFE_DIVIDE(
                    it.price_usd - LAG(it.price_usd) OVER (
                        PARTITION BY it.coin_id
                        ORDER BY it.event_timestamp
                    ),
                    LAG(it.price_usd) OVER (
                        PARTITION BY it.coin_id
                        ORDER BY it.event_timestamp
                    )
                ) * 100 AS price_change_pct
            FROM int_data_table AS it
        )

        SELECT
            *
        FROM calculations
        QUALIFY
            ROW_NUMBER() OVER (
                PARTITION BY coin_id
                ORDER BY event_timestamp DESC
            ) = 1
        ) AS source
    ON target.coin_id = source.coin_id
        AND target.event_timestamp = source.event_timestamp
    WHEN NOT MATCHED THEN
        INSERT (coin_id, event_timestamp, price_usd, market_cap_usd, volume_usd, ingestion_date, price_change, price_change_pct, ma_7, ma_30, volatility_7d, volatility_30d)
        VALUES (source.coin_id, source.event_timestamp, source.price_usd, source.market_cap_usd, source.volume_usd, source.ingestion_date, source.price_change, source.price_change_pct, NULL, NULL, NULL, NULL);
"""

query_to_compute_mas = """
    MERGE `crypto_analytics.market_history_analytics` AS t
    USING (
    WITH data_table AS (
        SELECT
            *
        FROM `crypto_raw.market_history`
        QUALIFY
            ROW_NUMBER() OVER (
                PARTITION BY coin_id
                ORDER BY event_timestamp DESC
            ) <= 8
        ORDER BY coin_id, event_timestamp
    ),

    calculation AS (
        SELECT
            *,
            CASE
                WHEN COUNT(price_usd) OVER (
                    PARTITION BY coin_id
                    ORDER BY event_timestamp
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) = 7
                THEN AVG(price_usd) OVER (
                    PARTITION BY coin_id
                    ORDER BY event_timestamp
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                )
                ELSE NULL
            END AS ma_7,
        FROM data_table AS d 
    )

    SELECT
        *
    FROM calculation
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY coin_id
            ORDER BY event_timestamp DESC
        ) = 1
    ORDER BY coin_id, event_timestamp
    ) AS s 
    ON t.coin_id = s.coin_id
        AND t.event_timestamp = s.event_timestamp
    WHEN MATCHED THEN 
        UPDATE SET
        t.ma_7 = s.ma_7;

    MERGE `crypto_analytics.market_history_analytics` AS t
    USING (
    WITH data_table AS (
        SELECT
        *
        FROM `crypto_raw.market_history`
        QUALIFY
            ROW_NUMBER() OVER (
                PARTITION BY coin_id
                ORDER BY event_timestamp DESC
            ) <= 31
        ORDER BY coin_id, event_timestamp
    ),

    calculation AS (
        SELECT
        *,
        CASE
            WHEN COUNT(price_usd) OVER (
                PARTITION BY coin_id
                ORDER BY event_timestamp
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) = 30
            THEN AVG(price_usd) OVER (
                PARTITION BY coin_id
                ORDER BY event_timestamp
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS ma_30,
        FROM data_table AS d 
    )

    SELECT
        *
    FROM calculation
    QUALIFY
        ROW_NUMBER() OVER (
        PARTITION BY coin_id
        ORDER BY event_timestamp DESC
        ) = 1
    ORDER BY coin_id, event_timestamp
    ) AS s 
    ON t.coin_id = s.coin_id
        AND t.event_timestamp = s.event_timestamp
    WHEN MATCHED THEN 
        UPDATE SET
        t.ma_30 = s.ma_30;
"""

query_to_compute_volatilities = """
    MERGE `crypto_analytics.market_history_analytics` AS t
    USING (
      WITH base AS (
        SELECT
          *,
          price_usd - LAG(price_usd) OVER (
            PARTITION BY coin_id
            ORDER BY event_timestamp
          ) AS price_change,

          SAFE_DIVIDE(
            price_usd - LAG(price_usd) OVER (
              PARTITION BY coin_id
              ORDER BY event_timestamp
            ),
            LAG(price_usd) OVER (
              PARTITION BY coin_id
              ORDER BY event_timestamp
            )
          ) * 100 AS price_change_pct
        FROM `crypto_raw.market_history`
      ),

      data_table AS (
          SELECT
              *
          FROM base
          QUALIFY
              ROW_NUMBER() OVER (
                  PARTITION BY coin_id
                  ORDER BY event_timestamp DESC
              ) <= 8
          ORDER BY coin_id, event_timestamp
      ),

      calculation AS (
          SELECT
              *,
              CASE
                WHEN COUNT(price_change_pct) OVER (
                  PARTITION BY coin_id
                  ORDER BY event_timestamp
                  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) = 7
                THEN STDDEV_SAMP(price_change_pct) OVER (
                  PARTITION BY coin_id
                  ORDER BY event_timestamp
                  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                )
                ELSE NULL
              END AS volatility_7d,
          FROM data_table AS d 
      )

      SELECT
          *
      FROM calculation
      QUALIFY
          ROW_NUMBER() OVER (
              PARTITION BY coin_id
              ORDER BY event_timestamp DESC
          ) = 1
      ORDER BY coin_id, event_timestamp
    ) AS s 
    ON t.coin_id = s.coin_id
        AND t.event_timestamp = s.event_timestamp
    WHEN MATCHED THEN 
        UPDATE SET
        t.volatility_7d = s.volatility_7d;

    MERGE `crypto_analytics.market_history_analytics` AS t
    USING (
    WITH base AS (
        SELECT
          *,
          price_usd - LAG(price_usd) OVER (
            PARTITION BY coin_id
            ORDER BY event_timestamp
          ) AS price_change,

          SAFE_DIVIDE(
            price_usd - LAG(price_usd) OVER (
              PARTITION BY coin_id
              ORDER BY event_timestamp
            ),
            LAG(price_usd) OVER (
              PARTITION BY coin_id
              ORDER BY event_timestamp
            )
          ) * 100 AS price_change_pct
        FROM `crypto_raw.market_history`
      ),

      data_table AS (
          SELECT
              *
          FROM base
          QUALIFY
              ROW_NUMBER() OVER (
                  PARTITION BY coin_id
                  ORDER BY event_timestamp DESC
              ) <= 31
          ORDER BY coin_id, event_timestamp
      ),

      calculation AS (
          SELECT
              *,
              CASE
                WHEN COUNT(price_change_pct) OVER (
                  PARTITION BY coin_id
                  ORDER BY event_timestamp
                  ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) = 30
                THEN STDDEV_SAMP(price_change_pct) OVER (
                  PARTITION BY coin_id
                  ORDER BY event_timestamp
                  ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                )
                ELSE NULL
              END AS volatility_30d,
          FROM data_table AS d 
      )

      SELECT
          *
      FROM calculation
      QUALIFY
          ROW_NUMBER() OVER (
              PARTITION BY coin_id
              ORDER BY event_timestamp DESC
          ) = 1
      ORDER BY coin_id, event_timestamp
    ) AS s 
    ON t.coin_id = s.coin_id
        AND t.event_timestamp = s.event_timestamp
    WHEN MATCHED THEN 
        UPDATE SET
        t.volatility_30d = s.volatility_30d;
"""