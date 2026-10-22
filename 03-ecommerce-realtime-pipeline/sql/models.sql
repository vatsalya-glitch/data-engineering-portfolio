-- ──────────────────────────────────────────────────────────────────────────
-- ANALYTICAL MODELS (views over the sink tables)
--
--   bronze  raw validated events, one row per event (JSON payload)
--   silver  typed, business-shaped tables (orders, items, payments)
--   gold    answers to business questions
--
-- Views, not tables: they are always consistent with whatever the sink has
-- written so far - a "streaming" warehouse with no batch job to schedule.
-- {window_seconds} is filled in from config/pipeline.yaml.
-- ──────────────────────────────────────────────────────────────────────────

-- ── SILVER ────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW silver.orders AS
SELECT
    payload->>'order_id'                    AS order_id,
    user_id,
    session_id,
    channel,
    CAST(payload->>'order_total' AS DOUBLE) AS order_total,
    payload->>'currency'                    AS currency,
    json_array_length(payload->'items')     AS item_count,
    event_time                              AS ordered_at
FROM bronze.events
WHERE event_type = 'order_placed';

CREATE OR REPLACE VIEW silver.order_items AS
SELECT
    order_id,
    item.product_id,
    item.category,
    item.quantity,
    item.unit_price,
    item.quantity * item.unit_price AS line_total,
    ordered_at
FROM (
    SELECT
        payload->>'order_id' AS order_id,
        event_time           AS ordered_at,
        unnest(from_json(
            payload->'items',
            '[{"product_id":"VARCHAR","category":"VARCHAR","quantity":"INTEGER","unit_price":"DOUBLE"}]'
        )) AS item
    FROM bronze.events
    WHERE event_type = 'order_placed'
);

-- Latest payment outcome per order (a failed payment can be retried).
CREATE OR REPLACE VIEW silver.payments AS
SELECT
    payload->>'order_id' AS order_id,
    CASE event_type WHEN 'payment_succeeded' THEN 'paid' ELSE 'failed' END AS status,
    payload->>'reason'   AS failure_reason,
    event_time           AS attempted_at
FROM bronze.events
WHERE event_type IN ('payment_succeeded', 'payment_failed')
QUALIFY row_number() OVER (PARTITION BY payload->>'order_id' ORDER BY event_time DESC) = 1;

-- ── GOLD ──────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW gold.revenue_by_category AS
SELECT
    i.category,
    COUNT(DISTINCT i.order_id)       AS paid_orders,
    SUM(i.quantity)                  AS units_sold,
    ROUND(SUM(i.line_total), 2)      AS revenue
FROM silver.order_items i
JOIN silver.payments p USING (order_id)
WHERE p.status = 'paid'
GROUP BY i.category
ORDER BY revenue DESC;

-- Session conversion funnel: how many sessions reached each step.
CREATE OR REPLACE VIEW gold.conversion_funnel AS
WITH steps AS (
    SELECT * FROM (VALUES
        (1, 'page_viewed'), (2, 'product_viewed'), (3, 'add_to_cart'),
        (4, 'checkout_started'), (5, 'order_placed'), (6, 'payment_succeeded')
    ) AS t(step, event_type)
),
reached AS (
    SELECT s.step, s.event_type, COUNT(DISTINCT e.session_id) AS sessions
    FROM steps s
    LEFT JOIN bronze.events e USING (event_type)
    GROUP BY s.step, s.event_type
)
SELECT
    step,
    event_type,
    sessions,
    ROUND(100.0 * sessions / NULLIF(FIRST_VALUE(sessions) OVER (ORDER BY step), 0), 1) AS pct_of_sessions,
    ROUND(100.0 * sessions / NULLIF(LAG(sessions) OVER (ORDER BY step), 0), 1)         AS pct_of_previous
FROM reached
ORDER BY step;

CREATE OR REPLACE VIEW gold.channel_mix AS
SELECT
    channel,
    COUNT(*)                    AS orders,
    ROUND(SUM(order_total), 2)  AS gross_revenue,
    ROUND(AVG(order_total), 2)  AS avg_order_value
FROM silver.orders
GROUP BY channel
ORDER BY gross_revenue DESC;

-- How "real-time" is real-time? Seconds from event_time to landing in DuckDB.
-- Late (offline) events are included on purpose - they show up in the p99.
CREATE OR REPLACE VIEW gold.ingestion_latency AS
SELECT
    COUNT(*) AS events,
    ROUND(quantile_cont(epoch(ingested_at - event_time), 0.50), 2) AS p50_seconds,
    ROUND(quantile_cont(epoch(ingested_at - event_time), 0.95), 2) AS p95_seconds,
    ROUND(quantile_cont(epoch(ingested_at - event_time), 0.99), 2) AS p99_seconds,
    ROUND(max(epoch(ingested_at - event_time)), 2)                 AS max_seconds
FROM bronze.events;

-- STREAM vs BATCH reconciliation.
-- The aggregator's real-time windows vs the same numbers recomputed from
-- bronze ("the truth", which includes late events). Every difference should be
-- explained by gold.late_events - if it isn't, something is broken.
CREATE OR REPLACE VIEW gold.window_reconciliation AS
WITH batch AS (
    SELECT
        time_bucket(INTERVAL '{window_seconds} seconds', event_time) AS window_start,
        COUNT(*) AS events,
        COUNT(*) FILTER (WHERE event_type = 'order_placed') AS orders,
        COALESCE(SUM(CAST(payload->>'order_total' AS DOUBLE))
                 FILTER (WHERE event_type = 'order_placed'), 0) AS revenue
    FROM bronze.events
    GROUP BY 1
),
late AS (
    SELECT time_bucket(INTERVAL '{window_seconds} seconds', event_time) AS window_start,
           COUNT(*) AS late_events
    FROM gold.late_events
    GROUP BY 1
)
SELECT
    b.window_start,
    COALESCE(s.events, 0)                AS stream_events,
    b.events                             AS batch_events,
    COALESCE(l.late_events, 0)           AS late_events,
    b.events - COALESCE(s.events, 0) - COALESCE(l.late_events, 0) AS unexplained,
    COALESCE(s.orders, 0)                AS stream_orders,
    b.orders                             AS batch_orders,
    ROUND(COALESCE(s.revenue, 0), 2)     AS stream_revenue,
    ROUND(b.revenue, 2)                  AS batch_revenue
FROM batch b
LEFT JOIN gold.window_metrics s USING (window_start)
LEFT JOIN late l USING (window_start)
ORDER BY b.window_start;

-- One-glance health check of the whole pipeline.
CREATE OR REPLACE VIEW ops.pipeline_health AS
SELECT 'bronze.events'              AS dataset, COUNT(*) AS rows FROM bronze.events
UNION ALL SELECT 'quarantine.invalid_events', COUNT(*) FROM quarantine.invalid_events
UNION ALL SELECT 'quarantine.dead_letters',   COUNT(*) FROM quarantine.dead_letters
UNION ALL SELECT 'gold.window_metrics',       COUNT(*) FROM gold.window_metrics
UNION ALL SELECT 'gold.late_events',          COUNT(*) FROM gold.late_events
UNION ALL SELECT 'ops.alerts',                COUNT(*) FROM ops.alerts;
