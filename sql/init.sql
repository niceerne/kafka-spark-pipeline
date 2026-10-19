-- Métriques par fenêtre d'une minute et par type d'événement
CREATE TABLE IF NOT EXISTS event_metrics (
    window_start TIMESTAMP        NOT NULL,
    window_end   TIMESTAMP        NOT NULL,
    event_type   VARCHAR(20)      NOT NULL,
    event_count  BIGINT           NOT NULL,
    revenue      DOUBLE PRECISION NOT NULL DEFAULT 0,
    PRIMARY KEY (window_start, event_type)
);

-- Top produits par fenêtre d'une minute
CREATE TABLE IF NOT EXISTS product_metrics (
    window_start TIMESTAMP        NOT NULL,
    window_end   TIMESTAMP        NOT NULL,
    product_id   VARCHAR(20)      NOT NULL,
    views        BIGINT           NOT NULL,
    carts        BIGINT           NOT NULL,
    purchases    BIGINT           NOT NULL,
    revenue      DOUBLE PRECISION NOT NULL DEFAULT 0,
    PRIMARY KEY (window_start, product_id)
);

-- Taux de conversion vue -> achat par minute
CREATE OR REPLACE VIEW conversion_rate AS
SELECT
    window_start,
    SUM(CASE WHEN event_type = 'view' THEN event_count ELSE 0 END)     AS views,
    SUM(CASE WHEN event_type = 'purchase' THEN event_count ELSE 0 END) AS purchases,
    ROUND(
        100.0 * SUM(CASE WHEN event_type = 'purchase' THEN event_count ELSE 0 END)
        / NULLIF(SUM(CASE WHEN event_type = 'view' THEN event_count ELSE 0 END), 0), 2
    ) AS conversion_pct
FROM event_metrics
GROUP BY window_start
ORDER BY window_start DESC;
