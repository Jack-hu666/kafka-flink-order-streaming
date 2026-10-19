-- 订单实时流处理 —— PostgreSQL 结果表
-- 由 docker-entrypoint-initdb.d 在首次启动时自动执行。

CREATE TABLE IF NOT EXISTS minutely_metrics (
    window_start    TIMESTAMP NOT NULL,
    window_end      TIMESTAMP NOT NULL,
    order_cnt       BIGINT,
    gmv             NUMERIC(18,2),
    avg_order_value NUMERIC(18,2),
    updated_at      TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (window_start)
);

CREATE TABLE IF NOT EXISTS province_metrics (
    window_start TIMESTAMP NOT NULL,
    province     VARCHAR(32) NOT NULL,
    order_cnt    BIGINT,
    gmv          NUMERIC(18,2),
    PRIMARY KEY (window_start, province)
);

CREATE TABLE IF NOT EXISTS top_items (
    window_start TIMESTAMP NOT NULL,
    item_id      VARCHAR(32) NOT NULL,
    total_amount NUMERIC(18,2),
    PRIMARY KEY (window_start, item_id)
);
