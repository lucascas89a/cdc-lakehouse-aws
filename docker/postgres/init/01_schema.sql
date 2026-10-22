-- Source OLTP schema for "ShopFlow", a fictional LATAM e-commerce.
-- This is the operational database that Debezium captures via logical replication.

CREATE SCHEMA IF NOT EXISTS shop;
SET search_path TO shop;

CREATE TABLE customers (
    customer_id  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email        TEXT        NOT NULL UNIQUE,          -- PII: masked in silver
    full_name    TEXT        NOT NULL,                 -- PII: masked in silver
    country      CHAR(2)     NOT NULL,                 -- ISO 3166-1 alpha-2
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    product_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku          TEXT          NOT NULL UNIQUE,
    name         TEXT          NOT NULL,
    category     TEXT          NOT NULL,
    unit_price   NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),  -- changes over time: SCD2 in silver
    is_active    BOOLEAN       NOT NULL DEFAULT true,
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id  BIGINT        NOT NULL REFERENCES customers,
    status       TEXT          NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'paid', 'shipped', 'delivered', 'cancelled')),
    currency     CHAR(3)       NOT NULL DEFAULT 'USD',
    total_amount NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (total_amount >= 0),
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_id     BIGINT        NOT NULL REFERENCES orders ON DELETE CASCADE,
    line_no      SMALLINT      NOT NULL,
    product_id   BIGINT        NOT NULL REFERENCES products,
    quantity     INTEGER       NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),  -- price at purchase time
    PRIMARY KEY (order_id, line_no)
);

CREATE TABLE payments (
    payment_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id     BIGINT        NOT NULL REFERENCES orders,
    provider     TEXT          NOT NULL CHECK (provider IN ('mercadopago', 'stripe', 'paypal')),
    attempt      SMALLINT      NOT NULL DEFAULT 1,       -- retries create new rows
    status       TEXT          NOT NULL DEFAULT 'authorized'
                 CHECK (status IN ('authorized', 'captured', 'failed', 'refunded')),
    amount       NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
    failure_code TEXT,                                   -- e.g. insufficient_funds, card_declined
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE shipments (
    shipment_id  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id     BIGINT      NOT NULL UNIQUE REFERENCES orders,
    carrier      TEXT        NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'label_created'
                 CHECK (status IN ('label_created', 'in_transit', 'delivered', 'returned')),
    promised_at  TIMESTAMPTZ NOT NULL,                   -- delivery SLA
    shipped_at   TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON orders (customer_id);
CREATE INDEX ON orders (status);
CREATE INDEX ON payments (order_id);
CREATE INDEX ON order_items (product_id);

-- Keep updated_at honest so CDC events carry a reliable change timestamp.
CREATE FUNCTION touch_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END $$;

DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['customers', 'products', 'orders', 'payments', 'shipments'] LOOP
        EXECUTE format('CREATE TRIGGER %I BEFORE UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION touch_updated_at()',
                       t || '_touch', t);
    END LOOP;
END $$;

-- Full before-images on UPDATE/DELETE, so silver can see what changed
-- (e.g. order status transitions, product price changes).
ALTER TABLE customers   REPLICA IDENTITY FULL;
ALTER TABLE products    REPLICA IDENTITY FULL;
ALTER TABLE orders      REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
ALTER TABLE payments    REPLICA IDENTITY FULL;
ALTER TABLE shipments   REPLICA IDENTITY FULL;

-- Publication consumed by Debezium (pgoutput plugin).
CREATE PUBLICATION shopflow_cdc FOR TABLES IN SCHEMA shop;
