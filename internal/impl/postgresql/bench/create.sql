-- PostgreSQL Benchmark Setup Script

CREATE TABLE IF NOT EXISTS public.users (
    id            SERIAL PRIMARY KEY,
    name          VARCHAR(100)    NOT NULL,
    surname       VARCHAR(100)    NOT NULL,
    about         TEXT            NOT NULL,
    email         VARCHAR(255)    NOT NULL,
    date_of_birth DATE,
    join_date     DATE,
    created_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    is_active     BOOLEAN         NOT NULL DEFAULT TRUE,
    login_count   INT             NOT NULL DEFAULT 0,
    balance       DECIMAL(10,2)   NOT NULL DEFAULT 0.00
);

CREATE TABLE IF NOT EXISTS public.products (
    id            SERIAL PRIMARY KEY,
    name          VARCHAR(255)    NOT NULL,
    description   TEXT            NOT NULL,
    category      VARCHAR(100)    NOT NULL,
    price         DECIMAL(10,2)   NOT NULL,
    stock         INT             NOT NULL DEFAULT 0,
    sku           VARCHAR(100)    NOT NULL,
    created_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    is_available  BOOLEAN         NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS public.cart (
    id         SERIAL PRIMARY KEY,
    user_id    INT           NOT NULL,
    product_id INT           NOT NULL,
    quantity   INT           NOT NULL DEFAULT 1,
    price      DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    info       TEXT          NOT NULL
);
