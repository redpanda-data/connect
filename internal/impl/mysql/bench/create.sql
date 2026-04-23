-- MySQL Benchmark Setup Script

CREATE TABLE IF NOT EXISTS users (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    name          VARCHAR(100)    NOT NULL,
    surname       VARCHAR(100)    NOT NULL,
    about         MEDIUMTEXT      NOT NULL,
    email         VARCHAR(255)    NOT NULL,
    date_of_birth DATE,
    join_date     DATE,
    created_at    DATETIME        NOT NULL DEFAULT NOW(),
    is_active     BOOLEAN         NOT NULL DEFAULT TRUE,
    login_count   INT             NOT NULL DEFAULT 0,
    balance       DECIMAL(10,2)   NOT NULL DEFAULT 0.00
);

CREATE TABLE IF NOT EXISTS products (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    name          VARCHAR(255)    NOT NULL,
    description   MEDIUMTEXT      NOT NULL,
    category      VARCHAR(100)    NOT NULL,
    price         DECIMAL(10,2)   NOT NULL,
    stock         INT             NOT NULL DEFAULT 0,
    sku           VARCHAR(100)    NOT NULL,
    created_at    DATETIME        NOT NULL DEFAULT NOW(),
    is_available  BOOLEAN         NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS cart (
    id         INT AUTO_INCREMENT PRIMARY KEY,
    user_id    INT           NOT NULL,
    product_id INT           NOT NULL,
    quantity   INT           NOT NULL DEFAULT 1,
    price      DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    info       TEXT          NOT NULL
);
