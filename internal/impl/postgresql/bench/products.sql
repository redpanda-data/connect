-- PostgreSQL Benchmark - Products Data (150K rows, ~500KB per row)
-- Prerequisites: Run create.sql first

INSERT INTO public.products (name, surname, about, email, date_of_birth, join_date, created_at, is_active, login_count, balance)
SELECT
    'product-' || n,
    'surname-' || n,
    repeat('This is about product ' || n || '. ', 25000),
    'product' || n || '@example.com',
    NOW() - (n % 10000 || ' days')::interval,
    NOW(),
    NOW(),
    (n % 2 = 0),
    n % 100,
    ((n % 1000) + (n % 100) / 100.0)::decimal(10,2)
FROM generate_series(1, 150000) AS n;
