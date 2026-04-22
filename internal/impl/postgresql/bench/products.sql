-- PostgreSQL Benchmark - Products Data (150K rows, large description blob for row-size parity with users)
-- Prerequisites: Run create.sql first

INSERT INTO public.products (name, description, category, price, stock, sku, created_at, is_available)
SELECT
    'Product ' || n,
    repeat('Description for product ' || n || '. ', 25000),
    (ARRAY['Electronics','Clothing','Books','Home & Garden','Sports','Toys','Food'])[1 + (n % 7)],
    ((n % 99000) / 100.0 + 1.0)::decimal(10,2),
    (n % 500),
    'SKU-' || LPAD(n::text, 8, '0'),
    NOW(),
    (n % 10 != 0)
FROM generate_series(1, 150000) AS n;
