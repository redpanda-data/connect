-- PostgreSQL Benchmark - Products drip feed
-- Inserts one batch of rows shaped like products.sql, so the streamed rows
-- match the backfilled ones in size. Driven in a loop by the
-- psql:data:products:drip task, which supplies the batch size and the ceiling.
--
-- :batch is how many rows to add; :ceiling caps the table. A batch that would
-- cross the ceiling is truncated to what remains, so the task can stop.
INSERT INTO public.products (name, description, category, price, stock, sku, created_at, is_available)
SELECT
    'Product ' || n,
    repeat('Description for product ' || n || '. ', 100),
    (ARRAY['Electronics','Clothing','Books','Home & Garden','Sports','Toys','Food'])[1 + (n % 7)],
    ((n % 99000) / 100.0 + 1.0)::decimal(10,2),
    (n % 500),
    'SKU-' || LPAD(n::text, 8, '0'),
    NOW(),
    (n % 10 != 0)
FROM generate_series(
    (SELECT COALESCE(MAX(id), 0) + 1 FROM public.products),
    (SELECT LEAST(COALESCE(MAX(id), 0) + :batch, :ceiling) FROM public.products)
) AS n;
