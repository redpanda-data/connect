-- PostgreSQL Benchmark - Cart Data (10M rows, small payloads)
-- Prerequisites: Run create.sql first

DO $$
DECLARE
    batch_size INT := 10000;
    total_rows INT := 10000000;
    inserted   INT := 0;
BEGIN
    WHILE inserted < total_rows LOOP
        INSERT INTO public.cart (user_id, product_id, quantity, price, info)
        SELECT
            (n % 10000) + 1,
            (n % 1000) + 1,
            (n % 10) + 1,
            ((n % 1000) + 0.99)::decimal(10,2),
            repeat('cart ' || n || ' ', 40)
        FROM generate_series(inserted + 1, inserted + batch_size) AS n;

        inserted := inserted + batch_size;
    END LOOP;
END $$;
