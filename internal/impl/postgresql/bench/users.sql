-- PostgreSQL Benchmark - Users Data (150K rows, ~500KB per row)
-- Prerequisites: Run create.sql first

DO $$
DECLARE
    tbl text;
    num_rows int := 5000;
BEGIN
    FOREACH tbl IN ARRAY ARRAY['public.users', 'tenant_a.users', 'tenant_b.users', 'tenant_c.users']
    LOOP
        EXECUTE format($fmt$
            INSERT INTO %s (name, surname, about, email, date_of_birth, join_date, created_at, is_active, login_count, balance)
            SELECT
                'user-' || n,
                'surname-' || n,
                repeat('This is about user ' || n || '. ', 25000),
                'user' || n || '@example.com',
                NOW() - (n %% 10000 || ' days')::interval,
                NOW(),
                NOW(),
                (n %% 2 = 0),
                n %% 100,
                ((n %% 1000) + (n %% 100) / 100.0)::decimal(10,2)
            FROM generate_series(1, %s) AS n
        $fmt$, tbl, num_rows);
    END LOOP;
END $$;
