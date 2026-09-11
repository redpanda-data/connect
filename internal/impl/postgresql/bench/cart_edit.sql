-- PostgreSQL Benchmark - Cart Updates
-- Prerequisites: Run create.sql and cart.sql first
--
-- Updates every cart row, committing each batch separately so the changes
-- reach the replication stream as they happen rather than arriving as one
-- transaction at the end. Run this alongside a snapshot benchmark to exercise
-- deduplication: a row updated while its chunk is buffered must be delivered
-- from the stream, not from the stale snapshot copy.
--
-- Walks ascending by id so the updates trail the backfill, which is the order
-- most likely to collide with a buffered chunk. Raise `pause` to spread the
-- updates over a longer window, and keep `info` the same width as cart.sql
-- writes so row sizes stay comparable between runs.

DO $$
DECLARE
    batch_size INT  := 5000;
    pause      REAL := 0;  -- seconds between batches; raise to overlap a slower backfill
    marker     TEXT := to_char(clock_timestamp(), 'HH24:MI:SS');
    min_id     INT;
    max_id     INT;
    next_id    INT;
    batch      BIGINT;
    updated    BIGINT := 0;
BEGIN
    SELECT MIN(id), MAX(id) INTO min_id, max_id FROM public.cart;
    IF min_id IS NULL THEN
        RAISE EXCEPTION 'public.cart is empty - run cart.sql first';
    END IF;

    next_id := min_id;
    WHILE next_id <= max_id LOOP
        UPDATE public.cart
        SET quantity = quantity + 1,
            price    = price + 0.01,
            info     = repeat('edit ' || marker || ' ' || id || ' ', 40)
        WHERE id >= next_id
          AND id < next_id + batch_size;

        GET DIAGNOSTICS batch = ROW_COUNT;
        updated := updated + batch;
        next_id := next_id + batch_size;

        COMMIT;

        IF pause > 0 THEN
            PERFORM pg_sleep(pause);
        END IF;
    END LOOP;

    RAISE NOTICE 'updated % cart row(s), marker %', updated, marker;
END $$;
