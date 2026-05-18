-- MySQL Benchmark - Cart Data (small payloads ~600 B each)
-- Called by Taskfile with: CALL _bench_insert_cart(N);

DELIMITER //
CREATE PROCEDURE _bench_insert_cart(IN total INT)
BEGIN
  DECLARE i INT DEFAULT 1;
  START TRANSACTION;
  WHILE i <= total DO
    INSERT INTO cart (user_id, product_id, quantity, price, info)
    VALUES (
      (i % 10000) + 1,
      (i % 1000) + 1,
      (i % 10) + 1,
      ROUND((i % 1000) + 0.99, 2),
      REPEAT(CONCAT('cart ', i, ' '), 40)
    );
    IF i % 10000 = 0 THEN COMMIT; START TRANSACTION; END IF;
    SET i = i + 1;
  END WHILE;
  COMMIT;
END //
DELIMITER ;
