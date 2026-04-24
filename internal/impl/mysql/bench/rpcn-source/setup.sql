CREATE TABLE IF NOT EXISTS cart (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  user_id    INT,
  product_id INT,
  quantity   INT,
  price      DECIMAL(10,2),
  status     VARCHAR(20),
  notes      MEDIUMTEXT,
  created_at DATETIME
);

DROP PROCEDURE IF EXISTS _bench_insert_cart;

DELIMITER //
CREATE PROCEDURE _bench_insert_cart(IN total INT)
BEGIN
  DECLARE i INT DEFAULT 0;
  WHILE i < total DO
    START TRANSACTION;
    SET @batch_end = LEAST(i + 10000, total);
    WHILE i < @batch_end DO
      INSERT INTO cart (user_id, product_id, quantity, price, status, notes, created_at) VALUES (
        FLOOR(1 + RAND() * 100000),
        FLOOR(1 + RAND() * 10000),
        FLOOR(1 + RAND() * 10),
        ROUND(1 + RAND() * 999, 2),
        ELT(FLOOR(1 + RAND() * 3), 'pending', 'confirmed', 'shipped'),
        REPEAT(CONCAT('note_', FLOOR(RAND() * 9999)), 10),
        NOW() - INTERVAL FLOOR(RAND() * 365) DAY
      );
      SET i = i + 1;
    END WHILE;
    COMMIT;
  END WHILE;
END //
DELIMITER ;
