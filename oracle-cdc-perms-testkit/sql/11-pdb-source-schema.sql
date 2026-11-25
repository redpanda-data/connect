-- Run as SYSTEM against the PDB (FREEPDB1).
-- Creates a stand-in source schema with one CDC'd table.

WHENEVER SQLERROR EXIT SQL.SQLCODE

CREATE USER space_app IDENTIFIED BY space123;
GRANT CONNECT, RESOURCE TO space_app;
GRANT UNLIMITED TABLESPACE TO space_app;

CREATE TABLE space_app.t_channel_def (
  id       NUMBER PRIMARY KEY,
  name     VARCHAR2(100),
  val      NUMBER
);
ALTER TABLE space_app.t_channel_def ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO space_app.t_channel_def VALUES (1, 'chan-a', 10);
INSERT INTO space_app.t_channel_def VALUES (2, 'chan-b', 20);
INSERT INTO space_app.t_channel_def VALUES (3, 'chan-c', 30);
COMMIT;

EXIT
