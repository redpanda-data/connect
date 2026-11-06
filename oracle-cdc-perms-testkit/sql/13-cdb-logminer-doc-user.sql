-- Run as SYS (sysdba) in CDB$ROOT (final GRANT FLASHBACK runs against the PDB).
-- Creates the LOGMINER-DOC-ROUTE test user: exactly the LogMiner-only grant set from the
-- draft permissions doc (§2) — LOGMINING + DBMS_LOGMNR/_D + SELECT ANY DICTIONARY +
-- session/container — plus ONE per-table FLASHBACK object grant for ALL_TABLES
-- visibility. This profile proves the doc's route streams fine, and that
-- SELECT_CATALOG_ROLE / EXECUTE_CATALOG_ROLE / the explicit V_$* grants /
-- SELECT ANY TRANSACTION / FLASHBACK ANY TABLE are each individually unnecessary
-- when this route is used. Grants dumped back out of the live test instance to
-- guarantee they match what was run.

WHENEVER SQLERROR EXIT SQL.SQLCODE

CREATE USER C##JOEDOC IDENTIFIED BY joedoc123 CONTAINER=ALL;

GRANT CREATE SESSION TO C##JOEDOC CONTAINER=ALL;
GRANT SET CONTAINER TO C##JOEDOC CONTAINER=ALL;
GRANT LOGMINING TO C##JOEDOC CONTAINER=ALL;
GRANT SELECT ANY DICTIONARY TO C##JOEDOC CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR TO C##JOEDOC CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR_D TO C##JOEDOC CONTAINER=ALL;

-- Per-table visibility grant, applied inside the PDB where the source table lives.
-- FLASHBACK object grants confer ALL_* catalog visibility but no row-read capability,
-- so streaming works while snapshots stay blocked.
ALTER SESSION SET CONTAINER = FREEPDB1;
GRANT FLASHBACK ON space_app.t_channel_def TO C##JOEDOC;

EXIT
