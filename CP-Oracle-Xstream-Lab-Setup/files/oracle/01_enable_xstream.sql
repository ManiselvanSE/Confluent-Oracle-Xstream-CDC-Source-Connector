-- Runs as SYSDBA in CDB$ROOT, database still in NOARCHIVELOG mode.
--   &1 = online redo log size in MB (0 = keep the XE defaults)
--   &2 = recovery area size in GB
WHENEVER SQLERROR EXIT SQL.SQLCODE
SET SERVEROUTPUT ON FEEDBACK OFF VERIFY OFF
DEFINE redo_mb = &1
DEFINE fra_gb  = &2

PROMPT == Enable XStream (enable_goldengate_replication)
ALTER SYSTEM SET enable_goldengate_replication=TRUE SCOPE=BOTH;

PROMPT == Streams pool for capture + outbound (takes effect after restart)
ALTER SYSTEM SET streams_pool_size=256M SCOPE=SPFILE;

PROMPT == Recovery area inside the persistent volume (archived redo goes here)
ALTER SYSTEM SET db_recovery_file_dest_size=&fra_gb.G SCOPE=BOTH;
ALTER SYSTEM SET db_recovery_file_dest='/opt/oracle/oradata/fast_recovery_area' SCOPE=BOTH;

PROMPT == Resize online redo logs (bigger logs = fewer log switches under CDC load)
DECLARE
  v_size NUMBER := &redo_mb;
  v_dir  VARCHAR2(512);
  v_cnt  NUMBER;
BEGIN
  IF v_size = 0 THEN
    DBMS_OUTPUT.PUT_LINE('redo_log_size_mb = 0, keeping default redo logs');
    RETURN;
  END IF;

  SELECT SUBSTR(member, 1, INSTR(member, '/', -1)) INTO v_dir
    FROM v$logfile WHERE ROWNUM = 1;

  -- New groups 4-6 next to the existing members
  FOR g IN 4 .. 6 LOOP
    SELECT COUNT(*) INTO v_cnt FROM v$log WHERE group# = g;
    IF v_cnt = 0 THEN
      EXECUTE IMMEDIATE 'ALTER DATABASE ADD LOGFILE GROUP ' || g ||
                        ' (''' || v_dir || 'redo0' || g || '.log'') SIZE ' || v_size || 'M REUSE';
    END IF;
  END LOOP;

  -- Switch away from groups 1-3 until they are INACTIVE, then drop them
  FOR g IN 1 .. 3 LOOP
    SELECT COUNT(*) INTO v_cnt FROM v$log WHERE group# = g;
    CONTINUE WHEN v_cnt = 0;
    FOR i IN 1 .. 60 LOOP
      SELECT COUNT(*) INTO v_cnt FROM v$log WHERE group# = g AND status IN ('CURRENT', 'ACTIVE');
      EXIT WHEN v_cnt = 0;
      EXECUTE IMMEDIATE 'ALTER SYSTEM SWITCH LOGFILE';
      EXECUTE IMMEDIATE 'ALTER SYSTEM CHECKPOINT';
      DBMS_SESSION.SLEEP(2);
    END LOOP;
    EXECUTE IMMEDIATE 'ALTER DATABASE DROP LOGFILE GROUP ' || g;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('Redo logs recreated at ' || v_size || ' MB in ' || v_dir);
END;
/

PROMPT == Supplemental logging: minimal + ALL columns at database level
WHENEVER SQLERROR CONTINUE
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
WHENEVER SQLERROR EXIT SQL.SQLCODE

SELECT supplemental_log_data_min AS min_logging, supplemental_log_data_all AS all_columns FROM v$database;
EXIT
