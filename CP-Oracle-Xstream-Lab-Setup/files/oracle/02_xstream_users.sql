-- Runs as SYSDBA in CDB$ROOT.
--   &1 = XStream admin user    &2 = its password
--   &3 = XStream connect user  &4 = its password
-- Both are common users (C##...) so they exist in CDB$ROOT and every PDB.
WHENEVER SQLERROR EXIT SQL.SQLCODE
SET SERVEROUTPUT ON FEEDBACK OFF VERIFY OFF
DEFINE admin_user = &1
DEFINE admin_pwd  = &2
DEFINE cdc_user   = &3
DEFINE cdc_pwd    = &4

ALTER SESSION SET CONTAINER = CDB$ROOT;

PROMPT == Create (or reset password of) the common users
DECLARE
  PROCEDURE ensure_user(p_user VARCHAR2, p_pwd VARCHAR2) IS
    n NUMBER;
  BEGIN
    SELECT COUNT(*) INTO n FROM dba_users WHERE username = p_user;
    IF n = 0 THEN
      EXECUTE IMMEDIATE 'CREATE USER ' || p_user || ' IDENTIFIED BY "' || p_pwd ||
                        '" DEFAULT TABLESPACE users QUOTA UNLIMITED ON users CONTAINER=ALL';
      DBMS_OUTPUT.PUT_LINE('Created ' || p_user);
    ELSE
      EXECUTE IMMEDIATE 'ALTER USER ' || p_user || ' IDENTIFIED BY "' || p_pwd || '" CONTAINER=ALL';
      DBMS_OUTPUT.PUT_LINE('Exists, password reset: ' || p_user);
    END IF;
  END;
BEGIN
  ensure_user('&admin_user', '&admin_pwd');
  ensure_user('&cdc_user', '&cdc_pwd');
END;
/

PROMPT == XStream admin: owns capture + outbound server
GRANT CREATE SESSION, SET CONTAINER TO &admin_user CONTAINER=ALL;
-- DBA mirrors the reference setup (lab convenience; not needed in production)
GRANT DBA TO &admin_user CONTAINER=ALL;
BEGIN
  DBMS_XSTREAM_AUTH.GRANT_ADMIN_PRIVILEGE(
    grantee                 => '&admin_user',
    privilege_type          => 'CAPTURE',
    grant_select_privileges => TRUE,
    container               => 'ALL');
END;
/

PROMPT == XStream connect user: what the connector logs in as
GRANT CREATE SESSION, SET CONTAINER TO &cdc_user CONTAINER=ALL;
GRANT SELECT_CATALOG_ROLE TO &cdc_user CONTAINER=ALL;
-- Needed for the initial snapshot
GRANT SELECT ANY TABLE TO &cdc_user CONTAINER=ALL;
GRANT LOCK ANY TABLE TO &cdc_user CONTAINER=ALL;
GRANT FLASHBACK ANY TABLE TO &cdc_user CONTAINER=ALL;

EXIT
