
-- Autor: Mario Heredia.
-- Select 1

SELECT A.TABLESPACE_NAME,
       A.CLASSE,
       A.SEGMENT_NAME,
       --A.HORAS,
       --A.SEMANAS,
       B.MAX_PARTITION_POSITION HORAS,
       B.MAX_PARTITION_POSITION / (24 * 7) SEMANAS,
       A.TAMANO_MBYTES,
       B.PARTITION_NAME_F,
       B.PARTITION_NAME_L,
       B.PARTITION_POSITION,
       B.MAX_PARTITION_POSITION,
       B.SUBPARTITION_COUNT
  FROM (
SELECT TABLESPACE_NAME,
       CASE WHEN INSTR(SEGMENT_NAME, 'UMTSC')  = 1 THEN 'UMTSC'
            WHEN INSTR(SEGMENT_NAME, 'UMTS_C') = 1 THEN 'UMTS_C'
            WHEN INSTR(SEGMENT_NAME, 'UMTS_D') = 1 THEN 'UMTS_D' ELSE NULL END CLASSE,
       SEGMENT_NAME,
       CANTIDAD,
       TAMANO_MBYTES
  FROM (
SELECT TABLESPACE_NAME,
       SEGMENT_NAME,
       COUNT(*) CANTIDAD,
       SUM((BYTES / 1024) / 1024) TAMANO_MBYTES
  FROM DBA_SEGMENTS
 WHERE TABLESPACE_NAME = 'TBS_UMTS_C_NSN_HOURLY'
 GROUP BY TABLESPACE_NAME,
          SEGMENT_NAME
       )
       ) A,
       (
SELECT TABLESPACE_NAME,
       TABLE_NAME,
       PARTITION_NAME_F,
       PARTITION_NAME_L,
       PARTITION_POSITION,
       MAX_PARTITION_POSITION,
       SUBPARTITION_COUNT
  FROM (
SELECT TABLESPACE_NAME,
       TABLE_NAME,
       PARTITION_NAME                                                                        PARTITION_NAME_F,
       LAG(PARTITION_NAME, 1) OVER(PARTITION BY TABLE_NAME ORDER BY PARTITION_POSITION DESC) PARTITION_NAME_L,
       PARTITION_POSITION,
       MAX_PARTITION_POSITION,
       SUBPARTITION_COUNT
  FROM (
SELECT TABLESPACE_NAME,
       TABLE_NAME,
       PARTITION_NAME,
       PARTITION_POSITION,
       MAX(PARTITION_POSITION) OVER(PARTITION BY TABLE_NAME) MAX_PARTITION_POSITION,
       SUBPARTITION_COUNT
  FROM DBA_TAB_PARTITIONS
 WHERE TABLESPACE_NAME IN ('TBS_UMTS_C_NSN_HOURLY')
       )
 WHERE PARTITION_POSITION IN (1, MAX_PARTITION_POSITION)
       )
 WHERE PARTITION_POSITION = 1
       ) B
 WHERE A.SEGMENT_NAME = B.TABLE_NAME
 ORDER BY TABLESPACE_NAME,
          TAMANO_MBYTES DESC;

-- Autor: Mario Heredia.
-- Select 5

SELECT GRANTEE, TABLE_NAME, PRIVILEGE,
       'GRANT '||PRIVILEGE||' ON '||TABLE_NAME||' TO '||GRANTEE||';' SE
  FROM DBA_TAB_PRIVS
 WHERE TABLE_NAME = 'OBJECTS_SP_UMTS'

-- Data Files, File System por Tablespace

SELECT FILE_SYSTEM,
       TABLESPACE_NAME,
       CANTIDAD,
       ROUND(BYTES / 1024 / 1024 / 1024, 4) GBYTES,
       ROUND(SUM(BYTES) OVER(PARTITION BY TABLESPACE_NAME) / 1024 / 1024 / 1024, 4) GBYTES_TBS,
       100 * ROUND(BYTES / SUM(BYTES) OVER(PARTITION BY TABLESPACE_NAME), 2) PRCT
  FROM (
SELECT SUBSTR(FILE_NAME, 1, 12) FILE_SYSTEM,
       TABLESPACE_NAME,
       COUNT(*) CANTIDAD,
       SUM(BYTES) BYTES
  FROM DBA_DATA_FILES
 GROUP BY SUBSTR(FILE_NAME, 1, 12),
          TABLESPACE_NAME
       )
 ORDER BY TABLESPACE_NAME, FILE_SYSTEM

-- Segmentos por Tablespace

SELECT TABLESPACE_NAME,
       SEGMENT_NAME,
       COUNT(*)                   CANTIDAD,
       SUM((BYTES / 1024) / 1024) TAMANO_MBYTES,
	   SYSDATE                    LAST_QUERY
  FROM DBA_SEGMENTS
 WHERE TABLESPACE_NAME = 'TBS_INDEXES_SVA_HOURLY'
 GROUP BY TABLESPACE_NAME,
          SEGMENT_NAME
 ORDER BY TABLESPACE_NAME,
          TAMANO_MBYTES DESC;

-- PGA Monitor

SELECT NAME,
       VALUE,
       UNIT,
       --VALUE_MBYTES,
       --TIPO,
       --LEAD_NEXT,
       ROUND(DECODE(TIPO, 1, ((VALUE_MBYTES - 118926907.12) /
                             ((VALUE_MBYTES - 118926907.12) + (LEAD_NEXT - 43974587.61))) * 100, NULL), 2) REAL_CACHE_HIT_RATIO
  FROM (
SELECT NAME,
       VALUE,
       UNIT,
       VALUE_MBYTES,
       TIPO,
       LEAD(VALUE_MBYTES) OVER(ORDER BY TIPO) LEAD_NEXT
  FROM (
SELECT NAME,
       DECODE(UNIT, 'bytes', ROUND(VALUE / 1024 / 1024, 4), VALUE) VALUE,
       DECODE(UNIT, 'bytes', 'Mbytes', UNIT) UNIT,
       DECODE(NAME, 'bytes processed', DECODE(UNIT, 'bytes', ROUND(VALUE / 1024 / 1024, 4), VALUE),
                    'extra bytes read/written', DECODE(UNIT, 'bytes', ROUND(VALUE / 1024 / 1024, 4), VALUE), NULL) VALUE_MBYTES,
       DECODE(NAME, 'bytes processed', 1,
                    'extra bytes read/written', 2, NULL) TIPO
  FROM V$PGASTAT
       )
       )

-- Valores al 16.11.2011
-- over allocation count     4248817 
-- bytes processed           118926907.12 Mbytes
-- extra bytes read/written  43974587.61 Mbytes

-- Parse Calls de Sentencias

SELECT EXECUTIONS EX,
       LOADS L,
       PARSE_CALLS PC,
       PARSING_SCHEMA_NAME,
       SYSDATE H,
       TO_DATE(FIRST_LOAD_TIME, 'YYYY-MM-DD/HH24:MI:SS') F_LOAD_TIME,
       SQL_TEXT,
       SQL_FULLTEXT, 
       SQL_ID,
       ROUND(SHARABLE_MEM / 1024 / 1024, 4) SHARED_MBYTES,
       LAST_ACTIVE_TIME
  FROM V$SQL
 WHERE PARSING_SCHEMA_NAME = 'OPS$CALIDAD'
 ORDER BY TO_DATE(FIRST_LOAD_TIME, 'YYYY-MM-DD/HH24:MI:SS') DESC;



SELECT PGA_TARGET_FOR_ESTIMATE,
       PGA_TARGET_FACTOR,
       ESTD_PGA_CACHE_HIT_PERCENTAGE,
       ESTD_OVERALLOC_COUNT
  FROM V$PGA_TARGET_ADVICE;

SELECT LOW_OPTIMAL_SIZE / 1024 "LOW (K)",
       (HIGH_OPTIMAL_SIZE + 1) / 1024 "HIGH (K)",
       OPTIMAL_EXECUTIONS OPTIMAL,
       ONEPASS_EXECUTIONS "1-PASS",
       MULTIPASSES_EXECUTIONS " >1 PASS"
  FROM V$SQL_WORKAREA_HISTOGRAM
 WHERE TOTAL_EXECUTIONS <> 0;

-- UNDO Monitor

SELECT STATUS, SUM (BYTES) / (1024 * 1024) AS SIZE_MB, COUNT (*) FROM DBA_UNDO_EXTENTS GROUP BY STATUS

SELECT * FROM V$UNDOSTAT

SELECT D.UNDO_SIZE / (1024 * 1024) "ACTUAL UNDO SIZE [MBYTE]",
       SUBSTR(E.VALUE, 1, 25) "UNDO RETENTION [SEC]",
       ROUND((D.UNDO_SIZE / (TO_NUMBER(F.VALUE) * G.UNDO_BLOCK_PER_SEC))) "OPTIMAL UNDO RETENTION [SEC]"
  FROM (SELECT SUM(A.BYTES) UNDO_SIZE
          FROM V$DATAFILE A, V$TABLESPACE B, DBA_TABLESPACES C
         WHERE C.CONTENTS = 'UNDO'
           AND C.STATUS = 'ONLINE'
           AND B.NAME = C.TABLESPACE_NAME
           AND A.TS# = B.TS#) D,
       V$PARAMETER E,
       V$PARAMETER F,
       (SELECT MAX(UNDOBLKS / ((END_TIME - BEGIN_TIME) * 3600 * 24)) UNDO_BLOCK_PER_SEC
          FROM V$UNDOSTAT) G
 WHERE E.NAME = 'undo_retention'
   AND F.NAME = 'db_block_size'

-- Para analisis de procesos

SELECT DECODE(B.NOMBRE, NULL, USERNAME_SO, B.NOMBRE) USERNAME_SO,
       USERNAME_SESSION USERNAME_SE,
       USERNAME_PROCESS USERNAME_PS,
       ROUND(PGA_ALLOC_MEM / 1024 / 1024, 4) PGA_SO,
       ROUND(SUM(PGA_ALLOC_MEM) OVER(PARTITION BY USERNAME_SESSION, USERNAME_PROCESS) / 1024 / 1024, 4) PGA_SE,
       ROUND(SUM(PGA_ALLOC_MEM) OVER() / 1024 / 1024, 4) PGA_TT,
       100 * ROUND(PGA_ALLOC_MEM / SUM(PGA_ALLOC_MEM) OVER(), 4) RATE_SO,
       100 * ROUND(PGA_ALLOC_MEM / SUM(PGA_ALLOC_MEM) OVER(PARTITION BY USERNAME_SESSION, USERNAME_PROCESS), 4) RATE_SE,
       CNT_SO,
       SUM(CNT_SO) OVER(PARTITION BY USERNAME_SESSION) CNT_SE
  FROM (
SELECT S.OSUSER USERNAME_SO,
       S.USERNAME USERNAME_SESSION,
       P.USERNAME USERNAME_PROCESS,
       SUM(PGA_ALLOC_MEM) PGA_ALLOC_MEM,
       COUNT(*) CNT_SO
  FROM V$SESSION S,
       V$PROCESS P
 WHERE S.PADDR = P.ADDR
 GROUP BY S.OSUSER,
          S.USERNAME,
          P.USERNAME
       ) A,
       (
SELECT LOWER(LEGAJO) LEGAJO,
       LOWER(NOMBRE||' '||APELLIDO) NOMBRE
  FROM SMART_USERS
       ) B
 WHERE A.USERNAME_SO = B.LEGAJO (+)
 ORDER BY PGA_SE DESC, PGA_SO DESC, CNT_SE DESC;

SELECT PGA_ALLOC_MEM_RATE,
       PGA_ALLOC_MEM,
       PGA_USED_MEM,
       --T_PGA_ALLOC_MEM,
       --T_PGA_USED_MEM,
       --T_PGA_USED_MEM_RATE,
       --T_MEMORIA_USED,
       --ACTION,
       MODULE,
       --PROCESS,
       USERNAME_SO,
       USERNAME_SESSION,
       USERNAME_PROCESS,
       S_KILL_SESSION,
       LOGON_TIME,
       SYSDATE SCR_TIME,
       --SQL_HASH_VALUE,
       S_KILL_SPID,
       SPID,
       STATUS
  FROM (
SELECT ACTION,
       MODULE,
       PROCESS,
       S.OSUSER USERNAME_SO,
       S.USERNAME USERNAME_SESSION,
       'ALTER SYSTEM KILL SESSION''' || S.SID || ',' || S.SERIAL# || ''';' S_KILL_SESSION,
       LOGON_TIME,
       SQL_HASH_VALUE,
       'kill -9 ' || P.SPID S_KILL_SPID,
       P.SPID,
       STATUS,
       P.USERNAME USERNAME_PROCESS,
       PGA_ALLOC_MEM,
       PGA_USED_MEM,
       SUM(PGA_ALLOC_MEM) OVER() T_PGA_ALLOC_MEM,
       SUM(PGA_USED_MEM) OVER() T_PGA_USED_MEM,
       100 * ROUND(PGA_ALLOC_MEM / SUM(PGA_ALLOC_MEM) OVER(), 4) PGA_ALLOC_MEM_RATE,
       100 * ROUND(PGA_USED_MEM / SUM(PGA_USED_MEM) OVER(), 4) T_PGA_USED_MEM_RATE,
       100 * ROUND(PGA_USED_MEM / PGA_ALLOC_MEM, 4) T_MEMORIA_USED
  FROM V$SESSION S, V$PROCESS P
 WHERE S.PADDR = P.ADDR
       )
 WHERE USERNAME_SESSION = 'OPS$CALIDAD'
 --WHERE USERNAME_SESSION = 'SMART'
   --AND USERNAME_SO = 'root'
 ORDER BY --PGA_ALLOC_MEM_RATE DESC
          USERNAME_SO, USERNAME_SESSION, LOGON_TIME;

-- Sessions Stats & Events

SELECT * FROM V$PARAMETER

SELECT * FROM V$SESSION WHERE USERNAME = 'OPS$CALIDAD' AND PROCESS = '6710'

SELECT * FROM V$SQLTEXT WHERE SQL_ID = '69vx62mg6p71v'

SELECT * FROM V$SQL WHERE SQL_ID = '69vx62mg6p71v'

SELECT * FROM V$SESSION_WAIT WHERE SID = 226

SELECT * FROM V$SESSION_EVENT WHERE SID = 226

SELECT * FROM V$SESSION_WAIT_HISTORY WHERE SID = 226


-- Kill Session

SELECT SID,
       SERIAL#,
       LOGON_TIME,
       STATUS,
       PROCESS,
       PROGRAM,
       'ALTER SYSTEM KILL SESSION '''||SID||', '||SERIAL#||''';' SE
 FROM V$SESSION
WHERE USERNAME = 'OPS$CALIDAD'
 
-- Dba Blocks

SELECT SUBSTR(V$SESSION.USERNAME, 1, 8) USERNAME,
       V$SESSION.OSUSER OSUSER,
       --  DECODE(V$SESSION.SERVER,'DEDICATED','D','SHARED','S','O') SERVER,
       V$SQLAREA.DISK_READS DISK_READS,
       V$SQLAREA.BUFFER_GETS BUFFER_GETS,
       SUBSTR(V$SESSION.LOCKWAIT, 1, 10) LOCKWAIT,
       V$SESSION.PROCESS PID,
       V$SESSION_WAIT.EVENT EVENT,
       V$SQLAREA.SQL_TEXT SQL
  FROM V$SESSION_WAIT, V$SQLAREA, V$SESSION
 WHERE V$SESSION.SQL_ADDRESS = V$SQLAREA.ADDRESS
   AND V$SESSION.SQL_HASH_VALUE = V$SQLAREA.HASH_VALUE
   AND V$SESSION.SID = V$SESSION_WAIT.SID(+)
   AND V$SESSION.STATUS = 'ACTIVE'
   AND V$SESSION_WAIT.EVENT != 'client message'
 ORDER BY V$SESSION.LOCKWAIT ASC, V$SESSION.USERNAME


SELECT BUFFER_GETS, DISK_READS, EXECUTIONS, DECODE(EXECUTIONS, 0, 0, BUFFER_GETS/EXECUTIONS) BE, SQL_TEXT FROM V$SQL

SELECT DF.NAME,
       FS.PHYBLKRD + FS.PHYBLKWRT TOTAL_IOS,
       FS.PHYBLKRD BLOCK_READ,
       FS.PHYBLKWRT BLOCK_WRITTEN 
  FROM V$FILESTAT FS,
       V$DATAFILE DF
 WHERE DF.FILE# = FS.FILE#
 ORDER BY DRIVE, FILE_NAME DESC

-- Anonymous PL/SQL Block's. Goal for optimizing !!!
-- tags: OPTIMIZACION, PLSQL

SELECT PARSING_SCHEMA_NAME SCHEME,
       SQL_FULLTEXT,
       SQL_TEXT,
       LENGTH(SQL_FULLTEXT) LARGO,
       SHARABLE_MEM / 1024 SHARABLE_MEM_KBYTES,
       COMMAND_TYPE
  FROM V$SQLAREA
 WHERE COMMAND_TYPE = 47
   AND LENGTH(SQL_TEXT) > 500;

-- Cantidad de Memoria Libre en la Shared Pool
SELECT BYTES / 1024 / 1024 MBYTES
  FROM V$SGASTAT
 WHERE NAME = 'free memory'
  AND POOL = 'shared pool';

SELECT SUBSTR(OWNER, 1, 10) || '.' || SUBSTR(NAME, 1, 35) "ObjectName",
       TYPE,
       SHARABLE_MEM,
       LOADS,
       EXECUTIONS,
       KEPT
  FROM V$DB_OBJECT_CACHE
 WHERE TYPE IN ('TRIGGER', 'PROCEDURE', 'PACKAGE BODY', 'PACKAGE')
   AND EXECUTIONS > 0
 ORDER BY EXECUTIONS DESC, LOADS DESC, SHARABLE_MEM DESC;

-- tags: KSMLRSIZ, SHARED_POOL, SHARED
/*

KSMLRSIZ column of this table shows the amount of contiguous memory being allocated.
Values over around 5K start to be a problem,
values over 10K are a serious problem, and
values over 20K are very serious problems.
Anything less then 5K should not be a problem. Again be careful to save spool the result when you query this table

*/

--Tab=Data Of X$KSMLRU View
SELECT SYSDATE TIEMPO,
       ADDR,
       INDX,
       KSMLRCOM,
       KSMLRSIZ / 1024 ALLOCATION_KBYTES,
       CASE WHEN KSMLRSIZ / 1024 < 5  THEN 'Ok'
            WHEN KSMLRSIZ / 1024 >= 5 AND KSMLRSIZ / 1024 < 10 THEN 'Problem'
            WHEN KSMLRSIZ / 1024 >= 10 AND KSMLRSIZ / 1024 < 20 THEN 'Serious Problem'
            WHEN KSMLRSIZ / 1024 >= 20 THEN 'Very Serious Problem' END DESC_PROBLEM,
       KSMLRNUM,
       KSMLRHON,
       KSMLRSES,
       B.*
  FROM X$KSMLRU  A,
       V$SESSION B
WHERE A.KSMLRSES = B.SADDR (+);

-- Tiempo de eventos
---------------------------------------------------------------------------------------------------------
-- tags: CHECK, EVENT, EVENTO, STAT
 
SELECT FECHA,
       SNAP_ID,
       EVENT_ID,
       EVENT_NAME,
       WAIT_CLASS,
       SEGUNDOS
  FROM (
SELECT TO_CHAR(BEGIN_INTERVAL_TIME,'DD-MON-YY HH24:MI:SS') FECHA,
       SNAP_ID,
       EVENT_ID,
       EVENT_NAME,
       WAIT_CLASS,
       NVL((NEXT_TIME_WAITED_MICRO - TIME_WAITED_MICRO) / 1000000, 0) SEGUNDOS
  FROM (
SELECT A.SNAP_ID,
       B.BEGIN_INTERVAL_TIME,
       EVENT_ID,
       EVENT_NAME,
       WAIT_CLASS,
       TIME_WAITED_MICRO,
       LEAD(TIME_WAITED_MICRO) OVER(PARTITION BY EVENT_ID ORDER BY A.SNAP_ID) NEXT_TIME_WAITED_MICRO
  FROM DBA_HIST_SYSTEM_EVENT A,
       DBA_HIST_SNAPSHOT B
 WHERE A.SNAP_ID = B.SNAP_ID
   AND EVENT_NAME IN ('direct path read',
                      'direct path read temp',
                      'direct path write',
                      'direct path write temp',

                      'free buffer waits',
                      'buffer busy waits',
                      'db file sequential read',
                      'db file scattered read',
                      
                      'SQL*Net message from client',
                      'SQL*Net message from dblink',
                      'SQL*Net message to client',
                      'SQL*Net message to dblink',
                      'SQL*Net more data from client',
                      'SQL*Net more data from dblink',
                      'SQL*Net more data to client',
                      'SQL*Net more data to dblink',
					  
                      'library cache pin'
                     )
       )
       )
 WHERE SEGUNDOS >= 0
 ORDER BY EVENT_ID, SNAP_ID

-------------------------------------------------------------------------------------------------------------

-- Generacion de Sentencias de 'Ampliacion de TBS'

SELECT A.FILE_NAME,
       B.CREATION_TIME,
       --A.FILE_ID,
       A.TABLESPACE_NAME,
       A.BYTES / 1024 / 1024 MBYTES,
       ROW_NUMBER () OVER(PARTITION BY A.TABLESPACE_NAME ORDER BY B.CREATION_TIME ASC) ORDEN,
       COUNT(*) OVER(PARTITION BY A.TABLESPACE_NAME) CANTIDAD,
       'ALTER TABLESPACE '||A.TABLESPACE_NAME||' ADD DATAFILE '''||A.FILE_NAME||''' SIZE '||A.BYTES / 1024 / 1024||'M AUTOEXTEND OFF;' SE
  FROM DBA_DATA_FILES A,
       V$DATAFILE     B
 WHERE A.FILE_NAME = B.NAME (+)
   AND A.TABLESPACE_NAME IN ('TBS_INDEXES_UMTS_HOURLY',
                             'TBS_INDEXES_GSM_HOURLY'
                            )

-- Generar Particiones con Level

SELECT '  PARTITION &ESQUEMA'||'_'||TO_CHAR(ADD_MONTHS(TO_DATE('&FECHA', 'DD.MM.YYYY'), (LEVEL -1)), 'YYYYMM')||
       ' VALUES LESS THAN (TO_DATE('''||TO_CHAR(ADD_MONTHS(TO_DATE('&FECHA', 'DD.MM.YYYY'), LEVEL), 'DD.MM.YYYY')||
       ''', ''DD.MM.YYYY'')),' LINEA,
       ADD_MONTHS(TO_DATE('&FECHA', 'DD.MM.YYYY'), (LEVEL -1)) FECHA
  FROM DUAL CONNECT BY LEVEL <= &CANTIDAD

SELECT '  PARTITION &ESQUEMA'||'_'||TO_CHAR(TO_DATE('&FECHA', 'DD.MM.YYYY') + ((LEVEL -1) * 7), 'YYYYMMDD')||
       ' VALUES LESS THAN (TO_DATE('''||TO_CHAR(TO_DATE('&FECHA', 'DD.MM.YYYY') + ((LEVEL -1) * 7) + 7, 'DD.MM.YYYY')||
       ''', ''DD.MM.YYYY'')),' LINEA,
       TO_DATE('&FECHA', 'DD.MM.YYYY') + ((LEVEL -1) * 7) FECHA
  FROM DUAL CONNECT BY LEVEL <= &CANTIDAD

SELECT '  PARTITION &ESQUEMA'||'_'||TO_CHAR(TO_DATE('&FECHA', 'DD.MM.YYYY') + (LEVEL -1), 'YYYYMMDD')||
       ' VALUES LESS THAN (TO_DATE('''||TO_CHAR(TO_DATE('&FECHA', 'DD.MM.YYYY') + LEVEL, 'DD.MM.YYYY')||
       ''', ''DD.MM.YYYY'')),' LINEA,
       TO_DATE('&FECHA', 'DD.MM.YYYY') + (LEVEL -1) FECHA
  FROM DUAL CONNECT BY LEVEL <= &CANTIDAD

SELECT '  PARTITION &ESQUEMA'||'_'||TO_CHAR(TO_DATE('&FECHA', 'DD.MM.YYYY') + (LEVEL -1) / 24, 'YYYYMMDDHH24')||
       ' VALUES LESS THAN (TO_DATE('''||TO_CHAR(TO_DATE('&FECHA', 'DD.MM.YYYY') + LEVEL / 24, 'DD.MM.YYYY HH24')||
       ''', ''DD.MM.YYYY HH24'')),' LINEA,
       TO_DATE('&FECHA', 'DD.MM.YYYY HH24') + (LEVEL -1) / 24 FECHA
  FROM DUAL CONNECT BY LEVEL <= (24 * 7 * &SEMANAS)

---- With Alter Table

SELECT '  ALTER TABLE &TABLA ADD PARTITION &ESQUEMA'||'_'||TO_CHAR(TO_DATE('&FECHA', 'DD.MM.YYYY') + (LEVEL -1) / 24, 'YYYYMMDDHH24')||
       ' VALUES LESS THAN (TO_DATE('''||TO_CHAR(TO_DATE('&FECHA', 'DD.MM.YYYY') + LEVEL / 24, 'DD.MM.YYYY HH24')||
       ''', ''DD.MM.YYYY HH24''));' LINEA,
       TO_DATE('&FECHA', 'DD.MM.YYYY HH24') + (LEVEL -1) / 24 FECHA
  FROM DUAL CONNECT BY LEVEL <= (24 * 7 * &SEMANAS)

--------------------------------   
-- Management Segments
--------------------------------

-- 87 Semanas = 14616
-- 86 Semanas = 14448
-- 55 Semanas = 9240
-- 52 Semanas = 9072
-- 21 Semanas = 3528
-- 20 Semanas = 3360
-- 19 Semanas = 3192
-- 18 Semanas = 3024
-- 13 Semanas = 2184
-- 12 Semanas = 2016
-- 10 Semanas = 1680
--  9 Semanas = 1512
--  8 Semanas = 1344
--  7 Semanas = 1176
--  6 Semanas = 1008
--  5 Semanas = 840
--  4 Semanas = 672
--  3 Semanas = 504

SELECT TABLESPACE_NAME,
       SEGMENT_NAME,
       COUNT(*) CANTIDAD,
       SUM((BYTES / 1024) / 1024) TAMANO_MBYTES,
       SYSDATE ULTIMA_CONSULTA
  FROM DBA_SEGMENTS
 WHERE TABLESPACE_NAME = 'DATA_CDR'
 GROUP BY TABLESPACE_NAME,
          SEGMENT_NAME
 ORDER BY TABLESPACE_NAME,
          TAMANO_MBYTES DESC;

-- por FS
SELECT SUBSTR(A.FILE_NAME, 1, INSTR(A.FILE_NAME, '/', -1)) DATA,
       A.FILE_NAME,
       A.FILE_ID,
       A.TABLESPACE_NAME,
       A.BYTES,
       B.BLOCK_SIZE,
       COUNT(*) OVER(PARTITION BY SUBSTR(A.FILE_NAME, 1, INSTR(A.FILE_NAME, '/', -1)), BLOCK_SIZE) CANTIDAD,
       COUNT(*) OVER(PARTITION BY SUBSTR(A.FILE_NAME, 1, INSTR(A.FILE_NAME, '/', -1)), BLOCK_SIZE ORDER BY FILE_ID) ORDEN
  FROM DBA_DATA_FILES A,
       DBA_TABLESPACES B
 WHERE FILE_NAME LIKE '/SMART/data_/%'
   AND A.TABLESPACE_NAME = B.TABLESPACE_NAME

-- WO Sub Partitions

SELECT OB.NAME TABLE_NAME,
       TS.NAME TABLESPACE_NAME,
       OB.SUBNAME PARTITION_NAME,
       ROW_NUMBER() OVER (PARTITION BY BO# ORDER BY PART#) PARTITION_POSITION,
       ANALYZETIME LAST_ANALYZED
  FROM SYS.OBJ$     OB,
       SYS.TABPART$ TP,
       SYS.TS$      TS
 WHERE OB.NAME = 'MVENDOR_GSM_BTS_HOUR'
   AND OB.TYPE# = 19
   AND OB.OBJ# = TP.OBJ#
   AND TP.TS# = TS.TS#
   
-- W Sub Partitions

SELECT OB.NAME TABLE_NAME,
       TS.NAME TABLESPACE_NAME,
       OB.SUBNAME PARTITION_NAME,
       ROW_NUMBER() OVER (PARTITION BY BO# ORDER BY PART#) PARTITION_POSITION,
       ANALYZETIME LAST_ANALYZED
  FROM SYS.OBJ$         OB,
       SYS.TABCOMPARTV$ TP,
       SYS.TS$          TS
 WHERE OB.NAME = 'MVENDOR_GSM_BTS_HOUR'
   AND OB.TYPE# = 19
   AND OB.OBJ# = TP.OBJ#
   AND TP.DEFTS# = TS.TS#

-- Only First and Last Partitions for Table.
-- W Sub Partitions.

SELECT TABLE_NAME,
       TABLESPACE_NAME,
       PARTITION_NAME,
       PARTITION_POSITION,
       MAX_PARTITION_POSITION,
       LAST_ANALYZED
  FROM (
SELECT TABLE_NAME,
       TABLESPACE_NAME,
       PARTITION_NAME,
       PARTITION_POSITION,
       MAX(PARTITION_POSITION) OVER(PARTITION BY TABLE_NAME) MAX_PARTITION_POSITION,
       LAST_ANALYZED
  FROM (
SELECT OB.NAME TABLE_NAME,
       TS.NAME TABLESPACE_NAME,
       OB.SUBNAME PARTITION_NAME,
       ROW_NUMBER() OVER (PARTITION BY BO# ORDER BY PART#) PARTITION_POSITION,
       ANALYZETIME LAST_ANALYZED
  FROM SYS.OBJ$         OB,
       SYS.TABCOMPARTV$ TP,
       SYS.TS$          TS
 WHERE OB.NAME LIKE 'GSM_C_NSN%HOU2'
   AND OB.TYPE# = 19
   AND OB.OBJ# = TP.OBJ#
   AND TP.DEFTS# = TS.TS#
       )
       )
 WHERE PARTITION_POSITION IN (1, MAX_PARTITION_POSITION)
 ORDER BY TABLE_NAME, PARTITION_POSITION

-- WO Sub Partitions.

SELECT TABLE_NAME,
       TABLESPACE_NAME,
       PARTITION_NAME,
       PARTITION_POSITION,
       MAX_PARTITION_POSITION,
       LAST_ANALYZED
  FROM (
SELECT TABLE_NAME,
       TABLESPACE_NAME,
       PARTITION_NAME,
       PARTITION_POSITION,
       MAX(PARTITION_POSITION) OVER(PARTITION BY TABLE_NAME) MAX_PARTITION_POSITION,
       LAST_ANALYZED
  FROM (
SELECT OB.NAME TABLE_NAME,
       TS.NAME TABLESPACE_NAME,
       OB.SUBNAME PARTITION_NAME,
       ROW_NUMBER() OVER (PARTITION BY BO# ORDER BY PART#) PARTITION_POSITION,
       ANALYZETIME LAST_ANALYZED
  FROM SYS.OBJ$     OB,
       SYS.TABPART$ TP,
       SYS.TS$      TS
 WHERE OB.NAME LIKE 'GSM_C_NSN%HOUR'
   AND OB.TYPE# = 19
   AND OB.OBJ# = TP.OBJ#
   AND TP.TS# = TS.TS#
       )
       )
 WHERE PARTITION_POSITION IN (1, MAX_PARTITION_POSITION)
 ORDER BY TABLE_NAME, PARTITION_POSITION

 -- SubPartition
 SELECT TABLE_OWNER,
       TABLE_NAME,
       PARTITION_NAME,
       SUBPARTITION_NAME,
       'ALTER TABLE '||TABLE_OWNER||'.'||TABLE_NAME||' MOVE SUBPARTITION '||SUBPARTITION_NAME||' TABLESPACE TBS_UMTS_C_NSN_HOURLY;' PE,
       SUBPARTITION_POSITION
  FROM DBA_TAB_SUBPARTITIONS
 WHERE TABLE_NAME = 'UMTS_C_NSN_L3IUB_WCEL_BHC'
 
-- Create Sentence. Alter Table Drop.

-- Usando Fecha_Desde y Fecha_Hasta (Periodo)

SELECT TABLE_NAME,
       TABLESPACE_NAME,
       PARTITION_NAME,
       PARTITION_POSITION,
       B.FECHA,
       'ALTER TABLE '||TABLE_NAME||' DROP PARTITION '||PARTITION_NAME||';' SE
  FROM (
SELECT TABLE_NAME,
       TABLESPACE_NAME,
       PARTITION_NAME,
       PARTITION_POSITION,
       TO_DATE(SUBSTR(PARTITION_NAME, INSTR(PARTITION_NAME, '_', -1) + 1, LENGTH(PARTITION_NAME)), 'YYYYMMDD') FECHA
  FROM (
SELECT OB.NAME TABLE_NAME,
       TS.NAME TABLESPACE_NAME,
       OB.SUBNAME PARTITION_NAME,
       ROW_NUMBER() OVER (PARTITION BY BO# ORDER BY PART#) PARTITION_POSITION
  FROM SYS.OBJ$         OB,
       SYS.TABCOMPARTV$ TP,
       SYS.TS$          TS
 WHERE OB.NAME = 'UMTS_NSN_SERVICE_WCEL_DA2'
   AND OB.TYPE# = 19
   AND OB.OBJ# = TP.OBJ#
   AND TP.DEFTS# = TS.TS#
       )
 ORDER BY PARTITION_POSITION ASC
       ) A,
       (
SELECT FECHA
  FROM CALIDAD_STATUS_REFERENCES
 WHERE FECHA BETWEEN TO_DATE('&1', 'DD.MM.YYYY') AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400
   AND HORA = '00'
       ) B
 WHERE A.FECHA (+) = B.FECHA

-- Generic Form

SELECT TABLE_NAME,
       TABLESPACE_NAME,
       PARTITION_NAME,
       DROP_SENTENCE,
       MOVE_SENTENCE
  FROM (
SELECT TABLE_NAME,
       TABLESPACE_NAME,
       PARTITION_NAME,
       PARTITION_POSITION,
       (MAX(PARTITION_POSITION) OVER(PARTITION BY TABLE_NAME) - 100) MAX_PARTITION_POSITION,
       'ALTER TABLE '||TABLE_OWNER||'.'||TABLE_NAME||' MOVE PARTITION '||PARTITION_NAME||' TABLESPACE DATA_DTO;' MOVE_SENTENCE,
       'ALTER TABLE '||TABLE_OWNER||'.'||TABLE_NAME||' DROP PARTITION '||PARTITION_NAME||';' DROP_SENTENCE
  FROM DBA_TAB_PARTITIONS
 WHERE TABLE_NAME IN ('TEKELEC_SMS_XDR',
                      'TEKELEC_BICC_XDR',
                      'TEKELEC_ISUP_XDR',
                      'TEKELEC_SMS333_FAIL',
                      'TEKELEC_SMS_NUM',
                      'TEKELEC_IUPS_MM',
                      'TEKELEC_SMS_PREMIUM_LOGICA',
                      'TEKELEC_SMS_TRAFFIC',
                      'TEKELEC_IN_TRAFFIC',
                      'TEKELEC_SMS_PREMIUM_COMVERSE',
                      'TEKELEC_IN_MARCACION')
 ORDER BY TABLE_NAME,
          PARTITION_POSITION
       )
 WHERE PARTITION_POSITION <= MAX_PARTITION_POSITION

-- Drop Partition

SELECT TABLE_NAME,
       PA,
       PARTITION_NAME,
       PARTITION_POSITION
  FROM (
SELECT TABLE_NAME,
       PARTITION_NAME,
       PARTITION_POSITION,
       'ALTER TABLE '||TABLE_OWNER||'.'||TABLE_NAME||' DROP PARTITION '||PARTITION_NAME||';' PA
  FROM DBA_TAB_PARTITIONS
 WHERE TABLE_NAME IN ('LOGICA_OMG_RAW')
 ORDER BY TABLE_NAME,
          PARTITION_POSITION
       )
 WHERE PARTITION_POSITION <= (24 * 7 * &SEMANAS)

--  Create Sentence Alter Table Drop Partitions.
-- WO Sub Partitions.

SELECT TABLE_NAME,
       PARTITION_NAME,
       TABLESPACE_NAME,
       PA,
       PARTITION_POSITION,
       LAST_ANALYZED
  FROM (
SELECT OB.NAME TABLE_NAME,
       TS.NAME TABLESPACE_NAME,
       OB.SUBNAME PARTITION_NAME,
       SUBSTR(OB.SUBNAME, 1, INSTR(OB.SUBNAME, '_', -1) - 1) PARTITION_SCHEME,
       --'LTE_NSN_'||SUBSTR(OB.SUBNAME, INSTR(OB.SUBNAME, '_', -1) + 1, LENGTH(OB.SUBNAME)) PARTITION_NAM2,
       ROW_NUMBER() OVER (PARTITION BY BO# ORDER BY PART#) PARTITION_POSITION,
       'ALTER TABLE '||DECODE(OB.OWNER#, 28, 'SCOTT', 207, 'HARRIAGUE', OB.OWNER#)||'.'||OB.NAME||' DROP PARTITION '||OB.SUBNAME||';' PA,
       'ALTER TABLE '||DECODE(OB.OWNER#, 28, 'SCOTT', 207, 'HARRIAGUE', OB.OWNER#)||'.'||OB.NAME||' MOVE PARTITION '||OB.SUBNAME||' TABLESPACE &TBS_NAME;' PE,
       'ALTER TABLE '||DECODE(OB.OWNER#, 28, 'SCOTT', 207, 'HARRIAGUE', OB.OWNER#)||'.'||OB.NAME||' RENAME PARTITION '||OB.SUBNAME||' TO LTE_NSN_'||SUBSTR(OB.SUBNAME, INSTR(OB.SUBNAME, '_', -1) + 1, LENGTH(OB.SUBNAME))||';' PO,
       ANALYZETIME LAST_ANALYZED
  FROM SYS.OBJ$     OB,
       SYS.TABPART$ TP,
       SYS.TS$      TS
 WHERE OB.NAME IN ('TELMEX_GPON_OLT_MIN_RAW')
   AND OB.TYPE# = 19
   AND OB.OBJ# = TP.OBJ#
   AND TP.TS# = TS.TS#
       )
 WHERE PARTITION_POSITION <= (24 * 7 * &SEMANAS)

-- W Sub Partitions.

SELECT TABLE_NAME,
       PARTITION_NAME,
       TABLESPACE_NAME,
       PA,
       PARTITION_POSITION,
       LAST_ANALYZED
  FROM (
SELECT OB.NAME TABLE_NAME,
       TS.NAME TABLESPACE_NAME,
       OB.SUBNAME PARTITION_NAME,
       SUBSTR(OB.SUBNAME, 1, INSTR(OB.SUBNAME, '_', -1) - 1) PARTITION_SCHEME,
       --'LTE_NSN_'||SUBSTR(OB.SUBNAME, INSTR(OB.SUBNAME, '_', -1) + 1, LENGTH(OB.SUBNAME)) PARTITION_NAM2,
       ROW_NUMBER() OVER (PARTITION BY BO# ORDER BY PART#) PARTITION_POSITION,
       'ALTER TABLE '||DECODE(OB.OWNER#, 28, 'SCOTT', 207, 'HARRIAGUE', OB.OWNER#)||'.'||OB.NAME||' DROP PARTITION '||OB.SUBNAME||';' PA,
       'ALTER TABLE '||DECODE(OB.OWNER#, 28, 'SCOTT', 207, 'HARRIAGUE', OB.OWNER#)||'.'||OB.NAME||' MOVE PARTITION '||OB.SUBNAME||' TABLESPACE &TBS_NAME;' PE,
       'ALTER TABLE '||DECODE(OB.OWNER#, 28, 'SCOTT', 207, 'HARRIAGUE', OB.OWNER#)||'.'||OB.NAME||' RENAME PARTITION '||OB.SUBNAME||' TO LTE_NSN_'||SUBSTR(OB.SUBNAME, INSTR(OB.SUBNAME, '_', -1) + 1, LENGTH(OB.SUBNAME))||';' PO,
       ANALYZETIME LAST_ANALYZED
  FROM SYS.OBJ$         OB,
       SYS.TABCOMPARTV$ TP,
       SYS.TS$          TS
 WHERE OB.NAME = 'GSM_C_NSN_COD_SCH_HOU2'
   AND OB.TYPE# = 19
   AND OB.OBJ# = TP.OBJ#
   AND TP.DEFTS# = TS.TS#
       )
 WHERE PARTITION_POSITION <= (24 * 7 * &SEMANAS)

-- Drop Partition (pero desde una fecha para atras o para adelante)

SELECT TABLE_NAME,
       PARTITION_NAME,
       TABLESPACE_NAME,
       PARTITION_POSITION,
       MAX_PARTITION_POSITION,
       PA
  FROM (
SELECT TABLE_NAME,
       PARTITION_NAME,
       TABLESPACE_NAME,
       PARTITION_POSITION,
       MAX(MAX_PARTITION_POSITION) OVER(PARTITION BY TABLE_NAME) MAX_PARTITION_POSITION,
       PA
  FROM (
SELECT TABLE_NAME,
       PARTITION_NAME,
       TABLESPACE_NAME,
       PARTITION_POSITION,
       'ALTER TABLE '||TABLE_OWNER||'.'||TABLE_NAME||' DROP PARTITION '||PARTITION_NAME||';' PA,
       DECODE(SIGN(INSTR(PARTITION_NAME, '20110101')), 1, PARTITION_POSITION, NULL) MAX_PARTITION_POSITION
  FROM DBA_TAB_PARTITIONS
 WHERE TABLE_NAME IN ('GSM_C_NSN_RXQUAL_TRX_DAY',
                      'GSM_C_NSN_RXQUAL_TRX_BH'/*,
                      'GSM_C_NSN_COD_SCH_DAY',
                      'GSM_C_NSN_COD_SCH_BH'*/
                     )
 ORDER BY TABLE_NAME,
          PARTITION_POSITION
       )
       )
 WHERE PARTITION_POSITION < MAX_PARTITION_POSITION

-- Create Sentence "Alter Table Add" for many tables at once.
-- Hourly Level

WITH TABLA_FECHAS AS
(
 SELECT TO_CHAR(TO_DATE('&FECHA', 'DD.MM.YYYY') + (LEVEL -1) / 24, 'YYYYMMDDHH24') F_MASCARA,
        TO_CHAR(TO_DATE('&FECHA', 'DD.MM.YYYY') + LEVEL / 24, 'DD.MM.YYYY HH24')   F_VALUES,
        TO_DATE('&FECHA', 'DD.MM.YYYY HH24') + (LEVEL -1) / 24                     FECHA
   FROM DUAL
CONNECT BY LEVEL <= (24 * 7 * &SEMANAS)
)
SELECT NOMBRE_TABLA,
       PARTICION_ESQUEMA,
       NOMBRE_TABLESPACE,
       F_MASCARA,
       'ALTER TABLE '||NOMBRE_TABLA||' ADD PARTITION '||PARTICION_ESQUEMA||F_MASCARA||
       ' VALUES LESS THAN (TO_DATE('''||F_VALUES||
       ''', ''DD.MM.YYYY HH24'')) TABLESPACE '||NOMBRE_TABLESPACE||' PCTFREE 10 PCTUSED 80;' LINEA
  FROM CALIDAD_PARAMETROS_TABLAS,
       TABLA_FECHAS
 WHERE NOMBRE_TABLA IN (--'GSM_C_NSN_SERVICE_HOU2'       ,
                        --'GSM_C_NSN_RXQUAL_HOU2'        ,
                        'NOKIA_MMSC_MMS_HOUR'          ,
                        --'GSM_C_NSN_TRAFFIC_HOU2'       ,
                        'MVENDOR_GSM_BTS_HOUR'         ,
                        'CLARO_RECLAMOS_DATA_HOUR_NEW' ,
                        'TECNOTREE_CM_HOUR'            ,
                        'SVA_RING_BACK_TONES_HOUR'     ,
                        'SVA_ROAMWARE_HOUR'            )--,
                        --'GSM_C_NSN_RESAVAIL_HOU2'      )

-- Daily Level.

WITH TABLA_FECHAS AS
(
 SELECT TO_CHAR(FECHA, 'YYYYMMDD')   F_MASCARA,
        TO_CHAR(FECHA + 1, 'DD.MM.YYYY') F_VALUES,
        FECHA
   FROM CALIDAD_STATUS_REFERENCES
  WHERE FECHA BETWEEN TO_DATE('&FECHA', 'DD.MM.YYYY')
                  AND TO_DATE('&FECH2', 'DD.MM.YYYY') + 86399/86400
    AND HORA = '00'
)
SELECT NOMBRE_TABLA,
       PARTICION_ESQUEMA,
       NOMBRE_TABLESPACE,
       F_MASCARA,
       'ALTER TABLE '||NOMBRE_TABLA||' ADD PARTITION '||PARTICION_ESQUEMA||F_MASCARA||
       ' VALUES LESS THAN (TO_DATE('''||F_VALUES||
       ''', ''DD.MM.YYYY'')) TABLESPACE '||NOMBRE_TABLESPACE||';' LINEA
  FROM CALIDAD_PARAMETROS_TABLAS,
       TABLA_FECHAS
 WHERE NOMBRE_TABLA IN ('UMTS_NSN_SERVICE_ALM_BHC',
                        'UMTS_NSN_SERVICE_ALM_BHP',
                        'UMTS_NSN_SERVICE_ALM_DAY'
                       )

-- Monthly Level

WITH TABLA_FECHAS AS
(
 SELECT TO_CHAR(FECHA, 'YYYYMM')                    F_MASCARA,
        TO_CHAR(ADD_MONTHS(FECHA, 1), 'DD.MM.YYYY') F_VALUES,
        FECHA
   FROM CALIDAD_STATUS_REFERENCES
  WHERE FECHA BETWEEN TO_DATE('&FECHA', 'DD.MM.YYYY')
                  AND TO_DATE('&FECH2', 'DD.MM.YYYY') + 86399/86400
    AND HORA = '00'
    AND TO_CHAR(DIA, 'DD') = '01'
)
SELECT NOMBRE_TABLA,
       PARTICION_ESQUEMA,
       NOMBRE_TABLESPACE,
       F_MASCARA,
       'ALTER TABLE '||NOMBRE_TABLA||' ADD PARTITION '||PARTICION_ESQUEMA||
       TO_CHAR(FECHA, PARTICION_ESQUEMA_MSC_FECHA)||
       ' VALUES LESS THAN (TO_DATE('''||F_VALUES||
       ''', ''DD.MM.YYYY'')) TABLESPACE '||NOMBRE_TABLESPACE||';' LINEA
  FROM CALIDAD_PARAMETROS_TABLAS,
       TABLA_FECHAS
 WHERE NOMBRE_TABLA IN ('UMTSC_NSN_HO_ALM_BH',
                        'UMTSC_NSN_HO_ALM_DAY',
                        'UMTSC_NSN_SERVICE_RNC_BHC',
                        'UMTSC_NSN_SERVICE_RNC_BHP',
                        'UMTSC_NSN_SERVICE_RNC_DAY'
                       )

-- Weekly Level

WITH TABLA_FECHAS AS
(
 SELECT TO_CHAR(FECHA, 'YYYYMMDD')       F_MASCARA,
        TO_CHAR(FECHA + 7, 'DD.MM.YYYY') F_VALUES,
        FECHA
   FROM CALIDAD_STATUS_REFERENCES
  WHERE FECHA BETWEEN TO_DATE('&FECHA', 'DD.MM.YYYY')
                  AND TO_DATE('&FECH2', 'DD.MM.YYYY') + 86399/86400
    AND HORA = '00'
    AND DIA_DESC = 'DOMINGO'
)
SELECT NOMBRE_TABLA,
       PARTICION_ESQUEMA,
       NOMBRE_TABLESPACE,
       F_MASCARA,
       'ALTER TABLE '||NOMBRE_TABLA||' ADD PARTITION '||PARTICION_ESQUEMA||F_MASCARA||
       ' VALUES LESS THAN (TO_DATE('''||F_VALUES||
       ''', ''DD.MM.YYYY'')) TABLESPACE '||NOMBRE_TABLESPACE||';' LINEA
  FROM CALIDAD_PARAMETROS_TABLAS,
       TABLA_FECHAS
 WHERE NOMBRE_TABLA IN ('UMTSC_NSN_MACD_ALM_DAYW',
                        'UMTSC_NSN_MACD_ALM_ISABHW',
                        'UMTSC_NSN_MACD_MKT_DAYW',
                        'UMTSC_NSN_MACD_MKT_ISABHW',
                        'UMTSC_NSN_MACD_PAIS_DAYW',
                        'UMTSC_NSN_MACD_PAIS_ISABHW',
                        'UMTSC_NSN_MACD_RNC_DAYW',
                        'UMTSC_NSN_MACD_RNC_ISABHW'
                       )

-- Insert Into Calidad_Parametros_Tablas

INSERT INTO CALIDAD_PARAMETROS_TABLAS
SELECT DECODE(NOMBRE_TABLA, 'UMTS_NSN_SERVICE_WCEL_DAY', 'UMTS_NSN_SERVICE_WCEL_DA2', NOMBRE_TABLA) NOMBRE_TABLA,
       NOMBRE_TABLESPACE,
       PARTICION_ESQUEMA,
       PARTICION_ESQUEMA_MSC_FECHA,
       PARTICION_FORMATO_MSC_FECHA,
       PARTICION_EXTENT_INITIAL,
       PARTICION_EXTENT_NEXT,
       PARTICION_PERMISO_CREATE,
       PARTICION_PERMISO_DROP,
       PARTICION_TIPO_TABLA,
       REPORTE_CAMPO_CANTIDAD_FORMULA,
       REPORTE_CAMPO_MEDICION_FORMULA,
       REPORTE_CAMPO_MEDICION_UNIDAD,
       REPORTE_CAMPO_FECHA,
       REPORTE_CLAUSULA_ORDER_BY,
       REPORTE_CLAUSULA_WHERE_BY,
       REPORTE_TIPO_STAT,
       REPORTE_PLATAFORMA,
       REPORTE_PLATAFORMA_DESCRIPCION,
       REPORTE_STATUS,
       REPORTE_TIPO_TABLA,
       NOMBRE_TABLA_OBJETO,
       DESCRIPCION_TABLA,
       OBSERVACIONES,
       N_PERIODO,
       N_ELEMENTO,
       DECODE(ID_TABLA, 'UMTSNSNSERVICEWCELDAY', 'UMTSNSNSERVICEWCELDA2', ID_TABLA) ID_TABLA
  FROM (
SELECT REPLACE(NOMBRE_TABLA, 'HOUR', 'DAY') NOMBRE_TABLA,
       CASE WHEN INSTR(NOMBRE_TABLA, 'UMTS_D_NSN') >= 1 THEN 'TBS_UMTS_C_NSN_DAILY'
                                                        ELSE 'TBS_UMTS_NSN_DAILY' END NOMBRE_TABLESPACE,
       CASE WHEN INSTR(NOMBRE_TABLA, 'UMTS_D_NSN') >= 1 THEN 'UMTS_D_NSN_'
                                                        ELSE 'UMTS_NSN_'          END PARTICION_ESQUEMA,
       'YYYYMMDD'   PARTICION_ESQUEMA_MSC_FECHA,
       'DD.MM.YYYY' PARTICION_FORMATO_MSC_FECHA,
       NULL         PARTICION_EXTENT_INITIAL,
       NULL         PARTICION_EXTENT_NEXT,
       'ENABLED'    PARTICION_PERMISO_CREATE,
       'DISABLED'   PARTICION_PERMISO_DROP,
       'Daily'      PARTICION_TIPO_TABLA,
       NULL REPORTE_CAMPO_CANTIDAD_FORMULA,
       NULL REPORTE_CAMPO_MEDICION_FORMULA,
       NULL REPORTE_CAMPO_MEDICION_UNIDAD,
       'PERIOD_START_TIME' REPORTE_CAMPO_FECHA,
       1 REPORTE_CLAUSULA_ORDER_BY,
       NULL REPORTE_CLAUSULA_WHERE_BY,
       'SERVICE'        REPORTE_TIPO_STAT,
       'UMTS'           REPORTE_PLATAFORMA,
       'Umts Statistic' REPORTE_PLATAFORMA_DESCRIPCION,
       'ENABLED'        REPORTE_STATUS,
       'Daily'          REPORTE_TIPO_TABLA,
       NULL             NOMBRE_TABLA_OBJETO,
       CASE WHEN NOMBRE_TABLA = 'UMTS_NSN_SERVICE_WCEL_HOUR'   THEN 'Umts Master Nsn Service Wcell Day'
            WHEN NOMBRE_TABLA = 'UMTS_NSN_SERVICE_NE_HOUR'     THEN 'Umts Master Nsn Service Network Element Day'
            WHEN NOMBRE_TABLA = 'UMTS_D_NSN_SERVICE_WCEL_HOUR' THEN 'Umts Detail Nsn Service Wcell Day'
            WHEN NOMBRE_TABLA = 'UMTS_D_NSN_SERVICE_NE_HOUR'   THEN 'Umts Detail Nsn Service Network Element Day'
            END DESCRIPCION_TABLA,
       'ENABLED'        OBSERVACIONES,
       NULL             N_PERIODO,
       NULL             N_ELEMENTO,
       REPLACE(REPLACE(NOMBRE_TABLA, 'HOUR', 'DAY'), '_', '') ID_TABLA
  FROM CALIDAD_PARAMETROS_TABLAS
 WHERE NOMBRE_TABLA IN ('UMTS_NSN_SERVICE_WCEL_HOUR',
                        'UMTS_NSN_SERVICE_NE_HOUR',
                        'UMTS_D_NSN_SERVICE_NE_HOUR',
                        'UMTS_D_NSN_SERVICE_WCEL_HOUR')
       )


---------------------------------------------------------------------------

-- Rename de tablas y reconstruccion de sinonimos

SELECT 'RENAME '||TABLE_NAME||' TO '||REPLACE(REPLACE(TABLE_NAME, 'GGSN', 'CORE'), 'STAT', 'GGSN')||';' SE,
       'DROP PUBLIC SYNONYM '||TABLE_NAME||';' SS,
       'CREATE OR REPLACE PUBLIC SYNONYM '||REPLACE(REPLACE(TABLE_NAME, 'GGSN', 'CORE'), 'STAT', 'GGSN')||' FOR SCOTT.'||
                                            REPLACE(REPLACE(TABLE_NAME, 'GGSN', 'CORE'), 'STAT', 'GGSN')||';' SO
  FROM DBA_TABLES WHERE TABLE_NAME LIKE 'CORE_CROSSBEAM%'

---------------------------------------------------------------------------

select count (*) "Tot" , event
from v$session_wait 
where event not like 'pmon%' 
and event not like 'smon%'
group by event 
order by 1 desc

select decode(v.block, 1, 'HOLDER','  WAITER') locker,
       v.sid,
       v.serial#,
       v.type,
       v.lmode,
       v.id1,
       v.id2,
       v.osuser,
       v.sql_hash_value,
       vs.username,
       vs.status,
       vs.last_call_et last_call
from (
     select view_lock.*,
            first_value(block)
              over (partition by id1,id2
                    order by block desc
                    rows between unbounded preceding
                             and unbounded following) first_block,
            last_value(block)
              over (partition by id1,id2
                    order by block desc
                    rows between unbounded preceding
                             and unbounded following) last_block
     from (
          select /*+ ORDERED */
                 a.block,
                 a.sid,
                 b.serial#,
                 a.type,
                 decode(a.lmode,1,'Null',2,'Sub-share',
                                3,'Sub-exclusive',4,'Share',
                                5,'Share/sub-exclusive',
                                6,'Exclusive','Other') lmode,
                 a.id1,
                 a.id2,
                 b.osuser,
                 b.sql_hash_value
          from v$lock a, v$session b
          where a.block = 1
            and a.sid = b.sid
          union all
          select /*+ ORDERED */
                 0,
                 a.sid,
                 b.serial#,
                 chr(bitand(a.p1,-16777216)/16777215) ||
                 chr(bitand(a.p1, 16711680)/65535),
                 decode(bitand(a.p1,65535),1,'Null',2,'Sub-share',
                                           3,'Sub-exclusive',4,'Share',
                                           5,'Share/sub-exclusive',
                                           6,'Exclusive','Other'),
                 a.p2,
                 a.p3,
                                  b.osuser,
                 b.sql_hash_value
          from v$session_wait a, v$session b
          where a.event like 'enq%'
            and a.sid = b.sid
          )  view_lock
     ) v,
     v$session vs
where v.first_block = 1
  and v.last_block  = 0
  and vs.sid        = v.sid
order by id1, locker;

---------------------------------------------------------------------------------------

SELECT O.PROCESS,
       O.SESSION_ID,
       O.LOCKED_MODE,
       A.OWNER,
       A.OBJECT_NAME,
       A.SUBOBJECT_NAME,
       A.LAST_DDL_TIME,
       A.STATUS,
       E.SERIAL#,
       E.OSUSER,
       E.MACHINE,
       E.LOGON_TIME,
       E.LAST_CALL_ET,
       E.EVENT#,
       E.EVENT
  FROM V$LOCKED_OBJECT O,
       DBA_OBJECTS     A,
       V$SESSION       E
 WHERE O.OBJECT_ID = A.OBJECT_ID
   AND O.SESSION_ID = E.SID

-- Clustering Factor
-- Relacion 1: Si el CF esta mas proximo a la cantidad de Filas, sera 1.
-- Relacion 2: Si el CF esta mas proximo a la cantidad de bloques, sera 1. Si crece empeora.

SELECT TABLE_NAME,
       INDEX_NAME,
       BLEVEL,
       LEAF_BLOCKS,
       NUM_ROWS,
       CLUSTERING_FACTOR,
       DECODE(NUM_ROWS, 0, 0, ROUND(CLUSTERING_FACTOR / NUM_ROWS, 4))    REL1,
       DECODE(LEAF_BLOCKS, 0, 0, ROUND(CLUSTERING_FACTOR / LEAF_BLOCKS, 4)) REL2,
       LAST_ANALYZED
  FROM DBA_INDEXES
 WHERE OWNER = 'SCOTT'
  ORDER BY BLEVEL, REL2;

-- Reconstruir Indices y Restricciones

SELECT TABLE_NAME,
       INDEX_NAME,
       COLUMN_NAME,
       COL_1,
       COL_2,
       COL_3,
       COL_4,
       COL_5,
       COL_6,
       COL_7,
       COL_8,
       REPLACE(REPLACE(SE, ' ,', ''), ', )', ')') SE,
       REPLACE(REPLACE(S2, ' ,', ''), ', )', ')') S2,
       REPLACE(REPLACE(S3, ' ,', ''), ', )', ')') S3,
       REPLACE(REPLACE(S4, ' ,', ''), ', )', ')') S4
  FROM (
SELECT TABLE_NAME,
       INDEX_NAME,
       COLUMN_NAME,
       COL_1, 
       COL_2,
       COL_3,
       COL_4,
       COL_5,
       COL_6,
       COL_7,
       COL_8,
       'ALTER TABLE '||TABLE_NAME||' DROP CONSTRAINT '||INDEX_NAME||' CASCADE DROP INDEX;' SE,
       'ALTER TABLE '||TABLE_NAME||' ADD CONSTRAINT '||INDEX_NAME||' PRIMARY KEY ('||COLUMN_NAME||', '||COL_1||', '||COL_2||', '||COL_3||', '||COL_4||', '||COL_5||', '||COL_6||', '||COL_7||', '||COL_8||') USING INDEX LOCAL TABLESPACE TBS_INDEXES;' S2,
       'DROP INDEX '||INDEX_NAME||';' S3,
       'CREATE INDEX '||INDEX_NAME||' ON '||TABLE_NAME||' ('||COLUMN_NAME||', '||COL_1||', '||COL_2||', '||COL_3||', '||COL_4||', '||COL_5||', '||COL_6||', '||COL_7||', '||COL_8||') TABLESPACE TBS_INDEXES LOCAL;' S4
  FROM (
SELECT TABLE_NAME,
       INDEX_NAME,
       COLUMN_NAME,
       COLUMN_POSITION,
       LAG(COLUMN_NAME, 1) OVER(PARTITION BY INDEX_NAME ORDER BY COLUMN_POSITION DESC) COL_1,
       LAG(COLUMN_NAME, 2) OVER(PARTITION BY INDEX_NAME ORDER BY COLUMN_POSITION DESC) COL_2,
       LAG(COLUMN_NAME, 3) OVER(PARTITION BY INDEX_NAME ORDER BY COLUMN_POSITION DESC) COL_3,
       LAG(COLUMN_NAME, 4) OVER(PARTITION BY INDEX_NAME ORDER BY COLUMN_POSITION DESC) COL_4,
       LAG(COLUMN_NAME, 5) OVER(PARTITION BY INDEX_NAME ORDER BY COLUMN_POSITION DESC) COL_5,
       LAG(COLUMN_NAME, 6) OVER(PARTITION BY INDEX_NAME ORDER BY COLUMN_POSITION DESC) COL_6,
       LAG(COLUMN_NAME, 7) OVER(PARTITION BY INDEX_NAME ORDER BY COLUMN_POSITION DESC) COL_7,
       LAG(COLUMN_NAME, 8) OVER(PARTITION BY INDEX_NAME ORDER BY COLUMN_POSITION DESC) COL_8
  FROM DBA_IND_COLUMNS
 WHERE INDEX_NAME IN (
SELECT INDEX_NAME
  FROM DBA_INDEXES
 WHERE STATUS = 'UNUSABLE'
       )
 ORDER BY INDEX_NAME, COLUMN_POSITION
       )
 WHERE COLUMN_POSITION = 1
       );

-- Check Status Indexes
SELECT * FROM DBA_INDEXES WHERE OWNER NOT IN ('SYS', 'SYSTEM') AND STATUS NOT IN ('VALID', 'N/A')

-- Check Status Particiones de Indexes
SELECT * FROM DBA_IND_PARTITIONS WHERE INDEX_OWNER NOT IN ('SYS', 'SYSTEM') AND STATUS NOT IN ('VALID', 'N/A', 'USABLE')

-- Check Status SubParticiones de Indexes
SELECT * FROM DBA_IND_SUBPARTITIONS WHERE INDEX_OWNER NOT IN ('SYS', 'SYSTEM') AND STATUS NOT IN ('VALID', 'N/A', 'USABLE')
	   
--- MIBS

SELECT *
  FROM (
SELECT FECHA,
       MIB_COLLECT,
       --MIB_SECTION,
       --PATH_OID,
       TYPE_VALOR,
       VALOR,
       FLEXI_IP,
       TO_NUMBER(FLEXI_PORT) FLEXI_PORT,
       SUBSTR(PATH_OID, 1, (INSTR(PATH_OID, '.', -1) -1)) PATH,
       TO_NUMBER(SUBSTR(PATH_OID, INSTR(PATH_OID, '.', -1) +1, LENGTH(PATH_OID))) INSTANCIA
  FROM NOKIA_GGSN_APN_ONE_MIN_RAW
 --WHERE FECHA = TO_DATE('&1','DD.MM.YYYY HH24:MI') --7019
       ) A,
       (
SELECT --APN_ID,
       APN_INSTANCE,
       APN_NAME,
       APN_GGSN_NAME,
       --APN_PAIS,
       --APN_TYPE,
       APN_IP,
       APN_PORT
  FROM NOKIA_GGSN_APN_OBJECTS
       ) B,
       (
SELECT MIB_PATH_OID,
       MIB_SECTION,
       MIB_NAME/*,
       MIB_COLLECT,
       MIB_OPERATION,
       MIB_DESCRIPTION*/
  FROM NOKIA_GGSN_APN_MIBS
       ) C
 WHERE A.FLEXI_IP = B.APN_IP (+)
   AND A.FLEXI_PORT = B.APN_PORT (+)
   AND A.INSTANCIA = B.APN_INSTANCE (+)
   AND A.PATH = C.MIB_PATH_OID-- (+)
   AND B.APN_NAME = 'internet.ctimovil.com.ar'
   AND A.FLEXI_IP = '10.104.33.66'
   AND A.PATH = '94.1.24.1.2.1.1.4'

-- Para comprobar Minutos 3G por Dia en el OSS3G

SELECT A.PERIOD_START_TIME,
       B.PAIS,
       ROUND((SUM(AVG_RAB_HLD_TM_CS_VOICE) / (100 * 60)), 2) CS_MOUS_VOZ,
       ROUND((SUM(RAB_HOLD_TIME_CS_CONV_64) * (64/12.2)) / (100 * 60), 2) CS_MOUS_VIDEOCALL
  FROM NOKRWW_PS_SERVLEV_WCEL_DAY@OSS3G A,
       UMTS_SP_OBJECTS                  B
 WHERE A.WCEL_ID = B.WCELL_ID
   AND A.PERIOD_START_TIME BETWEEN TO_DATE('&FECHA_DESDE', 'DD.MM.YYYY') 
                               AND TO_DATE('&FECHA_HASTA', 'DD.MM.YYYY') + 86399/86400
GROUP BY A.PERIOD_START_TIME,
         B.PAIS;


---------------------------------------- Define ventanas mas pequeñas para la ventana de procesamiento


SELECT TO_CHAR(DECODE(ORDEN, MIN_LEVEL, FECHA_DESDE, FECHA_DESDE + (ORDEN - 1)), 'DD.MM.YYYY HH24') FECHA_DESDE,
       TO_CHAR(DECODE(ORDEN, MAX_LEVEL, FECHA_HASTA, (FECHA_DESDE + (ORDEN)) - 1/24), 'DD.MM.YYYY HH24') FECHA_HASTA
  FROM (
SELECT TO_DATE('&1 &2', 'DD.MM.YYYY HH24') FECHA_DESDE,
       TO_DATE('&3 &4', 'DD.MM.YYYY HH24') FECHA_HASTA,
       MAX(LEVEL) OVER() MAX_LEVEL,
       MIN(LEVEL) OVER() MIN_LEVEL,
       LEVEL ORDEN
  FROM DUAL CONNECT BY LEVEL <= 
       (
SELECT DECODE(DIAS, 0, 1, DIAS) + CASE WHEN HORAS < 10 THEN 0 ELSE 1 END VUELTAS
  FROM (
SELECT FLOOR(FECHA_HASTA - FECHA_DESDE) DIAS,
       ROUND(MOD(FECHA_HASTA - FECHA_DESDE, FLOOR(FECHA_HASTA - FECHA_DESDE)) / (1/24), 0) HORAS
  FROM (SELECT TO_DATE('&1 &2', 'DD.MM.YYYY HH24') FECHA_DESDE,
               TO_DATE('&3 &4', 'DD.MM.YYYY HH24') FECHA_HASTA
          FROM DUAL
       )
       )
       )
       )

-------------------------------------------------------------------------------------------------------------------------
-- Reciclar Papelera

PURGE RECYCLEBIN;
-------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------
--- Actualizar Calidad_Status_references
-------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------

-- Para Insert mas datos

INSERT INTO CALIDAD_STATUS_REFERENCES (FECHA) 
SELECT TO_DATE('&FECHA', 'DD.MM.YYYY') + ((LEVEL -1) / 24) FECHA
  FROM DUAL CONNECT BY LEVEL <= (24 * &CANTIDAD)

-- Para Actualizar: Hora, Dia, Mes, Dia_Desc

UPDATE CALIDAD_STATUS_REFERENCES
   SET HORA = TO_CHAR(FECHA, 'HH24'),
       DIA = TRUNC(FECHA),
       DIA_DESC = DECODE(TRIM(TO_CHAR(DIA, 'DAY')), 'MONDAY'     , 'LUNES'
                                                  , 'TUESDAY'    , 'MARTES'
                                                  , 'WEDNESDAY'  , 'MIERCOLES'
                                                  , 'THURSDAY'   , 'JUEVES'
                                                  , 'FRIDAY'     , 'VIERNES'
                                                  , 'SATURDAY'   , 'SABADO'
                                                  , 'SUNDAY'     , 'DOMINGO'),
       MES = TRUNC(FECHA, 'MONTH')
 WHERE FECHA BETWEEN TO_DATE('&FECHA', 'DD.MM.YYYY')
                 AND TO_DATE('&FECH2', 'DD.MM.YYYY') + 86399/86400

-- Para Actualizar: Lunes, Martes, Miercoles, Jueves, Viernes, Sabado, Domingo

SELECT L_DIA_0,
       D_DIA_0,
       L_DIA_1,
       D_DIA_1,
       L_DIA_2,
       D_DIA_2,
       L_DIA_3,
       D_DIA_3,
       L_DIA_4,
       D_DIA_4,
       L_DIA_5,
       D_DIA_5,
       L_DIA_6,
       D_DIA_6,
       'UPDATE CALIDAD_STATUS_REFERENCES SET &DIA_DESC = TO_DATE('''||TO_CHAR(L_DIA_0,'DD.MM.YYYY') || ''',''DD.MM.YYYY'')'||
       ' WHERE DIA IN (TO_DATE('''||TO_CHAR(L_DIA_0,'DD.MM.YYYY') || ''',''DD.MM.YYYY''),'||
       ' TO_DATE('''||TO_CHAR(L_DIA_1,'DD.MM.YYYY') || ''',''DD.MM.YYYY''),'||
       ' TO_DATE('''||TO_CHAR(L_DIA_2,'DD.MM.YYYY') || ''',''DD.MM.YYYY''),'||
       ' TO_DATE('''||TO_CHAR(L_DIA_3,'DD.MM.YYYY') || ''',''DD.MM.YYYY''),'||
       ' TO_DATE('''||TO_CHAR(L_DIA_4,'DD.MM.YYYY') || ''',''DD.MM.YYYY''),'||
       ' TO_DATE('''||TO_CHAR(L_DIA_5,'DD.MM.YYYY') || ''',''DD.MM.YYYY''),'||
       ' TO_DATE('''||TO_CHAR(L_DIA_6,'DD.MM.YYYY') || ''',''DD.MM.YYYY''));' SE
  FROM (
SELECT DIA L_DIA_0,
       DI2 D_DIA_0,
       LEAD(DIA, 1) OVER(ORDER BY DIA) L_DIA_1,
       LEAD(DI2, 1) OVER(ORDER BY DIA) D_DIA_1,
       LEAD(DIA, 2) OVER(ORDER BY DIA) L_DIA_2,
       LEAD(DI2, 2) OVER(ORDER BY DIA) D_DIA_2,
       LEAD(DIA, 3) OVER(ORDER BY DIA) L_DIA_3,
       LEAD(DI2, 3) OVER(ORDER BY DIA) D_DIA_3,
       LEAD(DIA, 4) OVER(ORDER BY DIA) L_DIA_4,
       LEAD(DI2, 4) OVER(ORDER BY DIA) D_DIA_4,
       LEAD(DIA, 5) OVER(ORDER BY DIA) L_DIA_5,
       LEAD(DI2, 5) OVER(ORDER BY DIA) D_DIA_5,
       LEAD(DIA, 6) OVER(ORDER BY DIA) L_DIA_6,
       LEAD(DI2, 6) OVER(ORDER BY DIA) D_DIA_6
  FROM (
SELECT DIA,
       DECODE(TRIM(TO_CHAR(DIA, 'DAY')), 'MONDAY'     , 'LUNES'
                                       , 'TUESDAY'    , 'MARTES'
                                       , 'WEDNESDAY'  , 'MIERCOLES'
                                       , 'THURSDAY'   , 'JUEVES'
                                       , 'FRIDAY'     , 'VIERNES'
                                       , 'SATURDAY'   , 'SABADO'
                                       , 'SUNDAY'     , 'DOMINGO') DI2

  FROM CALIDAD_STATUS_REFERENCES
 WHERE FECHA BETWEEN TO_DATE('&FECHA', 'DD.MM.YYYY')
                 AND TO_DATE('&FECH2', 'DD.MM.YYYY') + 86399/86400 
 GROUP BY DIA
 ORDER BY DIA
       )
       )
 WHERE D_DIA_0 = '&DIA_DESC';

-- Para actualizar esos dias fuera de la ventana del update anterior

SELECT DIA_DESC,
       DOMINGO,
       LUNES,
       MARTES,
       MIERCOLES,
       JUEVES,
       VIERNES,
       SABADO,
       COUNT(*) CANTIDAD
  FROM (
SELECT DIA,
       DIA_DESC,
       DIA - DOMINGO   DOMINGO,
       DIA - LUNES     LUNES,
       DIA - MARTES    MARTES,
       DIA - MIERCOLES MIERCOLES,
       DIA - JUEVES    JUEVES,
       DIA - VIERNES   VIERNES,
       DIA - SABADO    SABADO
  FROM CALIDAD_STATUS_REFERENCES
/* WHERE FECHA BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                 AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400 */
 --ORDER BY FECHA DESC
       )
 GROUP BY DIA_DESC,
          DOMINGO,
          LUNES,
          MARTES,
          MIERCOLES,
          JUEVES,
          VIERNES,
          SABADO;

UPDATE CALIDAD_STATUS_REFERENCES SET LUNES =     DECODE(LUNES, NULL, DIA - DECODE(DIA_DESC, 'DOMINGO'  , 6,
                                                                                            'SABADO'   , 5,
                                                                                            'VIERNES'  , 4,
                                                                                            'JUEVES'   , 3,
                                                                                            'MIERCOLES', 2,
                                                                                            'MARTES'   , 1,
                                                                                            'LUNES'    , 0), LUNES),
                                     MARTES =    DECODE(MARTES, NULL, DIA - DECODE(DIA_DESC, 'DOMINGO'  , 5,
                                                                                             'SABADO'   , 4,
                                                                                             'VIERNES'  , 3,
                                                                                             'JUEVES'   , 2,
                                                                                             'MIERCOLES', 1,
                                                                                             'MARTES'   , 0,
                                                                                             'LUNES'    , 6), MARTES),
                                     MIERCOLES = DECODE(MIERCOLES, NULL, DIA - DECODE(DIA_DESC, 'DOMINGO'  , 4,
                                                                                                'SABADO'   , 3,
                                                                                                'VIERNES'  , 2,
                                                                                                'JUEVES'   , 1,
                                                                                                'MIERCOLES', 0,
                                                                                                'MARTES'   , 6,
                                                                                                'LUNES'    , 5), MIERCOLES),
                                     JUEVES =    DECODE(JUEVES, NULL, DIA - DECODE(DIA_DESC, 'DOMINGO'  , 3,
                                                                                             'SABADO'   , 2,
                                                                                             'VIERNES'  , 1,
                                                                                             'JUEVES'   , 0,
                                                                                             'MIERCOLES', 6,
                                                                                             'MARTES'   , 5,
                                                                                             'LUNES'    , 4), JUEVES),
                                     VIERNES =   DECODE(VIERNES, NULL, DIA - DECODE(DIA_DESC, 'DOMINGO'  , 2,
                                                                                              'SABADO'   , 1,
                                                                                              'VIERNES'  , 0,
                                                                                              'JUEVES'   , 6,
                                                                                              'MIERCOLES', 5,
                                                                                              'MARTES'   , 4,
                                                                                              'LUNES'    , 3), VIERNES),
                                     SABADO =    DECODE(SABADO, NULL, DIA - DECODE(DIA_DESC, 'DOMINGO'  , 1,
                                                                                             'SABADO'   , 0,
                                                                                             'VIERNES'  , 6,
                                                                                             'JUEVES'   , 5,
                                                                                             'MIERCOLES', 4,
                                                                                             'MARTES'   , 3,
                                                                                             'LUNES'    , 2), SABADO),
                                     DOMINGO =   DECODE(DOMINGO, NULL, DIA - DECODE(DIA_DESC, 'DOMINGO'  , 0,
                                                                                              'SABADO'   , 6,
                                                                                              'VIERNES'  , 5,
                                                                                              'JUEVES'   , 4,
                                                                                              'MIERCOLES', 3,
                                                                                              'MARTES'   , 2,
                                                                                              'LUNES'    , 1), DOMINGO)
 WHERE DIA IN (SELECT FECHA
                 FROM CALIDAD_STATUS_REFERENCES
                WHERE HORA = '00' AND DIA_DESC = 'LUNES' AND DOMINGO IS NULL)

SELECT * FROM CALIDAD_STATUS_REFERENCES
 WHERE FECHA BETWEEN TO_DATE('&1', 'DD.MM.YYYY') AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400
 ORDER BY FECHA DESC;

UPDATE CALIDAD_STATUS_REFERENCES SET LUNES =     DIA - DECODE(DIA_DESC, 'DOMINGO', 6, 'SABADO', 5, 'VIERNES', 4, 'JUEVES', 3, 'MIERCOLES', 2, 'MARTES', 1, 'LUNES', 0),
                                     MARTES =    DIA - DECODE(DIA_DESC, 'DOMINGO', 5, 'SABADO', 4, 'VIERNES', 3, 'JUEVES', 2, 'MIERCOLES', 1, 'MARTES', 0, 'LUNES', 6),
                                     MIERCOLES = DIA - DECODE(DIA_DESC, 'DOMINGO', 4, 'SABADO', 3, 'VIERNES', 2, 'JUEVES', 1, 'MIERCOLES', 0, 'MARTES', 6, 'LUNES', 5),
                                     JUEVES =    DIA - DECODE(DIA_DESC, 'DOMINGO', 3, 'SABADO', 2, 'VIERNES', 1, 'JUEVES', 0, 'MIERCOLES', 6, 'MARTES', 5, 'LUNES', 4),
                                     VIERNES =   DIA - DECODE(DIA_DESC, 'DOMINGO', 2, 'SABADO', 1, 'VIERNES', 0, 'JUEVES', 6, 'MIERCOLES', 5, 'MARTES', 4, 'LUNES', 3),
                                     SABADO =    DIA - DECODE(DIA_DESC, 'DOMINGO', 1, 'SABADO', 0, 'VIERNES', 6, 'JUEVES', 5, 'MIERCOLES', 4, 'MARTES', 3, 'LUNES', 2),
                                     DOMINGO =   DIA - DECODE(DIA_DESC, 'DOMINGO', 0, 'SABADO', 6, 'VIERNES', 5, 'JUEVES', 4, 'MIERCOLES', 3, 'MARTES', 2, 'LUNES', 1)
 WHERE DIA = TO_DATE('&1', 'DD.MM.YYYY');

-- QUARTER CONTROL

UPDATE CALIDAD_STATUS_REFERENCES
   SET QUARTER = TRUNC(FECHA, 'Q'),
       NUMERO_DIA = TO_CHAR(FECHA, 'DD'),
       NUMERO_MES = TO_CHAR(FECHA, 'MM'),
       NUMERO_ANIO = TO_CHAR(FECHA, 'YYYY')
 WHERE FECHA BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                 AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400;

SELECT QUARTER, COUNT(*) CANTIDAD FROM CALIDAD_STATUS_REFERENCES GROUP BY QUARTER

-- FLAG'S CONTROL

SELECT TRUNC(DIA, 'YEAR') ANIO,
       FLAG_MANTENIMIENTO,
       FLAG_HDAY,
       F_MAINTENANCE_INDISTINCT,
       COUNT(*) CANTIDAD
  FROM CALIDAD_STATUS_REFERENCES
 GROUP BY TRUNC(DIA, 'YEAR'),
          FLAG_MANTENIMIENTO,
          FLAG_HDAY,
          F_MAINTENANCE_INDISTINCT
  FROM CALIDAD_STATUS_REFERENCES
 WHERE FECHA BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                 AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400
 ORDER BY FECHA DESC

-- FLAG HDAY                   -- 07 A 22 DISABLED -- 23 A 06 ENABLED
-- FLAG MAINTENANCE INDISTINCT -- 00 A 06 ENABLED  -- 07 A 23 DISABLED

SELECT NUMERO_ANIO,
       --HORA,
       FLAG_HDAY,
       COUNT(*) CANTIDAD
  FROM CALIDAD_STATUS_REFERENCES
 WHERE FECHA BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                 AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400
 GROUP BY NUMERO_ANIO,--DIA_DESC,
          --HORA,
          FLAG_HDAY

UPDATE CALIDAD_STATUS_REFERENCES
   SET FLAG_HDAY = 'DISABLED'
 WHERE FECHA BETWEEN TO_DATE('&1', 'DD.MM.YYYY') AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400;

UPDATE CALIDAD_STATUS_REFERENCES
   SET FLAG_HDAY = 'ENABLED'
 WHERE HORA NOT IN ('23', '00', '01', '02', '03', '04', '05', '06')
   AND FECHA BETWEEN TO_DATE('&1', 'DD.MM.YYYY') AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400;

-- FLAG_MANTENIMIENTO -- SABADO Y DOMINGO ENABLED -- LUNES A VIERNES DE 00 A 06 ENABLED -- EL RESTO DEL TIEMPO DISABLED

SELECT DIA_DESC,
       HORA,
       FLAG_MANTENIMIENTO,
       COUNT(*) CANTIDAD
  FROM CALIDAD_STATUS_REFERENCES
 WHERE FECHA BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                 AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400
 GROUP BY DIA_DESC,
          HORA,
          FLAG_MANTENIMIENTO

UPDATE CALIDAD_STATUS_REFERENCES
   SET FLAG_MANTENIMIENTO = 'DISABLED'
 WHERE FECHA BETWEEN TO_DATE('&1', 'DD.MM.YYYY') AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400;

UPDATE CALIDAD_STATUS_REFERENCES
   SET FLAG_MANTENIMIENTO = 'ENABLED'
 WHERE DIA_DESC IN ('SABADO', 'DOMINGO')
   AND FECHA BETWEEN TO_DATE('&1', 'DD.MM.YYYY') AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400;

UPDATE CALIDAD_STATUS_REFERENCES
   SET FLAG_MANTENIMIENTO = 'ENABLED'
 WHERE DIA_DESC NOT IN ('SABADO', 'DOMINGO')
   AND HORA IN ('00', '01', '02', '03', '04', '05', '06')
   AND FECHA BETWEEN TO_DATE('&1', 'DD.MM.YYYY') AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400;

COMMIT;

-- FLAG MAINTENANCE INDISTINCT -- DE 00 A 06 ENABLED -- DE 07 A 23 DISABLED

SELECT --DIA_DESC,
       HORA,
       F_MAINTENANCE_INDISTINCT,
       COUNT(*) CANTIDAD
  FROM CALIDAD_STATUS_REFERENCES
 WHERE FECHA BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                 AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400
 GROUP BY --DIA_DESC,
          HORA,
          F_MAINTENANCE_INDISTINCT

UPDATE CALIDAD_STATUS_REFERENCES
   SET F_MAINTENANCE_INDISTINCT = 'DISABLED'
 WHERE FECHA BETWEEN TO_DATE('&1', 'DD.MM.YYYY') AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400;

UPDATE CALIDAD_STATUS_REFERENCES
   SET F_MAINTENANCE_INDISTINCT = 'ENABLED'
 WHERE HORA NOT IN ('00', '01', '02', '03', '04', '05', '06')
   AND FECHA BETWEEN TO_DATE('&1', 'DD.MM.YYYY') AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400;
                 
                 
-------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------

SELECT BCF_ID,
       BAJA_LATITUD,
       LATITUD,
       ALTA_LATITUD,
       BAJA_LONGITUD,
       LONGITUD,
       ALTA_LONGITUD,
       ORDEN,
       CANTIDAD,
       CALCULAR_DISTANCIA(LATITUD, LONGITUD, BAJA_LATITUD, BAJA_LONGITUD) DISTANCIA_BAJA,
       CALCULAR_DISTANCIA(LATITUD, LONGITUD, ALTA_LATITUD, ALTA_LONGITUD) DISTANCIA_ALTA
  FROM (
SELECT BCF_ID,
       LATITUD,
       LONGITUD,
       TO_NUMBER(SUBSTR(LATITUD,  1, INSTR(LATITUD , '.') + 8 )) + 0.7 BAJA_LATITUD,
       TO_NUMBER(SUBSTR(LONGITUD, 1, INSTR(LONGITUD, '.') + 8 )) + 0.7 BAJA_LONGITUD,
       TO_NUMBER(SUBSTR(LATITUD,  1, INSTR(LATITUD , '.') + 8 )) - 0.7 ALTA_LATITUD,
       TO_NUMBER(SUBSTR(LONGITUD, 1, INSTR(LONGITUD, '.') + 8 )) - 0.7 ALTA_LONGITUD,
       ORDEN,
       CANTIDAD
  FROM OPS$CALIDAD.AU2_CLARO_RECLAMOS_DATA
       );

-------------------------------------------------------------------------------------------------------------------------
-- Para actualizar Wcell Address con letra de cara

SELECT A.WCELL_ID,
       A.WCELL_NAME,
       A.WCELL_NAME_LAST_LETTER,
       A.WCELL_ADDRESS,
       B.LA_ADDRESS,
       B.LA_NAME,
       'UPDATE UTP_LEGACY SET LA_ADDRESS = '''||A.WCELL_ADDRESS||'_'||A.WCELL_NAME_LAST_LETTER||''' WHERE LA_CO_GID = '||A.WCELL_ID||';' SE
  FROM (
SELECT WCELL_ID,
       WCELL_NAME,
       WCELL_NAME_LAST_LETTER,
       WCELL_ADDRESS,
       WCELL_ADDRESS_LAST_LETTER,
       WCELL_VALID_START_DATE,
       WCELL_VALID_FINISH_DATE,
       WBTS_NAME,
       WBTS_ADDRESS,
       RNC_NAME,
       RNC_ADDRESS
  FROM (
SELECT WCELL_ID,
       WCELL_NAME,
       WCELL_ADDRESS,
       WCELL_VALID_START_DATE,
       WCELL_VALID_FINISH_DATE,
       WBTS_NAME,
       WBTS_ADDRESS,
       RNC_NAME,
       RNC_ADDRESS,
       SUBSTR(WCELL_NAME, LENGTH(WCELL_NAME)) WCELL_NAME_LAST_LETTER,
       SUBSTR(WCELL_ADDRESS, LENGTH(WCELL_ADDRESS)) WCELL_ADDRESS_LAST_LETTER
  FROM OBJECTS_SP_UMTS
 WHERE RNC_ORIGEN = 'RC2'
   AND SYSDATE BETWEEN WCELL_VALID_START_DATE AND WCELL_VALID_FINISH_DATE
       )
 WHERE WCELL_ADDRESS_LAST_LETTER NOT IN ('A', 'B', 'C', 'D', 'E', 'F') --1466
   AND WCELL_NAME_LAST_LETTER IN ('A', 'B', 'C', 'D', 'E', 'F') --1466
       ) A,
       (
SELECT LA_CO_GID,
       LA_NAME,
       LA_ADDRESS
  FROM UTP_LEGACY@OSS3G
 WHERE LA_OBJECT_CLASS = 813
       ) B
 WHERE A.WCELL_ID = B.LA_CO_GID (+)


 -- Actualizacion masiva de nombres de celdas a partir del nombre del sitio.
 -- tags: MASIVO, CAMBIO, NOMBRE, CELDA
 
 SELECT WCELL_ID,
       WCELL_NAME,
       WCELL_ADDRESS,
       WCELL_ADDRES2,
       WBTS_NAME,
       WBTS_ADDRESS,
       A.ALM,
       RNC_ORIGEN,
       SE,
       B.ALM,
       B.NOMBRE,
       B.PAIS
  FROM (
SELECT WCELL_ID,
       WCELL_NAME,
       WCELL_ADDRESS,
       WBTS_ADDRESS||'_'||CELL_LL WCELL_ADDRES2,
       WBTS_NAME,
       WBTS_ADDRESS,
       REGEXP_SUBSTR(WBTS_ADDRESS, '[A-Z0-9_]+') ALM,
       RNC_ORIGEN,
       'UPDATE UTP_LEGACY SET LA_ADDRESS = '''||WBTS_ADDRESS||'_'||CELL_LL||''' WHERE LA_CO_GID = '||WCELL_ID||' AND LA_OBJECT_CLASS = 813 AND LA_ADDRESS IS NULL;' SE
  FROM OBJECTS_SP_UMTS
 WHERE WCELL_VALID_FINISH_DATE > SYSDATE
   AND WCELL_ADDRESS IS NULL
   AND WBTS_ADDRESS IS NOT NULL
 ORDER BY WCELL_VALID_START_DATE
       ) A,
       ALM_MERCADO B
 WHERE A.ALM = B.ALM (+)
 
 
------------------------------------------------------------------------------------------------------------------------



CELLRES SERVLEV WBTSHW TRAFFIC RCPMRLC HSDPAW INTSYSHO INTERSHO SOFTHO RCPMUEQ RCOLPC

  -- Service
  --OK-- NOKRWW_PS_CELLRES_MNC1_RAW
  --OK-- NOKRWW_PS_SERVLEV_MNC1_RAW
  --OK-- NOKRWW_PS_WBTSHW_LCG_RAW
  --OK-- NOKRWW_PS_TRAFFIC_MNC1_RAW

  -- Traffic
  NOKRWW_PS_RCPMRLC_SDUBER1_RAW

  -- Power
  NOKRWW_PV_HSDPAW_WCEL_RAW
  --OK-- NOKRWW_PV_CELLRES_WCEL_RAW --es igual que NOKRWW_PS_CELLRES_MNC1_RAW

  -- Iu
  /* (
  NOKRWWRAW.NOKRWW_P_ATMVCC_ALU1_PMC
  NOKRWW_PS_ATMVCC_ALU1_RAW
  NOKRWW_PS_AALCAC_ALU1_RAW
  NOKRWW_PS_FPDH_PPTT_RAW
  NOKRWW_PS_ATMVP_VPI_RAW
  NOKRWW_PS_L3IU_MNC6_RAW
  NOKRWW_PS_IUPS_UNITID_RAW
  NOKRWW_PS_L3IU_MNC6_RAW
  NOKRWW_PS_IUPS_UNITID_RAW
  ) */ 

  -- Hsdpa
  --OK-- NOKRWW_PS_CELLRES_MNC1_RAW -- Ya esta
  NOKRWW_PS_HSDPAW_MNC1_RAW -- Ya esta
  --OK-- NOKRWW_PS_TRAFFIC_MNC1_RAW -- Ya esta
  --OK-- NOKRWW_PS_SERVLEV_MNC1_RAW -- Ya esta
  NOKRWW_PS_INTSYSHO_MNC1_RAW

  -- Ho
  NOKRWW_PS_INTERSHO_MNC1_RAW
  NOKRWW_PS_SOFTHO_MNC1_RAW
  NOKRWW_PS_INTSYSHO_MNC1_RAW -- Ya esta

  -- Bler
  NOKRWW_PS_RCPMUEQ_SDUBER1_RAW
  NOKRWW_PS_RCOLPC_SDUBER1_RAW

  -- Capacity (Service y de Traffic)
  -- NOC
  -- Traf-Mac

-----------------------------------------------------------------------------------------------------------

-- Consultar trafico de datos desde SMART

SELECT A.PERIOD_START_TIME,
       B.MERCADO,
       --WCELL_ID,
       SUM(HSDPA_MB) HSDPA_MB,
       SUM(DL_PS_TRAFFIC_MB) DL_PS_TRAFFIC_MB,
       SUM(UL_PS_TRAFFIC_MB) UL_PS_TRAFFIC_MB
  FROM (
SELECT FECHA PERIOD_START_TIME,
       INT_ID WCELL_ID,
       CASE WHEN RBDL_ID IN ('HS_DSCH') THEN
            ROUND(NVL(SUM(RLC_AM_SDU_DL_PS_VOL), 0)  /(1024 * 1024), 2) ELSE NULL END HSDPA_MB, --Trafico DL PS Traffic (HSDPA)
       CASE WHEN RBDL_ID NOT IN ('HS_DSCH') THEN
            ROUND(NVL(SUM(RLC_AM_SDU_DL_PS_VOL), 0)  /(1024 * 1024), 2) ELSE NULL END DL_PS_TRAFFIC_MB, --DL PS Traffic (Rel.99)
       ROUND(NVL(SUM(RLC_AM_SDU_UL_PS_VOL), 0)  /(1024 * 1024), 2) UL_PS_TRAFFIC_MB --UL PS Traffic (Rel.99)
  FROM UMTS_C_NSN_TRAFFIC_WCELL_HOUR
 WHERE FECHA BETWEEN TO_DATE('&FECHA','DD.MM.YYYY')
                 AND TO_DATE('&FECH2','DD.MM.YYYY') + 86399/86400
   AND TR_ID IN ('interactive', 'background')
 GROUP BY FECHA,
          INT_ID,
          RBDL_ID
       ) A,
       OBJECTS_SP_UMTS B
 WHERE A.WCELL_ID = B.WCELL_ID
--   AND B.MERCADO = 'Paraguay'
 GROUP BY A.PERIOD_START_TIME,
          B.MERCADO;

-- Consultar trafico de datos desde el OSS 3g

SELECT A.PERIOD_START_TIME,
       B.MERCADO,
       --WCELL_ID,
       SUM(HSDPA_MB) HSDPA_MB,
       SUM(DL_PS_TRAFFIC_MB) DL_PS_TRAFFIC_MB,
       SUM(UL_PS_TRAFFIC_MB) UL_PS_TRAFFIC_MB
  FROM (
SELECT PERIOD_START_TIME,
       WCEL_ID WCELL_ID,
       CASE WHEN RBDL_ID IN ('HS_DSCH') THEN
            ROUND(NVL(SUM(RLC_AM_SDU_DL_PS_VOL), 0)  /(1024 * 1024), 2) ELSE NULL END HSDPA_MB, --Trafico DL PS Traffic (HSDPA)
       CASE WHEN RBDL_ID NOT IN ('HS_DSCH') THEN
            ROUND(NVL(SUM(RLC_AM_SDU_DL_PS_VOL), 0)  /(1024 * 1024), 2) ELSE NULL END DL_PS_TRAFFIC_MB, --DL PS Traffic (Rel.99)
       ROUND(NVL(SUM(RLC_AM_SDU_UL_PS_VOL), 0)  /(1024 * 1024), 2) UL_PS_TRAFFIC_MB --UL PS Traffic (Rel.99)
  FROM NOKRWW_PS_RCPMRLC_SDUBER1_RAW@OSSV51
 WHERE PERIOD_START_TIME BETWEEN TO_DATE('&FECHA','DD.MM.YYYY')
                             AND TO_DATE('&FECH2','DD.MM.YYYY') + 86399/86400
   AND TR_ID IN ('interactive', 'background')
 GROUP BY PERIOD_START_TIME,
          WCEL_ID,
          RBDL_ID
       ) A,
       OBJECTS_SP_UMTS B
 WHERE A.WCELL_ID = B.WCELL_ID
--   AND B.MERCADO = 'Paraguay'
 GROUP BY A.PERIOD_START_TIME,
          B.MERCADO;

-- Periodos consultados por SMART para un periodo indicador. Usuario y cantidad de Dias. Reportes nivel de Celda, Hour.

SELECT FECHA,
       A.LEGAJO,
       A.NOMBRE,
       NOMBRE_TAB,
       NIVEL,
       TIPO,
       SUMARIZACION,
       COMBO1,
       COMBO2,
       COMBO3,
       COMBO4,
       COMBO5,
       COMBO6,
       FECHA_DESDE,
       FECHA_HASTA,
       TIPO_EJECUCION,
       RESTA_FECHAS,
       B.NOMBRE,
       B.APELLIDO,
       B.MAIL,
       B.AREA,
       B.GERENCIA,
       B.DIRECCION
  FROM (
SELECT FECHA,
       LEGAJO,
       NOMBRE,
       NOMBRE_TAB,
       NIVEL,
       TIPO,
       SUMARIZACION,
       COMBO1,
       COMBO2,
       COMBO3,
       COMBO4,
       COMBO5,
       COMBO6,
       FECHA_DESDE,
       FECHA_HASTA,
       TIPO_EJECUCION,
       TO_DATE(FECHA_HASTA, 'DD.MM.YYYY') - TO_DATE(FECHA_DESDE, 'DD.MM.YYYY') RESTA_FECHAS
  FROM SMART_REPORT_LOGS
 WHERE SUMARIZACION = 'HOUR'
   AND NIVEL = 'CELL'
   AND FECHA >= TO_DATE('&1','DD.MM.YYYY') 
       ) A,
       SMART_USERS B
 WHERE A.LEGAJO = B.LEGAJO
 ORDER BY FECHA DESC, RESTA_FECHAS DESC

-- tags: TIEMPO, REPORTE, UTILIZATION, SMART
 
SELECT FECHA,
       --SYSDATE AHORA,
       --LEGAJO,
       NOMBRE,
       --NOMBRE_TAB,
       NIVEL,
       --TIPO,
       SUMARIZACION,
       --COMBO1,
       --COMBO2,
       --COMBO3,
       COMBO4,
       --COMBO5,
       --COMBO6,
       FECHA_DESDE,
       FECHA_HASTA,
       TO_DATE(FECHA_HASTA, 'DD.MM.YYYY')  - TO_DATE(FECHA_DESDE, 'DD.MM.YYYY')  DIAS,
       --TIPO_EJECUCION,
       --QUERY_DURATION,
       --TOTAL_DURATION,
       TOTAL_DURATION / 60 MINUTOS--,
       --REPORT_EXECUTION_ID
  FROM SMART_REPORT_LOGS
 WHERE NOMBRE = 'UMTS_MAIN_CAP_ACC_KPI'
   AND SUMARIZACION = 'HOUR'
   AND FECHA >= TO_DATE('&1','DD.MM.YYYY HH24:MI')
   --AND FECHA <= TO_DATE('&1','DD.MM.YYYY HH24:MI') + 10/1344
 ORDER BY FECHA ASC;

SELECT FECHA,
       NOMBRE,
       NIVEL,
       SUMARIZACION,
       COMBO4,
       FECHA_DESDE,
       FECHA_HASTA,
       TO_DATE(FECHA_HASTA, 'DD.MM.YYYY')  - TO_DATE(FECHA_DESDE, 'DD.MM.YYYY')  DIAS,
       TOTAL_DURATION / 60 MINUTOS
  FROM SMART_REPORT_LOGS
 WHERE NOMBRE = 'UMTS_MAIN_CAP_ACC_KPI'
   AND NIVEL = 'CELL'
   AND SUMARIZACION = 'HOUR'
   AND FECHA BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                 AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400 
 ORDER BY FECHA ASC;

-- Reporte de Reportes mas usados en performance.cti
-- tags: REPORTE, UTILIZATION

SELECT NOMBRE, NIVEL, SUMARIZACION, COUNT(*) CANTIDAD
  FROM SMART_REPORT_LOGS
 WHERE FECHA BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                 AND TO_DATE('&2', 'DD.MM.YYYY') + 86399 / 86400
   AND FECHA_DESDE IS NOT NULL
   AND FECHA_HASTA IS NOT NULL
 GROUP BY NOMBRE, NIVEL, SUMARIZACION
 ORDER BY CANTIDAD DESC;

 

--------------------------- cantidad de sesiones abiertas en smart durante el lunes a la mañana -------------------------


 SELECT TRUNC(FECHA) FECHA,
        SUM(RANGO) CONEXIONES
   FROM (
 SELECT FECHA,
        ID_USUARIO,
        ORDEN,
        CANTIDAD,
        CASE WHEN TO_NUMBER(TO_CHAR(TRUNC_HORA, 'HH24')) BETWEEN 8 AND 12 THEN 1 ELSE 0 END RANGO
   FROM (
 SELECT FECHA,
        TRUNC(FECHA, 'HH24') TRUNC_HORA,
        ID_USUARIO,
        COUNT(*) OVER(PARTITION BY TRUNC(FECHA, 'HH24'), ID_USUARIO ORDER BY FECHA) ORDEN, 
        COUNT(*) OVER(PARTITION BY TRUNC(FECHA, 'HH24'), ID_USUARIO) CANTIDAD
   FROM (
 SELECT FECHA,
        ID_USUARIO,
        TRIM(TO_CHAR(FECHA, 'DAY')) DIA
   FROM SMART_USERS_LOGS
  WHERE FECHA BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                  AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400
        )
  WHERE DIA = 'LUNES'
        )
  WHERE ORDEN = 1
        )
  GROUP BY TRUNC(FECHA)

---------------------------------------------------------------------------------------------------------------------------
-- Consulta de datos por BSC/RNC al OSS RC1/RC2

-- GSM

SELECT TRUNC(PERIOD_START_TIME) PERIOD_START_TIME,
       BSC_NAME,
       COUNT(*) CANTIDAD
  FROM (
SELECT PERIOD_START_TIME,
       BSC_NAME,
       COUNT(*) CANTIDAD
  FROM P_NBSC_TRAFFIC@OSS A,
       (
SELECT NAME BSC_NAME,
       INT_ID BSC_GID,
       COUNT(*) OVER(PARTITION BY NAME ORDER BY VALID_FINISH_DATE DESC)  ORDEN
  FROM MULTIVENDOR_OBJECTS PARTITION (MULTIVENDOR_OC_3)
 WHERE TRUNC(SYSDATE) BETWEEN VALID_START_DATE AND VALID_FINISH_DATE
   AND INT_ID IS NOT NULL
       ) B
 WHERE A.BSC_GID = B.BSC_GID (+)
   AND A.PERIOD_START_TIME BETWEEN TO_DATE('&1', 'DD.MM.YYYY') AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400 
 GROUP BY PERIOD_START_TIME,
          BSC_NAME
       )
 GROUP BY trunc(PERIOD_START_TIME),
          BSC_NAME

-- UMTS

SELECT TRUNC(PERIOD_START_TIME) PERIOD_START_TIME,
       RNC_NAME,
       COUNT(*) CANTIDAD
  FROM (
SELECT PERIOD_START_TIME,
       RNC_NAME,
       COUNT(*) CANTIDAD
  FROM NOKRWW_PS_TRAFFIC_MNC1_RAW@OSS A,
       (
SELECT NAME RNC_NAME,
       INT_ID RNC_GID,
       COUNT(*) OVER(PARTITION BY NAME ORDER BY VALID_FINISH_DATE DESC)  ORDEN
  FROM MULTIVENDOR_OBJECTS PARTITION (MULTIVENDOR_OC_811)
 WHERE TRUNC(SYSDATE) BETWEEN VALID_START_DATE AND VALID_FINISH_DATE
   AND INT_ID IS NOT NULL
       ) B
 WHERE A.RNC_ID = B.RNC_GID (+)
   AND A.PERIOD_START_TIME BETWEEN TO_DATE('&1', 'DD.MM.YYYY') AND TO_DATE('&2', 'DD.MM.YYYY') + 86399/86400 
 GROUP BY PERIOD_START_TIME,
          RNC_NAME
       )
 GROUP BY TRUNC(PERIOD_START_TIME),
          RNC_NAME
 
-- Reclamos

SELECT TEC_PBL_ID,
       TEC_PBL_DESCRIPTION,
       DECODE(TEC_GRP_ID, 'INT', 'Cobertura', 'COB', 'Cobertura', 'CON', 'Red', 'OTR', 'Red') ALCANCE,
       DECODE(TEC_GRP_ID, 'INT', 'Datos', 'COB', 'Voz', 'CON', 'Voz', 'OTR', 'Datos') TIPO,
       TECNOLOGIA FROM CLARO_RECLAMOS_DESC
 WHERE FLAG_STATUS = 'ENABLED'
 
/*

Cobertura/Voz=COB
Cobertura/Datos=INT
Red/Voz=CON
Red/Datos=OTR

*/

---------------------------------------------------------------------------------------------------------------------
-- Administracion de Colas cuando fallan
---------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------
-- Counters Gsm
---------------------------------------------------------------------------------------------------------------------

DECLARE

V_DATO2 SCOTT.GSM_C_NSN_CYCLES;

CURSOR MSG_ENQ_WO_PROCESS IS
SELECT MSG_ID                                                       CYCLE_MSG_ID,
       TREAT(USER_DATA AS SCOTT.GSM_C_NSN_CYCLES).CYCLE_NAME        CYCLE_NAME,
       TO_CHAR(
       TREAT(USER_DATA AS SCOTT.GSM_C_NSN_CYCLES).CYCLE_START_DATE, 'DD.MM.YYYY')  CYCLE_START_DATE,
       TO_CHAR(
       TREAT(USER_DATA AS SCOTT.GSM_C_NSN_CYCLES).CYCLE_FINISH_DATE, 'DD.MM.YYYY') CYCLE_FINISH_DATE,
       TREAT(USER_DATA AS SCOTT.GSM_C_NSN_CYCLES).CYCLE_PLATAFORMA  CYCLE_PLAT,
       TREAT(USER_DATA AS SCOTT.GSM_C_NSN_CYCLES).CYCLE_RC          CYCLE_RC,
       TREAT(USER_DATA AS SCOTT.GSM_C_NSN_CYCLES).CYCLE_NE          CYCLE_NE,
       TREAT(USER_DATA AS SCOTT.GSM_C_NSN_CYCLES).CYCLE_LS          CYCLE_LS,
       TREAT(USER_DATA AS SCOTT.GSM_C_NSN_CYCLES).CYCLE_STATUS_NAME CYCLE_STT_NAME,
       MSG_STATE,
       ENQ_TIME,
       DEQ_TIME
  FROM AQ_T_IN_GSM_C_NSN_CYCLES
 WHERE MSG_ID IN ('&MSG_ID')
 ORDER BY ENQ_TIME DESC;

BEGIN

FOR SYN IN MSG_ENQ_WO_PROCESS LOOP

V_DATO2 := SCOTT.GSM_C_NSN_CYCLES(CYCLE_NAME             => SYN.CYCLE_NAME,
                                  CYCLE_STATUS_NAME      => 'FINALIZACION',
                                  CYCLE_STATUS_DATE      => SYSDATE,
                                  CYCLE_START_DATE       => TO_DATE(SYN.CYCLE_START_DATE , 'DD.MM.YYYY HH24'),
                                  CYCLE_FINISH_DATE      => TO_DATE(SYN.CYCLE_FINISH_DATE, 'DD.MM.YYYY HH24'),
                                  CYCLE_PLATAFORMA       => SYN.CYCLE_PLAT,
                                  CYCLE_RC               => SYN.CYCLE_RC,
                                  CYCLE_NE               => SYN.CYCLE_NE,
                                  CYCLE_LS               => SYN.CYCLE_LS,
                                  CYCLE_CHR_001          => SYN.CYCLE_MSG_ID,
                                  CYCLE_CHR_002          => NULL,
                                  CYCLE_CHR_003          => NULL,
                                  CYCLE_NBR_001          => NULL,
                                  CYCLE_NBR_002          => NULL,
                                  CYCLE_NBR_003          => NULL
                                 );

SCOTT.P_ENQUEUE_GSM_COUNTERS (V_DATO2, 'SCOTT.C_OUT_GSM_C_NSN_CYCLES');

END LOOP;

COMMIT;

END;

---------------------------------------------------------------------------------------------------------------------
-- Queues Gsm
---------------------------------------------------------------------------------------------------------------------

DECLARE

V_DATO2 SCOTT.TECH_MVENDOR_CYCLES;

CURSOR MSG_ENQ_WO_PROCESS IS
SELECT MSG_ID                                                                         CYCLE_MSG_ID,
       TREAT(USER_DATA AS SCOTT.TECH_MVENDOR_CYCLES).CYCLE_NAME                       CYCLE_NAME,
       TO_CHAR(
       TREAT(USER_DATA AS SCOTT.TECH_MVENDOR_CYCLES).CYCLE_START_DATE, 'DD.MM.YYYY')  CYCLE_S_DATE,
       TO_CHAR(
       TREAT(USER_DATA AS SCOTT.TECH_MVENDOR_CYCLES).CYCLE_FINISH_DATE, 'DD.MM.YYYY') CYCLE_F_DATE,
       TREAT(USER_DATA AS SCOTT.TECH_MVENDOR_CYCLES).CYCLE_TECHNOLOGY                 CYCLE_TECH,
       TREAT(USER_DATA AS SCOTT.TECH_MVENDOR_CYCLES).CYCLE_RC                         CYCLE_RC,
       TREAT(USER_DATA AS SCOTT.TECH_MVENDOR_CYCLES).CYCLE_NE                         CYCLE_NE,
       TREAT(USER_DATA AS SCOTT.TECH_MVENDOR_CYCLES).CYCLE_LS                         CYCLE_LS,
       MSG_STATE,
       ENQ_TIME,
       DEQ_TIME
  FROM AQ_T_GSM_MVENDOR_IN_CCLS
 WHERE MSG_ID = '&1';

V_CYCLE_IN_MSG_ID     VARCHAR2(50);

BEGIN

FOR SYN IN MSG_ENQ_WO_PROCESS LOOP

V_DATO2 := SCOTT.TECH_MVENDOR_CYCLES(CYCLE_NAME             => SYN.CYCLE_NAME,
                                     CYCLE_START_DATE       => TO_DATE(SYN.CYCLE_S_DATE, 'DD.MM.YYYY HH24'),
                                     CYCLE_FINISH_DATE      => TO_DATE(SYN.CYCLE_F_DATE, 'DD.MM.YYYY HH24'),
                                     CYCLE_TECHNOLOGY       => SYN.CYCLE_TECH,
                                     CYCLE_RC               => SYN.CYCLE_RC,
                                     CYCLE_NE               => SYN.CYCLE_NE,
                                     CYCLE_LS               => SYN.CYCLE_LS,
                                     CYCLE_CHR_001          => SYN.CYCLE_MSG_ID,
                                     CYCLE_CHR_002          => NULL,
                                     CYCLE_CHR_003          => NULL,
                                     CYCLE_NBR_001          => NULL,
                                     CYCLE_NBR_002          => NULL,
                                     CYCLE_NBR_003          => NULL
                                    );

P_ENQUEUE_TECH_MVENDOR (V_DATO2, 'SCOTT.C_GSM_MVENDOR_OUT_CCLS', V_CYCLE_IN_MSG_ID);

END LOOP;

COMMIT;

END;

---------------------------------------------------------------------------------------------------------------------
-- Queues Gprs
---------------------------------------------------------------------------------------------------------------------

DECLARE

V_DATO2 SCOTT.TECH_MVENDOR_CYCLES;

CURSOR MSG_ENQ_WO_PROCESS IS
SELECT MSG_ID                                                                         CYCLE_MSG_ID,
       TREAT(USER_DATA AS SCOTT.TECH_MVENDOR_CYCLES).CYCLE_NAME                       CYCLE_NAME,
       TO_CHAR(
       TREAT(USER_DATA AS SCOTT.TECH_MVENDOR_CYCLES).CYCLE_START_DATE, 'DD.MM.YYYY')  CYCLE_S_DATE,
       TO_CHAR(
       TREAT(USER_DATA AS SCOTT.TECH_MVENDOR_CYCLES).CYCLE_FINISH_DATE, 'DD.MM.YYYY') CYCLE_F_DATE,
       TREAT(USER_DATA AS SCOTT.TECH_MVENDOR_CYCLES).CYCLE_TECHNOLOGY                 CYCLE_TECH,
       TREAT(USER_DATA AS SCOTT.TECH_MVENDOR_CYCLES).CYCLE_RC                         CYCLE_RC,
       TREAT(USER_DATA AS SCOTT.TECH_MVENDOR_CYCLES).CYCLE_NE                         CYCLE_NE,
       TREAT(USER_DATA AS SCOTT.TECH_MVENDOR_CYCLES).CYCLE_LS                         CYCLE_LS,
       MSG_STATE,
       ENQ_TIME,
       DEQ_TIME
  FROM AQ_T_GPRS_MVENDOR_IN_CCLS
 WHERE MSG_ID = '&MSG_ID';

V_CYCLE_IN_MSG_ID     VARCHAR2(50);

BEGIN

FOR SYN IN MSG_ENQ_WO_PROCESS LOOP

V_DATO2 := SCOTT.TECH_MVENDOR_CYCLES(CYCLE_NAME             => SYN.CYCLE_NAME,
                                     CYCLE_START_DATE       => TO_DATE(SYN.CYCLE_S_DATE, 'DD.MM.YYYY HH24'),
                                     CYCLE_FINISH_DATE      => TO_DATE(SYN.CYCLE_F_DATE, 'DD.MM.YYYY HH24'),
                                     CYCLE_TECHNOLOGY       => SYN.CYCLE_TECH,
                                     CYCLE_RC               => SYN.CYCLE_RC,
                                     CYCLE_NE               => SYN.CYCLE_NE,
                                     CYCLE_LS               => SYN.CYCLE_LS,
                                     CYCLE_CHR_001          => SYN.CYCLE_MSG_ID,
                                     CYCLE_CHR_002          => NULL,
                                     CYCLE_CHR_003          => NULL,
                                     CYCLE_NBR_001          => NULL,
                                     CYCLE_NBR_002          => NULL,
                                     CYCLE_NBR_003          => NULL
                                    );

P_ENQUEUE_TECH_MVENDOR (V_DATO2, 'SCOTT.C_GPRS_MVENDOR_OUT_CCLS', V_CYCLE_IN_MSG_ID);

END LOOP;

COMMIT;

END;

------------------------------------------------------------------------------------------------------------------------

-- Autor: Mario Heredia. Fecha: 20.12.2013. Motivo: Consulta Counter en Smart por Regional.
-- Actualizacion: Mario Heredia. Fecha: 16.01.2014. Motivo: Se parametriza columna por Regional.

SELECT RFC.FECHA,
       TF2.CANTIDAD CANTIDAD_TFC,
       HO2.CANTIDAD CANTIDAD_HOV,
       SR2.CANTIDAD CANTIDAD_SRV,
       RE2.CANTIDAD CANTIDAD_RES,
       RC2.CANTIDAD CANTIDAD_REC,
       FE2.CANTIDAD CANTIDAD_FER,
       CO2.CANTIDAD CANTIDAD_COS,
       PC2.CANTIDAD CANTIDAD_PCU,
       QOS.CANTIDAD CANTIDAD_QOS,
       RXQ.CANTIDAD CANTIDAD_RXQ
  FROM (
        SELECT FECHA
          FROM CALIDAD_STATUS_REFERENCES
         WHERE FECHA BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                         AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
       ) RFC,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM GSM_C_NSN_TRAFFIC_HOU2 --
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&RC'
         GROUP BY PERIOD_START_TIME
       ) TF2,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM GSM_C_NSN_HO_HOU2 --
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&RC'
         GROUP BY PERIOD_START_TIME
       ) HO2,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM GSM_C_NSN_SERVICE_HOU2 --
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&RC'
         GROUP BY PERIOD_START_TIME
       ) SR2,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM GSM_C_NSN_RESAVAIL_HOU2 --
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&RC'
         GROUP BY PERIOD_START_TIME
       ) RE2,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM GSM_C_NSN_RESACC_HOU2 --
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&RC'
         GROUP BY PERIOD_START_TIME
       ) RC2,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM GSM_C_NSN_FER_HOU2 --
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&RC'
         GROUP BY PERIOD_START_TIME
       ) FE2,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM GSM_C_NSN_COD_SCH_HOU2 --
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&RC'
         GROUP BY PERIOD_START_TIME
       ) CO2,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM GSM_C_NSN_PCU_HOU2 --
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&RC'
         GROUP BY PERIOD_START_TIME
       ) PC2,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM GSM_C_NSN_QOSPCL_HOU2 --
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&RC'
         GROUP BY PERIOD_START_TIME
       ) QOS,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM GSM_C_NSN_RXQUAL_HOU2 --
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&RC'
         GROUP BY PERIOD_START_TIME
       ) RXQ
 WHERE RFC.FECHA = TF2.FECHA (+)
   AND RFC.FECHA = HO2.FECHA (+)
   AND RFC.FECHA = SR2.FECHA (+)
   AND RFC.FECHA = RE2.FECHA (+)
   AND RFC.FECHA = RC2.FECHA (+)
   AND RFC.FECHA = FE2.FECHA (+)
   AND RFC.FECHA = CO2.FECHA (+)
   AND RFC.FECHA = PC2.FECHA (+)
   AND RFC.FECHA = QOS.FECHA (+)
   AND RFC.FECHA = RXQ.FECHA (+)
 ORDER BY RFC.FECHA;

------------------------------------------------------------------------------------------------------------------------

-- Autor: Mario Heredia. Fecha: 20.12.2013. Motivo: Consulta Counter UMTS en Smart por Regional.
-- Actualizacion: Mario Heredia. Fecha: 08.10.2014.

SELECT RFC.FECHA,
       TRF.CANTIDAD TRF_CANTIDAD,
       HSW.CANTIDAD HSW_CANTIDAD,
       CTP.CANTIDAD CTP_CANTIDAD,
       RRC.CANTIDAD RRC_CANTIDAD,
       SRL.CANTIDAD SRL_CANTIDAD,
       CRS.CANTIDAD CRS_CANTIDAD,
       YHO.CANTIDAD YHO_CANTIDAD,
       SHO.CANTIDAD SHO_CANTIDAD,
       IHO.CANTIDAD IHO_CANTIDAD,
       CTW.CANTIDAD CTP_CANTIDAD
  FROM (
        SELECT FECHA
          FROM CALIDAD_STATUS_REFERENCES
         WHERE FECHA BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                         AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
       ) RFC,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM UMTS_C_NSN_TRAFFIC_MNC1_RAW
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&OSSRC'
         GROUP BY PERIOD_START_TIME
       ) TRF,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM UMTS_C_NSN_HSDPAW_MNC1_RAW
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&OSSRC'
         GROUP BY PERIOD_START_TIME
       ) HSW,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM UMTS_C_NSN_CELLTP_MNC1_RAW
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&OSSRC'
         GROUP BY PERIOD_START_TIME
       ) CTP,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM UMTS_C_NSN_RRC_MNC1_RAW
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&OSSRC'
         GROUP BY PERIOD_START_TIME
       ) RRC,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM UMTS_C_NSN_SERVLEV_MNC1_RAW
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&OSSRC'
         GROUP BY PERIOD_START_TIME
       ) SRL,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM UMTS_C_NSN_CELLRES_MNC1_RAW
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&OSSRC'
         GROUP BY PERIOD_START_TIME
       ) CRS,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM UMTS_C_NSN_INTSYSHO_MNC1_RAW
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&OSSRC'
         GROUP BY PERIOD_START_TIME
       ) YHO,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM UMTS_C_NSN_SOFTHO_MNC1_RAW
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&OSSRC'
         GROUP BY PERIOD_START_TIME
       ) SHO,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM UMTS_C_NSN_INTERSHO_MNC1_RAW
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&OSSRC'
         GROUP BY PERIOD_START_TIME
       ) IHO,
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM UMTS_C_NSN_CELTPW_MNC1_RAW
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                                     AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
           AND OSSRC = '&OSSRC'
         GROUP BY PERIOD_START_TIME
       ) CTW
 WHERE RFC.FECHA = TRF.FECHA (+)
   AND RFC.FECHA = HSW.FECHA (+)
   AND RFC.FECHA = CTP.FECHA (+)
   AND RFC.FECHA = RRC.FECHA (+)
   AND RFC.FECHA = SRL.FECHA (+)
   AND RFC.FECHA = CRS.FECHA (+)
   AND RFC.FECHA = YHO.FECHA (+)
   AND RFC.FECHA = SHO.FECHA (+)
   AND RFC.FECHA = IHO.FECHA (+)
   AND RFC.FECHA = CTW.FECHA (+)
 ORDER BY RFC.FECHA;

-- Autor: Mario Heredia.
-- Actualizacion: Mario Heredia. Fecha: 16.01.2014.
-- Motivo: Consulta las tablas RAW GSM y corre en el OSSRC correspondiente.

SELECT RFC.FECHA,
       TFF.CANTIDAD CANTIDAD_TFF,
       HOV.CANTIDAD CANTIDAD_HOV,
       SRV.CANTIDAD CANTIDAD_SRV,
       RES.CANTIDAD CANTIDAD_RES,
       REC.CANTIDAD CANTIDAD_REC,
       FER.CANTIDAD CANTIDAD_FER,
       COD.CANTIDAD CANTIDAD_COD,
       PCU.CANTIDAD CANTIDAD_PCU,
       RXQ.CANTIDAD CANTIDAD_RXQ,
       QOS.CANTIDAD CANTIDAD_QOS
  FROM (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM P_NBSC_TRAFFIC
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                                     AND TO_DATE('&1', 'DD.MM.YYYY') + 86399/86400
           AND BTS_GID > 0
         GROUP BY PERIOD_START_TIME
       ) TFF, --TRAFFIC
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM P_NBSC_HO
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                                     AND TO_DATE('&1', 'DD.MM.YYYY') + 86399/86400
           AND BTS_GID > 0
         GROUP BY PERIOD_START_TIME
       ) HOV, --HO
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM P_NBSC_SERVICE
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                                     AND TO_DATE('&1', 'DD.MM.YYYY') + 86399/86400
           AND BTS_GID > 0
         GROUP BY PERIOD_START_TIME
       ) SRV, --SERVICE
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM P_NBSC_RES_AVAIL
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                                     AND TO_DATE('&1', 'DD.MM.YYYY') + 86399/86400
           AND BTS_GID > 0
         GROUP BY PERIOD_START_TIME
       ) RES, --RESAVAIL
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM P_NBSC_RES_ACCESS
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                                     AND TO_DATE('&1', 'DD.MM.YYYY') + 86399/86400
           AND BTS_GID > 0
         GROUP BY PERIOD_START_TIME
       ) REC, --RESACC
       (
        SELECT FECHA, COUNT(*) CANTIDAD
          FROM (
        SELECT PERIOD_START_TIME FECHA, BTS_GID, COUNT(*) CANTIDAD
          FROM P_NBSC_FER
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                                     AND TO_DATE('&1', 'DD.MM.YYYY') + 86399/86400
           AND BTS_GID > 0
         GROUP BY PERIOD_START_TIME, BTS_GID
               )
         GROUP BY FECHA
       ) FER, --FER
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM P_NBSC_CODING_SCHEME
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                                     AND TO_DATE('&1', 'DD.MM.YYYY') + 86399/86400
           AND BTS_GID > 0
         GROUP BY PERIOD_START_TIME
       ) COD, --COD_SCH
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM P_NBSC_PACKET_CONTROL_UNIT@OSS
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                                     AND TO_DATE('&1', 'DD.MM.YYYY') + 86399/86400
           AND BTS_GID > 0
         GROUP BY PERIOD_START_TIME
       ) PCU, --PCU
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM RBS_P_RXQUAL_BTS_HOUR
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                                     AND TO_DATE('&1', 'DD.MM.YYYY') + 86399/86400
           AND BTS_GID > 0
         GROUP BY PERIOD_START_TIME
       ) RXQ, --RXQUAL
       (
        SELECT PERIOD_START_TIME FECHA, COUNT(*) CANTIDAD
          FROM RBS_P_QOS_QOSPCL_HOUR
         WHERE PERIOD_START_TIME BETWEEN TO_DATE('&1', 'DD.MM.YYYY')
                                     AND TO_DATE('&1', 'DD.MM.YYYY') + 86399/86400
           AND BSC_GID > 0
         GROUP BY PERIOD_START_TIME
       ) QOS, --QOSPCL
       (
        SELECT TO_DATE('&1', 'DD.MM.YYYY') + ((LEVEL - 1)/24) FECHA
          FROM DUAL CONNECT BY LEVEL <= 24
       ) RFC
 WHERE RFC.FECHA = TFF.FECHA (+)
   AND RFC.FECHA = HOV.FECHA (+)
   AND RFC.FECHA = SRV.FECHA (+)
   AND RFC.FECHA = RES.FECHA (+)
   AND RFC.FECHA = REC.FECHA (+)
   AND RFC.FECHA = FER.FECHA (+)
   AND RFC.FECHA = COD.FECHA (+)
   AND RFC.FECHA = PCU.FECHA (+)
   AND RFC.FECHA = RXQ.FECHA (+)
   AND RFC.FECHA = QOS.FECHA (+)
 ORDER BY RFC.FECHA;

 -- Gestion de Celdas en el OSS

SELECT *
  FROM (
SELECT O.CO_GID                                                     INT_ID,
       TRUNC(O.VALID_START_TIME)                                    VALID_START_DATE,
       TRUNC(O.VALID_FINISH_TIME)                                   VALID_FINISH_DATE,
       O.CO_PARENT_GID                                              PARENT_ID,
       O.CO_OBJECT_INSTANCE                                         OBJECT_NRO,
       O.CO_OC_ID                                                   OBJECT_CLASS,
       O.CO_DI_TOKEN,
       O.CO_STATE,
       O.CO_ADMIN_STATE,
       O.OBJECT_STATE,
       ROW_NUMBER() OVER(PARTITION BY O.CO_PARENT_GID, O.CO_GID
                             ORDER BY O.CO_TIME_STAMP DESC, O.VALID_FINISH_TIME DESC, O.OBJECT_STATE) ORDEN,
       O.CO_TIME_STAMP
  FROM ROH_UTP_COMMON_OBJECTS@OSSRC3 O
 WHERE CO_GID IN (
SELECT WCE.CO_GID
  FROM UTP_COMMON_OBJECTS@OSSRC3 WCE,
       UTP_COMMON_OBJECTS@OSSRC3 WBS,
       UTP_COMMON_OBJECTS@OSSRC3 RNC
 WHERE WCE.CO_PARENT_GID = WBS.CO_GID
   AND WBS.CO_PARENT_GID = RNC.CO_GID
   AND RNC.CO_OC_ID = 811
   AND WBS.CO_OC_ID = 812
   AND WCE.CO_OC_ID = 813
   AND RNC.CO_GID = 3000000000492
       )
   AND CO_OC_ID = 813
       )
 WHERE ORDEN = 1


SELECT WCE.CO_GID,
       WCE.CO_NAME,
       WCE.CO_STATE,
       WCE.CO_ADMIN_STATE,
       WCE.CO_TIME_STAMP,
       
       WBS.CO_GID,
       WBS.CO_NAME,
       WBS.CO_STATE,
       WBS.CO_ADMIN_STATE,
       WBS.CO_TIME_STAMP,
       
       RNC.CO_GID,
       RNC.CO_NAME,
       RNC.CO_STATE,
       RNC.CO_ADMIN_STATE,
       RNC.CO_TIME_STAMP
  FROM UTP_COMMON_OBJECTS@OSSRC3 WCE,
       UTP_COMMON_OBJECTS@OSSRC3 WBS,
       UTP_COMMON_OBJECTS@OSSRC3 RNC
 WHERE WCE.CO_PARENT_GID = WBS.CO_GID
   AND WBS.CO_PARENT_GID = RNC.CO_GID
   AND RNC.CO_OC_ID = 811
   AND WBS.CO_OC_ID = 812
   AND WCE.CO_OC_ID = 813
   AND RNC.CO_GID = 3000000000492

---------------------------------------------------------------------------------------------------------------
-- Autor: Mario Heredia. Fecha: 14.08.2014.
-- Generacion de sentencias para queue Horario

DECLARE

CURSOR FECHAS IS
SELECT DIA,
       RESULTADO,
       MAX(FECHA_DESDE) FECHA_DESDE,
       MAX(FECHA_HASTA) FECHA_HASTA
  FROM (
SELECT DIA,
       RESULTADO,
       DECODE(RESTO, 0, FECHA) FECHA_DESDE,
       DECODE(RESTO, 5, FECHA) FECHA_HASTA
  FROM (
SELECT DIA,
       TO_CHAR(FECHA, 'DD.MM.YYYY HH24') FECHA,
       FLOOR(TO_NUMBER(HORA)/ 6) RESULTADO,
       MOD(TO_NUMBER(HORA), 6) RESTO
  FROM CALIDAD_STATUS_REFERENCES
 WHERE FECHA BETWEEN TO_DATE('&fechaDesde', 'DD.MM.YYYY')
                 AND TO_DATE('&fechaHasta', 'DD.MM.YYYY') + 86399/86400
       )
       )
 GROUP BY DIA, RESULTADO
 ORDER BY DIA, RESULTADO;

CURSOR ELEMENTOS IS
SELECT 'OSSRC3' RC FROM DUAL UNION ALL
SELECT 'OSSRC2' RC FROM DUAL UNION ALL
SELECT 'OSSRC1' RC FROM DUAL;

BEGIN

FOR SYN IN FECHAS LOOP

    FOR SEN IN ELEMENTOS LOOP

        DBMS_OUTPUT.PUT_LINE('sqlplus -S / @ umtsCounterNsn_EnqIn_Rec.sql '||SYN.FECHA_DESDE||' '||SYN.FECHA_HASTA||' '||SEN.RC);
        DBMS_OUTPUT.PUT_LINE('sqlplus -S / @ umtsDetailServiceWcell_EnqInRec.sql '||SYN.FECHA_DESDE||' '||SYN.FECHA_HASTA||' '||SEN.RC);

    END LOOP;

DBMS_OUTPUT.PUT_LINE('');

END LOOP;

END;

-- Autor: Mario Heredia. Fecha: 18.10.2016.

DECLARE

CURSOR FECHAS IS
SELECT DIA,
       RESULTADO,
       MAX(FECHA_DESDE) FECHA_DESDE,
       MAX(FECHA_HASTA) FECHA_HASTA
  FROM (
SELECT DIA,
       RESULTADO,
       DECODE(RESTO, 0, FECHA) FECHA_DESDE,
       DECODE(RESTO, 5, FECHA) FECHA_HASTA
  FROM (
SELECT DIA,
       TO_CHAR(FECHA, 'DD.MM.YYYY HH24') FECHA,
       FLOOR(TO_NUMBER(HORA)/ 6) RESULTADO,
       MOD(TO_NUMBER(HORA), 6) RESTO
  FROM CALIDAD_STATUS_REFERENCES
 WHERE FECHA BETWEEN TO_DATE('&fechaDesde', 'DD.MM.YYYY')
                 AND TO_DATE('&fechaHasta', 'DD.MM.YYYY') + 86399/86400
       )
       )
 GROUP BY DIA, RESULTADO
 ORDER BY DIA, RESULTADO;

CURSOR ELEMENTOS IS
SELECT 'OSSRC3' RC FROM DUAL UNION ALL
SELECT 'OSSRC5' RC FROM DUAL UNION ALL
SELECT 'OSSRC2' RC FROM DUAL UNION ALL
SELECT 'OSSRC1' RC FROM DUAL;

V_SQL_NAME VARCHAR2(50);

BEGIN

SELECT DECODE('&3', 'DetailServiceWcell' , 'umtsDetailServiceWcellHour_EnqInRec.sql'
                  , 'DetailServiceWbts'  , 'umtsDetailServiceWbtsHour_EnqInRec.sql'
                  , 'MasterServiceWcell' , 'umtsMasterServiceWcellHour_EnqInRec.sql'
                  , 'MasterServiceWbts'  , 'umtsMasterServiceWbtsHour_EnqInRec.sql'

                  , 'DetailHoWcell'      , 'umtsDetailHoWcellHour_EnqInRec.sql'
                  , 'DetailHoWbts'       , 'umtsDetailHoWbtsHour_EnqInRec.sql'
                  , 'MasterHoWcell'      , 'umtsMasterHoWcellHour_EnqInRec.sql'
                  , 'MasterHoWbts'       , 'umtsMasterHoWbtsHour_EnqInRec.sql') SQL_NAME
  INTO V_SQL_NAME
  FROM DUAL;

DBMS_OUTPUT.PUT_LINE('');

FOR SYN IN FECHAS LOOP

    FOR SEN IN ELEMENTOS LOOP

        DBMS_OUTPUT.PUT_LINE('sqlplus -S / @ '||V_SQL_NAME||' '||SYN.FECHA_DESDE||' '||SYN.FECHA_HASTA||' '||SEN.RC);

    END LOOP;

DBMS_OUTPUT.PUT_LINE('');

END LOOP;

END;


-- Autor: Mario Heredia. Fecha: 14.08.2014.
-- Diario

DECLARE

CURSOR FECHAS IS
SELECT TO_CHAR(FECHA, 'DD.MM.YYYY') FECHA
  FROM CALIDAD_STATUS_REFERENCES
 WHERE FECHA BETWEEN TO_DATE('&fechaDesde', 'DD.MM.YYYY')
                 AND TO_DATE('&fechaHasta', 'DD.MM.YYYY') + 86399/86400
   AND HORA = '00'
 ORDER BY FECHA ASC;

CURSOR NIVELES IS
SELECT 'DAY' LS FROM DUAL UNION ALL
SELECT 'BHC' LS FROM DUAL UNION ALL
SELECT 'BHP' LS FROM DUAL;

BEGIN

FOR SEN IN NIVELES LOOP

    FOR SYN IN FECHAS LOOP

        DBMS_OUTPUT.PUT_LINE('sqlplus -S / @ umtsDetailServiceWcell_EnqInRec.sql '||SYN.FECHA||' '||SEN.LS||' OSSRC3');
        DBMS_OUTPUT.PUT_LINE('sqlplus -S / @ umtsDetailServiceWcell_EnqInRec.sql '||SYN.FECHA||' '||SEN.LS||' OSSRC2');
        DBMS_OUTPUT.PUT_LINE('sqlplus -S / @ umtsDetailServiceWcell_EnqInRec.sql '||SYN.FECHA||' '||SEN.LS||' OSSRC1');

    END LOOP;

DBMS_OUTPUT.PUT_LINE('');

END LOOP;

END;

-- Bloque PL/SQL

DECLARE

CURSOR FECHAS IS
SELECT TO_CHAR(FECHA, 'DD.MM.YYYY') FECH2
  FROM CALIDAD_STATUS_REFERENCES
 WHERE FECHA BETWEEN TO_DATE('&fechaDesde', 'DD.MM.YYYY')
                 AND TO_DATE('&fechaHasta', 'DD.MM.YYYY') + 86399/86400
   AND HORA = '00'
   AND DIA_DESC = 'DOMINGO'
 ORDER BY FECHA DESC;

CURSOR NIVELES IS
SELECT 'DAYW'    LS FROM DUAL UNION ALL
SELECT 'ISABHWC' LS FROM DUAL UNION ALL
SELECT 'ISABHWP' LS FROM DUAL;

BEGIN

FOR SEN IN NIVELES LOOP

    FOR SYN IN FECHAS LOOP

        DBMS_OUTPUT.PUT_LINE('sqlplus -S / @ umtsDetailServiceWcell_EnqInRec.sql '||SYN.FECH2||' '||SEN.LS||' OSSRC3');
        DBMS_OUTPUT.PUT_LINE('sqlplus -S / @ umtsDetailServiceWcell_EnqInRec.sql '||SYN.FECH2||' '||SEN.LS||' OSSRC2');
        DBMS_OUTPUT.PUT_LINE('sqlplus -S / @ umtsDetailServiceWcell_EnqInRec.sql '||SYN.FECH2||' '||SEN.LS||' OSSRC1');

    END LOOP;

DBMS_OUTPUT.PUT_LINE('');

END LOOP;

END;

-- Hourly Construction Sentences
DECLARE

CURSOR FECHAS IS
SELECT TO_CHAR(FECHA, 'DD.MM.YYYY HH24') FECHA,
       TO_CHAR(FECHA + 5/24, 'DD.MM.YYYY HH24') FECH2
       
  FROM CALIDAD_STATUS_REFERENCES
 WHERE FECHA BETWEEN TO_DATE('&fechaDesde', 'DD.MM.YYYY')
                 AND TO_DATE('&fechaHasta', 'DD.MM.YYYY') + 86399/86400
   AND HORA IN ('00', '06', '12', '18')
   --AND DIA_DESC = 'DOMINGO'
 ORDER BY FECHA ASC;

CURSOR NIVELES IS
SELECT 'HOUR' LS FROM DUAL;

CURSOR ELEMENTOS IS
SELECT 'RNC'     LS FROM DUAL UNION ALL
SELECT 'ALM'     LS FROM DUAL UNION ALL
SELECT 'MERCADO' LS FROM DUAL UNION ALL
SELECT 'PAIS'    LS FROM DUAL;

BEGIN

FOR SE2 IN ELEMENTOS LOOP

FOR SEN IN NIVELES LOOP

    FOR SYN IN FECHAS LOOP

        DBMS_OUTPUT.PUT_LINE('sqlplus -S / @ umts_c_nsn_hour_ne_service.sql '||SYN.FECHA||' '||SYN.FECH2||' '||SE2.LS);

    END LOOP;

DBMS_OUTPUT.PUT_LINE('');

END LOOP;

END LOOP;

END;

-- Daily Construction Sentences

DECLARE

CURSOR FECHAS IS
SELECT TO_CHAR(FECHA, 'DD.MM.YYYY') FECHA
  FROM CALIDAD_STATUS_REFERENCES
 WHERE FECHA BETWEEN TO_DATE('&fechaDesde', 'DD.MM.YYYY')
                 AND TO_DATE('&fechaHasta', 'DD.MM.YYYY') + 86399/86400
   AND HORA = '00'
   --AND DIA_DESC = 'DOMINGO'
 ORDER BY FECHA ASC;

CURSOR NIVELES IS
SELECT 'BH CS'  LS FROM DUAL UNION ALL
SELECT 'BH PS'  LS FROM DUAL UNION ALL
SELECT 'DAY PS' LS FROM DUAL;

CURSOR ELEMENTOS IS
SELECT 'RNC'     LS FROM DUAL UNION ALL
SELECT 'ALM'     LS FROM DUAL UNION ALL
SELECT 'MERCADO' LS FROM DUAL UNION ALL
SELECT 'PAIS'    LS FROM DUAL;

BEGIN

FOR SE2 IN ELEMENTOS LOOP

FOR SEN IN NIVELES LOOP

    FOR SYN IN FECHAS LOOP

        DBMS_OUTPUT.PUT_LINE('sqlplus -S / @ umts_c_nsn_service.sql '||SYN.FECHA||' '||SYN.FECHA||' '||SEN.LS||' '||SE2.LS);

    END LOOP;

DBMS_OUTPUT.PUT_LINE('');

END LOOP;

END LOOP;

END;



-- Parse Count

SELECT ROUND(100 * ((A.VALUE - B.VALUE) / A.VALUE), 4) PARSE_SOFT,
       ROUND(100 * (B.VALUE             / A.VALUE), 4) PARSE_HARD
  FROM (
SELECT VALUE FROM V$SYSSTAT WHERE NAME = 'parse count (total)'
       ) A,
       (
SELECT VALUE FROM V$SYSSTAT WHERE NAME = 'parse count (hard)'
       ) B

-- Construccion Cuadro Permisos por PROFILE

SELECT RESOURCE_NAME,
       RESOURCE_TYPE,
       MAX(CGR) CGR,
       MAX(RF)  RF,
       MAX(DEF) DEF,
       MAX(SCO) SCO,
       MAX(SRT) SRT,
       MAX(SPP) SPP
  FROM (
SELECT RESOURCE_NAME,
       RESOURCE_TYPE,
       DECODE(PROFILE, 'CGR_PROFILE'  , LIMIT) CGR,
       DECODE(PROFILE, 'RF_PROFILE'   , LIMIT) RF,
       DECODE(PROFILE, 'DEFAULT'      , LIMIT) DEF,
       DECODE(PROFILE, 'SCOTT_TEMP'   , LIMIT) SCO,
       DECODE(PROFILE, 'SMART_PROFILE', LIMIT) SRT,
       DECODE(PROFILE, 'SMART_APP'    , LIMIT) SPP
  FROM DBA_PROFILES
       )
 GROUP BY RESOURCE_NAME,
          RESOURCE_TYPE

-- Listado de Usuarios | Cruce con datos en Smart

SELECT *
  FROM (
SELECT USERNAME, PROFILE, CREATED FROM DBA_USERS WHERE ACCOUNT_STATUS = 'OPEN'
       ) A,
       (
SELECT LEGAJO, NOMBRE, APELLIDO, MAIL FROM SMART_USERS
       ) B
 WHERE A.USERNAME = B.LEGAJO (+)
 ORDER BY CREATED ASC

 SELECT USERNAME, PROFILE, CREATED FROM DBA_USERS WHERE ACCOUNT_STATUS = 'OPEN' ORDER BY CREATED ASC
 
-- Reproceso por Indicador. Modelo Radar AMX.

SELECT * FROM UMTS_CLDD_RADAR_INDICATORS

UPDATE UMTS_CLDD_RADAR_INDICATORS SET FLAG_STATUS = 'NOT ENABLED' WHERE FLAG_STATUS = 'ENABLED';

UPDATE UMTS_CLDD_RADAR_INDICATORS SET FLAG_STATUS = 'ENABLED' WHERE INDICADOR_NAME = 'TrafficLoad';

SELECT * FROM UMTS_CLDD_RADAR_INDICATORS WHERE FLAG_STATUS = 'ENABLED';

UPDATE UMTS_CLDD_RADAR_INDICATORS SET FLAG_STATUS = 'ENABLED' WHERE FLAG_STATUS = 'NOT ENABLED';

-- Generar Columnas

SELECT COLUMN_NAME,
       RPAD('       SUM('||COLUMN_NAME||')', 52, ' ')||RPAD(COLUMN_NAME, 32, ' ')||',' SE,
       RPAD('  DET_CEL_WCE_DAYM(I).'||COLUMN_NAME, 53, ' ')||':= '||RPAD('V_01_IDS(I).'||COLUMN_NAME, 51, ' ')||';' S2,
       COLUMN_ID
  FROM DBA_TAB_COLUMNS
 WHERE TABLE_NAME = 'UMTS_D_NSN_CELLRES_WCEL_HOUR'
 ORDER BY COLUMN_ID ASC

 SELECT COLUMN_NAME,
       RPAD('       AVG('||COLUMN_NAME||')', 52, ' ')||RPAD(COLUMN_NAME, 32, ' ')||',' SE,
       RPAD('  DET_CEL_WCE_DAYM(I).'||COLUMN_NAME, 53, ' ')||':= '||RPAD('V_01_IDS(I).'||COLUMN_NAME, 51, ' ')||';' S2,
       COLUMN_ID
  FROM DBA_TAB_COLUMNS
 WHERE TABLE_NAME = 'UMTS_D_NSN_HO_WCEL_BHC'
 ORDER BY COLUMN_ID ASC

-- Autor: Mario Heredia. Fecha: 07.04.2016.
-- Consulta Constructor de elementos para Procedimientos Insert en tablas RAW desde el AUX del parser.

SELECT COLUMN_NAME,
       RPAD('       NVL('||COLUMN_NAME, 41, ' ')||', 0) '||RPAD(COLUMN_NAME, 30, ' ')||',' SE,
       RPAD('  UMTS_C_WBTSMON_RAW(I).'||COLUMN_NAME, 55, ' ')||':= '||RPAD('NSN_WBS_RAW_OSSRC(I).'||COLUMN_NAME, 51, ' ')||';' S2,
       COLUMN_ID
  FROM DBA_TAB_COLUMNS
 WHERE TABLE_NAME = 'UMTS_C_NSN_WBTSMON_WBTS_RAW'
 ORDER BY COLUMN_ID ASC

 
 -- Acumulado de Procesos

SELECT FECHA,
       CANTIDAD,
       SUM(CANTIDAD) OVER(ORDER BY FECHA ASC) ACUMULADO
  FROM (
SELECT TRUNC(CREATED, 'DAY') FECHA, COUNT(*) CANTIDAD
  FROM DBA_OBJECTS
 WHERE OWNER = 'SCOTT' AND OBJECT_TYPE IN ('PROCEDURE', 'FUNCTION')
 GROUP BY TRUNC(CREATED, 'DAY')
       )
 
-- 	Find Count Partitions, Criteria Partitions Criteria SubPartition, Count SubPartitions, Name of TableSpace
 
 SELECT *
  FROM DBA_PART_TABLES
 WHERE TABLE_NAME IN (
SELECT OBJECT_NAME
  FROM DBA_OBJECTS
 WHERE CREATED >= TO_DATE('&1', 'DD.MM.YYYY')
   AND OWNER = 'SCOTT'
   AND OBJECT_TYPE = 'TABLE'
   AND OBJECT_NAME LIKE 'UMTS%'
       )
   AND OWNER = 'SCOTT'

-- Construyendo Scripts con un Select

SELECT COLUMN_NAME,
       RPAD('       SUM('||COLUMN_NAME||')', 52, ' ')||RPAD(COLUMN_NAME, 32, ' ')||',' SE,
       RPAD('  DET_CELLRES_NE(I).'||COLUMN_NAME, 53, ' ')||':= '||
       RPAD('V_'||LPAD(FLOOR(COLUMN_ID / 95) + 1, 2, '0')||'_IDS(I).'||COLUMN_NAME, 51, ' ')||';' S2,
       COLUMN_ID,
       MOD(COLUMN_ID, 95) MODA,
       LPAD(FLOOR(COLUMN_ID / 95) + 1, 2, '0') FF
  FROM DBA_TAB_COLUMNS
 WHERE TABLE_NAME = 'UMTS_D_NSN_CELLRES_WCEL_HOUR'
 ORDER BY COLUMN_ID ASC

 -- Historico de DataFiles por TBS
 
SELECT B.TABLESPACE_NAME,
      ROUND(B.BYTES / 1024 / 1024, 2) MBYTES,
      B.FILE_NAME,
      BLOCK_SIZE,
      TRUNC(A.CREATION_TIME) CREATION_TIME
 FROM V$DATAFILE A,
      DBA_DATA_FILES B
WHERE A.FILE# = B.FILE_ID
  AND B.TABLESPACE_NAME IN ('TBS_INDEXES_UMTS_HOURLY', 'TBS_UMTS_C_NSN_HOURLY')
ORDER BY B.TABLESPACE_NAME, A.CREATION_TIME ASC

-- Replace Masivo de Procedimientos
-- #MASSIVE #PROCEDURE #REBUILD #REPLACE

DECLARE

CURSOR A IS 
SELECT TEXT,
       NAME,
       LINE,
       MAX(LINE) OVER(PARTITION BY NAME) MAX_LINE 
  FROM DBA_SOURCE
 WHERE OWNER = 'SCOTT'
   AND NAME IN (
SELECT DISTINCT NAME--, COUNT(*) CANTIDAD
  FROM DBA_SOURCE
 WHERE OWNER = 'SCOTT'
   AND NAME LIKE 'P_UMTS_DET_CER_WBS_%'
   AND NAME NOT IN ('P_UMTS_DET_CER_WBS_HOUR_INS', 'P_UMTS_DET_CER_WBS_BHC_INS', 'P_UMTS_DET_CER_WBS_BHP_INS', 'P_UMTS_DET_CER_WBS_DAY_INS')

       )
   --AND INSTR(TEXT, 'INSERT INTO') > 1
 ORDER BY NAME, LINE ASC;

BEGIN
  
FOR SEN IN A LOOP

IF SEN.LINE = 1 THEN

   DBMS_OUTPUT.PUT_LINE(REPLACE(REPLACE(SEN.TEXT, CHR(10)), 'PROCEDURE', 'CREATE OR REPLACE PROCEDURE'));

ELSIF SEN.LINE = SEN.MAX_LINE THEN

   DBMS_OUTPUT.PUT_LINE(REPLACE(SEN.TEXT, CHR(10)));
   DBMS_OUTPUT.PUT_LINE('/');
   DBMS_OUTPUT.PUT_LINE('');

ELSE

   DBMS_OUTPUT.PUT_LINE(REPLACE(SEN.TEXT, CHR(10)));

END IF;

END LOOP;

END;

-- SYNONYM SINONIMOS RECONSTRUCCION

SELECT 'CREATE OR REPLACE PUBLIC SYNONYM '|| OBJECT_NAME ||' FOR '|| OWNER || '.' || OBJECT_NAME || ';' SE
  FROM DBA_PROCEDURES
 WHERE OWNER = 'SCOTT' AND OBJECT_NAME LIKE 'P_UMTS_C_%RAW_INS'

 
-- Autor: Mario Heredia. Fecha: 01.12.2015.
-- Replace Masivo de datos por Tabla
-- Construccion Sentencia de Delete

DECLARE

CURSOR COLUMNAS IS
SELECT TABLE_NAME,
       COLUMN_NAME,
       COLUMN_ID,
       MAX(COLUMN_ID) OVER(PARTITION BY TABLE_NAME) MAX_COLUMN_ID
  FROM DBA_TAB_COLUMNS
 WHERE TABLE_NAME IN (
SELECT TABLE_NAME FROM DBA_TABLES WHERE TABLE_NAME LIKE 'UMTS_C_NSN_%MNC1_RAW'
       )
   AND COLUMN_ID = 1
 ORDER BY COLUMN_ID ASC;

V_COMA VARCHAR2(1);

BEGIN
  
FOR SYN IN COLUMNAS LOOP

SELECT DECODE(SYN.COLUMN_ID, SYN.MAX_COLUMN_ID, '', ',') COMA
  INTO V_COMA
  FROM DUAL;


IF SYN.COLUMN_ID = 1 THEN

DBMS_OUTPUT.PUT_LINE('DELETE FROM '||SYN.TABLE_NAME);
DBMS_OUTPUT.PUT_LINE(' WHERE PERIOD_START_TIME BETWEEN TO_DATE(''#1 #2'', ''DD.MM.YYYY HH24'')');
DBMS_OUTPUT.PUT_LINE('                             AND TO_DATE(''#3 #4'', ''DD.MM.YYYY HH24'');');
DBMS_OUTPUT.PUT_LINE('');
DBMS_OUTPUT.PUT_LINE('COMMIT;');
DBMS_OUTPUT.PUT_LINE('');
DBMS_OUTPUT.PUT_LINE('');

END IF;

END LOOP;

END;

-- Construccion Sentencia de Insert

DECLARE

CURSOR COLUMNAS IS
SELECT TABLE_NAME,
       COLUMN_NAME,
       COLUMN_ID,
       MAX(COLUMN_ID) OVER(PARTITION BY TABLE_NAME) MAX_COLUMN_ID
  FROM DBA_TAB_COLUMNS
 WHERE TABLE_NAME IN (
SELECT TABLE_NAME FROM DBA_TABLES WHERE TABLE_NAME LIKE 'UMTS_C_NSN_%MNC1_RAW'
       )
 ORDER BY TABLE_NAME, COLUMN_ID ASC;

V_COMA VARCHAR2(1);

BEGIN
  
FOR SYN IN COLUMNAS LOOP

SELECT DECODE(SYN.COLUMN_ID, SYN.MAX_COLUMN_ID, '', ',') COMA
  INTO V_COMA
  FROM DUAL;


IF SYN.COLUMN_ID = 1 THEN

DBMS_OUTPUT.PUT_LINE('INSERT INTO '||SYN.TABLE_NAME);
DBMS_OUTPUT.PUT_LINE('SELECT PERIOD_START_TIME + 7'||V_COMA);

ELSIF SYN.COLUMN_ID = SYN.MAX_COLUMN_ID THEN

DBMS_OUTPUT.PUT_LINE('       '||SYN.COLUMN_NAME||V_COMA);
DBMS_OUTPUT.PUT_LINE('  FROM '||SYN.TABLE_NAME);
DBMS_OUTPUT.PUT_LINE(' WHERE PERIOD_START_TIME BETWEEN TO_DATE(''#1 #2'', ''DD.MM.YYYY HH24'')');
DBMS_OUTPUT.PUT_LINE('                             AND TO_DATE(''#3 #4'', ''DD.MM.YYYY HH24'');');
DBMS_OUTPUT.PUT_LINE('');
DBMS_OUTPUT.PUT_LINE('COMMIT;');
DBMS_OUTPUT.PUT_LINE('');
DBMS_OUTPUT.PUT_LINE('');

ELSE
  
DBMS_OUTPUT.PUT_LINE('       '||SYN.COLUMN_NAME||V_COMA);

END IF;

END LOOP;

END;

--TAB=xmlFilesRC2
SELECT SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)) PATH_HOUR,
       STATUS,
       COUNT(*) CANTIDAD
  FROM STATUS_PROCESS_ETL
 WHERE FILENAME LIKE '%.all'
   AND SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)) LIKE '/calidad/data/nsn/storage/xml/rc2/pm/&"day"__/'
 GROUP BY SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)), STATUS
 ORDER BY PATH_HOUR;

--TAB=xmlFilesRC3
SELECT SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)) PATH_HOUR,
       STATUS,
       COUNT(*) CANTIDAD
  FROM STATUS_PROCESS_ETL
 WHERE FILENAME LIKE '%.all'
   AND SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)) LIKE '/calidad/data/nsn/storage/xml/rc3/pm/&"day"__/'
 GROUP BY SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)), STATUS
 ORDER BY PATH_HOUR;
 
UPDATE STATUS_PROCESS_ETL
   SET STATUS = 0
 WHERE FILENAME='/calidad/data/nsn/storage/xml/rc2/pm/2015112301/etlexpmx_BTS_2015112301.SERVICE.csv.all'
   AND STATUS = 1
   --AND NETWORK_ELEMENT = 'BTS'
   --AND FILENAME LIKE '%.all'

UPDATE STATUS_PROCESS_ETL
   SET STATUS = 0
 WHERE SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)) LIKE '/calidad/data/nsn/storage/xml/rc2/pm/2015113003/'
   AND STATUS = 1
   AND NETWORK_ELEMENT = 'WCEL'
   AND FILENAME LIKE '%.all'

UPDATE STATUS_PROCESS_ETL
   SET STATUS = 1,
       DATE_PROCESS = SYSDATE
 WHERE SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)) LIKE '/calidad/data/nsn/storage/xml/rc2/pm/20151130__/'
   AND STATUS = 0
   AND NETWORK_ELEMENT = 'BTS'
   AND FILENAME LIKE '%.all'

UPDATE STATUS_PROCESS_ETL
   SET STATUS = 0
 WHERE FILENAME LIKE '%.all'
   AND NETWORK_ELEMENT = 'WCEL'
   AND SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)) = '/calidad/data/nsn/storage/xml/rc2/pm/2015112223/'
   AND STATUS = 5;

SELECT FILENAME,
       --INSTR(FILENAME, '/', -1) ULTIMO_SLASH,
       --SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)) PATH_HOUR,
       STATUS,
       NETWORK_ELEMENT,
       MEASUREMENT_TYPE
  FROM STATUS_PROCESS_ETL
 WHERE /*STATUS = 0
   AND */FILENAME LIKE '%.all'
   AND NETWORK_ELEMENT = 'BTS'
   --AND SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)) LIKE '/calidad/data/nsn/storage/xml/rc2/pm/20151122__/'
   --AND MEASUREMENT_TYPE = 'SERVICE'
   AND STATUS = 0
 --GROUP BY SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)), NETWORK_ELEMENT, STATUS
 ORDER BY DATE_IMPORT DESC;

SELECT SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)) PATH_HOUR,
       STATUS,
       NETWORK_ELEMENT,
       COUNT(*) CANTIDAD
  FROM STATUS_PROCESS_ETL
 WHERE FILENAME LIKE '%.all'
   AND SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)) LIKE '/calidad/data/nsn/storage/xml/rc2/pm/&"day"__/'
 GROUP BY SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)), NETWORK_ELEMENT, STATUS
 ORDER BY NETWORK_ELEMENT, PATH_HOUR;

SELECT SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)) PATH_HOUR,
       STATUS,
       COUNT(*) CANTIDAD
  FROM STATUS_PROCESS_ETL
 WHERE FILENAME LIKE '%.all'
   AND SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)) LIKE '/calidad/data/nsn/storage/xml/rc2/pm/&"day"__/'
 GROUP BY SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)), STATUS
 ORDER BY PATH_HOUR;

SELECT SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)) PATH,
       COUNT(*) CANTIDAD FROM HARRIAGUE.ALL_C_NSN_EXTERNAL_RC2
 WHERE FILENAME LIKE '%.all'
 GROUP BY SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1))
   --AND SUBSTR(FILENAME, 1, INSTR(FILENAME, '/', -1)) LIKE '/calidad/data/nsn/storage/xml/rc2/pm/&"day"__/'



-- Autor: Mario Heredia. Fecha: 05.05.2016.
-- Construccion de sentencia de Truncate Subpartition para las funciones de ReInsercion.

DECLARE

CURSOR C_VENTANA (P_PARTICION_ESQUEMA_MSC_FECHA CHAR) IS
SELECT TO_CHAR(FECHA, P_PARTICION_ESQUEMA_MSC_FECHA) FECHA
  FROM CALIDAD_STATUS_REFERENCES
 WHERE FECHA BETWEEN TO_DATE('&1 &2', 'DD.MM.YYYY HH24')
                 AND TO_DATE('&3 &4', 'DD.MM.YYYY HH24')
 ORDER BY FECHA DESC;

CURSOR C_TABLES (P_OSSRC CHAR) IS
SELECT A.NOMBRE_TABLA,
       A.PARTICION_ESQUEMA,
       A.PARTICION_ESQUEMA_MSC_FECHA,
       A.PARTICION_FORMATO_MSC_FECHA,
       CASE WHEN P_OSSRC = 'OSSRC3'  THEN 'RC3'
            WHEN P_OSSRC = 'OSSRC2'  THEN 'RC2'
            WHEN P_OSSRC = 'OSSRC1'  THEN 'RC1'
            ELSE NULL END SUBPARTICION_NAME
  FROM CALIDAD_PARAMETROS_TABLAS A
 WHERE NOMBRE_TABLA IN ('UMTS_C_NSN_CELLRES_MNC1_RAW',
                        'UMTS_C_NSN_CELLTP_MNC1_RAW',
                        'UMTS_C_NSN_CELTPW_MNC1_RAW',
                        'UMTS_C_NSN_CPICHQ_MNC1_RAW',
                        'UMTS_C_NSN_HSDPAW_MNC1_RAW',
                        'UMTS_C_NSN_INTERSHO_MNC1_RAW',
                        'UMTS_C_NSN_INTSYSHO_MNC1_RAW',
                        'UMTS_C_NSN_L3IUB_MNC1_RAW',
                        'UMTS_C_NSN_PKTCALL_MNC1_RAW',
                        'UMTS_C_NSN_RRC_MNC1_RAW',
                        'UMTS_C_NSN_SERVLEV_MNC1_RAW',
                        'UMTS_C_NSN_SOFTHO_MNC1_RAW',
                        'UMTS_C_NSN_TRAFFIC_MNC1_RAW');

V_LINEA VARCHAR2(200);

BEGIN

FOR SY2 IN C_TABLES ('&OSSRC') LOOP

FOR SYN IN C_VENTANA (SY2.PARTICION_ESQUEMA_MSC_FECHA) LOOP

SELECT 'ALTER TABLE SCOTT.'||SY2.NOMBRE_TABLA||' TRUNCATE SUBPARTITION '||
       SY2.PARTICION_ESQUEMA||SYN.FECHA||'_'||SY2.SUBPARTICION_NAME||';' LINEA
  INTO V_LINEA
  FROM DUAL;

/*BEGIN

EXECUTE IMMEDIATE V_LINEA;

EXCEPTION

WHEN OTHERS THEN
NULL;

END;*/

DBMS_OUTPUT.PUT_LINE(V_LINEA);

END LOOP;

DBMS_OUTPUT.PUT_LINE('');

END LOOP;

END;
/

-- Constructor de los Create Table

DECLARE

CURSOR TABLAS IS
SELECT TABLE_NAME NEW_TABLE_NAME, TABLE_NAME
  FROM DBA_TABLES
 WHERE TABLE_NAME IN (

'GSM_C_NSN_HO_AUX',
'GSM_C_NSN_CODINGSC_AUX',
'GSM_C_NSN_GBOVIP_AUX',
'GSM_C_NSN_RESACC_AUX',
'GSM_C_NSN_SERVICE_AUX',
'GSM_C_NSN_TRAFFIC_AUX',
'GSM_C_NSN_LOAD_AUX',
'GSM_C_NSN_QOS_AUX',
'GSM_C_NSN_POWER_AUX',
'GSM_C_NSN_RESAVAIL_AUX',
'GSM_C_NSN_PCU_AUX',
'GSM_C_NSN_RXQUAL_AUX',
'GSM_C_NSN_FER_AUX',
'GSM_C_NSN_DYNABIS_AUX'

)
   --AND PARTITIONED = 'YES'
 ORDER BY TABLE_NAME;

CURSOR COLUMNAS (TABLE_IN IN VARCHAR2) IS
SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH,
       CASE WHEN MAX(COLUMN_ID) OVER(PARTITION BY TABLE_NAME) = COLUMN_ID THEN
       RPAD(COLUMN_NAME, 35)||DATA_TYPE||DECODE(DATA_TYPE, 'VARCHAR2', '('||DATA_LENGTH||')') ELSE
       RPAD(COLUMN_NAME, 35)||DATA_TYPE||DECODE(DATA_TYPE, 'VARCHAR2', '('||DATA_LENGTH||')')||',' END SE
  FROM (
SELECT O.NAME           TABLE_NAME,
       C.NAME           COLUMN_NAME,
       --C.TYPE#,
       --C.CHARSETFORM,
       --C.SCALE,
       --C.PRECISION#,
       DECODE(C.TYPE#,   1, DECODE(C.CHARSETFORM, 2, 'NVARCHAR2', 'VARCHAR2'),
                         2, DECODE(C.SCALE, NULL, DECODE(C.PRECISION#, NULL, 'NUMBER', 'FLOAT'), 'NUMBER'),
                         8, 'LONG',
                        12, 'DATE',
                        23, 'RAW', 24, 'LONG RAW',
                        69, 'ROWID',
                        96, DECODE(C.CHARSETFORM, 2, 'NCHAR', 'CHAR'),
                       112, DECODE(C.CHARSETFORM, 2, 'NCLOB', 'CLOB'),
                       113, 'BLOB', 114, 'BFILE', 115, 'CFILE', 'UNDEFINED') DATA_TYPE,
       C.LENGTH                                    DATA_LENGTH,
       DECODE(C.COL#, 0, TO_NUMBER(NULL), C.COL#) COLUMN_ID
  FROM SYS.COL$ C,
       SYS.OBJ$ O
 WHERE O.OBJ# = C.OBJ#
   AND O.OWNER#=28
   AND O.TYPE#=2
   AND O.NAME = TABLE_IN
       )
 ORDER BY COLUMN_ID;


CURSOR PARTICIONES (TABLE_IN IN VARCHAR2) IS
SELECT TABLE_NAME,
       PARTITION_NAME,
       PARTITION_POSITION,
       H,
       --'PARTITION '||PARTITION_NAME||' VALUES LESS THAN (TO_DATE('''||H||''', ''YYYY.MM.DD HH24''))'||COMA SE
       'PARTITION '||PARTITION_NAME||' VALUES LESS THAN ('||H||')'||COMA SE
  FROM (
SELECT TABLE_NAME,
       PARTITION_NAME,
       PARTITION_POSITION,
       MAX_PARTITION_POSITION,
       DECODE(PARTITION_POSITION, MAX_PARTITION_POSITION, '', ',') COMA,
       --REPLACE(TRIM(SUBSTR(H, INSTR(H, ' ', 1), 11)), ', ''NLS_CALENDAR=GREGORIAN''', '') H
       REPLACE(REPLACE(REPLACE(H, ', ''NLS_CALENDAR=GREGORIAN''', ''), 'SYYYY', 'YYYY'), 'TO_DATE('' 201', 'TO_DATE(''201') H
  FROM (
SELECT TABLE_NAME,
       PARTITION_NAME,
       PARTITION_POSITION,
       MAX(PARTITION_POSITION) OVER(PARTITION BY TABLE_NAME) MAX_PARTITION_POSITION,
       DBMS_LOB.SUBSTR(HIGH_VALOR, 4000, 1) H
  FROM AUX_PARTICIONES
 WHERE TABLE_NAME = TABLE_IN
       )
 WHERE PARTITION_POSITION IN (1, 2, MAX_PARTITION_POSITION - 1, MAX_PARTITION_POSITION)
       )
 ORDER BY PARTITION_POSITION, TABLE_NAME;
   
BEGIN

FOR S_T IN TABLAS LOOP

    DBMS_OUTPUT.PUT_LINE('DROP TABLE '||S_T.NEW_TABLE_NAME||' PURGE;');
    DBMS_OUTPUT.PUT_LINE('');

    DBMS_OUTPUT.PUT_LINE('CREATE TABLE '||S_T.NEW_TABLE_NAME||' (');
    
    FOR S_C IN COLUMNAS (S_T.TABLE_NAME) LOOP
    
    DBMS_OUTPUT.PUT_LINE('  '||S_C.SE);
    
    END LOOP;

    DBMS_OUTPUT.PUT_LINE(') TABLESPACE TBS_AUXILIAR PCTFREE 5 PCTUSED 95;');
    --DBMS_OUTPUT.PUT_LINE('  PARTITION BY RANGE (FECHA)');
    --DBMS_OUTPUT.PUT_LINE('(');

    --FOR S_P IN PARTICIONES (S_T.TABLE_NAME) LOOP

    --DBMS_OUTPUT.PUT_LINE('  '||S_P.SE);

    --END LOOP;

    --DBMS_OUTPUT.PUT_LINE(');');
    DBMS_OUTPUT.PUT_LINE('');

END LOOP;

END;

-- Armado de las vistas para el acceso a las tablas AUX.

DECLARE

CURSOR CONTADORES IS
SELECT SPARE001_CHR,
       DECODE(COLUMN_NAME, 'STARTTIME', 'PERIOD_START_TIME',
                           'INTERVAL' , 'PERIOD_DURATION',
                           'OSSRC'    , 'OSSRC',
                           'WCEL'     , 'WCELL_GID',
                           'WBTS'     , 'WBTS_GID',
                           'RNC'      , 'RNC_GID',
                           'MCC'      , 'MCC_ID',
                           'MNC'      , 'MNC_ID',
                           SPARE002_CHR) SPARE002_CHR,
       /*SPARE006_CHR, SPARE010_CHR, */
       SPARE101_CHR,
       TABLE_NAME,
       COLUMN_NAME,
       --COLUMN_ID,
       TABLE_NAME_RAW,
       VIEW_NAME,
       COLUMN_ID ORDEN,
       MAX(COLUMN_ID) OVER(PARTITION BY TABLE_NAME) MAX_ORDEN
  FROM (
SELECT SPARE001_CHR, SPARE002_CHR, /*SPARE006_CHR, SPARE010_CHR, */SPARE101_CHR,
       ROW_NUMBER() OVER(PARTITION BY SPARE006_CHR ORDER BY SPARE001_CHR ASC) ORDEN
  FROM CALIDAD_MAP_INDICATORS
 WHERE SPARE100_CHR = 'ENABLED'
   AND SPARE101_CHR LIKE 'UMTS_C_NSN_%WBTSMON%_RAW'
       ) A,
       (
SELECT TABLE_NAME,
       COLUMN_NAME,
       COLUMN_ID,
       REPLACE(TABLE_NAME, 'AUX', 'RAW') TABLE_NAME_RAW,
       REPLACE(TABLE_NAME, 'AUX', 'VW') VIEW_NAME
  FROM DBA_TAB_COLUMNS
 WHERE TABLE_NAME LIKE 'UMTS_C_NSN%WBTSMON%AUX'
       ) B
 WHERE B.TABLE_NAME_RAW = A.SPARE101_CHR (+)
   AND B.COLUMN_NAME = A.SPARE001_CHR (+)
 ORDER BY TABLE_NAME, ORDEN ASC;

LINEA         VARCHAR2(50);
V_COMA        VARCHAR2(10);
V_WKEY        VARCHAR2(10);
V_COM2        VARCHAR2(10);
V_COLUMN_NAME VARCHAR2(30);
V_VALOR       VARCHAR2(50);

BEGIN

FOR SEN IN CONTADORES LOOP

    IF SEN.ORDEN = SEN.MAX_ORDEN THEN V_COMA := NULL;
                                 ELSE V_COMA := ',' ; END IF;


    IF SEN.ORDEN = 1 THEN
    
    DBMS_OUTPUT.PUT_LINE('CREATE OR REPLACE VIEW '||SEN.VIEW_NAME||' AS');
    --DBMS_OUTPUT.PUT_LINE('(');
    
    END IF;
    
    IF SEN.SPARE002_CHR = 'PERIOD_START_TIME' THEN

    DBMS_OUTPUT.PUT_LINE(RPAD('SELECT CAST(STARTTIME AS DATE)', 31, ' ')||SEN.SPARE002_CHR||V_COMA);

    ELSIF SEN.COLUMN_NAME IN ('WCEL', 'WBTS', 'RNC') THEN

    V_VALOR := '';
    --DBMS_OUTPUT.PUT_LINE('       '||'F_UMTS_CLDD_OBJ_GID_GET ('''||SEN.SPARE002_CHR||''', ');
  
    FOR N IN 1..3 LOOP

    SELECT DECODE(N, 3, 'WCEL'
                   , 2, 'WBTS'
                   , 1, 'RNC') WKEY,
           DECODE(N, 3, ')'
                   , 2, ', '
                   , 1, ', ') COMA,
           DECODE(N, 3, ' '||REPLACE(SEN.SPARE002_CHR, 'GID', 'ID')||V_COMA
                   , 2, NULL
                   , 1, NULL) COLUMN_NAME
      INTO V_WKEY,
           V_COM2,
           V_COLUMN_NAME
      FROM DUAL;

    SELECT V_VALOR||V_WKEY||V_COM2||V_COLUMN_NAME
      INTO V_VALOR
      FROM DUAL;

    --V_VALOR := V_VALOR||V_WKEY||V_COM2||V_COLUMN_NAME;

    --DBMS_OUTPUT.PUT_LINE(V_WKEY||V_COM2||V_COLUMN_NAME); 

    END LOOP;

    DBMS_OUTPUT.PUT_LINE('       '||'F_UMTS_CLDD_OBJ_GID_GET ('''||SEN.SPARE002_CHR||''', '||V_VALOR);

    ELSE

    DBMS_OUTPUT.PUT_LINE(RPAD('       '||SEN.COLUMN_NAME, 31, ' ')||SEN.SPARE002_CHR||V_COMA);

    END IF;
    
    IF SEN.ORDEN = SEN.MAX_ORDEN THEN
      
    DBMS_OUTPUT.PUT_LINE('  FROM '||SEN.TABLE_NAME);

    DBMS_OUTPUT.PUT_LINE('  WITH READ ONLY;');
    DBMS_OUTPUT.PUT_LINE('');

    END IF;

END LOOP;

END;

-- Bloque Reconstruccion de Vistas. Fecha: Ago.2018.

DECLARE

CURSOR VISTAS IS
SELECT VIEW_NAME,
       TEXT
  FROM DBA_VIEWS
 WHERE OWNER = 'SCOTT'
   AND VIEW_NAME LIKE 'GSM_C_%';

BEGIN
  

FOR SYN IN VISTAS LOOP

DBMS_OUTPUT.PUT_LINE('CREATE OR REPLACE VIEW '||SYN.VIEW_NAME||' AS');  
DBMS_OUTPUT.PUT_LINE(SYN.TEXT);

END LOOP;

END;


-- Bloque construccion. Sentencias de disponibilidad GSM

DECLARE

CURSOR FECHAS IS
SELECT TO_CHAR(FECHA, 'DD.MM.YYYY') FECHA
  FROM CALIDAD_STATUS_REFERENCES
 WHERE FECHA BETWEEN TO_DATE('&fechaDesde', 'DD.MM.YYYY')
                 AND TO_DATE('&fechaHasta', 'DD.MM.YYYY') + 86399/86400
   AND HORA = '00'
 ORDER BY FECHA DESC;

CURSOR ELEMENTOS IS
SELECT 'BTS'          LS FROM DUAL UNION ALL
SELECT 'BCF'          LS FROM DUAL UNION ALL
SELECT 'BSC'          LS FROM DUAL UNION ALL
SELECT 'CO'           LS FROM DUAL UNION ALL
SELECT 'SUPERVISION'  LS FROM DUAL UNION ALL
SELECT 'GERENCIA'     LS FROM DUAL UNION ALL
SELECT 'ALM'          LS FROM DUAL UNION ALL
SELECT 'MERCADO'      LS FROM DUAL UNION ALL
SELECT 'PAIS'         LS FROM DUAL;

BEGIN

FOR SYN IN FECHAS LOOP

    FOR SE2 IN ELEMENTOS LOOP

      IF SE2.LS = 'BTS' THEN 

      DBMS_OUTPUT.PUT_LINE('sqlplus -S / @ gsm_nsn_noc_bts_dayo_ins.sql '||SYN.FECHA);

      ELSE
      
      DBMS_OUTPUT.PUT_LINE('sqlplus -S / @ gsm_nsn_noc_dayo_ins.sql '||SYN.FECHA||' '||SE2.LS||' DAYO');
      
      END IF;

      IF SE2.LS IN ('BTS', 'BCF') THEN 
      
      DBMS_OUTPUT.PUT_LINE('sqlplus -S / @ gsm_nsn_noc_mge.sql '||SYN.FECHA||' '||SE2.LS||' DAYO');
      
      END IF;


    END LOOP;

DBMS_OUTPUT.PUT_LINE('');

END LOOP;

END;

-- Bloque construccion. Sentencias de disponibilidad UMTS

DECLARE

CURSOR FECHAS IS
SELECT TO_CHAR(FECHA, 'DD.MM.YYYY') FECHA
  FROM CALIDAD_STATUS_REFERENCES
 WHERE FECHA BETWEEN TO_DATE('&fechaDesde', 'DD.MM.YYYY')
                 AND TO_DATE('&fechaHasta', 'DD.MM.YYYY') + 86399/86400
   AND HORA = '00'
 ORDER BY FECHA DESC;

CURSOR ELEMENTOS IS
SELECT 'WCELL'        LS FROM DUAL UNION ALL
SELECT 'WBTS'         LS FROM DUAL UNION ALL
SELECT 'RNC'          LS FROM DUAL UNION ALL
SELECT 'CO'           LS FROM DUAL UNION ALL
SELECT 'SUPERVISION'  LS FROM DUAL UNION ALL
SELECT 'GERENCIA'     LS FROM DUAL UNION ALL
SELECT 'ALM'          LS FROM DUAL UNION ALL
SELECT 'MERCADO'      LS FROM DUAL UNION ALL
SELECT 'PAIS'         LS FROM DUAL;

BEGIN

FOR SYN IN FECHAS LOOP

    FOR SE2 IN ELEMENTOS LOOP

      IF SE2.LS = 'BTS' THEN 

      DBMS_OUTPUT.PUT_LINE('sqlplus -S / @ umts_nsn_noc_wcell_dayo_ins.sql '||SYN.FECHA);

      ELSE
      
      DBMS_OUTPUT.PUT_LINE('sqlplus -S / @ umts_nsn_noc_summary_dayo.sql '||SYN.FECHA||' '||SE2.LS||' DAYO');
      
      END IF;

      IF SE2.LS IN ('WCELL', 'WBTS') THEN 
      
      DBMS_OUTPUT.PUT_LINE('sqlplus -S / @ umts_nsn_noc_mge.sql '||SYN.FECHA||' '||SE2.LS||' DAYO');
      
      END IF;


    END LOOP;

DBMS_OUTPUT.PUT_LINE('');

END LOOP;

END;


#####################################################################################################################
# Counter List Management
#####################################################################################################################

SELECT * FROM AUX_CALIDAD_MEAS_COUNTERS

SELECT ELEMENT_CLASS, MEASUREMENT_NAME, COUNT(*) CANTIDAD FROM CALIDAD_MEAS_COUNTERS
 WHERE OSSRC = 'RC3'
 GROUP BY ELEMENT_CLASS, MEASUREMENT_NAME


SELECT COUNTER_NAME FROM CALIDAD_MEAS_COUNTERS WHERE MEASUREMENT_NAME = 'Packet_call' AND ELEMENT_CLASS = 'WCEL' AND OSSRC = 'RC2'
 MINUS
SELECT COUNTER_NAME FROM CALIDAD_MEAS_COUNTERS WHERE MEASUREMENT_NAME = 'Packet_call' AND ELEMENT_CLASS = 'WCEL' AND OSSRC = 'RC3'
 MINUS
SELECT COUNTER_NAME FROM CALIDAD_MEAS_COUNTERS WHERE MEASUREMENT_NAME = 'Packet_call' AND ELEMENT_CLASS = 'WCEL' AND OSSRC = 'RC2'

SELECT COUNTER_NAME FROM CALIDAD_MEAS_COUNTERS WHERE MEASUREMENT_NAME = 'Cell_Throughput_WBTS' AND ELEMENT_CLASS = 'SBTS' AND OSSRC = 'RC3'
 MINUS
SELECT COUNTER_NAME FROM CALIDAD_MEAS_COUNTERS WHERE MEASUREMENT_NAME = 'Cell_Throughput_WBTS' AND ELEMENT_CLASS = 'WCEL' AND OSSRC = 'RC3'


#####################################################################################################################
# Parser
#####################################################################################################################

select 'nsnProcessFtpPMDataHourlyRecover2.sh '||TO_CHAR(TO_DATE('&1', 'DD.MM.YYYY HH24') + (LEVEL -1) / 24, 'DD.MM.YYYY HH24')||' OSSRC1' SE FROM DUAL CONNECT BY LEVEL <= 36 

-- BORRADO DE TXT CON LAS LISTAS DE LOS .ALL A INSERTAR
SELECT 'rm *_'||TO_CHAR(TRUNC(SYSDATE) -LEVEL, 'DD.MM.YYYY.')||'*.txt' FROM DUAL CONNECT BY LEVEL < 30


#####################################################################################################################
# UNIX
#####################################################################################################################

sed 's/OSSRC2/OSSRC3/g' managedObject.BCF.OSSRC2.ctl    > managedObject.BCF.OSSRC3.ctl
sed 's/OSSRC2/OSSRC3/g' managedObject.BSC.OSSRC2.ctl    > managedObject.BSC.OSSRC3.ctl
sed 's/OSSRC2/OSSRC3/g' managedObject.BTS.OSSRC2.ctl    > managedObject.BTS.OSSRC3.ctl
sed 's/OSSRC2/OSSRC3/g' managedObject.LNBTS.OSSRC2.ctl  > managedObject.LNBTS.OSSRC3.ctl
sed 's/OSSRC2/OSSRC3/g' managedObject.LNCEL.OSSRC2.ctl  > managedObject.LNCEL.OSSRC3.ctl
sed 's/OSSRC2/OSSRC3/g' managedObject.MRBTS.OSSRC2.ctl  > managedObject.MRBTS.OSSRC3.ctl
sed 's/OSSRC2/OSSRC3/g' managedObject.RNC.OSSRC2.ctl    > managedObject.RNC.OSSRC3.ctl
sed 's/OSSRC2/OSSRC3/g' managedObject.TRX.OSSRC2.ctl    > managedObject.TRX.OSSRC3.ctl
sed 's/OSSRC2/OSSRC3/g' managedObject.WBTS.OSSRC2.ctl   > managedObject.WBTS.OSSRC3.ctl
sed 's/OSSRC2/OSSRC3/g' managedObject.WCEL.OSSRC2.ctl   > managedObject.WCEL.OSSRC3.ctl

 -- Crea lista de ManagedObjectUnica
 grep managedObject plmnrc7_Nokia3g | nawk 'BEGIN { FS = "\"" }{ print $2 }' | sort -u
 
 -- Lista los .log anteriores al dia de ayer.
 find . -name "*.log" -mtime +1 -exec ls -ltr {} \;
 -- Borra todos los .log anteriores al dia de ayer.
 find . -name "*.log" -mtime +1 -exec rm -f {} \;
  
# Busquedas masivas

find . -name "*.sql" > sql.txt
nawk -v P="grep 'LATENCY' " -v A="echo '" -v B="' >> result.txt | " '{ printf "%-5s%-5s%-5s%-5s%-5s%-3s\n", A, $0, B, P, $0, " >> result.txt" }' sql.txt > sql.tx2


nawk -v P="grep 'NOKRWW_PS_ULOAD_UNITID_RAW' " -v A="echo '" -v B="' >> aa.txt | " '{ printf "%-5s%-5s%-5s%-5s%-5s%-3s\n", A, $0, B, P, $0, " >> aa.txt" }' sql.txt > sql.txt.2
nawk -v P="grep 'NOKRWW_PS_ULOAD_UNITID_RAW' " -v A="echo '" -v B="' >> aa.sh.txt | " '{ printf "%-5s%-5s%-5s%-5s%-5s%-3s\n", A, $0, B, P, $0, " >> aa.sh.txt" }' sh.txt > sh.txt.2




