
---------------------------------------------------------------------------------------------------------
-- Autor: Mario Heredia. Motivo: Tiempo de Eventos
-- Tags: CHECK, EVENT, EVENTO, STAT
--------------------------------------------------------------------------------------------------------- 

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
 ORDER BY EVENT_ID, SNAP_ID;
