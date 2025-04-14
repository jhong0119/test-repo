import bigdataquery as bdq
import datetime as dt
import pandas as pd
import bdq_login
from datetime import datetime, timezone
import multiprocessing
import time
import sqlite3
import os


import logging
logger = logging.getLogger(__name__)

def bdq_TPSS기준정보(job_id, job_start_times, lock):

  #db로 저장할 파일명
  SAVE_FILE_NAME = 'TPSS_기준정보.db'

  try:
  
    if lock.acquire(timeout=30):  # 30초 동안 락을 기다립니다.
      try:
        job_start_times[job_id] = datetime.now()
      finally:
         lock.release()
    else:
       error_message = f"❌ 변수 수정을 위해 락 해제를 기다렸으나 30초 이상 락 해제 안됨"
       raise RuntimeError(error_message)

    bdq_login.bdq_login()

    query = f"""
      SELECT
      A.STATION AS EQPID,
      A.MACHINE_TYPE AS EQP_TYPE,
      A.STATION_NAME AS EQP_MODEL,
      A.PRC_GROUP,
      A.SDWT_PROD AS 분임조,
      A.STATUS AS 효율산정,
      C.S_NAME AS SIMAX_LINE,
      B.S_NAME AS INDEX_LINE

      FROM fab.m_tpss_station_master A
      LEFT JOIN fab.f_area_master B ON A.INDEX_AREA = B.AREA
      LEFT JOIN fab.f_area_master C ON A.AREA = C.AREA
      WHERE A.ROOM LIKE '%9'
      AND B.GBM IN ('MF', 'MFS', 'MO')
      AND C.S_NAME NOT REGEXP '^X|TSV'
      AND A.SDWT_PROD NOT LIKE '%NRD%'
      AND A.impala_insert_time = (
      SELECT MAX(impala_insert_time)
      FROM fab.m_tpss_station_master
      )

      """
    #bdq 실행
    df = bdq.getData(query, user_name='jhong.lee')

    # SQLite 데이터베이스 파일에 연결 (없으면 새로 생성)
    conn = sqlite3.connect(f'/config/work/sharedworkspace/db/{SAVE_FILE_NAME}')

    # DataFrame을 SQLite의 테이블로 저장 (덮어쓰기)
    df.to_sql('results', conn, if_exists='replace', index=False)

    logger.info(f"{SAVE_FILE_NAME} 파일 저장 완료")
  except Exception as e:
    # 오류 발생 시 오류 메시지 출력
    logger.error(f"❌{SAVE_FILE_NAME} bdq 실행 / 파일 저장 중 에러 : {e}")

  finally:
    # 연결을 닫아 자원을 해제 (항상 실행됨)
    if conn:
      conn.close()

  
    
