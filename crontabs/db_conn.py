#!/usr/bin/env python
# -*- coding:utf-8 -*-
import sys
from datetime import datetime
sys.path.append('/data/wwwroot/scs_server')

from dc_scs_jobs import config

from libs.DB_Mysql import Connection as Mysql_Connection
from libs.DB_PostgreSQL import Connection as PG_Connection

from libs.warning_way import send_sms


def main():
    # 统一全局使用，不要每个脚本去连接了
    print(datetime.now())
    try:
        pg_db = PG_Connection(
            host=config.PG_DB_HOST,
            database=config.PG_DB_NAME,
            user=config.PG_DB_USER,
            password=config.PG_DB_PSWD
        )
        pg_db.query('select 1;')
        print('pg %s connect success!' % config.PG_DB_HOST)
    except Exception as e:
        print(str(e))
        send_sms(['TomZhu'], '调度系统连不上PG数据库: %s' % config.PG_DB_HOST, 8)
    finally:
        pg_db.close()

    try:
        pgcluster = PG_Connection(
            host=config.PGCLUSTER_HOST,
            database=config.PG_DB_NAME,
            user=config.PG_SCS_USER,
            password=config.PG_SCS_PSWD
        )
        pgcluster.query('select 1;')
        print('pg %s connect success!' % config.PGCLUSTER_HOST)
    except Exception as e:
        print(str(e))
        send_sms(['TomZhu'], '调度系统连不上PG数据库: %s' % config.PGCLUSTER_HOST, 8)
    finally:
        pgcluster.close()

    try:
        mysql_db = Mysql_Connection(
            host=config.DB_HOST,
            database=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PSWD
        )
        mysql_db.query('select 1;')
        print('mysql %s connect success!' % config.DB_HOST)
    except Exception as e:
        print(str(e))
        send_sms(['TomZhu'], '调度系统连不上Mysql数据库: %s' % config.DB_HOST, 8)
    finally:
        mysql_db.close()


if __name__ == '__main__':
    main()
