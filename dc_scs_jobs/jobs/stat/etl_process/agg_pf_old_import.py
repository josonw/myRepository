#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_pf_old_import(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create  table if not exists stage.game_performance_stg_import_tmp
        (
            fbpid   varchar(64),
            fuid    bigint ,
            fplatform_uid   varchar(32),
            flts_at string ,
            fptype  int,
            fact_type   string,
            fstart_time string ,
            fend_time   string ,
            fret_code   int,
            fret_msg    varchar(1000)  ,
            ip  varchar(32)
        )
        partitioned by (dt date)

        """
        res = self.hq.exe_sql(hql)

        if res != 0:
            return res
        return res


    def stat(self):
        """ 重要部分，统计内容 """

        self.hq.debug = 0

        args_dic = {
            "ld_begin": self.stat_date
        }

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """
        insert overwrite table stage.game_performance_stg_import_tmp
        partition( dt="%(ld_begin)s" )
        select fbpid,fuid, fplatform_uid, flts_at, 1, 1, fstart_time, fend_time, fret_code, trim(fret_msg), ''
        from stage.game_loading_stg  where dt = '%(ld_begin)s'
        union all
        select fbpid,fuid, fplatform_uid, flts_at, 1, 1, fstart_time, fend_time, fret_code, trim(fret_msg), ''
        from stage.pf_game_loading_stg  where dt = '%(ld_begin)s'
        union all
        select fbpid,fuid, fplatform_uid, flts_at, 1, 2, fstart_time, fend_time, fret_code, trim(fret_msg), ''
        from stage.room_enter_loading_stg  where dt = '%(ld_begin)s'
        union all
        select fbpid,fuid, fplatform_uid, flts_at, 1, 2, fstart_time, fend_time, fret_code, trim(fret_msg), ''
        from stage.pf_room_enter_loading_stg  where dt = '%(ld_begin)s'
        union all
        select fbpid,fuid, fplatform_uid, flts_at, 1, 3, fstart_time, fend_time, fret_code, trim(fret_msg), ''
        from stage.pf_user_sit_down_stg  where dt = '%(ld_begin)s'
        union all
        select fbpid,fuid, fplatform_uid, flts_at, 1, 4, fstart_time, fend_time, fret_code, trim(fret_msg), ''
        from stage.pf_party_loading_stg  where dt = '%(ld_begin)s'

        """% args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res
        return res


if __name__ == '__main__':
    a = agg_pf_old_import()
    a()

