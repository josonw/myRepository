#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class load_active_user_mid(BaseStat):
    """建立维度表
    """
    def create_tab(self):
        hql = """
        create external table if not exists stage.active_user_mid
        (
            fdate                date,
            fbpid                varchar(50),
            fuid                 bigint,
            fnew_login_num       bigint,
            fnew_game_party_num  bigint
        )
        partitioned by (dt date)
        stored as orc
        location '/dw/stage/active_user_mid'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        create external table if not exists stage.active_user_month_mid
        (
            fdate date,
            fbpid varchar(50),
            fuid bigint,
            fnew_login_num bigint,
            fnew_game_party_num bigint
        )
        partitioned by (dt date)
        stored as orc
        location '/dw/stage/active_user_month_mid'"""
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        # self.hq.debug = 0
        date = PublicFunc.date_define( self.stat_date )
        query = { 'statdate':self.stat_date,
                'ld_monthbegin': date['ld_monthbegin'],
                'ld_monthend': date['ld_monthbegin']
                 }
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """ % query)
        if res != 0: return res

        # 日活跃中间表
        hql ="""
        insert overwrite table stage.active_user_mid partition (dt='%(statdate)s')
        select '%(statdate)s' fdate,
            fbpid,
            fuid,
            sum(is_login) fnew_login_num,
            count(distinct game_party_uni) fnew_game_party_num
        from
        (
            select fbpid, fuid, 1 is_login, null game_party_uni
            from user_login_stg uls
            where dt = '%(statdate)s'

            union all

            select fbpid, fuid, null is_login, null game_party_uni
            from pb_gamecoins_stream_stg
            where dt = '%(statdate)s'
            and act_id not in ('8', '169', '960')
            and act_type != 0
            and (cast(act_id as int) is null or act_id >=0)

            union all

            select fbpid, fuid, null is_login, concat_ws('0',gameparty_id, tbl_id) game_party_uni
            from finished_gameparty_uid_mid
            where dt = '%(statdate)s'
        ) a
        group by fbpid, fuid
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        # 月活跃中间表
        hql = """
        insert overwrite table active_user_month_mid partition ( dt='%(ld_monthbegin)s')
            select '%(ld_monthbegin)s' fmonth,
            fbpid,
            fuid,
            sum(fnew_login_num) fnew_login_num,
            sum(fnew_game_party_num) fnew_game_party_num
        from active_user_mid
        where dt >= '%(ld_monthbegin)s' and dt < '%(ld_monthend)s'
        group by fbpid, fuid
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]

#生成统计实例
a = load_active_user_mid(statDate)
a()
