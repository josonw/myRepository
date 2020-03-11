#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class load_user_imei_mid(BaseStat):
    """建立用户设备号中间表
    """
    def create_tab(self):
        hql = """create external table if not exists stage.user_imei_mid
                (
                fdate date,
                fbpid varchar(64),
                fuid bigint,
                fimei varchar(200),
                fplatform_uid varchar(64),
                fchannel_id varchar(128),
                fsignup_at date
                )
                partitioned by (dt date)
                stored as orc
                location '/dw/stage/user_imei_mid'"""
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )
        res = self.hq.exe_sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
        if res != 0: return res
        # self.hq.debug = 0
        hql = """

        set io.sort.mb=256;


        insert overwrite table stage.user_imei_mid
        partition(dt='%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate, fbpid, fuid, fimei,
                max(fplatform_uid) as fplatform_uid,
                max(fchannel_id) as fchannel_id,
                min(fsignup_at) as fsignup_at
        from
        (
            select fbpid, fuid, fimei, fplatform_uid, fchannel_id, cast(fsignup_at as string)
            from user_imei_mid
            where dt = '%(ld_1dayago)s'
            union all
            select fbpid, fuid, fudid as fimei, null as fplatform_uid, fchannel_id, fsignup_at
            from channel_market_new_reg_mid
            where dt = '%(ld_daybegin)s' and fuid is not null
            union all
            select fbpid, fuid, fm_imei as fimei,
                max(fplatform_uid) as fplatform_uid,
                max(fchannel_code) as fchannel_id,
                min(flogin_at) as fsignup_at
            from user_login_stg
            where dt = '%(ld_daybegin)s' and fm_imei is not null
            group by fbpid, fuid, fm_imei
        ) tmp group by fbpid, fuid, fimei;
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
a = load_user_imei_mid(statDate)
a()
