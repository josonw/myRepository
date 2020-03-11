#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class other_load_ddz_login_data(BaseStat):
    """斗地主用户登陆记录
    """
    def create_tab(self):
        hql = """create table if not exists analysis.ddz_user_login_info_new
                (
                fbpid varchar(50),
                fuid bigint,
                fplatform_uid varchar(50),
                flogin_at string,
                fip varchar(20),
                fis_first bigint,
                fuser_agent varchar(4000),
                fversion_info varchar(50),
                ffirst_at date,
                fm_dtype varchar(100)
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict""")
        if res != 0: return res
        hql = """insert overwrite table analysis.ddz_user_login_info_new
                    partition (dt = '%(ld_daybegin)s')
                      select a.fbpid,
                             fuid,
                             fplatform_uid,
                             flogin_at,
                             fip,
                             fis_first,
                             fuser_agent,
                             fversion_info,
                             ffirst_at,
                             fm_dtype
                        from stage.user_login_stg a
                        LEFT SEMI JOIN analysis.bpid_platform_game_ver_map b
                          on a.fbpid = b.fbpid
                         and b.fgamename in ('新斗地主', '斗地主')
                         and a.fplatform_uid is not null
                        where a.dt = '%(ld_daybegin)s'
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

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
a = other_load_ddz_login_data(statDate)
a()
