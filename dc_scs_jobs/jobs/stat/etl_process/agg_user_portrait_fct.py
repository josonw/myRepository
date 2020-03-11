#!/user/local/python272/bin/python
#-*- coding:UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

class agg_user_portrait_fct(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_portrait_fct
        (
            fdate    date,
            fgamefsk    bigint,
            fplatformfsk    bigint,
            fversionfsk    bigint,
            fterminalfsk    bigint,
            fgender    bigint,
            fage    bigint,
            fcountry    varchar(200),
            fcity    varchar(200),
            fprofession    bigint,
            finterest    varchar(100),
            feducation    varchar(100),
            fbloodtype    varchar(100),
            factcnt    bigint,
            fdsucnt    bigint,
            fpaycnt    bigint
            )
            partitioned by(dt date)
            location '/dw/analysis/user_portrait_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        query = {'statdate':statDate}
        res = self.hq.exe_sql(""" use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res !=0:
            return res

        hql = """
        insert overwrite table analysis.user_portrait_fct partition(dt = "%(statdate)s")

            select "%(statdate)s" fdate,
                   d.fgamefsk fgamefsk,
                   d.fplatformfsk fplatformfsk,
                   d.fversionfsk fversionfsk,
                   d.fterminalfsk fterminalfsk,
                   coalesce(b.fgender, -1) gender,
                   coalesce(b.fage, -1) age,
                   coalesce(b.fcountry, '-1') country,
                   coalesce(b.fcity, '-1') city,
                   coalesce(b.fprofession, -1) profession,
                   coalesce(b.finterest, '-1') interest,
                   coalesce(b.feducation, '-1') education,
                   coalesce(b.fbloodtype, '-1') bloodtype,
                   coalesce(count(case
                                    when is_dau = 1 then
                                     a.fuid
                                  end),
                            0) fdaucnt,
                   coalesce(count(case
                                    when is_dsu = 1 then
                                     a.fuid
                                  end),
                            0) fdsucnt,
                   coalesce(count(case
                                    when is_pay = 1 then
                                     a.fuid
                                  end),
                            0) fpaycnt
              from (select fbpid,
                           fuid,
                           max(is_dau) is_dau,
                           max(is_dsu) is_dsu,
                           max(is_pay) is_pay
                      from (select fbpid, fuid, 1 is_dau, 0 is_dsu, 0 is_pay
                              from stage.active_user_mid
                             where dt = "%(statdate)s"
                            union all
                            select fbpid, fuid, 0 is_dau, 1 is_dsu, 0 is_pay
                              from stage.user_dim
                             where dt = "%(statdate)s"
                            union all
                            select fbpid, fuid, 0 is_dau, 0 is_dsu, 1 is_pay
                              from stage.user_pay_info
                             where dt = "%(statdate)s") aa
                     group by fbpid, fuid) a
              left outer join stage.user_async_dim b
                on a.fbpid = b.fbpid
               and a.fuid = b.fuid
              join analysis.bpid_platform_game_ver_map d
                on a.fbpid = d.fbpid
             group by d.fgamefsk,
                      d.fplatformfsk,
                      d.fversionfsk,
                      d.fterminalfsk,
                      coalesce(b.fgender, -1),
                      coalesce(b.fage, -1),
                      coalesce(b.fcountry, '-1'),
                      coalesce(b.fcity, '-1'),
                      coalesce(b.fprofession, -1),
                      coalesce(b.finterest, '-1'),
                      coalesce(b.feducation, '-1'),
                      coalesce(b.fbloodtype, '-1');

        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res
        return res


#愉快的统计要开始啦
global statDate
eid = 0
if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
elif len(sys.argv) >= 3:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
    eid = int(sys.argv[2])
else :
    args = sys.argv[1].split(',')
    statDate = args[0]

a=agg_user_portrait_fct(statDate, eid)
a()
