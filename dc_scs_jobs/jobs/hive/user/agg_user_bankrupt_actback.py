# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_user_bankrupt_actback(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.user_bankrupt_actback (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               f1dayback_unum      bigint        comment '1日回头用户数',
               f2dayback_unum      bigint        comment '2日回头用户数',
               f3dayback_unum      bigint        comment '3日回头用户数',
               f4dayback_unum      bigint        comment '4日回头用户数',
               f5dayback_unum      bigint        comment '5日回头用户数',
               f6dayback_unum      bigint        comment '6日回头用户数',
               f7dayback_unum      bigint        comment '7日回头用户数',
               f14dayback_unum     bigint        comment '14日回头用户数',
               f30dayback_unum     bigint        comment '30日回头用户数'
               )comment '破产回头用户'
               partitioned by(dt date)
        location '/dw/dcnew/user_bankrupt_actback'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0:
            return res

        hql = """ insert overwrite table dcnew.user_bankrupt_actback  partition( dt="%(statdate)s" )
                select "%(statdate)s" fdate,
                       a.fgamefsk,
                       a.fplatformfsk,
                       a.fhallfsk,
                       a.fsubgamefsk,
                       a.fterminaltypefsk,
                       a.fversionfsk,
                       a.fchannelcode,
                       count(distinct case when b.dt="%(ld_1day_ago)s" then b.fuid else null end) f1dayback_unum,
                       count(distinct case when b.dt="%(ld_2day_ago)s" then b.fuid else null end) f2dayback_unum,
                       count(distinct case when b.dt="%(ld_3day_ago)s" then b.fuid else null end) f3dayback_unum,
                       count(distinct case when b.dt="%(ld_4day_ago)s" then b.fuid else null end) f4dayback_unum,
                       count(distinct case when b.dt="%(ld_5day_ago)s" then b.fuid else null end) f5dayback_unum,
                       count(distinct case when b.dt="%(ld_6day_ago)s" then b.fuid else null end) f6dayback_unum,
                       count(distinct case when b.dt="%(ld_7day_ago)s" then b.fuid else null end) f7dayback_unum,
                       count(distinct case when b.dt="%(ld_14day_ago)s" then b.fuid else null end) f14dayback_unum,
                       count(distinct case when b.dt="%(ld_30day_ago)s" then b.fuid else null end) f30dayback_unum
                  from dim.user_act_array a
                  join dim.user_bankrupt_array b
                    on a.fuid = b.fuid
                   and a.fgamefsk = b.fgamefsk
                   and a.fplatformfsk = b.fplatformfsk
                   and a.fhallfsk = b.fhallfsk
                   and a.fsubgamefsk = b.fsubgamefsk
                   and a.fterminaltypefsk = b.fterminaltypefsk
                   and a.fversionfsk = b.fversionfsk
                   and a.fchannelcode = b.fchannelcode
                   and cast(b.dt as string) in
                       ("%(ld_1day_ago)s",
                        "%(ld_2day_ago)s",
                        "%(ld_3day_ago)s",
                        "%(ld_4day_ago)s",
                        "%(ld_5day_ago)s",
                        "%(ld_6day_ago)s",
                        "%(ld_7day_ago)s",
                        "%(ld_14day_ago)s",
                        "%(ld_30day_ago)s")
                 where a.dt="%(statdate)s"
              group by a.fgamefsk,a.fplatformfsk,a.fhallfsk,a.fsubgamefsk,a.fterminaltypefsk,a.fversionfsk,a.fchannelcode
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 生成统计实例
a = agg_user_bankrupt_actback(sys.argv[1:])
a()
