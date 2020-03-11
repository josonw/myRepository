#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_gameparty_ante_partynum_dis(BaseStatModel):
    """牌局底注，牌局数分布
    """
    def create_tab(self):
        hql = """
        create table if not exists dcnew.gameparty_ante_partynum_dis
        (
          fdate        date,
          fgamefsk                   bigint,
          fplatformfsk               bigint,
          fhallfsk                   bigint,
          fsubgamefsk                bigint,
          fterminaltypefsk           bigint,
          fversionfsk                bigint,
          fchannelcode               bigint,
          fmust_blind  bigint,
          fpname       varchar(200),
          fsubname     varchar(200),
          f1usernum    bigint,
          f10usernum   bigint,
          f20usernum   bigint,
          f50usernum   bigint,
          f100usernum  bigint,
          f150usernum  bigint,
          f250usernum  bigint,
          f400usernum  bigint
        )
        partitioned by(dt date)
        location '/dw/dcnew/gameparty_ante_partynum_dis'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """

        extend_group = {
                     'fields':[ 'fpname', 'fante', 'fsubname', 'fuid'],
                     'groups':[ [1, 1, 0, 1],
                                [0, 1, 0, 1]]
                                }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
                select
                    fgamefsk,
                    coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                    coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                    coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                    coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                    coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                    coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                    fante fmust_blind,
                    coalesce(fpname, '%(null_str_group_rule)s') fpname,
                    fsubname,
                    fuid,
                    sum(fparty_num) fparty_num
                from dim.user_gameparty a
                where dt = "%(statdate)s"
                 and nvl(fante,0) != 0
                 and hallmode=%(hallmode)s
            group by fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
                     fpname, fsubname, fante, fuid
            """

        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.gameparty_ante_partynum_dis
        partition( dt="%(statdate)s" )
        select "%(statdate)s" fdate,
              fgamefsk,
              fplatformfsk,
              fhallfsk,
              fgame_id,
              fterminaltypefsk,
              fversionfsk,
              fchannel_code,
              fmust_blind,
              fpname,
              fsubname,
              count(case when fparty_num < 10 then fuid end) f1usernum,
              count(case when fparty_num >= 10 and fparty_num < 20 then fuid end) f10usernum,
              count(case when fparty_num >= 20 and fparty_num < 50 then fuid end) f20usernum,
              count(case when fparty_num >= 50 and fparty_num < 100 then fuid end) f50usernum,
              count(case when fparty_num >= 100 and fparty_num < 150 then fuid end) f100usernum,
              count(case when fparty_num >= 150 and fparty_num < 250 then fuid end) f150usernum,
              count(case when fparty_num >= 250 and fparty_num < 400 then fuid end) f250usernum,
              count(case when fparty_num >= 400 then fuid end) f400usernum
        from (
            %(sql_template)s
        ) a group by fgamefsk,
                    fplatformfsk,
                    fhallfsk,
                    fgame_id,
                    fterminaltypefsk,
                    fversionfsk,
                    fchannel_code,
                    fmust_blind,
                    fpname,
                    fsubname
        ;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res


#生成统计实例
a = agg_gameparty_ante_partynum_dis(sys.argv[1:])
a()
