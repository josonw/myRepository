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


class agg_gamecoin_detail(BaseStatModel):
    """牌局底注，牌局数分布
    """
    def create_tab(self):
        hql = """
        create table if not exists dcnew.gamecoin_detail
        (
          fdate                       date,
          fgamefsk                    bigint,
          fplatformfsk                bigint,
          fhallfsk                    bigint,
          fsubgamefsk                 bigint,
          fterminaltypefsk            bigint,
          fversionfsk                 bigint,
          fchannelcode                bigint,
          fcointype                   varchar(50),
          fdirection                  varchar(50),
          ftype                       varchar(50),
          fnum                        bigint,
          fusernum                    int,
          fpayusernum                 int,
          fpaynum                     bigint,
          fcnt                        bigint,
          fpaidnum                    bigint,
          fpaidusernum                bigint
        )
        partitioned by (dt date);
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """

        extend_group = {
                     'fields':[ 'fdirection', 'ftype'],
                     'groups':[ [1, 1]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.gamecoin_detail_%(statdatenum)s;
        create table work.gamecoin_detail_%(statdatenum)s
        as
        select fbpid, fuid, max(paid_user) paid_user, max(pay_user) pay_user
              from (
                    select fbpid, fuid, 1 paid_user, 0 pay_user
                      from dim.user_act_paid
                     where dt = "%(statdate)s"
                      union all
                    select fbpid, fuid, 0 paid_user, 1 pay_user
                      from dim.user_pay_day
                     where dt = "%(statdate)s"
                   ) t
             group by fbpid, fuid
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.gamecoin_detail_2_%(statdatenum)s;
        create table work.gamecoin_detail_2_%(statdatenum)s
        as
                select b.fgamefsk,
                       b.fplatformfsk,
                       b.fhallfsk,
                       b.hallmode,
                       b.fterminaltypefsk,
                       b.fversionfsk,
                       coalesce(fgame_id,cast (0 as bigint)) fgame_id,
                       coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code,
                       case g.act_type when 1 then 'IN' when 2 then 'OUT' end fdirection,
                       g.act_id ftype,
                       sum(abs(act_num)) act_num ,
                       count(1) ucnt,
                       g.fuid ,
                       pay_user,
                       paid_user
                  from stage.pb_gamecoins_stream_stg g
                  left join work.gamecoin_detail_%(statdatenum)s t
                    on g.fbpid = t.fbpid
                   and g.fuid = t.fuid
                  join dim.bpid_map b
                      on g.fbpid = b.fbpid
                where g.dt = '%(statdate)s' and g.act_type in (1, 2)
                group by  fgamefsk,
                          fplatformfsk,
                          fhallfsk,
                          hallmode,
                          fterminaltypefsk,
                          fversionfsk,
                          fgame_id,
                          fchannel_code,
                          case g.act_type when 1 then 'IN' when 2 then 'OUT' end ,
                          g.act_id,
                          g.fuid ,
                          pay_user,
                          paid_user
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """
             select "%(statdate)s" fdate,
                    fgamefsk,
                    coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                    coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                    coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                    coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                    coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                    coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                    'GAMECOIN' fcointype,
                    fdirection,
                    ftype,
                    sum(act_num) fnum,
                    count(distinct fuid) fusernum,
                    count(distinct case when pay_user = 1 then fuid end) fpayusernum,
                    sum(case when pay_user = 1 then act_num end) fpaynum,
                    sum(ucnt) fcnt,
                    sum(case when paid_user = 1 then act_num end) fpaidnum,
                    count(distinct case when paid_user = 1 then fuid end) fpaidusernum
                from work.gamecoin_detail_2_%(statdatenum)s a
               where hallmode=%(hallmode)s
            group by fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
                     fdirection,
                     ftype
            """

        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.gamecoin_detail
        partition(dt='%(statdate)s')
         %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.gamecoin_detail_%(statdatenum)s;
                 drop table if exists work.gamecoin_detail_2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

#生成统计实例
a = agg_gamecoin_detail(sys.argv[1:])
a()
