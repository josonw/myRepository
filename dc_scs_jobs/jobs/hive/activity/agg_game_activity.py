#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel


class agg_game_activity(BaseStatModel):
    """ 游戏活动概要数据"""
    def create_tab(self):
        hql = """
        create table if not exists dcnew.game_activity
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
            fact_id                     varchar(50),
            fact_name                   varchar(100),
            fusercnt                     bigint,
            fusernum                     bigint,
            fregusercnt                     bigint,
            fregusernum                     bigint,
            fpayusercnt                     bigint,
            fpayusernum                     bigint,
            ffpayusercnt                     bigint,
            ffpayusernum                     bigint,
            fincome                     decimal(20,2)
        )
        partitioned by (dt date)
        """
        result = self.sql_exe(hql)
        if result != 0: return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        extend_group = {'fields':['fact_id'],
                        'groups':[[1]]
                       }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.game_activity_%(statdatenum)s;
        create table work.game_activity_%(statdatenum)s as
            select b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk, b.hallmode,
                    a.fbpid, a.fuid, a.fact_id, a.fact_name,
                    %(null_int_report)d fgame_id, %(null_int_report)d fchannel_code,
                    c.fuid rfuid, d.fuid pfuid, e.fuid ffuid,
                    fcnt,
                    sum(d.ftotal_usd_amt) fincome

              from (
                select a.fbpid, a.fuid, a.fact_id, a.fact_name,
                count(distinct concat(fuid, flts_at)) fcnt
                from stage.game_activity_stg a
                where a.dt = '%(statdate)s'
                group by a.fbpid, a.fuid, a.fact_id, a.fact_name
              ) a
              left join dim.reg_user_main_additional c
                on a.fbpid = c.fbpid
               and a.fuid = c.fuid
               and c.dt='%(statdate)s'
              left join dim.user_pay_day d
                on a.fbpid = d.fbpid
               and a.fuid = d.fuid
               and d.dt='%(statdate)s'
              left join dim.user_pay e
                on a.fbpid = e.fbpid
               and a.fuid = e.fuid
               and e.dt='%(statdate)s'
              join dim.bpid_map b
                on a.fbpid=b.fbpid
             group by b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk,b.hallmode,
                    a.fbpid, a.fuid, a.fact_id, a.fact_name,
                    c.fuid,d.fuid, e.fuid, fcnt
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
         select '%(statdate)s' fdate,
                fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                fact_id,
                max(fact_name) fact_name,
                sum(fcnt) fusercnt,
                count(DISTINCT a.fuid) fusernum,
                sum(if(a.rfuid IS NULL, 0, fcnt)) fregusercnt,
                count(DISTINCT a.rfuid) fregusernum,
                sum(if(a.pfuid IS NULL, 0, fcnt)) fpayusercnt,
                count(DISTINCT a.pfuid) fpayusernum,
                sum(if(a.ffuid IS NULL, 0, fcnt)) ffpayusercnt,
                count(DISTINCT a.ffuid) ffpayusernum,
                sum(coalesce(fincome,0)) fincome
           from work.game_activity_%(statdatenum)s a
          where hallmode = %(hallmode)s
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,fact_id
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.game_activity
        partition (dt = "%(statdate)s")
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.game_activity_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



#生成统计实例
a = agg_game_activity(sys.argv[1:])
a()
