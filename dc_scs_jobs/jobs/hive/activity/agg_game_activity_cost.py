#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel


class agg_game_activity_cost(BaseStatModel):
    """ 游戏活动发放结果表"""
    def create_tab(self):
        hql = """
        create table if not exists dcnew.game_activity_cost
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
            frule_id                    varchar(100),
            fio_type                    bigint,
            fitem_type                  bigint,
            fitem_id                    varchar(100),
            fitem_num                   bigint,
            fusernum                    bigint,
            fusercnt                    bigint
        )
        partitioned by (dt date)
        """
        result = self.sql_exe(hql)
        if result != 0: return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        extend_group = {'fields':['fact_id','frule_id','fio_type','fitem_type','fitem_id'],
                        'groups':[[1,1,1,1,1]]
                       }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.game_activity_cost_%(statdatenum)s;
        create table work.game_activity_cost_%(statdatenum)s as
            select b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk, b.hallmode,
                    a.fbpid, a.fitem_num, a.fuid, a.flts_at,
                    a.fact_id, a.frule_id, a.fio_type, a.fitem_type, a.fitem_id,
                    %(null_int_report)d fgame_id, %(null_int_report)d fchannel_code
              from stage.game_activity_stg a
              join dim.bpid_map b
                on a.fbpid=b.fbpid
             where a.dt = '%(statdate)s'
             group by b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk,b.hallmode,
                    a.fbpid, a.fitem_num, a.fuid, a.flts_at,
                    a.fact_id, a.frule_id, a.fio_type, a.fitem_type, a.fitem_id
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
                frule_id,
                fio_type,
                fitem_type,
                fitem_id,
                sum(fitem_num) fitem_num,
                count(distinct a.fuid) fusernum,
                count(distinct concat(flts_at, a.fuid)) fusercnt
           from work.game_activity_cost_%(statdatenum)s a
          where hallmode = %(hallmode)s
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,
                fact_id, frule_id, fio_type, fitem_type, fitem_id
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.game_activity_cost
        partition (dt = "%(statdate)s")
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.game_activity_cost_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



#生成统计实例
a = agg_game_activity_cost(sys.argv[1:])
a()
