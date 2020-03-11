#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel


class agg_act_user_actret(BaseStatModel):
    """ 活跃留存用户统计"""
    def create_tab(self):
        hql = """
        create table if not exists dcnew.act_user_actret
            (
            fdate date,
            fgamefsk bigint,
            fplatformfsk bigint,
            fhallfsk bigint,
            fsubgamefsk bigint,
            fterminaltypefsk bigint,
            fversionfsk bigint,
            fchannelcode bigint,
            f1daycnt bigint,
            f2daycnt bigint,
            f3daycnt bigint,
            f4daycnt bigint,
            f5daycnt bigint,
            f6daycnt bigint,
            f7daycnt bigint,
            f14daycnt bigint,
            f30daycnt bigint,
            f60daycnt bigint,
            f90daycnt bigint
            )
            partitioned by (dt date)
        """
        result = self.sql_exe(hql)
        if result != 0: return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        extend_group = {'fields':[],
                        'groups':[]
                       }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.act_user_actret_temp_%(statdatenum)s;
        create table work.act_user_actret_temp_%(statdatenum)s as
         select ub.dt fdate,
                ub.fgamefsk,
                ub.fplatformfsk,
                ub.fhallfsk,
                ub.fsubgamefsk,
                ub.fterminaltypefsk,
                ub.fversionfsk,
                ub.fchannelcode,
                datediff('%(statdate)s', ub.dt) retday,
                ub.fuid
           from dim.user_act_array ub
           join dim.user_act_array ua
             on ua.fgamefsk = ub.fgamefsk
            and ua.fplatformfsk = ub.fplatformfsk
            and ua.fhallfsk = ub.fhallfsk
            and ua.fsubgamefsk = ub.fsubgamefsk
            and ua.fterminaltypefsk = ub.fterminaltypefsk
            and ua.fversionfsk = ub.fversionfsk
            and ua.fchannelcode = ub.fchannelcode
            and ua.fuid = ub.fuid
            and ua.dt = '%(statdate)s'
          where ((ub.dt >= '%(ld_7day_ago)s' and ub.dt <= '%(statdate)s')
                 or ub.dt='%(ld_14day_ago)s'
                 or ub.dt='%(ld_30day_ago)s'
                 or ub.dt='%(ld_60day_ago)s'
                 or ub.dt='%(ld_90day_ago)s')
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.act_user_actret_temp_2_%(statdatenum)s;
        create table work.act_user_actret_temp_2_%(statdatenum)s as
         select fdate,
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fsubgamefsk,
                fterminaltypefsk,
                fversionfsk,
                fchannelcode,
                retday,
                count(DISTINCT a.fuid) fdregrtducnt
           from work.act_user_actret_temp_%(statdatenum)s a
          group by fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
                   retday
        """

        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
                insert overwrite table dcnew.act_user_actret
                   partition( dt )
                   select fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
                       max(f1daycnt) f1daycnt,
                       max(f2daycnt) f2daycnt,
                       max(f3daycnt) f3daycnt,
                       max(f4daycnt) f4daycnt,
                       max(f5daycnt) f5daycnt,
                       max(f6daycnt) f6daycnt,
                       max(f7daycnt) f7daycnt,
                       max(f14daycnt) f14daycnt,
                       max(f30daycnt) f30daycnt,
                       max(f60daycnt) f60daycnt,
                       max(f90daycnt) f90daycnt,
                       fdate dt
                   from (
                       select fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
                           if( retday=1, fdregrtducnt, 0 ) f1daycnt,
                           if( retday=2, fdregrtducnt, 0 ) f2daycnt,
                           if( retday=3, fdregrtducnt, 0 ) f3daycnt,
                           if( retday=4, fdregrtducnt, 0 ) f4daycnt,
                           if( retday=5, fdregrtducnt, 0 ) f5daycnt,
                           if( retday=6, fdregrtducnt, 0 ) f6daycnt,
                           if( retday=7, fdregrtducnt, 0 ) f7daycnt,
                           if( retday=14, fdregrtducnt, 0 ) f14daycnt,
                           if( retday=30, fdregrtducnt, 0 ) f30daycnt,
                           if( retday=60, fdregrtducnt, 0 ) f60daycnt,
                           if( retday=90, fdregrtducnt, 0 ) f90daycnt
                       from  work.act_user_actret_temp_2_%(statdatenum)s
                       union all
                       SELECT fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
                              f1daycnt,
                              f2daycnt,
                              f3daycnt,
                              f4daycnt,
                              f5daycnt,
                              f6daycnt,
                              f7daycnt,
                              f14daycnt,
                              f30daycnt,
                              f60daycnt,
                              f90daycnt
                       FROM dcnew.act_user_actret
                       WHERE dt >= '%(ld_90day_ago)s'
                         AND dt <= '%(statdate)s'
                               ) tmp group by fdate, fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode
        """

        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """--最近三十天的数据放到同一个分区中，提高同步效率
        insert overwrite table dcnew.act_user_actret partition(dt='3000-01-01')
                   select fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
                          f1daycnt,
                          f2daycnt,
                          f3daycnt,
                          f4daycnt,
                          f5daycnt,
                          f6daycnt,
                          f7daycnt,
                          f14daycnt,
                          f30daycnt,
                          f60daycnt,
                          f90daycnt
                     FROM dcnew.act_user_actret
                    WHERE dt >= '%(ld_90day_ago)s'
                      AND dt <= '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        drop table if exists work.act_user_actret_temp_%(statdatenum)s;
        drop table if exists work.act_user_actret_temp_2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



#生成统计实例
a = agg_act_user_actret(sys.argv[1:])
a()
