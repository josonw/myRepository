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


class agg_user_currencies_by_detail_dis(BaseStatModel):
    def create_tab(self):

        hql = """
        --用户博雅币分布，当日用户博雅币初始携带量分布
        create table if not exists dcnew.user_currencies_by_detail_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               flft                bigint     comment '区间下界',
               frgt                bigint     comment '区间上界',
               fby_unum            bigint     comment '携带博雅币的用户数'
               )comment '用户博雅币分布'
               partitioned by(dt date)
        location '/dw/dcnew/user_currencies_by_detail_dis';

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['flft','frgt'],
                        'groups':[[1, 1]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """--算出当日用户博雅币初始携带量
        drop table if exists work.currencies_by_detail_b_%(statdatenum)s;
        create table work.currencies_by_detail_b_%(statdatenum)s as
              select c.fgamefsk,
                     c.fplatformfsk,
                     c.fhallfsk,
                     c.fterminaltypefsk,
                     c.fversionfsk,
                     c.hallmode,
                     coalesce(t.fgame_id,cast (0 as bigint)) fgame_id,
                     coalesce(t.fchannel_code,%(null_int_report)d) fchannel_code,
                     t.fuid,
                     t.user_bycoins
                from  ( select ss.fbpid,
                               ss.fuid,
                               ss.fgame_id,
                               ss.fchannel_code,
                               ss.user_bycoins
                          from (select t1.fbpid,
                                       t1.fuid,
                                       t2.fgame_id,
                                       cast (t1.fchannel_code as bigint) fchannel_code,
                                       t1.user_bycoins,
                                       row_number() over(partition by t1.fbpid, t1.fuid order by t1.flogin_at) rown
                                  from dim.user_login_additional t1
                                  left join dim.user_act t2
                                    on t1.fbpid = t2.fbpid
                                   and t1.fuid = t2.fuid
                                   and t2.dt = "%(statdate)s"
                                 where t1.dt = "%(statdate)s"
                               ) ss
                         where ss.rown = 1
                      ) t
                join dim.bpid_map c
                  on t.fbpid=c.fbpid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--将用户当日博雅币初始携带量，转换为区间数据
        drop table if exists work.currencies_by_detail_b_2_%(statdatenum)s;
        create table work.currencies_by_detail_b_2_%(statdatenum)s as
              select a.fgamefsk,
                     a.fplatformfsk,
                     a.fhallfsk,
                     a.fterminaltypefsk,
                     a.fversionfsk,
                     a.hallmode,
                     a.fgame_id,
                     a.fchannel_code,
                     b.flft,
                     b.frgt,
                     a.fuid
                from work.currencies_by_detail_b_%(statdatenum)s a
                join stage.jw_qujian_grain b
               where a.user_bycoins >= b.flft
                 and a.user_bycoins < b.frgt;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合数据 并导入到正式表上
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       flft,
                       frgt,
                       count(distinct gs.fuid) fcnt
                  from work.currencies_by_detail_b_2_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       flft,
                       frgt
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)


        hql = """
        insert overwrite table dcnew.user_currencies_by_detail_dis
        partition (dt = '%(statdate)s')
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.currencies_by_detail_b_%(statdatenum)s;
                 drop table if exists work.currencies_by_detail_b_2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_user_currencies_by_detail_dis(sys.argv[1:])
a()
