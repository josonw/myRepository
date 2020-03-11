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


class agg_gameparty_subname_play_actret(BaseStatModel):
    def create_tab(self):

        hql = """
create table if not exists dcnew.gameparty_subname_play_actret(
       fdate              date,
       fgamefsk           bigint,
       fplatformfsk       bigint,
       fhallfsk           bigint,
       fsubgamefsk        bigint,
       fterminaltypefsk   bigint,
       fversionfsk        bigint,
       fchannelcode       bigint,
       fpname             varchar(100)      comment '一级场次',
       fsubname           varchar(100)      comment '二级场次',
       play_unum          bigint            comment '玩牌人数',
       f1day_unum         bigint            comment '一日留存',
       f2day_unum         bigint            comment '两日留存',
       f3day_unum         bigint            comment '三日留存',
       f4day_unum         bigint            comment '四日留存',
       f5day_unum         bigint            comment '五日留存',
       f6day_unum         bigint            comment '六日留存',
       f7day_unum         bigint            comment '七日留存',
       f14day_unum        bigint            comment '十四日留存',
       f30day_unum        bigint            comment '三十日留存',
       f60day_unum        bigint            comment '六十日留存',
       f90day_unum        bigint            comment '九十日留存'
    )comment '玩牌用户活跃留存'
    partitioned by(dt date)
        location '/dw/dcnew/gameparty_subname_play_actret'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fdate','fpname','fsubname'],
                        'groups':[[1,1,1]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set mapreduce.reduce.shuffle.memory.limit.percent=0.2;""")
        if res != 0: return res

        hql = """--取历史玩牌在今日活跃的用户数据
               drop table if exists work.subname_play_actret_tmp_%(statdatenum)s;
             create table work.subname_play_actret_tmp_%(statdatenum)s as
                   select /*+ MAPJOIN(c) */ distinct a.dt fdate,
                          c.fgamefsk,
                          c.fplatformfsk,
                          c.fhallfsk,
                          c.fterminaltypefsk,
                          c.fversionfsk,
                          c.hallmode,
                          a.fgame_id,
                          a.fchannel_code,
                          a.fpname,
                          a.fsubname,
                          datediff('%(statdate)s', a.dt) retday,
                          a.fuid play_fuid,
                          b.fuid act_fuid
                     from dim.user_gameparty a
                     left join dim.user_act b
                       on a.fbpid = b.fbpid
                      and a.fuid = b.fuid
                      and b.dt = '%(statdate)s'
                     join dim.bpid_map c
                       on a.fbpid=c.fbpid
                    where a.dt >= '%(ld_7day_ago)s' and a.dt <= '%(statdate)s'
                       or a.dt='%(ld_14day_ago)s'
                       or a.dt='%(ld_30day_ago)s'
                       or a.dt='%(ld_60day_ago)s'
                       or a.dt='%(ld_90day_ago)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合牌局结算相关数据
                   select fdate,
                          fgamefsk,
                          coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                          coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                          coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                          coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                          coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                          coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                          fpname,
                          fsubname,
                          count(distinct play_fuid) play_unum,
                          count(distinct case when retday = 1 then act_fuid end) f1day_unum,
                          count(distinct case when retday = 2 then act_fuid end) f2day_unum,
                          count(distinct case when retday = 3 then act_fuid end) f3day_unum,
                          count(distinct case when retday = 4 then act_fuid end) f4day_unum,
                          count(distinct case when retday = 5 then act_fuid end) f5day_unum,
                          count(distinct case when retday = 6 then act_fuid end) f6day_unum,
                          count(distinct case when retday = 7 then act_fuid end) f7day_unum,
                          count(distinct case when retday = 14 then act_fuid end) f14day_unum,
                          count(distinct case when retday = 30 then act_fuid end) f30day_unum,
                          count(distinct case when retday = 60 then act_fuid end) f60day_unum,
                          count(distinct case when retday = 90 then act_fuid end) f90day_unum
                     from work.subname_play_actret_tmp_%(statdatenum)s a
                    where hallmode = %(hallmode)s
                    group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                             fdate,
                             fpname,
                             fsubname


        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.subname_actret_tmp_zh_%(statdatenum)s;
        create table work.subname_actret_tmp_zh_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--汇总目标表

            insert overwrite table dcnew.gameparty_subname_play_actret
            partition( dt )
            select fdate,
                   fgamefsk,
                   fplatformfsk,
                   fhallfsk,
                   fsubgamefsk,
                   fterminaltypefsk,
                   fversionfsk,
                   fchannelcode,
                   fpname,
                   fsubname,
                   max(play_unum) play_unum,
                   max(f1day_unum) f1day_unum,
                   max(f2day_unum) f2day_unum,
                   max(f3day_unum) f3day_unum,
                   max(f4day_unum) f4day_unum,
                   max(f5day_unum) f5day_unum,
                   max(f6day_unum) f6day_unum,
                   max(f7day_unum) f7day_unum,
                   max(f14day_unum) f14day_unum,
                   max(f30day_unum) f30day_unum,
                   max(f60day_unum) f60day_unum,
                   max(f90day_unum) f90day_unum,
                   fdate dt
              from (select fdate,
                           fgamefsk,
                           fplatformfsk,
                           fhallfsk,
                           fsubgamefsk,
                           fterminaltypefsk,
                           fversionfsk,
                           fchannelcode,
                           fpname,
                           fsubname,
                           play_unum,
                           f1day_unum,
                           f2day_unum,
                           f3day_unum,
                           f4day_unum,
                           f5day_unum,
                           f6day_unum,
                           f7day_unum,
                           f14day_unum,
                           f30day_unum,
                           f60day_unum,
                           f90day_unum
                      from work.subname_actret_tmp_zh_%(statdatenum)s
                     union all
                    select fdate,
                           fgamefsk,
                           fplatformfsk,
                           fhallfsk,
                           fsubgamefsk,
                           fterminaltypefsk,
                           fversionfsk,
                           fchannelcode,
                           fpname,
                           fsubname,
                           play_unum,
                           f1day_unum,
                           f2day_unum,
                           f3day_unum,
                           f4day_unum,
                           f5day_unum,
                           f6day_unum,
                           f7day_unum,
                           f14day_unum,
                           f30day_unum,
                           f60day_unum,
                           f90day_unum
                      from dcnew.gameparty_subname_play_actret
                     where dt >= '%(ld_90day_ago)s'
                       and dt <= '%(statdate)s'
                   ) tmp
             group by fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,fpname,fsubname

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """
        insert overwrite table dcnew.gameparty_subname_play_actret partition(dt='3000-01-01')
        select fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
               fpname,
               fsubname,
               play_unum,
               f1day_unum,
               f2day_unum,
               f3day_unum,
               f4day_unum,
               f5day_unum,
               f6day_unum,
               f7day_unum,
               f14day_unum,
               f30day_unum,
               f60day_unum,
               f90day_unum
        from dcnew.gameparty_subname_play_actret
        where dt >= '%(ld_90day_ago)s'
        and dt <= '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        # 统计完清理掉临时表
        hql = """drop table if exists work.subname_play_actret_tmp_%(statdatenum)s;
                 drop table if exists work.subname_actret_tmp_zh_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return resS
        return res


#生成统计实例
a = agg_gameparty_subname_play_actret(sys.argv[1:])
a()
