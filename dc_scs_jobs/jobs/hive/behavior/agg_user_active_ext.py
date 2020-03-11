# -*- coding: UTF-8 -*-
#src     :dim.user_act,stage.pb_gamecoins_stream_stg,dim.user_login_additional,dim.bpid_map
#dst     :dcnew.user_active_ext
#authot  :SimonRen
#date    :2016-09-06


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


class agg_user_active_ext(BaseStatModel):
    def create_tab(self):

        hql = """--用户活跃扩展
        create table if not exists dcnew.user_active_ext (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fgcusernum          bigint      comment '金流变化用户',
               fgccbactcnt         bigint      comment '金流变化但用户未登录用户',
               ffullactcnt         bigint      comment '活跃用户',
               flogin_unum         bigint      comment '登陆人数',
               flogin_num          bigint      comment '登陆人次',
               f7dlogin_unum       bigint      comment '7日内登陆人数',
               f7dlogin_num        bigint      comment '7日内登陆人次',
               f30dlogin_unum      bigint      comment '30日内登陆人数',
               f30dlogin_num       bigint      comment '30日内登陆人次'
               )comment '用户活跃扩展'
               partitioned by(dt date)
        location '/dw/dcnew/user_active_ext'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':[],
                        'groups':[] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        res = self.sql_exe("""set mapreduce.reduce.shuffle.memory.limit.percent=0.1""")

        if res != 0: return res

        hql = """--取各类活跃用户数
          drop table if exists work.user_active_ext_tmp_1_b_%(statdatenum)s;
        create table work.user_active_ext_tmp_1_b_%(statdatenum)s as
         select d.fgamefsk,
                d.fplatformfsk,
                d.fhallfsk,
                d.fterminaltypefsk,
                d.fversionfsk,
                d.hallmode,
                coalesce(a.fgame_id,%(null_int_report)d) fgame_id,
                coalesce(a.fchannel_code,%(null_int_report)d) fchannel_code,
                a.fuid,
                a.is_gcoin,
                a.is_login,
                a.is_act
           from (select fbpid, fuid, fgame_id, cast (fchannel_code as bigint) fchannel_code,
                        max(is_act) is_act,
                        max(is_gcoin) is_gcoin,
                        max(is_login) is_login
                   from (select a.fbpid, a.fuid, a.fgame_id, a.fchannel_code, 1 is_act, 0 is_gcoin, 0 is_login
                           from dim.user_act a
                          where a.dt = '%(statdate)s'
                          union all
                         select distinct a.fbpid, a.fuid,coalesce(fgame_id,cast (0 as bigint)) fgame_id, cast (a.fchannel_code as bigint), 0 is_act, 1 is_gcoin, 0 is_login
                           from stage.pb_gamecoins_stream_stg a
                          where a.dt = '%(statdate)s'
                          union all
                         select distinct a.fbpid, a.fuid, null fgame_id, null fchannel_code, 0 is_act, 0 is_gcoin, 1 is_login
                           from dim.user_login_additional a
                          where a.dt = '%(statdate)s'
                        ) t
                  group by fbpid, fuid, fgame_id, fchannel_code
                ) a
           join dim.bpid_map d
             on a.fbpid = d.fbpid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--组合各类活跃用户数
                select fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       count(distinct case when is_gcoin = 1 then fuid end) fgcusernum,
                       count(distinct case when is_gcoin = 1 and is_login = 0 then fuid end) fgccbactcnt,
                       count(distinct case when is_act = 1 then fuid end) ffullactcnt
                  from work.user_active_ext_tmp_1_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
                 group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code
        """

        self.sql_template_build(sql=hql)

        hql = """
        drop table if exists work.user_active_ext_tmp_1_%(statdatenum)s;
        create table work.user_active_ext_tmp_1_%(statdatenum)s as
        %(sql_template)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--加上登录相关数据
          drop table if exists work.user_active_ext_tmp_2_b_%(statdatenum)s;
        create table work.user_active_ext_tmp_2_b_%(statdatenum)s as
         select d.fgamefsk,
                d.fplatformfsk,
                d.fhallfsk,
                d.fterminaltypefsk,
                d.fversionfsk,
                d.hallmode,
                a.fgame_id,
                coalesce(a.fchannel_code,%(null_int_report)d) fchannel_code,
                a.fuid,
                a.dt,
                a.loginusernum
           from (select a.dt, a.fbpid, a.fuid, %(null_int_report)d fgame_id, cast (a.fchannel_code as int) fchannel_code, count(1) loginusernum
                   from dim.user_login_additional a
                  where a.dt >  '%(ld_30day_ago)s'
                    and a.dt <= '%(statdate)s'
                  group by a.dt, a.fbpid, a.fuid, a.fchannel_code
                ) a
           join dim.bpid_map d
             on a.fbpid = d.fbpid

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--组合登录相关数据
                select fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       count(distinct if(dt >= '%(statdate)s', fuid, null)) flogin_unum, --登陆人数
                       sum(if(dt >= '%(statdate)s', loginusernum, null)) flogin_num, --登陆人次
                       count(distinct if(dt >= '%(ld_6day_ago)s', fuid, null)) f7dlogin_unum, --7日内登陆人数
                       sum(if(dt >= '%(ld_6day_ago)s', loginusernum, null)) f7dlogin_num, --7日内登陆人次
                       count(distinct fuid) f30dlogin_unum, --30日内登陆人次
                       sum(loginusernum) f30dlogin_num --30日内登陆人次
                  from work.user_active_ext_tmp_2_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
                 group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code
        """

        self.sql_template_build(sql=hql)

        hql = """
        drop table if exists work.user_active_ext_tmp_2_%(statdatenum)s;
        create table work.user_active_ext_tmp_2_%(statdatenum)s as
        %(sql_template)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res



        hql = """--子游戏登陆数据
        drop table if exists work.user_active_ext_tmp_3_%(statdatenum)s;
        create table work.user_active_ext_tmp_3_%(statdatenum)s as
            select coalesce(bm.fgamefsk,%(null_int_group_rule)d) fgamefsk,
                   coalesce(bm.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                   coalesce(bm.fhallfsk,%(null_int_group_rule)d) fhallfsk,
                   coalesce(bm.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                   coalesce(bm.fversionfsk,%(null_int_group_rule)d) fversionfsk,
                   coalesce(us.fchannel_code,%(null_int_group_rule)d) fchannel_code,
                   coalesce(us.fgame_id,%(null_int_group_rule)d) fgame_id,
                   count(distinct if(dt >= '%(statdate)s', fuid, null)) flogin_unum, --登陆人数
                   sum(if(dt >= '%(statdate)s', loginusernum, null)) flogin_num, --登陆人次
                   count(distinct if(dt >= '%(ld_6day_ago)s', fuid, null)) f7dlogin_unum, --7日内登陆人数
                   sum(if(dt >= '%(ld_6day_ago)s', loginusernum, null)) f7dlogin_num, --7日内登陆人次
                   count(distinct fuid) f30dlogin_unum, --30日内登陆人次
                   sum(loginusernum) f30dlogin_num --30日内登陆人次
              from (select fbpid,
                           fuid,
                           a.dt,
                           coalesce(fgame_id,%(null_int_report)d) fgame_id,
                           coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code,
                           count(1) loginusernum
                      from stage.user_enter_stg a --子游戏登陆数据
                     where a.dt >  '%(ld_30day_ago)s'
                       and a.dt <= '%(statdate)s'
                       and fgame_id is not null
                     group by fbpid,fuid,dt,fgame_id,fchannel_code
                   ) us
              join dim.bpid_map bm
                on us.fbpid = bm.fbpid
             group by bm.fgamefsk, bm.fplatformfsk, bm.fhallfsk, bm.fterminaltypefsk, bm.fversionfsk, us.fgame_id, us.fchannel_code
          grouping sets ((bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,us.fgame_id,bm.fterminaltypefsk,bm.fversionfsk),
                         (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,us.fgame_id),
                         (bm.fgamefsk,us.fgame_id))
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        res = self.sql_exe("""set mapreduce.reduce.shuffle.memory.limit.percent=0.25""")

        hql = """--插入目标表
        insert overwrite table dcnew.user_active_ext
        partition( dt="%(statdate)s" )
         select "%(statdate)s" fdate,
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fgame_id,
                fterminaltypefsk,
                fversionfsk,
                fchannel_code,
                sum(fgcusernum) fgcusernum,
                sum(fgccbactcnt) fgccbactcnt,
                sum(ffullactcnt) ffullactcnt,
                sum(flogin_unum) flogin_unum,
                sum(flogin_num) flogin_num,
                sum(f7dlogin_unum) f7dlogin_unum,
                sum(f7dlogin_num) f7dlogin_num,
                sum(f30dlogin_unum) f30dlogin_unum,
                sum(f30dlogin_num) f30dlogin_num
           from (select fgamefsk,
                        fplatformfsk,
                        fhallfsk,
                        fterminaltypefsk,
                        fversionfsk,
                        fgame_id,
                        fchannel_code,
                        fgcusernum,
                        fgccbactcnt,
                        ffullactcnt,
                        0 flogin_unum,
                        0 flogin_num,
                        0 f7dlogin_unum,
                        0 f7dlogin_num,
                        0 f30dlogin_unum,
                        0 f30dlogin_num
                   from work.user_active_ext_tmp_1_%(statdatenum)s
                  where ffullactcnt > 0
                  union all
                 select fgamefsk,
                        fplatformfsk,
                        fhallfsk,
                        fterminaltypefsk,
                        fversionfsk,
                        fgame_id,
                        fchannel_code,
                        0 fgcusernum,
                        0 fgccbactcnt,
                        0 ffullactcnt,
                        flogin_unum, --登陆人数
                        flogin_num, --登陆人次
                        f7dlogin_unum, --7日内登陆人数
                        f7dlogin_num, --7日内登陆人次
                        f30dlogin_unum, --30日内登陆人次
                        f30dlogin_num --30日内登陆人次
                   from work.user_active_ext_tmp_2_%(statdatenum)s
                  union all
                 select fgamefsk,
                        fplatformfsk,
                        fhallfsk,
                        fterminaltypefsk,
                        fversionfsk,
                        fgame_id,
                        fchannel_code,
                        0 fgcusernum,
                        0 fgccbactcnt,
                        0 ffullactcnt,
                        flogin_unum, --登陆人数
                        flogin_num, --登陆人次
                        f7dlogin_unum, --7日内登陆人数
                        f7dlogin_num, --7日内登陆人次
                        f30dlogin_unum, --30日内登陆人次
                        f30dlogin_num --30日内登陆人次
                   from work.user_active_ext_tmp_3_%(statdatenum)s
                ) t
          group by fgamefsk,
                   fplatformfsk,
                   fhallfsk,
                   fterminaltypefsk,
                   fversionfsk,
                   fgame_id,
                   fchannel_code
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res


        # 统计完清理掉临时表
        hql = """drop table if exists work.user_active_ext_tmp_1_b_%(statdatenum)s;
                 drop table if exists work.user_active_ext_tmp_2_b_%(statdatenum)s;
                 drop table if exists work.user_active_ext_tmp_1_%(statdatenum)s;
                 drop table if exists work.user_active_ext_tmp_2_%(statdatenum)s;
                 drop table if exists work.user_active_ext_tmp_3_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_user_active_ext(sys.argv[1:])
a()
