# -*- coding: UTF-8 -*-
#src     :dim.user_gameparty_stream,dim.user_login_additional,dim.bpid_map
#dst     :dcnew.loss_user_rupt_dis
#authot  :SimonRen
#date    :2016-09-02


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


class agg_user_playhabit_hour_dis(BaseStatModel):
    def create_tab(self):

        hql = """--游戏用户游戏时段分布
        create table if not exists dcnew.user_playhabit_hour_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fhourfsk            int        comment '游戏时段:1-24',
               fplay_unum          bigint     comment '玩牌人数',
               fgame_num           bigint     comment '牌局数',
               freg_unum           bigint     comment '注册人数',
               flogin_unum         bigint     comment '登陆人数',
               flogin_ucnt         bigint     comment '登录次数'
               )comment '游戏习惯游戏时段分布'
               partitioned by(dt date)
        location '/dw/dcnew/user_playhabit_hour_dis'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fhourfsk'],
                        'groups':[[1] ] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """
        drop table if exists work.habit_hour_dis_tmp_b_%(statdatenum)s;
        create table work.habit_hour_dis_tmp_b_%(statdatenum)s as
        select fgamefsk,
               fplatformfsk,
               fhallfsk,
               fterminaltypefsk,
               fversionfsk,
               hallmode,
               coalesce(fgame_id,%(null_int_report)d) fgame_id,
               coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code,
               fhourfsk,
               fuid,
               ftbl_id,
               flogin_cnt
          from (select c.fgamefsk,
                       c.fplatformfsk,
                       c.fhallfsk,
                       c.fterminaltypefsk,
                       c.fversionfsk,
                       c.hallmode,
                       a.fgame_id,
                       a.fchannel_code,
                       hour(a.flts_at)+1 fhourfsk,
                       fuid,
                       concat_ws('0',ftbl_id, finning_id) ftbl_id,
                       0 flogin_cnt
                  from dim.user_gameparty_stream a --牌局数据
                  join dim.bpid_map c
                    on a.fbpid = c.fbpid
                 where a.dt="%(statdate)s"
                 union all
                select c.fgamefsk,
                       c.fplatformfsk,
                       c.fhallfsk,
                       c.fterminaltypefsk,
                       c.fversionfsk,
                       c.hallmode,
                       %(null_int_report)d fgame_id,
                       coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code,
                       hour(flogin_at)+1 fhourfsk,
                       a.fuid,
                       null ftbl_id,
                       count(1) flogin_cnt
                  from dim.user_login_additional a --登陆数据
                  join dim.bpid_map c
                    on a.fbpid = c.fbpid
                 where dt = '%(statdate)s'
                 group by c.fgamefsk,
                          c.fplatformfsk,
                          c.fhallfsk,
                          c.fterminaltypefsk,
                          c.fversionfsk,
                          c.hallmode,
                          a.fchannel_code,
                          hour(flogin_at)+1,
                          a.fuid) t
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--子游戏登陆数据
        drop table if exists work.habit_hour_dis_tmp_g_%(statdatenum)s;
        create table work.habit_hour_dis_tmp_g_%(statdatenum)s as
            select coalesce(bm.fgamefsk,%(null_int_group_rule)d) fgamefsk,
                   coalesce(bm.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                   coalesce(bm.fhallfsk,%(null_int_group_rule)d) fhallfsk,
                   coalesce(bm.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                   coalesce(bm.fversionfsk,%(null_int_group_rule)d) fversionfsk,
                   coalesce(us.fchannel_code,%(null_int_group_rule)d) fchannel_code,
                   coalesce(us.fgame_id,%(null_int_group_rule)d) fgame_id,
                   fhourfsk,
                   count(distinct fuid) flogin_unum,
                   sum(flogin_cnt) flogin_ucnt
              from (select fbpid,
                           fuid,
                           hour(flts_at)+1 fhourfsk,
                           coalesce(fgame_id,cast (0 as bigint)) fgame_id,
                           coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code,
                           count(1) flogin_cnt
                      from stage.user_enter_stg us --子游戏登陆数据
                     where dt = '%(statdate)s'
                     group by fbpid,fuid,hour(flts_at)+1,fgame_id,fchannel_code
                   ) us
              join dim.bpid_map bm
                on us.fbpid = bm.fbpid
             group by bm.fgamefsk, bm.fplatformfsk, bm.fhallfsk, bm.fterminaltypefsk, bm.fversionfsk, us.fgame_id, us.fchannel_code,
                      fhourfsk
          grouping sets ((bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,us.fgame_id,bm.fterminaltypefsk,bm.fversionfsk,fhourfsk),
                         (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,us.fgame_id,fhourfsk),
                         (bm.fgamefsk,us.fgame_id,fhourfsk))
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合数据导入到临时表上
                select fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fhourfsk,
                       count(distinct case when ftbl_id is not null then fuid end) fplay_unum,
                       count(distinct ftbl_id) fgame_num,
                       count(distinct case when flogin_cnt >0 then fuid end) flogin_unum,
                       sum(flogin_cnt) flogin_ucnt
                  from work.habit_hour_dis_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,fhourfsk

        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.habit_hour_dis_tmp_%(statdatenum)s;
        create table work.habit_hour_dis_tmp_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.user_playhabit_hour_dis
        partition( dt="%(statdate)s" )
          select "%(statdate)s" fdate,
                 fgamefsk,
                 fplatformfsk,
                 fhallfsk,
                 fgame_id,
                 fterminaltypefsk,
                 fversionfsk,
                 fchannel_code,
                 fhourfsk,
                 sum(fplay_unum) fplay_unum,
                 sum(fgame_num) fgame_num,
                 sum(freg_unum) freg_unum,
                 sum(flogin_unum) flogin_unum,
                 sum(flogin_ucnt) flogin_ucnt
            from (select fgamefsk,
                         fplatformfsk,
                         fhallfsk,
                         fgame_id,
                         fterminaltypefsk,
                         fversionfsk,
                         fchannel_code,
                         fhourfsk,
                         fplay_unum,
                         fgame_num,
                         0 freg_unum,
                         flogin_unum,
                         flogin_ucnt
                    from work.habit_hour_dis_tmp_%(statdatenum)s
                   union all
                  select fgamefsk,
                         fplatformfsk,
                         fhallfsk,
                         fgame_id,
                         fterminaltypefsk,
                         fversionfsk,
                         fchannel_code,
                         fhourfsk,
                         0 fplay_unum,
                         0 fgame_num,
                         0 freg_unum,
                         flogin_unum,
                         flogin_ucnt
                    from work.habit_hour_dis_tmp_g_%(statdatenum)s a
                   union all
                  select fgamefsk,
                         fplatformfsk,
                         fhallfsk,
                         fgame_id,
                         fterminaltypefsk,
                         fversionfsk,
                         fchannel_code,
                         hour(a.fsignup_at)+1 fhourfsk,
                         0 fplay_unum,
                         0 fgame_num,
                         count(distinct fuid) freg_unum,
                         0 flogin_unum,
                         0 flogin_ucnt
                    from dim.reg_user_array a
                   where dt="%(statdate)s"
                   group by fgamefsk,
                            fplatformfsk,
                            fhallfsk,
                            fgame_id,
                            fterminaltypefsk,
                            fversionfsk,
                            fchannel_code,
                            hour(a.fsignup_at)+1
                 ) t
           group by fgamefsk,
                    fplatformfsk,
                    fhallfsk,
                    fgame_id,
                    fterminaltypefsk,
                    fversionfsk,
                    fchannel_code,
                    fhourfsk
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.habit_hour_dis_tmp_b_%(statdatenum)s;
                 drop table if exists work.habit_hour_dis_tmp_%(statdatenum)s;
                 drop table if exists work.habit_hour_dis_tmp_g_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_user_playhabit_hour_dis(sys.argv[1:])
a()
