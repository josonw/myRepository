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


class agg_xxx_user_play_info(BaseStatModel):

    def create_tab(self):
        hql = """
        create table if not exists dcnew.xxx_user_play_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fuser_type          varchar(10)   comment '用户类型：ad等',
               fuser_type_id       varchar(100)  comment '用户类型id',
               fplay_unum          bigint        comment '玩牌人数',
               fplay_cnt           bigint        comment '玩牌人次',
               fpaly_num           bigint        comment '牌局数',
               fcharge             bigint        comment '台费',
               fpaly_time          bigint        comment '玩牌时长',
               fwin_amt            bigint        comment '赢得的游戏币',
               flose_amt           bigint        comment '输得的游戏币'
               )comment 'xxx用户玩牌信息表'
               partitioned by(dt date)
        location '/dw/dcnew/xxx_user_play_info'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fuser_type', 'fuser_type_id'],
                        'groups': [[1, 1]]
                        }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """--取当日新增活跃玩牌付费用户
                  drop table if exists work.xxx_user_play_info_tmp_a_%(statdatenum)s;
                create table work.xxx_user_play_info_tmp_a_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,%(null_int_report)d fchannel_code
                 ,t1.fpname
                 ,t1.fsubname
                 ,t1.fuid
                 ,coalesce(t5.fuser_type,'ad') fuser_type
                 ,coalesce(t5.fuser_type_id,'%(null_str_report)s') fuser_type_id
                 ,t1.fparty_num play_cnt  --玩牌次数
            from dim.user_gameparty t1
            left join dim.xxx_user t5
              on t1.fbpid = t5.fbpid
             and t1.fuid = t5.fuid
             and t5.dt <= '%(statdate)s'
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fuser_type,       --用户类型：ad等
                       fuser_type_id,    --用户类型id
                       count(distinct fuid) fplay_unum,          --玩牌人数
                       sum(play_cnt) fplay_cnt,                  --玩牌人次
                       cast (0 as bigint) fpaly_num,             --牌局数
                       cast (0 as bigint) fcharge,               --台费
                       cast (0 as bigint) fpaly_time,            --玩牌时长
                       cast (0 as bigint) fwin_amt,              --赢得的游戏币
                       cast (0 as bigint) flose_amt              --输得的游戏币
                  from work.xxx_user_play_info_tmp_a_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fuser_type,
                       fuser_type_id
        """

        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """drop table if exists work.xxx_user_play_info_tmp_%(statdatenum)s;
          create table work.xxx_user_play_info_tmp_%(statdatenum)s as
            %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--取当日牌局
        drop table if exists work.xxx_user_play_info_tmp_b_%(statdatenum)s;
      create table work.xxx_user_play_info_tmp_b_%(statdatenum)s as
          select distinct t1.fbpid
                 ,ftbl_id                               --桌子编号
                 ,finning_id                            --牌局编号
                 ,coalesce(t5.fuser_type,'ad') fuser_type
                 ,coalesce(t5.fuser_type_id,'%(null_str_report)s') fuser_type_id
            from stage.user_gameparty_stg t1
            join dim.xxx_user t5
              on t1.fbpid = t5.fbpid
             and t1.fuid = t5.fuid
             and t5.dt <= '%(statdate)s'
           where t1.dt = '%(statdate)s';

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--取当日新增活跃玩牌付费用户
        drop table if exists work.xxx_user_play_info_tmp_c_%(statdatenum)s;
      create table work.xxx_user_play_info_tmp_c_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,%(null_int_report)d fchannel_code
                 ,t1.fpname
                 ,t1.fsubname
                 ,coalesce(t5.fuser_type,'ad') fuser_type
                 ,coalesce(t5.fuser_type_id,'%(null_str_report)s') fuser_type_id
                 ,t1.ftbl_id                            --桌子编号
                 ,t1.finning_id                         --牌局编号
                 ,fcharge                               --台费
                 ,fwin_amt                              --赢得的游戏币
                 ,flose_amt                             --输得的游戏币
                 ,fplay_time fplaytime                  --玩牌时长
                 ,fparty_time fmax_paly_time            --牌局时长
                 ,fpalyer_cnt fplayer_cnt               --该场牌局参与人数
            from dim.gameparty_stream t1
            left join work.xxx_user_play_info_tmp_b_%(statdatenum)s t5
              on t1.fbpid = t5.fbpid
             and t1.ftbl_id = t5.ftbl_id
             and t1.finning_id = t5.finning_id
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fuser_type,       --用户类型：ad等
                       fuser_type_id,    --用户类型id
                       0 fplay_unum,          --玩牌人数
                       0 fplay_cnt,                  --玩牌人次
                       count(distinct concat_ws('0', ftbl_id, finning_id) ) fpaly_num,               --牌局数
                       sum(fcharge) fcharge,             --台费
                       sum(fplaytime) fpaly_time,        --玩牌时长
                       sum(fwin_amt) fwin_amt,                --赢得的游戏币
                       sum(flose_amt) flose_amt               --输得的游戏币
                  from work.xxx_user_play_info_tmp_c_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fuser_type,
                       fuser_type_id
        """

        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """insert into table work.xxx_user_play_info_tmp_%(statdatenum)s
            %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 插入目标表
        hql = """  insert overwrite table dcnew.xxx_user_play_info partition(dt='%(statdate)s')
                   select "%(statdate)s" fdate
                          ,fgamefsk
                          ,fplatformfsk
                          ,fhallfsk
                          ,fgame_id
                          ,fterminaltypefsk
                          ,fversionfsk
                          ,fchannel_code
                          ,fuser_type
                          ,fuser_type_id
                          ,max(fplay_unum) fplay_unum            --玩牌人数
                          ,max(fplay_cnt) fplay_cnt              --玩牌人次
                          ,max(fpaly_num) fpaly_num              --牌局数
                          ,max(fcharge) fcharge                  --台费
                          ,max(fpaly_time) fpaly_time            --玩牌时长
                          ,max(fwin_amt) fwin_amt                --赢得的游戏币
                          ,max(flose_amt) flose_amt              --输得的游戏币
                     from work.xxx_user_play_info_tmp_%(statdatenum)s
                    group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                             fuser_type,
                             fuser_type_id;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.xxx_user_play_info_tmp_a_%(statdatenum)s;
                 drop table if exists work.xxx_user_play_info_tmp_b_%(statdatenum)s;
                 drop table if exists work.xxx_user_play_info_tmp_c_%(statdatenum)s;
                 drop table if exists work.xxx_user_play_info_tmp_%(statdatenum)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_xxx_user_play_info(sys.argv[1:])
a()
