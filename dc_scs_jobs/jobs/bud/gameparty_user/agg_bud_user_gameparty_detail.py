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


class agg_bud_user_gameparty_detail(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_gameparty_detail (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fpname              varchar(200)   comment '牌局场次一级分类',
               fsubname            varchar(200)   comment '牌局场次二级分类',
               fplay_unum          bigint         comment '玩牌人数',
               fplay_cnt           bigint         comment '玩牌人次',
               fpay_play_unum      bigint         comment '付费用户玩牌人数',
               fpay_play_cnt       bigint         comment '付费用户玩牌人次',
               fpaly_num           bigint         comment '牌局数',
               fcharge             bigint         comment '台费',
               fpaly_time          bigint         comment '玩牌时长',
               fmax_paly_time      bigint         comment '牌局时长',
               fwin_amt            bigint         comment '赢得的游戏币',
               flose_amt           bigint         comment '输得的游戏币',
               fwin_partynum       bigint         comment '赢牌次数', --增加输赢次数
               flose_partynum      bigint         comment '输牌次数'
               )comment '玩牌用户场次分类表'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_gameparty_detail';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        # 两组组合，共4种
        extend_group_1 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fpname, fsubname),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fpname, fsubname),
                               (fgamefsk, fgame_id, fpname, fsubname) )
                union all """

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, fpname, fsubname),
                               (fgamefsk, fplatformfsk, fhallfsk, fpname, fsubname),
                               (fgamefsk, fplatformfsk, fpname, fsubname) )
                union all """

        extend_group_3 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fpname),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fpname),
                               (fgamefsk, fgame_id, fpname) )
                union all """

        extend_group_4 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, fpname),
                               (fgamefsk, fplatformfsk, fhallfsk, fpname),
                               (fgamefsk, fplatformfsk, fpname) ) """

        query = {'extend_group_1': extend_group_1,
                 'extend_group_2': extend_group_2,
                 'extend_group_3': extend_group_3,
                 'extend_group_4': extend_group_4,
                 'null_str_group_rule': sql_const.NULL_STR_GROUP_RULE,
                 'null_int_group_rule': sql_const.NULL_INT_GROUP_RULE,
                 'statdatenum': """%(statdatenum)s""",
                 'statdate': """%(statdate)s"""}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--取玩牌相关指标
            drop table if exists work.bud_user_gameparty_detail_tmp_1_%(statdatenum)s;
          create table work.bud_user_gameparty_detail_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fpname
                 ,t1.fsubname
                 ,t1.fuid
                 ,t1.fparty_num play_cnt  --玩牌次数
                 ,case when t2.fuid is not null then 1 else 0 end is_pay  --是否付费
                 ,t1.fwin_party_num
                 ,t1.flose_party_num
            from dim.user_gameparty t1
            left join (select distinct fbpid,fuid
                         from dim.user_pay_day
                        where dt = '%(statdate)s'
                      ) t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt='%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        base_hql = """
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,coalesce(fpname,'%(null_str_group_rule)s') fpname
                 ,coalesce(fsubname,'%(null_str_group_rule)s') fsubname
                 ,count(distinct fuid) fplay_unum                                    --玩牌人数
                 ,sum(play_cnt) fplay_cnt                                            --玩牌人次
                 ,count(distinct case when is_pay = 1 then fuid end) fpay_play_unum  --付费用户玩牌人数
                 ,sum(case when is_pay = 1 then play_cnt end) fpay_play_cnt          --付费用户玩牌人次
                 ,cast (0 as bigint) fpaly_num                   --牌局数
                 ,cast (0 as bigint) fcharge                     --台费
                 ,cast (0 as bigint) fpaly_time                  --玩牌时长
                 ,cast (0 as bigint) fmax_paly_time              --牌局时长
                 ,cast (0 as bigint) fwin_amt                    --赢得的游戏币
                 ,cast (0 as bigint) flose_amt                   --输得的游戏币
                 ,sum(fwin_party_num) fwin_partynum              --赢牌局数
                 ,sum(flose_party_num) flose_partynum            --输牌局数
            from work.bud_user_gameparty_detail_tmp_1_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fpname,fsubname
         """

        # 组合
        hql = (
            """drop table if exists work.bud_user_gameparty_detail_tmp_%(statdatenum)s;
          create table work.bud_user_gameparty_detail_tmp_%(statdatenum)s as """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """ +
            base_hql + """%(extend_group_3)s """ +
            base_hql + """%(extend_group_4)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取玩牌相关指标
            drop table if exists work.bud_user_gameparty_detail_tmp_2_%(statdatenum)s;
          create table work.bud_user_gameparty_detail_tmp_2_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,t1.fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fpname
                 ,t1.fsubname
                 ,ftbl_id                               --桌子编号
                 ,finning_id                            --牌局编号
                 ,fcharge                               --台费
                 ,fwin_amt                              --赢得的游戏币
                 ,flose_amt                             --输得的游戏币
                 ,fplay_time fplaytime                  --玩牌时长
                 ,fparty_time fmax_paly_time            --牌局时长
                 ,fpalyer_cnt fplayer_cnt               --该场牌局参与人数
            from dim.gameparty_stream t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总2
        base_hql = """
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,coalesce(fpname,'%(null_str_group_rule)s') fpname
                 ,coalesce(fsubname,'%(null_str_group_rule)s') fsubname
                 ,0 fplay_unum             --玩牌人数
                 ,0 fplay_cnt              --玩牌人次
                 ,0 fpay_play_unum         --付费用户玩牌人数
                 ,0 fpay_play_cnt          --付费用户玩牌人次
                 ,count(distinct concat_ws('0', ftbl_id, finning_id) ) fpaly_num               --牌局数
                 ,sum(fcharge) fcharge             --台费
                 ,sum(fplaytime) fpaly_time        --玩牌时长
                 ,sum(fmax_paly_time) fmax_paly_time    --牌局时长
                 ,sum(fwin_amt) fwin_amt                --赢得的游戏币
                 ,sum(flose_amt) flose_amt              --输得的游戏币
                 ,0 fwin_partynum             --赢牌局数
                 ,0 flose_partynum            --输牌局数
            from work.bud_user_gameparty_detail_tmp_2_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fpname,fsubname
         """

        # 组合
        hql = (
            """insert into table work.bud_user_gameparty_detail_tmp_%(statdatenum)s """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """ +
            base_hql + """%(extend_group_3)s """ +
            base_hql + """%(extend_group_4)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 插入目标表
        hql = """  insert overwrite table bud_dm.bud_user_gameparty_detail  partition(dt='%(statdate)s')
                   select "%(statdate)s" fdate
                          ,fgamefsk
                          ,fplatformfsk
                          ,fhallfsk
                          ,fgame_id
                          ,fterminaltypefsk
                          ,fversionfsk
                          ,fpname
                          ,fsubname
                          ,max(fplay_unum) fplay_unum            --玩牌人数
                          ,max(fplay_cnt) fplay_cnt              --玩牌人次
                          ,max(fpay_play_unum) fpay_play_unum    --付费用户玩牌人数
                          ,max(fpay_play_cnt) fpay_play_cnt      --付费用户玩牌人次
                          ,max(fpaly_num) fpaly_num              --牌局数
                          ,max(fcharge) fcharge                  --台费
                          ,max(fpaly_time) fpaly_time            --玩牌时长
                          ,max(fmax_paly_time) fmax_paly_time    --最长单局时长
                          ,max(fwin_amt) fwin_amt                --赢得的游戏币
                          ,max(flose_amt) flose_amt              --输得的游戏币
                          ,max(fwin_partynum) fwin_partynum      --赢牌局数
                          ,max(flose_partynum) flose_partynum    --输牌局数
                     from work.bud_user_gameparty_detail_tmp_%(statdatenum)s
                    group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                             ,fpname,fsubname;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_gameparty_detail_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_user_gameparty_detail_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_user_gameparty_detail_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_gameparty_detail(sys.argv[1:])
a()
