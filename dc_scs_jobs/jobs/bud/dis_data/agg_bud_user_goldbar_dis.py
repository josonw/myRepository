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


class agg_bud_user_goldbar_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_goldbar_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fuser_type          varchar(20)      comment '用户类型:reg_user,act_user',
               fparty_type         varchar(20)      comment '牌局类型',
               fmatch_rule_type    varchar(50)      comment '赛制类型',
               fgsubname           varchar(50)      comment '三级场次',
               fdata_type          varchar(20)      comment '数据类型:unum-用户,num-总金额',
               num_0               bigint           comment '0',
               num_5               bigint           comment '[0,5]',
               num_10              bigint           comment '[6,10]',
               num_30              bigint           comment '[11,30]',
               num_60              bigint           comment '[31,60]',
               num_100             bigint           comment '[61,100]',
               num_200             bigint           comment '[101,200]',
               num_300             bigint           comment '[201,300]',
               num_400             bigint           comment '[301,400]',
               num_500             bigint           comment '[401,500]',
               num_1000            bigint           comment '[501,1000]',
               num_2000            bigint           comment '[1001,2000]',
               num_5000            bigint           comment '[2001,5000]',
               num_10000           bigint           comment '[5001,10000]',
               num_50000           bigint           comment '[10001,50000]',
               num_100000          bigint           comment '[50001,100000]',
               num_m100000         bigint           comment '>100000'
               )comment '用户金条分布'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_goldbar_dis';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fparty_type', 'fmatch_rule_type', 'fgsubname', 'fuid'],
                        'groups': [[1, 1, 1, 1],
                                   [1, 0, 0, 1],
                                   [0, 0, 1, 1],
                                   [1, 1, 0, 1],
                                   [0, 0, 0, 1],
                                   [0, 1, 0, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--报名用户
            drop table if exists work.bud_user_goldbar_dis_tmp_1_%(statdatenum)s;
          create table work.bud_user_goldbar_dis_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t2.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t2.fuid
                 ,t2.fparty_type
                 ,t2.fgsubname
                 ,t2.fmatch_rule_type
            from dim.join_gameparty t2  --报名
            join dim.bpid_map_bud tt
              on t2.fbpid = tt.fbpid
           where t2.dt = '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总 --报名用户
        hql = """
          select fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,coalesce(fparty_type,'%(null_str_group_rule)s')  fparty_type
                 ,coalesce(fgsubname,'%(null_str_group_rule)s')  fgsubname
                 ,coalesce(fmatch_rule_type, '%(null_str_group_rule)s') fmatch_rule_type
                 ,'join_user' fuser_type
                 ,fuid
                 ,count(1) num
            from work.bud_user_goldbar_dis_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fparty_type
                    ,fgsubname
                    ,fmatch_rule_type
                    ,fuid
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
            drop table if exists work.bud_user_goldbar_dis_tmp_%(statdatenum)s;
          create table work.bud_user_goldbar_dis_tmp_%(statdatenum)s as
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--比赛用户
            drop table if exists work.bud_user_goldbar_dis_tmp_2_%(statdatenum)s;
          create table work.bud_user_goldbar_dis_tmp_2_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t2.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t2.fuid
                 ,t2.fparty_type
                 ,t2.fgsubname
                 ,t2.fmatch_rule_type
                 ,case when t3.fuid is not null then 1 else 0 end is_fr
                 ,case when t4.fuid is not null then 1 else 0 end is_co
            from dim.match_gameparty t2  --牌局
            left join (select distinct fmatch_id,fuid
                         from dim.join_gameparty
                        where dt = '%(statdate)s'
                          and coalesce(fentry_fee,0) = 0
                      ) t3
              on t2.fmatch_id = t3.fmatch_id
             and t2.fuid = t3.fuid
            left join (select distinct fmatch_id,fuid
                         from dim.join_gameparty
                        where dt = '%(statdate)s'
                          and coalesce(fentry_fee,0) > 0
                      ) t4
              on t2.fmatch_id = t4.fmatch_id
             and t2.fuid = t4.fuid
            join dim.bpid_map_bud tt
              on t2.fbpid = tt.fbpid
           where t2.dt = '%(statdate)s'
             and coalesce(t2.fmatch_id,'0')<>'0';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总 --比赛用户
        hql = """
          select fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,coalesce(fparty_type,'%(null_str_group_rule)s')  fparty_type
                 ,coalesce(fgsubname,'%(null_str_group_rule)s')  fgsubname
                 ,coalesce(fmatch_rule_type, '%(null_str_group_rule)s') fmatch_rule_type
                 ,'match_user' fuser_type
                 ,fuid
                 ,count(1) num
            from work.bud_user_goldbar_dis_tmp_2_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fparty_type
                    ,fgsubname
                    ,fmatch_rule_type
                    ,fuid
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
            insert into work.bud_user_goldbar_dis_tmp_%(statdatenum)s
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总 --免费报名比赛用户
        hql = """
          select fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,coalesce(fparty_type,'%(null_str_group_rule)s')  fparty_type
                 ,coalesce(fgsubname,'%(null_str_group_rule)s')  fgsubname
                 ,coalesce(fmatch_rule_type, '%(null_str_group_rule)s') fmatch_rule_type
                 ,'fr_match_user' fuser_type
                 ,fuid
                 ,count(1) num
            from work.bud_user_goldbar_dis_tmp_2_%(statdatenum)s t
           where hallmode = %(hallmode)s and is_fr = 1
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fparty_type
                    ,fgsubname
                    ,fmatch_rule_type
                    ,fuid
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
            insert into work.bud_user_goldbar_dis_tmp_%(statdatenum)s
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总 --付费报名比赛用户
        hql = """
          select fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,coalesce(fparty_type,'%(null_str_group_rule)s')  fparty_type
                 ,coalesce(fgsubname,'%(null_str_group_rule)s')  fgsubname
                 ,coalesce(fmatch_rule_type, '%(null_str_group_rule)s') fmatch_rule_type
                 ,'co_match_user' fuser_type
                 ,fuid
                 ,count(1) num
            from work.bud_user_goldbar_dis_tmp_2_%(statdatenum)s t
           where hallmode = %(hallmode)s and is_co = 1
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fparty_type
                    ,fgsubname
                    ,fmatch_rule_type
                    ,fuid
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
            insert into work.bud_user_goldbar_dis_tmp_%(statdatenum)s
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """ --活跃用户
            insert into work.bud_user_goldbar_dis_tmp_%(statdatenum)s
            select fgamefsk
                   ,fplatformfsk
                   ,fhallfsk
                   ,fsubgamefsk fgame_id
                   ,fterminaltypefsk
                   ,fversionfsk
                   ,'-21379' fparty_type
                   ,'-21379' fgsubname
                   ,'-21379' fmatch_rule_type
                   ,'act_user' fuser_type
                   ,fuid
                   ,0 num
              from dim.user_act_array t
             where t.dt = "%(statdate)s"
               and fgamefsk = 4132314431;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """ --新增用户
            insert into work.bud_user_goldbar_dis_tmp_%(statdatenum)s
            select fgamefsk
                   ,fplatformfsk
                   ,fhallfsk
                   ,fgame_id
                   ,fterminaltypefsk
                   ,fversionfsk
                   ,'-21379' fparty_type
                   ,'-21379' fgsubname
                   ,'-21379' fmatch_rule_type
                   ,'reg_user' fuser_type
                   ,fuid
                   ,0 num
              from dim.reg_user_array t
             where t.dt = "%(statdate)s"
               and fgamefsk = 4132314431;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--所有用户
            insert into work.bud_user_goldbar_dis_tmp_%(statdatenum)s
            select fgamefsk
                   ,fplatformfsk
                   ,fhallfsk
                   ,fsubgamefsk fgame_id
                   ,fterminaltypefsk
                   ,fversionfsk
                   ,'-21379' fparty_type
                   ,'-21379' fgsubname
                   ,'-21379' fmatch_rule_type
                   ,'all_user' fuser_type
                   ,fuid
                   ,0 num
              from dim.user_currencies_balance_array t
             where t.dt = "%(statdate)s"
               and fgamefsk = 4132314431
               and fcurrencies_type = '11';
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总 --总金额
        hql = """
         insert overwrite table bud_dm.bud_user_goldbar_dis
         partition( dt="%(statdate)s" )
          select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,coalesce(t.fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(t.fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(t.fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(t.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(t.fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuser_type
                 ,coalesce(fparty_type,'%(null_str_group_rule)s')  fparty_type
                 ,coalesce(fmatch_rule_type, '%(null_str_group_rule)s') fmatch_rule_type
                 ,coalesce(fgsubname,'%(null_str_group_rule)s')  fgsubname
                 ,'num_all' fdata_type
                 ,sum(case when nvl(fcurrencies_num + fbank_gamecoins_num,0) = 0 then nvl(fcurrencies_num + fbank_gamecoins_num,0) end) num_0
                 ,sum(case when fcurrencies_num + fbank_gamecoins_num between 1 and 5 then nvl(fcurrencies_num + fbank_gamecoins_num,0) end) num_5
                 ,sum(case when fcurrencies_num + fbank_gamecoins_num between 6 and 10 then nvl(fcurrencies_num + fbank_gamecoins_num,0) end) num_10
                 ,sum(case when fcurrencies_num + fbank_gamecoins_num between 11 and 30 then nvl(fcurrencies_num + fbank_gamecoins_num,0) end) num_30
                 ,sum(case when fcurrencies_num + fbank_gamecoins_num between 31 and 60 then nvl(fcurrencies_num + fbank_gamecoins_num,0) end) num_60
                 ,sum(case when fcurrencies_num + fbank_gamecoins_num between 61 and 100 then nvl(fcurrencies_num + fbank_gamecoins_num,0) end) num_100
                 ,sum(case when fcurrencies_num + fbank_gamecoins_num between 101 and 200 then nvl(fcurrencies_num + fbank_gamecoins_num,0) end) num_200
                 ,sum(case when fcurrencies_num + fbank_gamecoins_num between 201 and 300 then nvl(fcurrencies_num + fbank_gamecoins_num,0) end) num_300
                 ,sum(case when fcurrencies_num + fbank_gamecoins_num between 301 and 400 then nvl(fcurrencies_num + fbank_gamecoins_num,0) end) num_400
                 ,sum(case when fcurrencies_num + fbank_gamecoins_num between 401 and 500 then nvl(fcurrencies_num + fbank_gamecoins_num,0) end) num_500
                 ,sum(case when fcurrencies_num + fbank_gamecoins_num between 501 and 1000 then nvl(fcurrencies_num + fbank_gamecoins_num,0) end) num_1000
                 ,sum(case when fcurrencies_num + fbank_gamecoins_num between 1001 and 2000 then nvl(fcurrencies_num + fbank_gamecoins_num,0) end) num_2000
                 ,sum(case when fcurrencies_num + fbank_gamecoins_num between 2001 and 5000 then nvl(fcurrencies_num + fbank_gamecoins_num,0) end) num_5000
                 ,sum(case when fcurrencies_num + fbank_gamecoins_num between 5001 and 10000 then nvl(fcurrencies_num + fbank_gamecoins_num,0) end) num_10000
                 ,sum(case when fcurrencies_num + fbank_gamecoins_num between 10001 and 50000 then nvl(fcurrencies_num + fbank_gamecoins_num,0) end) num_50000
                 ,sum(case when fcurrencies_num + fbank_gamecoins_num between 50001 and 100000 then nvl(fcurrencies_num + fbank_gamecoins_num,0) end) num_100000
                 ,sum(case when fcurrencies_num + fbank_gamecoins_num > 100000 then nvl(fcurrencies_num + fbank_gamecoins_num,0) end) num_m100000
            from work.bud_user_goldbar_dis_tmp_%(statdatenum)s t
            left join dim.user_currencies_balance_array t1
              on t.fgamefsk = t1.fgamefsk
             and t.fplatformfsk = t1.fplatformfsk
             and t.fhallfsk = t1.fhallfsk
             and t.fterminaltypefsk = t1.fterminaltypefsk
             and t.fversionfsk = t1.fversionfsk
             and t.fuid = t1.fuid
             and t1.fcurrencies_type = '11'
             and t1.dt = "%(statdate)s"
           where t.fgamefsk = 4132314431
           group by t.fgamefsk,t.fplatformfsk,t.fhallfsk,t.fgame_id,t.fterminaltypefsk,t.fversionfsk
                    ,fparty_type
                    ,fgsubname
                    ,fmatch_rule_type
                    ,fuser_type
            union all

          select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,coalesce(t.fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(t.fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(t.fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(t.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(t.fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuser_type
                 ,coalesce(fparty_type,'%(null_str_group_rule)s')  fparty_type
                 ,coalesce(fmatch_rule_type, '%(null_str_group_rule)s') fmatch_rule_type
                 ,coalesce(fgsubname,'%(null_str_group_rule)s')  fgsubname
                 ,'unum_all' fdata_type
                 ,count(distinct case when nvl(fcurrencies_num + fbank_gamecoins_num,0) = 0 then t.fuid end) num_0
                 ,count(distinct case when fcurrencies_num + fbank_gamecoins_num between 1 and 5 then t.fuid end) num_5
                 ,count(distinct case when fcurrencies_num + fbank_gamecoins_num between 6 and 10 then t.fuid end) num_10
                 ,count(distinct case when fcurrencies_num + fbank_gamecoins_num between 11 and 30 then t.fuid end) num_30
                 ,count(distinct case when fcurrencies_num + fbank_gamecoins_num between 31 and 60 then t.fuid end) num_60
                 ,count(distinct case when fcurrencies_num + fbank_gamecoins_num between 61 and 100 then t.fuid end) num_100
                 ,count(distinct case when fcurrencies_num + fbank_gamecoins_num between 101 and 200 then t.fuid end) num_200
                 ,count(distinct case when fcurrencies_num + fbank_gamecoins_num between 201 and 300 then t.fuid end) num_300
                 ,count(distinct case when fcurrencies_num + fbank_gamecoins_num between 301 and 400 then t.fuid end) num_400
                 ,count(distinct case when fcurrencies_num + fbank_gamecoins_num between 401 and 500 then t.fuid end) num_500
                 ,count(distinct case when fcurrencies_num + fbank_gamecoins_num between 501 and 1000 then t.fuid end) num_1000
                 ,count(distinct case when fcurrencies_num + fbank_gamecoins_num between 1001 and 2000 then t.fuid end) num_2000
                 ,count(distinct case when fcurrencies_num + fbank_gamecoins_num between 2001 and 5000 then t.fuid end) num_5000
                 ,count(distinct case when fcurrencies_num + fbank_gamecoins_num between 5001 and 10000 then t.fuid end) num_10000
                 ,count(distinct case when fcurrencies_num + fbank_gamecoins_num between 10001 and 50000 then t.fuid end) num_50000
                 ,count(distinct case when fcurrencies_num + fbank_gamecoins_num between 50001 and 100000 then t.fuid end) num_100000
                 ,count(distinct case when fcurrencies_num + fbank_gamecoins_num > 100000 then t.fuid end) num_m100000
            from work.bud_user_goldbar_dis_tmp_%(statdatenum)s t
            left join dim.user_currencies_balance_array t1
              on t.fgamefsk = t1.fgamefsk
             and t.fplatformfsk = t1.fplatformfsk
             and t.fhallfsk = t1.fhallfsk
             and t.fterminaltypefsk = t1.fterminaltypefsk
             and t.fversionfsk = t1.fversionfsk
             and t.fuid = t1.fuid
             and t1.fcurrencies_type = '11'
             and t1.dt = "%(statdate)s"
           where t.fgamefsk = 4132314431
           group by t.fgamefsk,t.fplatformfsk,t.fhallfsk,t.fgame_id,t.fterminaltypefsk,t.fversionfsk
                    ,fparty_type
                    ,fgsubname
                    ,fmatch_rule_type
                    ,fuser_type
            union all
          select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,coalesce(t.fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(t.fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(t.fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(t.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(t.fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuser_type
                 ,coalesce(fparty_type,'%(null_str_group_rule)s')  fparty_type
                 ,coalesce(fmatch_rule_type, '%(null_str_group_rule)s') fmatch_rule_type
                 ,coalesce(fgsubname,'%(null_str_group_rule)s')  fgsubname
                 ,'num_bring' fdata_type
                 ,sum(case when nvl(fcurrencies_num,0) = 0 then nvl(fcurrencies_num,0) end) num_0
                 ,sum(case when fcurrencies_num between 1 and 5 then nvl(fcurrencies_num,0) end) num_5
                 ,sum(case when fcurrencies_num between 6 and 10 then nvl(fcurrencies_num,0) end) num_10
                 ,sum(case when fcurrencies_num between 11 and 30 then nvl(fcurrencies_num,0) end) num_30
                 ,sum(case when fcurrencies_num between 31 and 60 then nvl(fcurrencies_num,0) end) num_60
                 ,sum(case when fcurrencies_num between 61 and 100 then nvl(fcurrencies_num,0) end) num_100
                 ,sum(case when fcurrencies_num between 101 and 200 then nvl(fcurrencies_num,0) end) num_200
                 ,sum(case when fcurrencies_num between 201 and 300 then nvl(fcurrencies_num,0) end) num_300
                 ,sum(case when fcurrencies_num between 301 and 400 then nvl(fcurrencies_num,0) end) num_400
                 ,sum(case when fcurrencies_num between 401 and 500 then nvl(fcurrencies_num,0) end) num_500
                 ,sum(case when fcurrencies_num between 501 and 1000 then nvl(fcurrencies_num,0) end) num_1000
                 ,sum(case when fcurrencies_num between 1001 and 2000 then nvl(fcurrencies_num,0) end) num_2000
                 ,sum(case when fcurrencies_num between 2001 and 5000 then nvl(fcurrencies_num,0) end) num_5000
                 ,sum(case when fcurrencies_num between 5001 and 10000 then nvl(fcurrencies_num,0) end) num_10000
                 ,sum(case when fcurrencies_num between 10001 and 50000 then nvl(fcurrencies_num,0) end) num_50000
                 ,sum(case when fcurrencies_num between 50001 and 100000 then nvl(fcurrencies_num,0) end) num_100000
                 ,sum(case when fcurrencies_num > 100000 then nvl(fcurrencies_num,0) end) num_m100000
            from work.bud_user_goldbar_dis_tmp_%(statdatenum)s t
            left join dim.user_currencies_balance_array t1
              on t.fgamefsk = t1.fgamefsk
             and t.fplatformfsk = t1.fplatformfsk
             and t.fhallfsk = t1.fhallfsk
             and t.fterminaltypefsk = t1.fterminaltypefsk
             and t.fversionfsk = t1.fversionfsk
             and t.fuid = t1.fuid
             and t1.fcurrencies_type = '11'
             and t1.dt = "%(statdate)s"
           where t.fgamefsk = 4132314431
           group by t.fgamefsk,t.fplatformfsk,t.fhallfsk,t.fgame_id,t.fterminaltypefsk,t.fversionfsk
                    ,fparty_type
                    ,fgsubname
                    ,fmatch_rule_type
                    ,fuser_type
            union all

          select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,coalesce(t.fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(t.fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(t.fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(t.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(t.fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuser_type
                 ,coalesce(fparty_type,'%(null_str_group_rule)s')  fparty_type
                 ,coalesce(fmatch_rule_type, '%(null_str_group_rule)s') fmatch_rule_type
                 ,coalesce(fgsubname,'%(null_str_group_rule)s')  fgsubname
                 ,'unum_bring' fdata_type
                 ,count(distinct case when nvl(fcurrencies_num,0) = 0 then t.fuid end) num_0
                 ,count(distinct case when fcurrencies_num between 1 and 5 then t.fuid end) num_5
                 ,count(distinct case when fcurrencies_num between 6 and 10 then t.fuid end) num_10
                 ,count(distinct case when fcurrencies_num between 11 and 30 then t.fuid end) num_30
                 ,count(distinct case when fcurrencies_num between 31 and 60 then t.fuid end) num_60
                 ,count(distinct case when fcurrencies_num between 61 and 100 then t.fuid end) num_100
                 ,count(distinct case when fcurrencies_num between 101 and 200 then t.fuid end) num_200
                 ,count(distinct case when fcurrencies_num between 201 and 300 then t.fuid end) num_300
                 ,count(distinct case when fcurrencies_num between 301 and 400 then t.fuid end) num_400
                 ,count(distinct case when fcurrencies_num between 401 and 500 then t.fuid end) num_500
                 ,count(distinct case when fcurrencies_num between 501 and 1000 then t.fuid end) num_1000
                 ,count(distinct case when fcurrencies_num between 1001 and 2000 then t.fuid end) num_2000
                 ,count(distinct case when fcurrencies_num between 2001 and 5000 then t.fuid end) num_5000
                 ,count(distinct case when fcurrencies_num between 5001 and 10000 then t.fuid end) num_10000
                 ,count(distinct case when fcurrencies_num between 10001 and 50000 then t.fuid end) num_50000
                 ,count(distinct case when fcurrencies_num between 50001 and 100000 then t.fuid end) num_100000
                 ,count(distinct case when fcurrencies_num > 100000 then t.fuid end) num_m100000
            from work.bud_user_goldbar_dis_tmp_%(statdatenum)s t
            left join dim.user_currencies_balance_array t1
              on t.fgamefsk = t1.fgamefsk
             and t.fplatformfsk = t1.fplatformfsk
             and t.fhallfsk = t1.fhallfsk
             and t.fterminaltypefsk = t1.fterminaltypefsk
             and t.fversionfsk = t1.fversionfsk
             and t.fuid = t1.fuid
             and t1.fcurrencies_type = '11'
             and t1.dt = "%(statdate)s"
           where t.fgamefsk = 4132314431
           group by t.fgamefsk,t.fplatformfsk,t.fhallfsk,t.fgame_id,t.fterminaltypefsk,t.fversionfsk
                    ,fparty_type
                    ,fgsubname
                    ,fmatch_rule_type
                    ,fuser_type
            union all
          select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,coalesce(t.fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(t.fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(t.fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(t.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(t.fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuser_type
                 ,coalesce(fparty_type,'%(null_str_group_rule)s')  fparty_type
                 ,coalesce(fmatch_rule_type, '%(null_str_group_rule)s') fmatch_rule_type
                 ,coalesce(fgsubname,'%(null_str_group_rule)s')  fgsubname
                 ,'num_bank' fdata_type
                 ,sum(case when nvl(fbank_gamecoins_num,0) = 0 then nvl(fbank_gamecoins_num,0) end) num_0
                 ,sum(case when fbank_gamecoins_num between 1 and 5 then nvl(fbank_gamecoins_num,0) end) num_5
                 ,sum(case when fbank_gamecoins_num between 6 and 10 then nvl(fbank_gamecoins_num,0) end) num_10
                 ,sum(case when fbank_gamecoins_num between 11 and 30 then nvl(fbank_gamecoins_num,0) end) num_30
                 ,sum(case when fbank_gamecoins_num between 31 and 60 then nvl(fbank_gamecoins_num,0) end) num_60
                 ,sum(case when fbank_gamecoins_num between 61 and 100 then nvl(fbank_gamecoins_num,0) end) num_100
                 ,sum(case when fbank_gamecoins_num between 101 and 200 then nvl(fbank_gamecoins_num,0) end) num_200
                 ,sum(case when fbank_gamecoins_num between 201 and 300 then nvl(fbank_gamecoins_num,0) end) num_300
                 ,sum(case when fbank_gamecoins_num between 301 and 400 then nvl(fbank_gamecoins_num,0) end) num_400
                 ,sum(case when fbank_gamecoins_num between 401 and 500 then nvl(fbank_gamecoins_num,0) end) num_500
                 ,sum(case when fbank_gamecoins_num between 501 and 1000 then nvl(fbank_gamecoins_num,0) end) num_1000
                 ,sum(case when fbank_gamecoins_num between 1001 and 2000 then nvl(fbank_gamecoins_num,0) end) num_2000
                 ,sum(case when fbank_gamecoins_num between 2001 and 5000 then nvl(fbank_gamecoins_num,0) end) num_5000
                 ,sum(case when fbank_gamecoins_num between 5001 and 10000 then nvl(fbank_gamecoins_num,0) end) num_10000
                 ,sum(case when fbank_gamecoins_num between 10001 and 50000 then nvl(fbank_gamecoins_num,0) end) num_50000
                 ,sum(case when fbank_gamecoins_num between 50001 and 100000 then nvl(fbank_gamecoins_num,0) end) num_100000
                 ,sum(case when fbank_gamecoins_num > 100000 then nvl(fbank_gamecoins_num,0) end) num_m100000
            from work.bud_user_goldbar_dis_tmp_%(statdatenum)s t
            left join dim.user_currencies_balance_array t1
              on t.fgamefsk = t1.fgamefsk
             and t.fplatformfsk = t1.fplatformfsk
             and t.fhallfsk = t1.fhallfsk
             and t.fterminaltypefsk = t1.fterminaltypefsk
             and t.fversionfsk = t1.fversionfsk
             and t.fuid = t1.fuid
             and t1.fcurrencies_type = '11'
             and t1.dt = "%(statdate)s"
           where t.fgamefsk = 4132314431
           group by t.fgamefsk,t.fplatformfsk,t.fhallfsk,t.fgame_id,t.fterminaltypefsk,t.fversionfsk
                    ,fparty_type
                    ,fgsubname
                    ,fmatch_rule_type
                    ,fuser_type
            union all

          select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,coalesce(t.fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(t.fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(t.fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(t.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(t.fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuser_type
                 ,coalesce(fparty_type,'%(null_str_group_rule)s')  fparty_type
                 ,coalesce(fmatch_rule_type, '%(null_str_group_rule)s') fmatch_rule_type
                 ,coalesce(fgsubname,'%(null_str_group_rule)s')  fgsubname
                 ,'unum_bank' fdata_type
                 ,count(distinct case when nvl(fbank_gamecoins_num,0) = 0 then t.fuid end) num_0
                 ,count(distinct case when fbank_gamecoins_num between 1 and 5 then t.fuid end) num_5
                 ,count(distinct case when fbank_gamecoins_num between 6 and 10 then t.fuid end) num_10
                 ,count(distinct case when fbank_gamecoins_num between 11 and 30 then t.fuid end) num_30
                 ,count(distinct case when fbank_gamecoins_num between 31 and 60 then t.fuid end) num_60
                 ,count(distinct case when fbank_gamecoins_num between 61 and 100 then t.fuid end) num_100
                 ,count(distinct case when fbank_gamecoins_num between 101 and 200 then t.fuid end) num_200
                 ,count(distinct case when fbank_gamecoins_num between 201 and 300 then t.fuid end) num_300
                 ,count(distinct case when fbank_gamecoins_num between 301 and 400 then t.fuid end) num_400
                 ,count(distinct case when fbank_gamecoins_num between 401 and 500 then t.fuid end) num_500
                 ,count(distinct case when fbank_gamecoins_num between 501 and 1000 then t.fuid end) num_1000
                 ,count(distinct case when fbank_gamecoins_num between 1001 and 2000 then t.fuid end) num_2000
                 ,count(distinct case when fbank_gamecoins_num between 2001 and 5000 then t.fuid end) num_5000
                 ,count(distinct case when fbank_gamecoins_num between 5001 and 10000 then t.fuid end) num_10000
                 ,count(distinct case when fbank_gamecoins_num between 10001 and 50000 then t.fuid end) num_50000
                 ,count(distinct case when fbank_gamecoins_num between 50001 and 100000 then t.fuid end) num_100000
                 ,count(distinct case when fbank_gamecoins_num > 100000 then t.fuid end) num_m100000
            from work.bud_user_goldbar_dis_tmp_%(statdatenum)s t
            left join dim.user_currencies_balance_array t1
              on t.fgamefsk = t1.fgamefsk
             and t.fplatformfsk = t1.fplatformfsk
             and t.fhallfsk = t1.fhallfsk
             and t.fterminaltypefsk = t1.fterminaltypefsk
             and t.fversionfsk = t1.fversionfsk
             and t.fuid = t1.fuid
             and t1.fcurrencies_type = '11'
             and t1.dt = "%(statdate)s"
           where t.fgamefsk = 4132314431
           group by t.fgamefsk,t.fplatformfsk,t.fhallfsk,t.fgame_id,t.fterminaltypefsk,t.fversionfsk
                    ,fparty_type
                    ,fgsubname
                    ,fmatch_rule_type
                    ,fuser_type
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总 --总金额，反插子游戏数据
        hql = """
         insert into table bud_dm.bud_user_goldbar_dis
         partition( dt="%(statdate)s" )
          select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,%(null_int_group_rule)d fplatformfsk
                 ,%(null_int_group_rule)d fhallfsk
                 ,fsubgamefsk
                 ,%(null_int_group_rule)d fterminaltypefsk
                 ,%(null_int_group_rule)d fversionfsk
                 ,fuser_type
                 ,fparty_type
                 ,fmatch_rule_type
                 ,fgsubname
                 ,fdata_type
                 ,sum(num_0) num_0
                 ,sum(num_5) num_5
                 ,sum(num_10) num_10
                 ,sum(num_30) num_30
                 ,sum(num_60) num_60
                 ,sum(num_100) num_100
                 ,sum(num_200) num_200
                 ,sum(num_300) num_300
                 ,sum(num_400) num_400
                 ,sum(num_500) num_500
                 ,sum(num_1000) num_1000
                 ,sum(num_2000) num_2000
                 ,sum(num_5000) num_5000
                 ,sum(num_10000) num_10000
                 ,sum(num_50000) num_50000
                 ,sum(num_100000) num_100000
                 ,sum(num_m100000) num_m100000
            from bud_dm.bud_user_goldbar_dis t
           where dt="%(statdate)s"
             and fplatformfsk <> %(null_int_group_rule)d
             and fhallfsk <> %(null_int_group_rule)d
             and fsubgamefsk <> %(null_int_group_rule)d
             and fterminaltypefsk = %(null_int_group_rule)d
             and fversionfsk = %(null_int_group_rule)d
           group by t.fgamefsk,t.fsubgamefsk
                    ,fuser_type
                    ,fparty_type
                    ,fmatch_rule_type
                    ,fgsubname
                    ,fdata_type
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_goldbar_dis_tmp_%(statdatenum)s;
                 drop table if exists work.bud_user_goldbar_dis_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_user_goldbar_dis_tmp_2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_goldbar_dis(sys.argv[1:])
a()
