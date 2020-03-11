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


class agg_bud_user_level_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_level_dis (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               flevel                int         comment '用户等级',
               act_unum              bigint      comment '活跃用户数',
               play_unum             bigint      comment '玩牌用户数',
               rupt_unum             bigint      comment '破产用户数',
               pay_unum              bigint      comment '付费用户数',
               first_pay_unum        bigint      comment '首付用户数'
               )comment '用户等级分布'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_level_dis';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1
        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        extend_group = {'fields': ['flevel'],
                        'groups': [[1]]}

        # 取基础数据
        hql = """
            drop table if exists work.bud_user_level_dis_tmp_1_%(statdatenum)s;
          create table work.bud_user_level_dis_tmp_1_%(statdatenum)s as
              select fbpid,
                     fuid,
                     max(flevel) flevel
                from (
                      -- 一个用户一天里等级有很多级，有很多垃圾数据，所以取最后一条
                      -- 等级大于300的当垃圾处理
                      select fbpid, fuid, nvl(flevel,0) flevel
                        from (select fbpid,
                                     fuid,
                                     flevel,
                                     row_number() over(partition by fbpid, fuid order by fgrade_at desc, flevel desc) as rowcn
                                from stage.user_grade_stg
                               where dt = '%(statdate)s'
                                 and flevel < 300
                             ) a
                       where rowcn = 1
                       union all
                          -- 还要从登陆的级别里更新，最后根据升级和登陆取最大值
                      select fbpid, fuid,nvl(flevel,0) flevel
                        from (select fbpid,
                                     fuid,
                                     flevel,
                                     row_number() over(partition by fbpid,fuid order by flogin_at desc ) rowcn
                                from dim.user_login_additional
                               where dt='%(statdate)s'
                                 and flevel < 300 and nvl(flevel,0) > 0
                             ) a
                       where rowcn = 1
                    ) t1
                    join dim.bpid_map tt
                      on t1.fbpid = tt.fbpid
                    group by t1.fbpid, fuid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--
            drop table if exists work.bud_user_level_dis_tmp_2_%(statdatenum)s;
          create table work.bud_user_level_dis_tmp_2_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,t1.fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,coalesce(t2.flevel, 0) flevel
                 ,t1.fuid
                 ,3 type
                 ,frupt_cnt type_num
            from dim.user_bankrupt_relieve t1
            left join work.bud_user_level_dis_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,t1.fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,coalesce(t2.flevel, 0) flevel
                 ,t1.fuid
                 ,4 type
                 ,fparty_num type_num
            from dim.user_act t1
            left join work.bud_user_level_dis_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,t1.fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,coalesce(t2.flevel, 0) flevel
                 ,t1.fuid
                 ,5 type
                 ,ftotal_usd_amt type_num
            from dim.user_pay_day t1
            left join work.bud_user_level_dis_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,t1.fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,coalesce(t2.flevel, 0) flevel
                 ,t1.fuid
                 ,6 type
                 ,cast (0 as decimal(20,2)) type_num
            from dim.user_pay t1
            left join work.bud_user_level_dis_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,flevel flevel          --等级
                 ,count(distinct case when type = 4 then fuid end)  act_unum              --活跃用户数
                 ,count(distinct case when type = 4 and type_num >0 then fuid end) play_unum              --玩牌用户数
                 ,count(distinct case when type = 3 then fuid end) rupt_unum              --破产用户数
                 ,count(distinct case when type = 5 then fuid end) pay_unum               --付费用户数
                 ,count(distinct case when type = 6 then fuid end) first_pay_unum         --首付用户数
            from work.bud_user_level_dis_tmp_2_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,flevel
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """insert overwrite table bud_dm.bud_user_level_dis
                      partition(dt='%(statdate)s')
                      %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_level_dis_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_user_level_dis_tmp_2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_level_dis(sys.argv[1:])
a()
