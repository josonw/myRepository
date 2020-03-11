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


class agg_user_version_detail(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.user_version_detail (
               fdate                  date,
               fgamefsk               bigint,
               fplatformfsk           bigint,
               fhallfsk               bigint,
               fsubgamefsk            bigint,
               fterminaltypefsk       bigint,
               fversionfsk            bigint,
               fchannelcode           bigint,
               fversion_info         varchar(20)      comment '版本类型',
               reg_unum              bigint           comment '新增用户数',
               game_unum             bigint           comment '登录用户数',
               act_unum              bigint           comment '活跃用户数',
               play_unum             bigint           comment '玩牌用户数',
               rupt_unum             bigint           comment '破产用户数',
               rupt_num              bigint           comment '破产次数',
               pay_unum              bigint           comment '付费用户数',
               first_pay_unum        bigint           comment '首付用户数',
               reg_pay_unum          bigint           comment '注册付费用户数',
               pay_income            decimal(20,2)    comment '付费总额',
               frealname_unum        bigint           comment '实名用户数',
               frealname_adult       bigint           comment '实名用户数（成年）',
               frealname_minor       bigint           comment '实名用户数（未成年）'
               )comment '用户版本分布'
               partitioned by(dt date)
        location '/dw/dcnew/user_version_detail'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0:
            return res

        hql = """--大厅版本数据
       drop table if exists work.user_version_detail_tmp_a_%(statdatenum)s;
     create table work.user_version_detail_tmp_a_%(statdatenum)s as
          select t1.fbpid
                 ,fuid
                 ,max(fversion_info) fversion_info
            from (select fbpid, fuid, coalesce(fversion_info, '%(null_str_report)s') fversion_info
                    from dim.user_login_additional
                   where dt= '%(statdate)s'
                   union all
                  select fbpid, fuid, coalesce(fversion_info, '%(null_str_report)s') fversion_info
                    from dim.reg_user_main_additional
                   where dt= '%(statdate)s'
                   union all
                  select fbpid, fuid, coalesce(fversion_info, '%(null_str_report)s') fversion_info
                    from stage.pb_gamecoins_stream_stg
                   where dt= '%(statdate)s'
                   union all
                  select fbpid, fuid, coalesce(fversion_info, '%(null_str_report)s') fversion_info
                    from stage.user_gameparty_stg
                   where dt= '%(statdate)s'
                 ) t1
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           group by t1.fbpid, fuid;
          """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--大厅用户数据
       drop table if exists work.user_version_detail_tmp_b_%(statdatenum)s;
     create table work.user_version_detail_tmp_b_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,%(null_int_report)d fgame_id
                 ,%(null_int_report)d fchannel_code
                 ,tt.hallmode
                 ,coalesce(t1.fversion_info, '0') fversion_info
                 ,t1.fuid
                 ,1 type
                 ,t3.ftotal_usd_amt type_num
            from dim.reg_user_main_additional t1
            left join dim.user_pay_day t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
             and t3.dt = '%(statdate)s'
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,%(null_int_report)d fgame_id
                 ,%(null_int_report)d fchannel_code
                 ,tt.hallmode
                 ,coalesce(t2.fversion_info, '0') fversion_info
                 ,t1.fuid
                 ,2 type
                 ,0 type_num
            from dim.user_login_additional t1
            left join work.user_version_detail_tmp_a_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,%(null_int_report)d fgame_id
                 ,%(null_int_report)d fchannel_code
                 ,tt.hallmode
                 ,coalesce(t2.fversion_info, '0') fversion_info
                 ,t1.fuid
                 ,3 type
                 ,frupt_cnt type_num
            from dim.user_bankrupt_relieve t1
            left join work.user_version_detail_tmp_a_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,%(null_int_report)d fgame_id
                 ,%(null_int_report)d fchannel_code
                 ,tt.hallmode
                 ,coalesce(t2.fversion_info, '0') fversion_info
                 ,t1.fuid
                 ,4 type
                 ,fparty_num type_num
            from dim.user_act t1
            left join work.user_version_detail_tmp_a_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,%(null_int_report)d fgame_id
                 ,%(null_int_report)d fchannel_code
                 ,tt.hallmode
                 ,coalesce(t2.fversion_info, '0') fversion_info
                 ,cast (t1.fplatform_uid as bigint) fuid
                 ,5 type
                 ,round(fcoins_num * frate, 2) type_num
            from stage.payment_stream_stg t1
            left join dim.user_pay t
              on t1.fbpid = t.fbpid
             and t1.fplatform_uid = t.fplatform_uid
            left join work.user_version_detail_tmp_a_%(statdatenum)s t2
              on t.fbpid = t2.fbpid
             and t.fuid = t2.fuid
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,%(null_int_report)d fgame_id
                 ,%(null_int_report)d fchannel_code
                 ,tt.hallmode
                 ,coalesce(t2.fversion_info, '0') fversion_info
                 ,cast (t1.fplatform_uid as bigint) fuid
                 ,6 type
                 ,cast (0 as decimal(20,2)) type_num
            from stage.payment_stream_stg t1
            join dim.user_pay t
              on t1.fbpid = t.fbpid
             and t1.fplatform_uid = t.fplatform_uid
             and t.dt = '%(statdate)s'
            left join work.user_version_detail_tmp_a_%(statdatenum)s t2
              on t.fbpid = t2.fbpid
             and t.fuid = t2.fuid
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,%(null_int_report)d fgame_id
                 ,%(null_int_report)d fchannel_code
                 ,tt.hallmode
                 ,coalesce(t2.fversion_info, '0') fversion_info
                 ,t1.mid fuid
                 ,7 type
                 ,age type_num
            from veda.dfqp_user_portrait_basic t1
            join work.user_version_detail_tmp_a_%(statdatenum)s t2
              on t1.signup_bpid = t2.fbpid
             and t1.mid = t2.fuid
            join dim.bpid_map tt
              on t1.signup_bpid = tt.fbpid
           where id_card_number is not null
             and id_card_number <> '';
          """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--大厅用户
        insert overwrite table dcnew.user_version_detail partition( dt="%(statdate)s" )
                select  "%(statdate)s" fdate
                       ,fgamefsk
                       ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                       ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                       ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                       ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                       ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                       ,coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code
                       ,fversion_info          --版本类型
                       ,count(distinct case when type = 1 then fuid end) reg_unum               --新增用户数
                       ,count(distinct case when type = 2 then fuid end) game_unum              --登录用户数
                       ,count(distinct case when type = 4 then fuid end)  act_unum              --活跃用户数
                       ,count(distinct case when type = 4 and type_num >0 then fuid end) play_unum              --玩牌用户数
                       ,count(distinct case when type = 3  and type_num > 0 then fuid end) rupt_unum              --破产用户数
                       ,sum(case when type = 3 then type_num end) rupt_num               --破产次数
                       ,count(distinct case when type = 5 then fuid end) pay_unum               --付费用户数
                       ,count(distinct case when type = 6 then fuid end) first_pay_unum         --首付用户数
                       ,count(distinct case when type = 1 and type_num > 0 then fuid end) reg_pay_unum           --注册付费用户数
                       ,sum(case when type = 5 then type_num end) pay_income             --付费总额
                       ,count(distinct case when type = 7 then fuid end) frealname_unum  --实名用户数
                       ,count(distinct case when type = 7 and type_num >= 18 then fuid end) frealname_adult --实名用户数（成年）
                       ,count(distinct case when type = 7 and type_num < 18 then fuid end) frealname_minor --实名用户数（未成年）
                  from work.user_version_detail_tmp_b_%(statdatenum)s gs
                 where hallmode = 1
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,fversion_info
           grouping sets ((fgamefsk,fplatformfsk,fhallfsk,fterminaltypefsk,fversionfsk,fversion_info),
                          (fgamefsk,fplatformfsk,fhallfsk,fversion_info),
                          (fgamefsk,fplatformfsk,fversion_info))
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--大厅用户
        insert into table dcnew.user_version_detail partition( dt="%(statdate)s" )
                select  "%(statdate)s" fdate
                       ,fgamefsk
                       ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                       ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                       ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                       ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                       ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                       ,coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code
                       ,fversion_info          --版本类型
                       ,count(distinct case when type = 1 then fuid end) reg_unum               --新增用户数
                       ,count(distinct case when type = 2 then fuid end) game_unum              --登录用户数
                       ,count(distinct case when type = 4 then fuid end)  act_unum              --活跃用户数
                       ,count(distinct case when type = 4 and type_num >0 then fuid end) play_unum              --玩牌用户数
                       ,count(distinct case when type = 3  and type_num > 0 then fuid end) rupt_unum              --破产用户数
                       ,sum(case when type = 3 then type_num end) rupt_num               --破产次数
                       ,count(distinct case when type = 5 then fuid end) pay_unum               --付费用户数
                       ,count(distinct case when type = 6 then fuid end) first_pay_unum         --首付用户数
                       ,count(distinct case when type = 1 and type_num > 0 then fuid end) reg_pay_unum           --注册付费用户数
                       ,sum(case when type = 5 then type_num end) pay_income             --付费总额
                       ,count(distinct case when type = 7 then fuid end) frealname_unum  --实名用户数
                       ,count(distinct case when type = 7 and type_num >= 18 then fuid end) frealname_adult --实名用户数（成年）
                       ,count(distinct case when type = 7 and type_num < 18 then fuid end) frealname_minor --实名用户数（未成年）
                  from work.user_version_detail_tmp_b_%(statdatenum)s gs
                 where hallmode = 0
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,fversion_info
grouping sets ((fgamefsk,fplatformfsk,fterminaltypefsk,fversionfsk,fversion_info),
               (fgamefsk,fplatformfsk,fversion_info))
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--非大厅版本数据
       drop table if exists work.user_version_detail_tmp_c_%(statdatenum)s;
     create table work.user_version_detail_tmp_c_%(statdatenum)s as
          select t1.fbpid
                 ,fuid
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,max(fversion_info) fversion_info
            from (select fbpid, fuid,fgame_id, coalesce(fversion_info, '%(null_str_report)s') fversion_info
                    from stage.user_enter_stg
                   where dt= '%(statdate)s'
                 ) t1
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
            group by t1.fbpid, fuid,fgame_id;
          """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--非大厅用户数据
       drop table if exists work.user_version_detail_tmp_d_%(statdatenum)s;
     create table work.user_version_detail_tmp_d_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,%(null_int_report)d fchannel_code
                 ,tt.hallmode
                 ,coalesce(t1.fversion_info, '0') fversion_info
                 ,t1.fuid
                 ,1 type
                 ,t3.ftotal_usd_amt type_num
            from dim.reg_user_sub t1
            left join dim.user_pay_day t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
             and t3.dt = '%(statdate)s'
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
             and t1.fis_first = 1
           union all
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,%(null_int_report)d fchannel_code
                 ,tt.hallmode
                 ,coalesce(t2.fversion_info, '0') fversion_info
                 ,t1.fuid
                 ,2 type
                 ,0 type_num
            from dim.reg_user_sub t1
            left join work.user_version_detail_tmp_c_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t1.fgame_id = t2.fgame_id
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,%(null_int_report)d fchannel_code
                 ,tt.hallmode
                 ,coalesce(t2.fversion_info, '0') fversion_info
                 ,t1.fuid
                 ,3 type
                 ,frupt_cnt type_num
            from dim.user_bankrupt_relieve t1
            left join work.user_version_detail_tmp_c_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t1.fgame_id = t2.fgame_id
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,%(null_int_report)d fchannel_code
                 ,tt.hallmode
                 ,coalesce(t2.fversion_info, '0') fversion_info
                 ,t1.fuid
                 ,4 type
                 ,fparty_num type_num
            from dim.user_act t1
            left join work.user_version_detail_tmp_c_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t1.fgame_id = t2.fgame_id
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,coalesce(t3.fgame_id,cast (0 as bigint)) fgame_id
                 ,%(null_int_report)d fchannel_code
                 ,tt.hallmode
                 ,coalesce(t2.fversion_info, '0') fversion_info
                 ,cast (t1.fplatform_uid as bigint) fuid
                 ,5 type
                 ,round(fcoins_num * frate, 2) type_num
            from stage.payment_stream_stg t1
            left join stage.user_generate_order_stg t3
              on t1.forder_id = t3.forder_id
             and t3.dt = '%(statdate)s'
            left join dim.user_pay t
              on t3.fbpid = t.fbpid
             and t3.fplatform_uid = t.fplatform_uid
            left join work.user_version_detail_tmp_c_%(statdatenum)s t2
              on t.fbpid = t2.fbpid
             and t.fuid = t2.fuid
             and t3.fgame_id = t2.fgame_id
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,coalesce(t.fgame_id,%(null_int_report)d) fgame_id
                 ,%(null_int_report)d fchannel_code
                 ,tt.hallmode
                 ,coalesce(t2.fversion_info, '0') fversion_info
                 ,cast (t1.fplatform_uid as bigint) fuid
                 ,6 type
                 ,cast (0 as decimal(20,2)) type_num
            from stage.payment_stream_stg t1
            join dim.user_pay t
              on t1.fbpid = t.fbpid
             and t1.fplatform_uid = t.fplatform_uid
             and t.dt = '%(statdate)s'
            left join work.user_version_detail_tmp_c_%(statdatenum)s t2
              on t.fbpid = t2.fbpid
             and t.fuid = t2.fuid
             and t.fgame_id = t2.fgame_id
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';
          """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--子游戏
        insert into table dcnew.user_version_detail partition( dt="%(statdate)s" )
                select  "%(statdate)s" fdate
                       ,fgamefsk
                       ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                       ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                       ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                       ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                       ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                       ,coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code
                       ,fversion_info          --版本类型
                       ,count(distinct case when type = 1 then fuid end) reg_unum               --新增用户数
                       ,count(distinct case when type = 2 then fuid end) game_unum              --登录用户数
                       ,count(distinct case when type = 4 then fuid end)  act_unum              --活跃用户数
                       ,count(distinct case when type = 4 and type_num >0 then fuid end) play_unum              --玩牌用户数
                       ,count(distinct case when type = 3  and type_num > 0 then fuid end) rupt_unum              --破产用户数
                       ,sum(case when type = 3 then type_num end) rupt_num               --破产次数
                       ,count(distinct case when type = 5 then fuid end) pay_unum               --付费用户数
                       ,count(distinct case when type = 6 then fuid end) first_pay_unum         --首付用户数
                       ,count(distinct case when type = 1 and type_num > 0 then fuid end) reg_pay_unum           --注册付费用户数
                       ,sum(case when type = 5 then type_num end) pay_income             --付费总额
                       ,0 frealname_unum  --实名用户数
                       ,0 frealname_adult --实名用户数（成年）
                       ,0 frealname_minor --实名用户数（未成年）
                  from work.user_version_detail_tmp_d_%(statdatenum)s gs
                 where hallmode = 1
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,fversion_info
grouping sets ((fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fversion_info),
               (fgamefsk,fplatformfsk,fhallfsk,fgame_id,fversion_info),
               (fgamefsk,fgame_id,fversion_info))
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.user_version_detail_tmp_a_%(statdatenum)s;
                 drop table if exists work.user_version_detail_tmp_b_%(statdatenum)s;
                 drop table if exists work.user_version_detail_tmp_c_%(statdatenum)s;
                 drop table if exists work.user_version_detail_tmp_d_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_user_version_detail(sys.argv[1:])
a()
