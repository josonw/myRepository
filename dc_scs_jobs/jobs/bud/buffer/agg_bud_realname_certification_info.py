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


class agg_bud_realname_certification_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_realname_certification_info (
               fdate                 string,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               fnew_realname_unum    bigint         comment  '新增用户实名数',
               fact_realname_unum    bigint         comment  '活跃用户实名数',
               fall_realname_num     bigint         comment  '累计实名人数',
               fall_realname_unum    bigint         comment  '累计实名用户数',
               fall_realname_adult   bigint         comment  '累计实名用户数（未成年）',
               fall_realname_minor   bigint         comment  '累计实名用户数（成年）'
               )comment '实名认证信息表'
               partitioned by(dt string)
        location '/dw/bud_dm/bud_realname_certification_info';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        extend_group = {'fields': ['fuid', 'age', 'id_card_number'],
                        'groups': [[1, 1, 1]]}

        # 取基础数据
        hql = """--实名认证数据
            drop table if exists work.bud_realname_certification_info_tmp_1_%(statdatenum)s;
          create table work.bud_realname_certification_info_tmp_1_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.age
                 ,t1.id_card_number
                 ,t1.mid fuid
            from veda.dfqp_user_portrait_basic t1
            join dim.bpid_map tt
              on t1.signup_bpid = tt.fbpid
           where id_card_number is not null
             and id_card_number <> ''
             and t1.realname_certify_date <= '%(statdate)s'
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
                 ,fuid
                 ,age
                 ,id_card_number
                 ,count(1) cnt
            from work.bud_realname_certification_info_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                 ,fuid
                 ,age
                 ,id_card_number
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
            drop table if exists work.bud_realname_certification_info_tmp_%(statdatenum)s;
          create table work.bud_realname_certification_info_tmp_%(statdatenum)s as
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
                insert overwrite table bud_dm.bud_realname_certification_info
                      partition(dt='%(statdate)s')
              select '%(statdate)s' fdate
                     ,t1.fgamefsk
                     ,t1.fplatformfsk
                     ,t1.fhallfsk
                     ,t1.fgame_id fsubgamefsk
                     ,t1.fterminaltypefsk
                     ,t1.fversionfsk
                     ,count(distinct t2.fuid) fnew_realname_unum
                     ,count(distinct t3.fuid) fact_realname_unum
                     ,count(distinct t1.id_card_number) fall_realname_num
                     ,count(distinct t1.fuid) fall_realname_unum
                     ,count(distinct case when t1.age >= 18 then t1.fuid end) fall_realname_adult
                     ,count(distinct case when t1.age < 18 then t1.fuid end) fall_realname_minor
                from work.bud_realname_certification_info_tmp_%(statdatenum)s t1
                left join dim.reg_user_array t2
                  on t1.fuid = t2.fuid
                 and t1.fgamefsk = t2.fgamefsk
                 and t1.fplatformfsk = t2.fplatformfsk
                 and t1.fhallfsk = t2.fhallfsk
                 and t1.fgame_id = t2.fgame_id
                 and t1.fterminaltypefsk = t2.fterminaltypefsk
                 and t1.fversionfsk = t2.fversionfsk
                 and t2.dt = '%(statdate)s'
                left join dim.user_act_array t3
                  on t1.fuid = t3.fuid
                 and t1.fgamefsk = t3.fgamefsk
                 and t1.fplatformfsk = t3.fplatformfsk
                 and t1.fhallfsk = t3.fhallfsk
                 and t1.fgame_id = t3.fsubgamefsk
                 and t1.fterminaltypefsk = t3.fterminaltypefsk
                 and t1.fversionfsk = t3.fversionfsk
                 and t3.dt = '%(statdate)s'
               group by t1.fgamefsk
                       ,t1.fplatformfsk
                       ,t1.fhallfsk
                       ,t1.fgame_id
                       ,t1.fterminaltypefsk
                       ,t1.fversionfsk
                """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_realname_certification_info_tmp_%(statdatenum)s;
                 drop table if exists work.bud_realname_certification_info_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_realname_certification_info(sys.argv[1:])
a()
