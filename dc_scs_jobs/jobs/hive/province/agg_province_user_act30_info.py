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


class agg_province_user_act30_info(BaseStatModel):
    def create_tab(self):
        hql = """--分省数据_用户相关
        create table if not exists dcnew.province_user_act30_info (
                fdate               date,
                fgamefsk            bigint,
                fplatformfsk        bigint,
                fhallfsk            bigint,
                fsubgamefsk         bigint,
                fterminaltypefsk    bigint,
                fversionfsk         bigint,
                fchannelcode        bigint,
                fprovince           varchar(64)     comment '省份',
                f30act_unum         bigint          comment '最近30天活跃用户数'
                )comment '分省数据_用户相关'
                partitioned by(dt date)
          location '/dw/dcnew/province_user_act30_info'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        #四组组合，共14种
        extend_group_1 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id),
                               (fgamefsk, fplatformfsk, fgame_id) )
                union all """

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk),
                               (fgamefsk, fplatformfsk, fhallfsk),
                               (fgamefsk, fplatformfsk, fterminaltypefsk),
                               (fgamefsk, fplatformfsk) )
                union all """

        extend_group_3 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fprovince),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fprovince),
                               (fgamefsk, fplatformfsk, fgame_id, fprovince) )
                union all """

        extend_group_4 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fprovince),
                               (fgamefsk, fplatformfsk, fhallfsk, fprovince),
                               (fgamefsk, fplatformfsk, fterminaltypefsk, fprovince),
                               (fgamefsk, fplatformfsk, fprovince) ) """

        query = {'extend_group_1':extend_group_1,
                 'extend_group_2':extend_group_2,
                 'extend_group_3':extend_group_3,
                 'extend_group_4':extend_group_4,
                 'null_str_group_rule' : sql_const.NULL_STR_GROUP_RULE,
                 'null_int_group_rule' : sql_const.NULL_INT_GROUP_RULE,
                 'statdatenum'         : """%(statdatenum)s""",
                 'statdate'         : """%(statdate)s"""}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0: return res

        hql = """--取最近30天活跃用户数
        drop table if exists work.province_user_act30_tmp_1_%(statdatenum)s;
        create table work.province_user_act30_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(d) */ distinct c.fgamefsk,
                 c.fplatformfsk,
                 c.fhallfsk,
                 c.fterminaltypefsk,
                 c.fplatformname fprovince,
                 coalesce(t1.fgame_id,%(null_int_report)d) fgame_id,
                 t1.fuid
            from dim.user_act t1
            join dim.bpid_map c
              on t1.fbpid=c.fbpid
             and c.fgamefsk = 4132314431  --地方棋牌
           where t1.dt between '%(ld_29day_ago)s' and '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        base_hql = """
                select '%(statdate)s' fdate,
                       fgamefsk,
                       fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       -1 fversionfsk,
                       -1 fchannel_code,
                       coalesce(fprovince,'%(null_str_group_rule)s') fprovince,
                       count(distinct fuid) f30act_unum
                  from work.province_user_act30_tmp_1_%(statdatenum)s gs
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,
                       fprovince """

        hql = (""" insert overwrite table dcnew.province_user_act30_info
                    partition(dt='%(statdate)s') """ +
               base_hql + """%(extend_group_1)s """ +
               base_hql + """%(extend_group_2)s """ +
               base_hql + """%(extend_group_3)s """ +
               base_hql + """%(extend_group_4)s """)% query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.province_user_act30_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res



#生成统计实例
a = agg_province_user_act30_info(sys.argv[1:])
a()
