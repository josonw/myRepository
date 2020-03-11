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

#将原脚本拆为四个，分别计算底层数据，事件维度一、二、三
#
#本脚本计算事件维度三，前置依赖底层数据  event_args

class agg_event_args(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.event_args (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fet_id              varchar(100)   comment '事件ID',
               farg_name           varchar(100)  comment '事件详细操作',
               farg_value          varchar(100)  comment '事件详细操作值',
               farg_cnt            bigint        comment '详细操作次数',
               farg_unum           bigint        comment '参与人数'
               )comment '事件详细操作汇总统计'
               partitioned by(dt date)
        location '/dw/dcnew/event_args';
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
        if res != 0: return res

        hql = """--大厅模式事件+事件参数+事件参数值粒度
                insert overwrite table dcnew.event_args partition(dt="%(statdate)s")
                    select "%(statdate)s" fdate,
                           fgamefsk,
                           coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                           coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                           coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                           coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                           coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                           coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                           fet_id,
                           farg_name,
                           farg_value,
                           count(distinct fet_rk_id) farg_cnt,
                           count(distinct fuid) farg_unum
                      from work.by_event_args_stg_tmp t
                     group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                              fet_id,
                              farg_name,
                              farg_value
                  grouping sets ((fgamefsk,fplatformfsk,fhallfsk,fterminaltypefsk,fversionfsk,fet_id,farg_name,farg_value),
                                 (fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fet_id,farg_name,farg_value))
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 额外处理,只统计参数值不超过150个的事件
        hql = """
                insert overwrite table dcnew.event_args
                  partition(dt="%(statdate)s")
                    select b.fdate,
                           b.fgamefsk,
                           b.fplatformfsk,
                           b.fhallfsk,
                           b.fsubgamefsk,
                           b.fterminaltypefsk,
                           b.fversionfsk,
                           b.fchannelcode,
                           b.fet_id,
                           b.farg_name,
                           b.farg_value,
                           b.farg_cnt,
                           b.farg_unum
                      from dcnew.event_args b
            left semi join (
                            select fgamefsk,
                                   fplatformfsk,
                                   fhallfsk,
                                   fsubgamefsk,
                                   fterminaltypefsk,
                                   fversionfsk,
                                   fchannelcode,
                                   fet_id,
                                   farg_name,
                                   count(distinct farg_value) value_count
                              from dcnew.event_args
                             where dt = "%(statdate)s"
                          group by fgamefsk,
                                   fplatformfsk,
                                   fhallfsk,
                                   fsubgamefsk,
                                   fterminaltypefsk,
                                   fversionfsk,
                                   fchannelcode,
                                   fet_id,
                                   farg_name
                            ) t
                        on b.fgamefsk = t.fgamefsk
                       and b.fplatformfsk = t.fplatformfsk
                       and b.fhallfsk = t.fhallfsk
                       and b.fsubgamefsk = t.fsubgamefsk
                       and b.fterminaltypefsk = t.fterminaltypefsk
                       and b.fversionfsk = t.fversionfsk
                       and b.fchannelcode = t.fchannelcode
                       and b.fet_id = t.fet_id
                       and b.farg_name = t.farg_name
                       and t.value_count < 150
                     where b.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


#生成统计实例
a = agg_event_args(sys.argv[1:])
a()
