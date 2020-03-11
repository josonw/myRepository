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
#本脚本计算事件维度二，前置依赖底层数据  event_args_new

class agg_event_args_new(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.event_args_new (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fet_id              varchar(100) comment '事件ID',
               farg_name           varchar(100) comment '事件详细操作',
               farg_cnt            bigint       comment '详细操作次数',
               farg_unum           bigint       comment '参与人数',
               farg_value_sum      bigint       comment '事件详细操作汇总值'
               )comment '事件详细操作统计'
               partitioned by(dt date)
        location '/dw/dcnew/event_args_new';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """
                --大厅模式事件+事件参数粒度
                insert overwrite table dcnew.event_args_new partition(dt="%(statdate)s")
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
                           count(distinct fet_rk_id) farg_cnt,
                           count(distinct fuid) farg_unum,
                           sum(cast(farg_value as bigint)) farg_value_sum
                      from work.by_event_args_stg_tmp t
                     group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                              fet_id,
                              farg_name
                  grouping sets ((fgamefsk,fplatformfsk,fhallfsk,fterminaltypefsk,fversionfsk,fet_id,farg_name),
                                 (fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fet_id,farg_name))
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


#生成统计实例
a = agg_event_args_new(sys.argv[1:])
a()
