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

class load_card_room_partner_new(BaseStatModel):
    def create_tab(self):
        """ 棋牌室代理商新增中间表 """
        hql = """create table if not exists dim.card_room_partner_new
            (fbpid           varchar(50)    comment  'BPID',
             fpartner_uid    bigint         comment  '代理商UID',
             fpartner_name   varchar(50)    comment  '代理商名称/昵称',
             fpromoter       varchar(50)    comment  '推广员',
             flts_at         varchar(50)    comment  '时间'
            )
            partitioned by (dt string)
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分,统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dim.card_room_partner_new partition (dt = "%(statdate)s" )
                select  t1.fbpid
                       ,t1.fuid fpartner_uid
                       ,t1.fpartner_name
                       ,case when t1.fpromoter <> '' and t1.fpromoter is not null then t1.fpromoter else '%(null_str_report)s' end fpromoter
                       ,t1.flts_at
                  from stage.partner_join_stg t1
                 where t1.dt = '%(statdate)s'
                   and t1.fpartner_type = '1'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_card_room_partner_new(sys.argv[1:])
a()
