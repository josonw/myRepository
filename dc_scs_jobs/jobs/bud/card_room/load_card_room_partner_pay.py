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

class load_card_room_partner_pay(BaseStatModel):
    def create_tab(self):
        """ 棋牌室代理商付费中间表 """
        hql = """create table if not exists dim.card_room_partner_pay
            (fbpid           varchar(50)    comment  'BPID',
             fpartner_uid    bigint         comment  '代理商UID',
             fpartner_name   varchar(50)    comment  '代理商名称/昵称',
             fpromoter       varchar(50)    comment  '推广员',
             fcard_num       bigint         comment  '购买房卡数量',
             fcard_cnt       bigint         comment  '购买房卡次数',
             fcard_income    decimal(20,4)  comment  '购买房卡金额'
            )
            partitioned by (dt string)
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
        insert overwrite table dim.card_room_partner_pay partition (dt = "%(statdate)s" )
                select  t1.fbpid
                        ,t1.fuid fpartner_uid
                        ,t2.fpartner_name
                        ,coalesce(t2.fpromoter, '%(null_str_report)s') fpromoter
                        ,sum(t1.fp_num) fcard_num
                        ,count(1) fcard_cnt
                        ,sum(t1.fpamount_usd) fcard_income
                  from stage.payment_stream_stg t1
                  left join dim.card_room_partner_new t2
                    on t1.fbpid = t2.fbpid
                   and t1.fuid = t2.fpartner_uid
                 where t1.dt = '%(statdate)s'
                   and (t1.fproduct_id = '6757' or t1.fproduct_name like '%%房卡%%')
                 group by t1.fbpid
                          ,t1.fuid
                          ,t2.fpartner_name
                          ,t2.fpromoter
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_card_room_partner_pay(sys.argv[1:])
a()
