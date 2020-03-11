#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_pay_user_order(BaseStatModel):
    """ 统计下单用户数，下单时破产用户数 """
    def create_tab(self):
        hql = """
        create table if not exists dcnew.pay_user_order
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
            fordercnt                   bigint,
            forderusercnt               bigint,
            fbankruptincome             decimal(38,2),
            fbankruptusercnt            bigint
        ) partitioned by (dt date);
        """
        result = self.sql_exe(hql)
        if result != 0:return result


    def stat(self):
        """ 重要部分，统计内容 """
        # self.hq.debug = 1

        alias_dic = {'bpid_tbl_alias':'b.','src_tbl_alias':'a.', 'const_alias':''}
        query = sql_const.query_list(self.stat_date, alias_dic, None)

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        for k, v in enumerate(query):
            hql = """
            select '%(statdate)s' fdate,
                    %(select_field_str)s,
                    count(1) fordercnt,
                    count(distinct fuid) forderusercnt,
                    sum( case when fbankrupt = 1 then coalesce(fincome,0) else 0 end ) fbankruptincome,
                    count(distinct case when fincome>0 and fbankrupt>0 then fuid end) fbankruptusercnt
                from dim.user_pay_payscene a
                join dim.bpid_map b
                  on a.fbpid = b.fbpid and b.hallmode=%(hallmode)s
                where a.dt = '%(statdate)s'
                %(group_by)s
            """
            self.sql_args['sql_'+ str(k)] = self.sql_build(hql, v)

        hql = """
        insert overwrite table dcnew.pay_user_order
        partition (dt = "%(statdate)s")
        %(sql_0)s;
        insert into table dcnew.pay_user_order
        partition( dt="%(statdate)s" )
        %(sql_1)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res
        return res

    #生成统计实例
a = agg_pay_user_order(sys.argv[1:])
a()
