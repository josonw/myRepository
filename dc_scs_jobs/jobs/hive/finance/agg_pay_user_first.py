#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
import sys
from sys import path

path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_pay_user_first(BaseStatModel):
    """ 用户首次付费结果表 """
    def create_tab(self):
        hql = """
        create table if not exists dcnew.pay_user_first
            (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
            ffpayucnt                   bigint,
            ffpayincome                 decimal(38,2),
            fregpayusernum              bigint,
            fpay_cnt            INT         COMMENT '当日付费次数' --20160912增加
            )
            partitioned by (dt date);
        """
        result = self.sql_exe(hql)
        if result != 0:return result


    def stat(self):
        """ 重要部分，统计内容  """

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0: return res


        hql_list = []
        alias_dic = {'bpid_tbl_alias':'b.','src_tbl_alias':'a.', 'const_alias':''}
        query = sql_const.query_list(self.stat_date, alias_dic, None)

        for k, v in enumerate(query):
            hql = """
            -- 首付用户数和首付金额
            select '%(statdate)s' fdate,
                %(select_field_str)s,
                count(distinct a.fuid) ffpayucnt,
                sum( nvl(c.ftotal_usd_amt,0)) ffpayincome,
                0 fregpayusernum,
                sum(c.fpay_cnt) fpay_cnt
            from dim.user_pay a
            join dim.user_pay_day c
              on a.fbpid = c.fbpid
             and a.fuid = c.fuid
             and c.dt = '%(statdate)s'
            join dim.bpid_map b
              on a.fbpid = b.fbpid
             and b.hallmode=%(hallmode)s
            where a.dt = '%(statdate)s'
            %(group_by)s
            """
            self.sql_args['sql_'+ str(k)] = self.sql_build(hql, v)


        hql = """
        insert overwrite table dcnew.pay_user_first
        partition (dt = "%(statdate)s")
        %(sql_0)s;
        insert into table dcnew.pay_user_first
        partition( dt="%(statdate)s" )
        %(sql_1)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """
        insert into table dcnew.pay_user_first
        partition( dt="%(statdate)s" )
        select  '%(statdate)s' fdate,
            b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fsubgamefsk, b.fterminaltypefsk, b.fversionfsk, b.fchannelcode,
            0 ffpayucnt,
            0 ffpayincome,
            count(distinct a.fuid) fregpayusernum,
            0 fpay_cnt
        from dim.reg_user_array a
        join dim.user_pay_array b
          on a.fgamefsk = b.fgamefsk
         and a.fplatformfsk = b.fplatformfsk
         and a.fhallfsk = b.fhallfsk
         and a.fgame_id = b.fsubgamefsk
         and a.fterminaltypefsk = b.fterminaltypefsk
         and a.fversionfsk = b.fversionfsk
         and a.fchannel_code = b.fchannelcode
         and a.fuid = b.fuid
         and b.dt = '%(statdate)s'
       where a.dt = '%(statdate)s'
        group by b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fsubgamefsk, b.fterminaltypefsk, b.fversionfsk, b.fchannelcode
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """
        insert overwrite table dcnew.pay_user_first
        partition( dt="%(statdate)s" )
        select
            "%(statdate)s" fdate,
            fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
            max(ffpayucnt) ffpayucnt,
            max(ffpayincome) ffpayincome,
            max(fregpayusernum) fregpayusernum,
            max(fpay_cnt) fpay_cnt
        from dcnew.pay_user_first
        where dt="%(statdate)s"
        group by fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res




a = agg_pay_user_first(sys.argv[1:])
a()
