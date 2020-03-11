#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel


class agg_pay_user_chan_hour_dis(BaseStatModel):
    """ 付费用户各渠道的时段分布"""
    def create_tab(self):
        hql = """
        create table if not exists dcnew.pay_user_chan_hour_dis
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
            fhourfsk                    bigint,
            fm_id                       varchar(256),
            forder_num                  bigint,           --订单数
            fpay_unum                   bigint,          --付费人数
            fincome                     decimal(20,2)          --付费金额
        )
        partitioned by (dt date)
        """
        result = self.sql_exe(hql)
        if result != 0: return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        extend_group = {'fields':['fhourfsk','fm_id'],
                        'groups':[[1,1],
                                  [1,0] ]
                       }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.pay_user_chan_hour_dis_%(statdatenum)s;
        create table work.pay_user_chan_hour_dis_%(statdatenum)s as
            select b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk,b.hallmode,
                    a.fbpid, a.fuid, round(sum(a.fcoins_num * a.frate), 6) fincome,
                    c.fgame_id, c.fchannel_code, count(1) fpaycnt,
                    coalesce(a.fm_id,'%(null_str_report)s') fm_id,
                    hd.fhour fhourfsk
              from stage.payment_stream_stg a
              join dim.user_pay_day c
                on a.fbpid=c.fbpid and a.fuid = c.fuid and c.dt='%(statdate)s'
              join dim.bpid_map b
                on a.fbpid=b.fbpid
              join analysis.hour_dim hd
                on hour(a.fdate) = hd.fhourid
             where a.dt = '%(statdate)s'
             group by b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk,b.hallmode,
                    a.fbpid, a.fuid, a.fm_id, c.fgame_id, c.fchannel_code, hd.fhour
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
         select '%(statdate)s' fdate,
                fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                fhourfsk,
                coalesce(fm_id,'%(null_str_group_rule)s') fm_id,
                sum(fpaycnt) forder_num,
                count(DISTINCT a.fuid) fpay_unum,
                sum(fincome) fincome
           from work.pay_user_chan_hour_dis_%(statdatenum)s a
          where hallmode = %(hallmode)s
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,fhourfsk,fm_id
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.pay_user_chan_hour_dis
        partition (dt = "%(statdate)s")
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.pay_user_chan_hour_dis_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



#生成统计实例
a = agg_pay_user_chan_hour_dis(sys.argv[1:])
a()