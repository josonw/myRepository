#! /usr/local/python272/bin/python
#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime

from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStat import BaseStat
import service.sql_const as sql_const


"""
创建留存用户中间表，可以用来计算玩牌，付费，活跃，新增等单维度留存统计
"""
class load_user_retain_array(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists dim.user_retain_array
        (
            fdate                string,            --字符串类型的活跃日期
            fgamefsk             bigint,            --游戏ID
            fplatformfsk         bigint,            --平台ID
            fhallfsk             bigint,            --大厅ID
            fsubgamefsk          bigint,            --子游戏ID
            fterminaltypefsk     bigint,            --终端ID
            fversionfsk          bigint,            --版本ID
            fchannelcode         bigint,            --渠道ID
            fuid                 bigint,            --用户ID
            flogin_cnt           int,               --登录次数
            fparty_num           int,               --牌局数
            fsignup_at           string,            --注册时间
            fpay_cnt             int,               --支付成功次数
            fincome              decimal(20,2),     --支付成功总金额，以美元为单位
            fplatform_uid        varchar(50)        --平台uid
        )
        partitioned by (dt date)
        stored as orc
        """

        res = self.hq.exe_sql(hql)
        return res

    def stat(self):
        query = {}

        query.update(PublicFunc.date_define(self.stat_date))

        # res = self.hq.exe_sql("""set mapreduce.job.name= %s; """%self.__class__.__name__)
        # if res != 0: return res

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dim.user_retain_array
        partition( dt="%(ld_daybegin)s" )
           select "%(ld_daybegin)s" fdate,
              a.fgamefsk, a.fplatformfsk, a.fhallfsk, a.fsubgamefsk, a.fterminaltypefsk, a.fversionfsk, a.fchannelcode,
              a.fuid,
              a.flogin_cnt ,
              a.fparty_num ,
              to_date(b.fsignup_at) fsignup_at,
              coalesce(c.fpay_cnt,0) fpay_cnt,
              coalesce(c.ftotal_usd_amt,0) fincome,
              c.fplatform_uid
           from dim.user_act_array a
           left join dim.reg_user_array b
             on a.fgamefsk = b.fgamefsk
            and a.fplatformfsk = b.fplatformfsk
            and a.fhallfsk = b.fhallfsk
            and a.fsubgamefsk = b.fgame_id
            and a.fterminaltypefsk = b.fterminaltypefsk
            and a.fversionfsk = b.fversionfsk
            and a.fchannelcode = b.fchannel_code
            and a.fuid = b.fuid
            and b.dt >= '%(ld_30dayago)s' and b.dt < '%(ld_dayend)s'
           left join dim.user_pay_array c
             on a.fgamefsk = c.fgamefsk
            and a.fplatformfsk = c.fplatformfsk
            and a.fhallfsk = c.fhallfsk
            and a.fsubgamefsk = c.fsubgamefsk
            and a.fterminaltypefsk = c.fterminaltypefsk
            and a.fversionfsk = c.fversionfsk
            and a.fchannelcode = c.fchannelcode
            and a.fuid = c.fuid
            and c.dt = '%(ld_daybegin)s'
          where a.dt = '%(ld_daybegin)s'
          """ % query

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话,日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取,实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = load_user_retain_array(statDate)
a()
