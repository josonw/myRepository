#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_paycenter_repair_cheat_mid(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists stage.paycenter_repair_cheat_ord_mid
        (
          flts_at       string,
          fdate         string,
          fbpid         varchar(50),
          sid           decimal(11),
          appid         decimal(11),
          pmode         decimal(5),
          forder_id     varchar(256),
          fplatform_uid varchar(256),
          appname       varchar(128),
          pmodename     varchar(128),
          fusd          decimal(20,7),
          pcoins        decimal(20),
          pchips        decimal(20),
          pdealno       varchar(256),
          pbankno       varchar(256),
          pstatus       decimal(3)
        )partitioned by (dt date);

        create table if not exists analysis.pay_center_repair_cheat_fct
        (
          flts_at           date,
          fsid              decimal(20),
          fappid            decimal(20),
          fbpid             varchar(100),
          frepair_order_num decimal(20),
          frepair_income    decimal(20,4),
          fcheat_order_num  decimal(20),
          fcheat_income     decimal(20,4)
        )partitioned by (dt date);

     """


        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 0

        #加上当天的分区
        hql = """
        use analysis;
        alter table pay_center_repair_cheat_fct add if not exists partition (dt='%(ld_begin)s')
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        -- pstatus = 5 表示欺诈单
        -- pstatus = 2 and (ext_9 = '2') 表示手工补单
        -- 插入欺诈单明细
        insert overwrite table stage.paycenter_repair_cheat_ord_mid partition(dt)
        select flts_at, fdate, fbpid, sid, appid, pmode, forder_id,
                fplatform_uid, appname, pmodename,
                fusd, pcoins, pchips, pdealno, pbankno, pstatus, dt
          from stage.payment_stream_mid
         where dt >= date_add('%(ld_begin)s', -150) and dt < '%(ld_end)s'
           and (pstatus = 2 and ext_9 = '2' or pstatus = 5);
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        -- 将欺诈单和手工补单按天进行排序，放入结果表
        insert overwrite table analysis.pay_center_repair_cheat_fct partition(dt)
        select dt flts_at,
               sid,
               appid,
               fbpid,
               count(case when pstatus = 2 then forder_id end) frepair_order_num,
               sum(case when pstatus = 2 then fusd end) frepair_income,
               count(case when pstatus = 5 then  forder_id end) fcheat_order_num,
               sum(case when pstatus = 5 then fusd end) fcheat_income,
               dt
          from stage.paycenter_repair_cheat_ord_mid
          where dt >= date_add('%(ld_begin)s', -150) and dt < '%(ld_end)s'
          group by dt, sid, appid, fbpid;
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.pay_center_repair_cheat_fct partition(dt='3000-01-01')
        select
        flts_at,
        fsid,
        fappid,
        fbpid,
        frepair_order_num,
        frepair_income,
        fcheat_order_num,
        fcheat_income
        from analysis.pay_center_repair_cheat_fct
        where dt >= '%(ld_30dayago)s' and dt < '%(ld_end)s'
        """ % self.hql_dict
        hql_list.append( hql )


        result = self.exe_hql_list(hql_list)
        return result



if __name__ == "__main__":

    stat_date = ''
    if len(sys.argv) == 1:
        #没有输入参数的话，日期默认取昨天
        stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    else:
        #从输入参数取，实际是第几个就取第几个
        args = sys.argv[1].split(',')
        stat_date = args[0]

    #生成统计实例
    a = agg_paycenter_repair_cheat_mid(stat_date)
    a()
