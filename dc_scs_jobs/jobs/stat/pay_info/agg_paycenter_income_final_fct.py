#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_paycenter_income_final_fct(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.pay_center_final_order_fct
        (
          forder_date       date,
          fsid              decimal(20),
          fappid            decimal(20),
          fpmode            decimal(20),
          fbpid             varchar(100),
          fpay_user_num     decimal(20),
          forder_num        decimal(20),
          fincome           decimal(20,4),
          frepair_order_num decimal(20),
          frepair_income    decimal(20,4),
          fcheat_income     decimal(20,4)
        )
        partitioned by (dt date);
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
        alter table pay_center_final_order_fct add if not exists partition (dt='%(ld_begin)s')
        """ % self.hql_dict
        hql_list.append( hql )

        # 注意这里计算的，手工补单和欺诈单统计的是下单日期的值
        # 而页面展示的手工补单和欺诈单的时间是操作日期

        # 在此把支付的逻辑梳理下
        # pstatus = 2 and ext_9 = 2    表示手工补单
        # pstatus = 3 and ext_9 = 3    表示手工退单
        # pstatus = 5 表示fb欺诈单（有fb平台自动发起的退单）
        hql = """
        add jar hdfs://192.168.0.92:8020/dw/udf/nexr-hive-udf-0.4.jar;
        CREATE TEMPORARY FUNCTION trunc AS 'com.nexr.platform.hive.udf.UDFTrunc';

        insert overwrite table analysis.pay_center_final_order_fct partition(dt)
        select forder_date, fsid, fappid, fpmode,
            max(fbpid) as fbpid,
            sum(fpay_user_num) as fpay_user_num,
            sum(forder_num) as forder_num,
            sum(fincome) as fincome,
            sum(frepair_order_num) as frepair_order_num,
            sum(frepair_income) as frepair_income,
            sum(fcheat_income) as fcheat_income,
            forder_date dt
        from
        (
            select trunc(fdate, 'DD') forder_date,
                sid as fsid, appid as fappid, pmode as fpmode,
                max(fbpid) as fbpid,
                count(distinct fplatform_uid) as fpay_user_num,
                count(distinct forder_id) as forder_num,
                sum(fusd) as fincome,
                0 as frepair_order_num,
                0 as frepair_income,
                0 as fcheat_income
            from stage.payment_stream_mid
           where dt >= date_add('%(ld_begin)s', -150) and dt < '%(ld_end)s'
             and pstatus = 2
             and fdate >= date_add('%(ld_begin)s', -150)
            group by trunc(fdate, 'DD'), sid, appid, pmode

            union all

            -- 退单补单要看的是流水日发生的，不是订单日发生的
            -- 11.27日，改为原来的逻辑
            select dt forder_date,
                sid as fsid, appid as fappid, pmode as fpmode,
                max(fbpid) as fbpid,
                0 as fpay_user_num,
                0 as forder_num,
                0 as fincome,
                count(distinct case when pstatus = 2 and ext_9 = '2' then forder_id else null end) as frepair_order_num,
                sum(case when pstatus = 2 and ext_9 = '2' then fusd else 0 end) as frepair_income,
                sum(case when pstatus = 5 then fusd else 0 end) as fcheat_income
            from stage.payment_stream_mid
            where dt >= date_add('%(ld_begin)s', -150) and dt < '%(ld_end)s'
                and (pstatus = 2 and ext_9 = '2' or pstatus = 5)
            group by dt, sid, appid, pmode
        ) t
        group by forder_date, fsid, fappid, fpmode
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.pay_center_final_order_fct partition(dt='3000-01-01')
        select
        forder_date,
        fsid,
        fappid,
        fpmode,
        fbpid,
        fpay_user_num,
        forder_num,
        fincome,
        frepair_order_num,
        frepair_income,
        fcheat_income
        from analysis.pay_center_final_order_fct
        where
        dt >= '%(ld_30dayago)s' and dt < '%(ld_end)s'

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
    a = agg_paycenter_income_final_fct(stat_date)
    a()
