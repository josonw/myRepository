#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

# 已经注释不使用
class agg_register_retain_pay_dim(BaseStat):

    def create_tab(self):
        hql = """

        create table if not exists stage.register_retain_pay_dim
        (
        fpay_date           date,
        fsignup_at          date,
        fbpid               string,
        fretain_day         bigint,
        fpay_money          decimal(20,4),
        fpay_user_num       bigint
        )
        partitioned by (dt date);

        -- 特殊同步,全表
        create table if not exists analysis.register_retain_pay_fct
        (
        fsignup_at          date,
        fbpid               varchar(100),
        fretain_day         bigint,
        fpay_money          decimal(20,4),
        fpay_user_num       bigint
        );
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        self.hq.debug = 0

        # 求注册用户后续的留存付费金额
        hql = """
        -- 插入当天付费用户的注册信息
        insert overwrite table stage.register_retain_pay_dim partition
        (dt = "%(stat_date)s")
        select '%(stat_date)s' fpay_date,
               c.dt fsignup_at,
               a.fbpid,
               datediff('%(stat_date)s', c.dt) fretain_day,
               sum(a.fusd) fpay_money,
               count(distinct a.fuid) fpay_user_num
          from stage.payment_stream_mid a
          join stage.user_dim c
            on a.fbpid = c.fbpid
           and a.fuid = c.fuid
           and c.dt >= date_add('%(ld_begin)s', -545)
         where a.dt = '%(stat_date)s'
           and a.pstatus = 2
         group by c.dt,
                  a.fbpid,
                  datediff('%(stat_date)s', c.dt)
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        -- 每天清空结果表
        use analysis;
        truncate table register_retain_pay_fct
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        -- 每天从中间表去全量计算结果表
        insert into table analysis.register_retain_pay_fct
          select fsignup_at, fbpid, fretain_day, fpay_money, fpay_user_num, fsignup_at dt
            from stage.register_retain_pay_dim
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
    a = agg_register_retain_pay_dim(stat_date)
    a()
