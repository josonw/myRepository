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
class app_pay_company_dim(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.paycenter_company_dim
        (
          companyid         int,
          companyname       varchar(128),
          company_type      int,
          company_type_name varchar(32),
          sort_id           int
        );

        create table if not exists analysis.paycenter_company_dim_bak
        (
          fdate             date,
          companyid         int,
          companyname       varchar(128),
          company_type      int,
          company_type_name varchar(32),
          sort_id           int
        )partitioned by (dt date);        """

        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 0

        hql = """
        insert into table analysis.paycenter_company_dim
        select  a.companyid,
                a.companyname,
                a.company_type,
                a.company_type_name,
                a.sort_id
        from analysis.paycenter_company_dim_bak a
        left join analysis.paycenter_company_dim b
          on a.companyid = b.companyid
       where a.dt = '%(ld_begin)s'
         and b.companyid is null
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
    a = app_pay_company_dim(stat_date)
    a()
