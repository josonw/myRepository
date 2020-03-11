#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date

# 已经注释不使用
class load_pay_rate_dim(BasePGStat):

    def stat(self):
        sql = """
            insert into analysis.pay_rate_dim
              select t.pmode, t.sid , t.unit, t.rate
              from
              (
                select -1 as sid, -1 as pmode, unit, rate
                from analysis.paycenter_rate_dim
                where fdate=date'%(ld_begin)s'
                union
                select sid, pmode, '-1', rate
                from analysis.paycenter_chanel_dim_bak
                where fdate=date'%(ld_begin)s'
              ) as t
              left join analysis.pay_rate_dim as p
                on p.pmode = t.pmode and p.sid = t.sid and p.unit = t.unit
              where p.sid is null
        """ % self.sql_dict
        self.append(sql)


        sql = """
            insert into analysis.paycenter_chanel_dim
              select a.sid, a.pmode, a.pmodename, a.unit, a.rate, a.companyid, a.tag, a.statid, a.statname,
                a.parter_id, a.parter_name, a.companyname, to_number(a.company_type) company_type, a.company_type_name
              from (
                select sid, pmode, max(pmodename) pmodename, max(unit) unit, max(rate) rate, max(companyid) companyid,
                  max(tag) tag, max(statid) statid, max(statname) statname, max(parter_id) parter_id,
                  max(parter_name) parter_name, max(companyname) companyname, max(company_type) company_type,
                  max(company_type_name) company_type_name
                from analysis.paycenter_chanel_dim_bak
                where fdate = date'%(ld_begin)s'
                group by sid, pmode
              ) a
              left join analysis.paycenter_chanel_dim b
                on a.sid = b.sid and a.pmode = b.pmode
              where b.sid is null
        """ % self.sql_dict
        self.append(sql)

        sql = """
          insert into analysis.paycenter_apps_dim
            select a.appid, a.sid, a.appname, a.bpid, a.is_lianyun, a.childid, a.game_id,
              a.lang_id, a.sitename, a.game_name, a.lang_name
            from (
              select appid, sid, max(appname) appname, max(bpid) bpid, max(is_lianyun) is_lianyun,
                max(childid) childid, max(game_id) game_id, max(lang_id) lang_id,
                max(sitename) sitename, max(game_name) game_name, max(lang_name) lang_name
              from analysis.paycenter_apps_dim_bak
              where fdate = date'%(ld_begin)s'
              group by appid, sid
            ) a
            left join analysis.paycenter_apps_dim b
             on a.sid = b.sid and a.appid = b.appid
            where b.sid is null
        """ % self.sql_dict
        self.append(sql)


        sql = """
          insert into analysis.paycenter_company_dim
            select a.companyid, a.companyname, a.company_type, a.company_type_name, a.sort_id
            from (
                select companyid, companyname, company_type, company_type_name, sort_id
                from analysis.paycenter_company_dim_bak
                where fdate = date'%(ld_begin)s'
            )a
            left join analysis.paycenter_company_dim b
                on a.companyid = b.companyid
            where b.companyid is null
        """ % self.sql_dict
        self.append(sql)

        self.exe_hql_list()


if __name__ == "__main__":

    stat_date = get_stat_date()
    #生成统计实例
    a = load_pay_rate_dim(stat_date)
    a()