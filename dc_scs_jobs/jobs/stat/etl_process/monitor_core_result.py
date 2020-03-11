#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
from sys import path
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
from libs.warning_way import send_sms
import dc_scs_jobs.config as config
from dc_scs_jobs.BaseStat import BasePGCluster

# 中文解码解决方案，指定默认字符集
reload(sys)
sys.setdefaultencoding('utf-8')


class monitor_core_result(BasePGCluster):
    """ PG执行存储过程运算 """
    def incrRatio( self, num1, num2):
        """ 获取num2对num1的比率"""
        if 0 == num1 or None == num1:
            return '100% '

        num1 = int(num1)
        num2 = int(num2)
        incr = num2 - num1
        return '%s%% ' % str(incr*100/num1)

    def stat(self):
        warning_msg = ""

        ##用户新增 dsu
        sql = """
        select sum(fdregucnt) as dsu from dcnew.reg_user where fdate = DATE'%(stat_date)s'
        """ % self.sql_dict
        data = self.pg.query(sql)
        if None == data[0]['dsu']:
            warning_msg += "%(stat_date)s结果GP[用户新增]获取为空\n" % self.sql_dict
        if 0 == data[0]['dsu']:
            warning_msg += "%(stat_date)s结果GP[用户新增]为0\n" % self.sql_dict


        ##用户活跃 dau
        sql = """
        select sum(fdactucnt) as dau from dcnew.act_user where fdate = DATE'%(stat_date)s'
        """ % self.sql_dict
        data = self.pg.query(sql)
        if None == data[0]['dau']:
            warning_msg += "%(stat_date)s结果GP[用户活跃]获取为空\n" % self.sql_dict
        if 0 == data[0]['dau']:
            warning_msg += "%(stat_date)s结果GP[用户活跃]为0\n" % self.sql_dict

        ##付费总额 dip
        sql = """
        select sum(fincome) as dip from dcnew.pay_user_income where fdate = DATE'%(stat_date)s'
        """ % self.sql_dict
        data = self.pg.query(sql)
        if None == data[0]['dip']:
            warning_msg += "%(stat_date)s结果GP[付费总额]获取为空\n" % self.sql_dict
        if 0 == data[0]['dip']:
            warning_msg += "%(stat_date)s结果GP[付费总额]为0\n" % self.sql_dict

        ##用户牌局 pun
        sql = """
        select sum(fusernum) as pun from dcnew.gameparty_total where fdate = DATE'%(stat_date)s'
        """ % self.sql_dict
        data = self.pg.query(sql)
        if None == data[0]['pun']:
            warning_msg += "%(stat_date)s结果GP[用户牌局]获取为空\n" % self.sql_dict
        if 0 == data[0]['pun']:
            warning_msg += "%(stat_date)s结果GP[用户牌局]为0\n" % self.sql_dict

        ##用户金流 gamecoins
        sql = """
        select sum(fnum) as gamecoins from dcnew.gamecoin_detail where fdate = DATE'%(stat_date)s'
        """ % self.sql_dict
        data = self.pg.query(sql)
        if None == data[0]['gamecoins']:
            warning_msg += "%(stat_date)s结果GP[用户金流]获取为空\n" % self.sql_dict
        if 0 == data[0]['gamecoins']:
            warning_msg += "%(stat_date)s结果GP[用户金流]为0\n" % self.sql_dict

        ## 发送短信,电话了
        if "" != warning_msg:
            send_sms(config.CONTACTS_LIST_ENAME, warning_msg, 8)


if __name__ == '__main__':
    #生成统计实例
    a = monitor_core_result()
    a()
