#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
@description: 每天上班前(每天上午9点)检查关键指标数据接口是否正常
            http://dcserver.oa.com/new/common/single_table_data_check/?charttype=key&dims=dau|dsu|pun|dpu|dip&edate=2016-11-22&sdate=2016-11-16&gpv=0|0|0|0|0|0|0
@author: HymanShi <HymanShi@boyaa.com>
@datetime: 2016-11-23 19:46:34
"""

from __future__ import division
import os
import sys
import datetime
import time
import requests
import json

# 把 str 编码由 ascii 改为 utf8 (或 gb18030)
reload(sys)
sys.setdefaultencoding("utf-8")


dc_path = os.getenv('DC_SERVER_PATH')
from sys import path
path.append(dc_path)

from dc_scs_jobs import config
from libs.DB_Mysql import Connection as m_db
from libs.warning_way import send_sms


def get_members(mdb, is_time, sort_item=None):
    """ 找出相应配置的人员，并排好序"""
    sql = 'SELECT * FROM loop_member where isworktime >= %s' % is_time
    data = mdb.query(sql)
    if sort_item == None:
        sort_item = 'priority'
    temp = sorted(data, key=lambda x: x[sort_item], reverse=False)
    result = [item['user'] for item in temp]
    return result


def is_worktime():
    """ 判断当前时间是否是工作时间 """
    h = int(time.strftime("%H", time.localtime()))
    # d = int(time.strftime("%w", time.localtime()))
    if 9 <= h <= 24:
        return 1
    else:
        return 0


def checkKeyDataStatus(mdb):
    # 昨天
    dtime = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=1), "%Y-%m-%d")
    # 前天
    tdtime = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=2), "%Y-%m-%d")
    url = 'http://dcserver.oa.com/new/common/single_table_data_check/?charttype=key&dims=dau|dsu|pun|dpu|dip&edate=%s&sdate=%s&gpv=0|0|0|0|0|0|0'

    msg = '数据关键指标监控报警：\r\n'
    err_msg = '数据关键指标监控报警：数据监控接口异常，%s' % (url % (dtime, dtime))
    #sms_user = get_members(mdb, is_worktime())
    # # if not len(sms_user):
    sms_user = ['HymanShi', 'JackieZhang', 'KylinmuLiu']
    print("time: %s" % datetime.datetime.strftime(datetime.datetime.today(), "%Y-%m-%d %H:%M:%S"))
    print(sms_user)
    try:
        oneday_ago_data_ret = requests.get((url % (dtime, dtime)), timeout=15)
        twoday_ago_data_ret = requests.get((url % (tdtime, tdtime)), timeout=15)

        stat = False
        if hasattr(oneday_ago_data_ret, 'text') and hasattr(twoday_ago_data_ret, 'text'):
            oneday_ago_data = json.loads(oneday_ago_data_ret.text)
            twoday_ago_data = json.loads(twoday_ago_data_ret.text)

            # 关键指标
            # 1.dau
            o_dau = oneday_ago_data.get('dau').get('data')
            t_dau = twoday_ago_data.get('dau').get('data')
            stat_dau, msg_dau = compare_data('dau', o_dau, t_dau)
            msg += msg_dau

            # 2.dpu
            o_dpu = oneday_ago_data.get('dpu').get('data')
            t_dpu = twoday_ago_data.get('dpu').get('data')
            stat_dpu, msg_dpu = compare_data('dpu', o_dpu, t_dpu)
            msg += msg_dpu

            # 3.dsu
            o_dsu = oneday_ago_data.get('dsu').get('data')
            t_dsu = twoday_ago_data.get('dsu').get('data')
            stat_dsu, msg_dsu = compare_data('dsu', o_dsu, t_dsu)
            msg += msg_dsu

            # 4.dip
            o_dip = oneday_ago_data.get('dip').get('data')
            t_dip = twoday_ago_data.get('dip').get('data')
            stat_dip, msg_dip = compare_data('dip', o_dip, t_dip)
            msg += msg_dip

            # 5.pun
            o_pun = oneday_ago_data.get('pun').get('data')
            t_pun = twoday_ago_data.get('pun').get('data')
            stat_pun, msg_pun = compare_data('pun', o_pun, t_pun)
            msg += msg_pun

            if stat_dau or stat_dpu or stat_dsu or stat_dip or stat_pun:
                # 数据异常，电话报警
                send_sms(sms_user, msg, 8)
            else:
                # 数据正常，短信报警
                send_sms(sms_user, msg)
        else:
            send_sms(sms_user, err_msg, 8)
    except Exception as e:
        send_sms(sms_user, err_msg, 8)


def compare_data(otype, odata, tdata, rate=0.25):
    orate = (int(odata) - int(tdata)) / int(tdata)
    orate_str = "%.2f" % (orate * 100)
    msg = "%s：%s" % (otype, orate_str) + "%"
    msg += ', 今日：%s, 昨日：%s ' % (odata, tdata)
    stat = False
    if str(odata) <= '0':
        msg += '(异常，请关注)'
        stat = True

    if abs(orate) > rate:
        stat = True
        msg += '(异常，请关注)'

    msg += '\r\n'
    print(msg)
    return (stat, msg)


if __name__ == '__main__':
    mdb = m_db(config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD)
    checkKeyDataStatus(mdb)
    mdb.close()
