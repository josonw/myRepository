#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import json
import sys
import os
import datetime
from sys import path
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
path.append('%s/' % os.getenv('SCS_PATH'))
from libs.DB_PostgreSQL import Connection
import dc_scs_jobs.config as config
from libs.warning_way import send_sms
from BaseStat import BasePGStat, get_stat_date

# 中文解码解决方案，指定默认字符集
reload(sys)
sys.setdefaultencoding('utf-8')

wrong_msg = ''

def get_paycenter_rate_config( config_name ):
    """
    可选两个参数：

        getdaterrate    这个是国家代号，对应的汇率

        getdaterchanel    这个是支付方式，对应的汇率

        getdatercmpy    支付方式和

        getdatercmpy    支付公司的配置信息
        getdaterapps
    """
    global wrong_msg
    paycenter_api = "http://paycms.boyaa.com/api/%s"% config_name
    # paycenter_api = "http://payadmin.oa.com/Interface/get_public.php?act=%s" % config_name

    reslut = {}
    try:
        reslut = json.loads( requests.get(paycenter_api, headers ={"Accept-Encoding": "gzip"}).text)
    except Exception, e:
        print paycenter_api, 'please check this url return.'

    if reslut.get('code', 0 ) == 200:
        reslut = reslut.get('data', [])
    else:
        print 'return systn err '
        wrong_msg="%s,"%paycenter_api
        reslut = []

    return reslut


def load_paymode_rate_data(DB, stat_date):
    """
        加载支付渠道的汇率表，一次性插入大量。
    """
    config_name = 'getdaterchanel'
    config_list = get_paycenter_rate_config( config_name )
    data = []
    keys = ['sid', 'pmode', 'pmodename', 'unit', 'rate', 'companyid', 'tag', 'statid', 'statname', 'parter_id', 'parter_name', 'companyname', 'company_type', 'company_type_name', 'use_status' ]

    for c in config_list:
        tmp = {}
        for k in keys:
            tmp.update({k:c.get(k, 0)})
        data.append(tmp)

    if data:
        DB.execute("delete from analysis.paycenter_chanel_dim where 1 = 1")
        sql = """
        insert into analysis.paycenter_chanel_dim
          ( sid, pmode, pmodename, unit, rate, companyid, tag, statid, statname, parter_id, parter_name, companyname, company_type, company_type_name, is_use )
        values
          ( %(sid)s, %(pmode)s, %(pmodename)s, %(unit)s, %(rate)s, %(companyid)s, %(tag)s, %(statid)s, %(statname)s, %(parter_id)s, %(parter_name)s, %(companyname)s, %(company_type)s, %(company_type_name)s, %(use_status)s)
        """
        DB.executemany_rowcount(sql, data)
        print u'getdaterchanel loads successfully'
    else:
        pass


def load_country_rate_data(DB, stat_date):
    """
        国家地区的汇率表，一次性插入大量。
    """
    config_name = 'getdaterrate'
    config_list = get_paycenter_rate_config( config_name )
    data = []
    keys = [ 'id', 'rate', 'unit', 'ext']
    for c in config_list:
        tmp = {'fdate':stat_date}
        for k in keys:
            tmp.update({k:c.get(k, 0)})
        data.append(tmp)

    if data:
        DB.execute("delete from analysis.paycenter_rate_dim where fdate='%s'" % stat_date)
        sql = """
        insert into analysis.paycenter_rate_dim
          ( fdate, id, rate, unit, ext )
        values
          (%(fdate)s, %(id)s, %(rate)s, %(unit)s, %(ext)s)
        """
        DB.executemany_rowcount(sql, data)
        print u'getdaterrate loads successfully'
    else:
        pass


def load_apps_bpid_data(DB, stat_date):
    """
        支付中心应用id，bpid映射关系，一次性插入大量。
    """
    config_name = 'getdaterapps'
    config_list = get_paycenter_rate_config( config_name )
    data = []
    keys = [ 'appid','sid','appname','bpid','childid','is_lianyun','gameid','langid','sitename','gamename','langname' ]
    for c in config_list:
        tmp = {}
        for k in keys:
            tmp.update({k:c.get(k, 0)})
        data.append(tmp)

    if data:
        DB.execute("delete from analysis.paycenter_apps_dim where 1=1")
        sql = """
        insert into analysis.paycenter_apps_dim
        ( appid ,sid, appname, bpid, childid, is_lianyun, game_id, lang_id, sitename, game_name, lang_name )
        values
        (%(appid)s,%(sid)s,%(appname)s,%(bpid)s,%(childid)s,%(is_lianyun)s,%(gameid)s,%(langid)s,%(sitename)s,%(gamename)s,%(langname)s)
        """
        DB.executemany_rowcount(sql, data)
        print u'getdaterapps loads successfully'
    else:
        pass


def extend_apps_has_child(DB):
    sql = """ select sid, appid, appname, game_id, lang_id, bpid, is_lianyun, childid, 1 is_mother, game_name, lang_name
          FROM paycenter_apps_dim
         where childid is not null and childid != '' """
    datas = DB.query(sql)

    handle_datas = []
    for data in datas:
        childid = data.get('childid')
        for cid in childid.strip(',').split(','):
            data['childid'] = cid
            handle_datas.append( data.copy() )

    return handle_datas


def extend_apps_no_child(DB):
    sql = """ select sid, appid, appname, game_id, lang_id, bpid, is_lianyun, to_char(appid) childid, 0 is_mother, game_name, lang_name
          from paycenter_apps_dim
         where childid is null or childid = '' """
    datas = DB.query(sql)
    return datas

def insert_extend_tables(DB, datas):
    DB.execute("delete from analysis.paycenter_extern_cld_apps_dim ")
    try:
        for data in datas:
            sql = """
            insert into analysis.paycenter_extern_cld_apps_dim
             (sid, appid, appname, game_id, lang_id, bpid, is_lianyun, childid, is_mother, game_name, lang_name)
             values
            (%(sid)s, %(appid)s, '%(appname)s', %(game_id)s, %(lang_id)s, '%(bpid)s', '%(is_lianyun)s', %(childid)s, %(is_mother)s, '%(game_name)s', '%(lang_name)s')
            """%data
            DB.execute(sql)
    except Exception, e:
        print e

# 将childid拆开
def extend_apps_child_info(DB):
    child_datas = extend_apps_has_child(DB)
    no_child_datas = extend_apps_no_child(DB)

    datas = child_datas
    datas.extend(no_child_datas)

    if datas:
        insert_extend_tables(DB, datas)


def load_pay_company_data(DB, stat_date):
    """
        支付中心，各个支付公司的配置信息
    """
    config_name = 'getdatercmpy'
    config_list = get_paycenter_rate_config( config_name )

    data = []
    keys = [ 'companyid','companyname','company_type','company_type_name','sort_id' ]

    for c in config_list:
        tmp = {}
        for k in keys:
            tmp.update({k:c.get(k, 0)})
        data.append(tmp)

    if data:
        DB.execute("delete from analysis.paycenter_company_dim where 1=1")

        sql = """
        insert into analysis.paycenter_company_dim
        (companyid, companyname, company_type, company_type_name, sort_id)
        values
        (%(companyid)s,%(companyname)s,%(company_type)s,%(company_type_name)s,%(sort_id)s)
        """
        DB.executemany_rowcount(sql, data)
        print u'getdatercmpy loads successfully'
    else:
        pass


class agg_pg_paycenter_config(BasePGStat):
    """ PG执行存储过程运算 """
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
                from analysis.paycenter_chanel_dim
              ) as t
              left join analysis.pay_rate_dim as p
                on p.pmode = t.pmode and p.sid = t.sid and p.unit = t.unit
              where p.sid is null
        """ % self.sql_dict
        self.append(sql)

        self.exe_hql_list()


if __name__ == '__main__':
    stat_date = get_stat_date()

    DB = Connection( config.PG_DB_HOST, config.PG_DB_NAME, config.PG_DB_USER, config.PG_DB_PSWD, True )

    load_paymode_rate_data(DB, stat_date)
    load_country_rate_data(DB, stat_date)
    #load_apps_bpid_data(DB, stat_date)

    extend_apps_child_info(DB)
    load_pay_company_data(DB, stat_date)
    if wrong_msg:
        wrong_msg = "request %s failed"%wrong_msg
        send_sms(config.CONTACTS_PAYAPI_ENAME,wrong_msg)
    #生成统计实例
    a = agg_pg_paycenter_config(stat_date)
    a()
