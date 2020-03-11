# coding:utf-8
# AUTHOR:tommyjiang DATE:2014-09-29

from urllib import urlencode
import urllib2
import os
import time
import datetime
import sys
#import pycurl
#from StringIO import StringIO
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))

from libs import DB_PostgreSQL as pgdb
from dc_scs_jobs import config
from libs.warning_way import send_sms

from libs import real_redis
from libs.real_redis import RealRedis

# 防止乱码
reload(sys)
sys.setdefaultencoding('utf-8')


class Bomb_load():

    """ 实时概况数据异常告警 """

    def __init__(self, DB, NAME):
        self.db = DB
        self.sdate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=7), "%Y-%m-%d")
        self.edate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
        self.contacts = NAME
        self.dims = {'dau':u'日活跃用户','dsu': u'日新增用户','pun':u'玩牌用户','dgcc':u'游戏币发放','dgci':u'游戏币消耗'}
        self.alarm = {}
        self.got_main_platform()

    def got_main_platform(self):
        """ 取出连续7天活跃用户数都大于1000的gpv组合 """
        sql = """
        select fgamefsk, fplatformfsk, fversionfsk
          from
             (
            select fdate, fgamefsk, fplatformfsk, fversionfsk
              from analysis.user_true_active_fct
             where fdate >= to_date('%s', 'yyyy-mm-dd')
               and fdate <= to_date('%s', 'yyyy-mm-dd')
             group by fdate, fgamefsk, fplatformfsk, fversionfsk
            having sum(factcnt)>1000
             ) a
            group by fgamefsk, fplatformfsk, fversionfsk
            having count(*)=7
              """%(self.sdate, self.edate)
        data = self.db.query(sql)
        self.gpv_list = [[item['fgamefsk'], item['fplatformfsk'], item['fversionfsk']] for item in data]

    def realredis_alert(self):
        msg = ''
        #适当提前500秒(8分钟左右)，否则在**:05容易误报
        h = time.strftime("%H", time.localtime(time.time()-500))
        for k,v in self.dims.iteritems():
            data = real_redis.realtime_hour(self.gpv_list, k)
            if not data or not data.get(h,0):
                msg = '%s,%s'%(msg,v)
        if msg:
            msg =  "实时概况数据异常告警:%s异常"%msg
            print u"%s %s" %(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()), msg)
            self.do_warning(msg)
        else:
            print u"%s\n实时数据检查ok\n"% time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())

        return 'over'

    def do_warning(self,msg):
        # 5分钟间隔告警发1次，10分钟间隔告警1次，达到间隔时间一次顺延，60分钟之后不变
                   # interval间隔时间    wtimes已发送的告警次数
        interval_config = [{'interval':60,'wtimes':3},
                           {'interval':30,'wtimes':2},
                           {'interval':10,'wtimes':1}]
        insertdata = {}
        sql = """ select warntime from  analysis.real_warning_record
                   where fdate = to_date('%s', 'yyyy-mm-dd') and fname ='%s' order by warntime asc
              """%(self.sdate,'realtime_hour')

        data = self.db.query(sql)
        sql  = """ insert into analysis.real_warning_record
                   (fdate,warntime,fname)
                   values ('%(fdate)s',%(warntime)s,'%(fname)s')
               """
        insertdata['fdate'] = self.sdate
        insertdata['warntime'] = int(time.time())
        insertdata['fname'] = 'realtime_hour'

        data = [item['warntime'] for item in data]
        if data:
            interval = int(time.time()) - data[-1]
            for item in interval_config:
                if interval >= item['interval'] and len(data) > item['wtimes']:
                    send_sms(self.contacts, msg)
                    self.db.execute(sql%insertdata)
                    break
        else:
            # 今天还没有告警记录就立即告警
            send_sms(self.contacts, msg)
            self.db.execute(sql%insertdata)



if __name__ == '__main__':
    # time.sleep(60)  #休眠1分钟，等数据上传

    DB = pgdb.Connection(host      = config.PG_DB_HOST,
                         database  = config.PG_DB_NAME,
                         user      = config.PG_DB_USER,
                         password  = config.PG_DB_PSWD)

    NAME = config.CONTACTS_REALTIME_ENAME
    #config.CONTACTS_LIST_ENAME
    # 同一天内告警频率配置


    run = Bomb_load(DB, NAME)
    run.realredis_alert()
