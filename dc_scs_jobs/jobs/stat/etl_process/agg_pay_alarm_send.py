#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
from libs import warning_way
from dateutil.relativedelta import *
from PublicFunc import PublicFunc
from BaseStat import BasePGStat, get_stat_date, pg_db
import config
reload(sys)
sys.setdefaultencoding('utf-8')

class pay_alarm_send(BasePGStat):

    def get_send_all_data(self):
        """ 获取告警数据 """
        self.sms_title = u'支付通道监控报警：\n'
        self.sms_alarm_data = {}
        self.mail_title = u'支付通道监控报警：\n'
        self.mail_alarm_data = {}
        self.fdim_dict = {'fmoney':u'付费金额', 'forder':u'下单数', 'fpayuser_cnt':u'付费次数', 'ocr':u'订单完成率'}

        sql = """SELECT  to_char(a.fdate,'YYYY-MM-DD') as dt, a.fgamefsk, a.fpay_kind as pk, a.fmobilename as opt, a.fm_name as fname, a.fcountry as ct, a.fprovince as prv, a.fdim as fdim, a.fdays_pre dpr, a.fdim_pre,
                         a.avg_val as avg, a.hb_val as hb, a.tb_val as tb, b.fsms,  b.fmail, b.frtx,  b.fuser, c.fname as name
                 FROM  analysis.alarm_pay_data a
                 LEFT JOIN analysis.alarm_pay_sms_mail_config b
                 ON a.fgamefsk = b.fgamefsk
                 AND a.fpay_kind = b.fpay_kind
                 AND a.fmobilename = b.foperators
                 AND a.fm_id = b.fm_id
                 AND a.fcountry = b.fcountry
                 AND a.fprovince = b.fprovince
                 AND a.fdim = b.fdim
                 LEFT JOIN  analysis.game_dim c
                 ON a.fgamefsk = c.fsk
                 WHERE fdate = date'2016-03-14'
                 AND (b.fsms >0 or b.fmail >0 or b.frtx >0)
              """ % self.hql_dict

        data = self.query(sql)

        for row in data:
            if row['fgamefsk']==0:
                contents = u"""%s年%s月%s日, %s(%s-%s)\n%s环比下降:%s%%, 同比下降:%s%%"""\
                %(row['dt'][0:4],row['dt'][5:7],row['dt'][8:],row['fname'],row['ct'],row['prv'],self.fdim_dict[row['fdim']],row['hb'],row['tb'])

            else:
                contents = u"""%s年%s月%s日, %s游戏, %s(%s-%s)\n%s环比下降:%s%%, 同比下降:%s%%"""\
                %(row['dt'][0:4],row['dt'][5:7],row['dt'][8:],row['name'],row['fname'],row['ct'],row['prv'],self.fdim_dict[row['fdim']],row['hb'],row['tb'])

            print ( contents,row['fsms'],row['fmail'],row['frtx'],row['fuser'])
            if row['fsms'] == 1:
                self.sms_alarm_data[(contents,row['fsms'],row['fmail'],row['frtx'],row['fuser'])] = row

            if row['fmail'] == 1:
                self.mail_alarm_data[(contents,row['fsms'],row['fmail'],row['frtx'],row['fuser'])] = row


    def send_alarm_msg(self):
        """ 开始发送告警信息 """
        for k,v in  self.sms_alarm_data.items():
            cnt = '%s%s'%(self.sms_title,k[0] )
            warning_way.send_sms([ v['fuser'] ],cnt)

        for k,v in  self.mail_alarm_data.items():
            warning_way.send_email([ "%s@boyaa.com" %v['fuser'] ],self.mail_title, '%s%s'%(k[0],u'详情请查看：URL网址 http://d.oa.com/channel/control/'))


    def __call__(self):
        self.get_send_all_data()
        self.send_alarm_msg()


if __name__ == "__main__":
    stat_date = get_stat_date()
    a = pay_alarm_send(stat_date)
    a()
    print 'ok'
