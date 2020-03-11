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
from BaseStat import BasePGCluster, get_stat_date, pg_db
import config
reload(sys)
sys.setdefaultencoding('utf-8')

class agg_alarm_send(BasePGCluster):

    def get_send_all_data(self):
        sql = """ SELECT to_char(fdate,'yyyy年mm月dd日') fdate,fgamefsk,fplatformfsk,fversionfsk,flag_cn,
            coalesce(gd.fname, '所有') ||'-'||coalesce(pd.fname, '所有') ||'-'||coalesce(vd.fname, '所有') as name,
            dim_cn||','||thbi||updown||abs(fpercent)||'%%%%' as content
                from analysis.alarm_data aa
                left join analysis.game_dim gd
                    on aa.fgamefsk = gd.fsk
                left join analysis.platform_dim pd
                    on aa.fplatformfsk = pd.fsk
                left join analysis.version_dim vd
                    on aa.fversionfsk = vd.fsk
                where fdate>=date'%(ld_begin)s'
                    and fdate<date'%(ld_end)s'
        """ % self.hql_dict
        data = self.query(sql)
        tmp_alarm = {}
        for row in data:
            key = (row.fgamefsk,row.fplatformfsk,row.fversionfsk)
            info =  row.flag_cn + ':' + row.name.replace('-所有','') + ',' + row.fdate + ',' + row.content
            if key in tmp_alarm:
                tmp_alarm[key].append(info)
            tmp_alarm.setdefault(key,[info,])
        return tmp_alarm


    def get_send_way_config(self):
        sql = """ SELECT fgame_id, fplat_id, fver_id, fname, fis_rtx, fis_sms, fis_mail
                    from analysis.byl_account_gpv_maps a
                     join analysis.byl_query_account b
                    on a.fuid=b.fuid
                    where fis_rtx =1
                    or fis_mail =1
                    or fis_sms =1
                        """ % self.hql_dict
        data = self.query(sql)
        tmp_config ={}
        for row in data:
            tmp_config.setdefault(row.fname,{})
            key = (row.fgame_id,row.fplat_id,row.fver_id)
            tmp_config[row.fname].setdefault(key,{})
            tmp_config[row.fname][key] = {'fis_sms':row.fis_sms,'fis_mail':row.fis_mail,'fis_rtx':row.fis_rtx}
        return tmp_config


    def send_message(self):
        tmp_alarm = self.get_send_all_data()
        tmp_config = self.get_send_way_config()

        for k in tmp_config.keys():
            tmp1=tmp_config[k]
            rtx,sms,mail = [] ,[] ,[]
            for j in tmp1.keys():
                if j in tmp_alarm:
                    rtx += tmp_alarm[j] if tmp1[j]['fis_rtx'] ==1 else []
                    sms += tmp_alarm[j] if tmp1[j]['fis_sms'] ==1 else []
                    mail+= tmp_alarm[j] if tmp1[j]['fis_mail'] ==1 else []
            # if rtx:
            #     rtx = '\n'.join(rtx)
            #     warning_way.send_rtx([k],'this is message','rtx','rtx','数据告警通知')
            if sms:
                sms = '\n'.join(sms)
                warning_way.send_sms([k],sms)
            if mail:
                mail = u'\n'.join(mail) + ',' + u'\n通知链接：http://d.oa.com/user/alarm/inform/?fflag=playgame'
                warning_way.send_email(["%s@boyaa.com" % k], u'你有新的数据告警通知', mail)


    def __call__(self):
        self.send_message()


if __name__ == "__main__":
    stat_date = get_stat_date()
    a = agg_alarm_send(stat_date)
    a()
