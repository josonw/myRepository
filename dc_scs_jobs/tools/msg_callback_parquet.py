#-*- coding: UTF-8 -*-
import time
import sys
import os
from sys import path
import requests
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
import config
from libs.DB_Mysql import Connection as m_db
from libs.warning_way import send_sms
from BaseStat import BaseStat


class msg_callback_parquet(BaseStat):
    """ 推送消息回调 """

    def result_callback(self, mdb):
        # push_num:今天的推送次数
        # success_num:今天推送成功次数
        # click_num:今天推送的点击次数
        # appear_num:今天推送的展示次数
        today24h_ago = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(int(time.time())-3600*24))
        ids = mdb.query("""select distinct jobname as push_id
                             from (select jobname,
                                          from_unixtime(max(substring(substring_index(tempsql,'begin_time',-1), 8, 10) + 0)) begin_time,
                                          from_unixtime(max(substring(substring_index(tempsql,'end_time',-1), 8, 10) + 0)) end_time
                                     from tjob_detail t where jobtype='pushjobs' group by jobname
                                   ) t
                            where end_time >= '%s' """%today24h_ago)
        msgids = ','.join(["'%s'"%i.get("push_id") for i in ids])
        pushids = ','.join([str(i.get("push_id")) for i in ids])
        print today24h_ago
        print msgids
        print pushids

        for pushid in ids:
            hql = """
                alter table stage.user_push_report_stg add if not exists partition(msgid='%(push_id)s')
                location '/dw/stage/user_push_report/%(push_id)s';
            """%pushid
            data1 = self.exe_hql(hql)

        data1=None
        if msgids:
            hql= """select msgid fpushid, faction, count(distinct fclient_id) nums, min(flts_at) first_time
            from stage.user_push_report_stg
            where msgid in (%s)
            group by msgid, faction
            """%msgids
            data1 = self.query(hql)

            hql= """select fpushid, count(distinct ftoken) push_num
            from pushdb.push_user_parquet
            where fpushid in (%s)
            group by fpushid
            """%msgids
            data2 = self.query(hql)

        if data1:
            data1 = [i for i in data1]
            data2 = [i for i in data2]
            print "hive query data1:"
            print data1, data2
            self.callback_pushid(mdb, ids, data1, data2)
            print "**************************************************************\n"


    def callback_pushid(self, mdb, ids, data1, data2):
        # data1:[[228,1,3],[228,2,1]]
        # data2:[[228, 3]]
        for item in data1:
            for pushtem in ids:
                if item[1]==1 and item[0]==str(pushtem.get("push_id")):
                    pushtem['success_num']=item[2]
                if item[1]==2 and item[0]==str(pushtem.get("push_id")):
                    pushtem['click_num']=item[2]
                if item[1]==4 and item[0]==str(pushtem.get("push_id")):
                    pushtem['appear_num']=item[2]

                if item[3] < pushtem.get('first_time',time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())):
                    pushtem['first_time']=item[3]

        for item in data2:
            for pushtem in ids:
                if item[0]==pushtem.get("push_id"):
                    pushtem['push_num']=item[1]

        for push in ids:
            push.setdefault('push_num',0)
            push.setdefault('success_num',0)
            push.setdefault('click_num',0)
            push.setdefault('appear_num',0)
            push.setdefault('last_time',time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()))
            push.setdefault('first_time',time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()))
            # pushd-dev.boyaa.com开发域名
            # pushd.boyaa.com正式域名
            sql = """INSERT INTO  push_callback  (push_id,push_num,success_num,click_num,appear_num,first_time,last_time)
            values (%(push_id)s,%(push_num)s, %(success_num)s, %(click_num)s, %(appear_num)s, %(first_time)s ,%(last_time)s)
            ON DUPLICATE KEY UPDATE push_num = %(push_num)s, success_num = %(success_num)s, click_num = %(click_num)s,appear_num = %(appear_num)s,first_time= %(first_time)s ,last_time = %(last_time)s
            """

            mdb.execute(sql, push)
            url = "https://pushd.boyaa.com/pushd/count?push_id=%(push_id)s&push_num=%(push_num)s&success_num=%(success_num)s&click_num=%(click_num)s&appear_num=%(appear_num)s"%push
            headers = {"User-Agent":
                    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36"}
            print url

            e=''
            try:
                req = requests.get(url = url, headers = headers, timeout=2)
                print req.json()
            except Exception, e:
                print str(e)
                # proxyDict = {"http"  : http_proxy, "https"  : http_proxy}
                # req = requests.get(url = url, headers = headers, proxies=proxyDict,timeout=2)
                # print req.json()


    def stat(self):
        mdb = m_db( config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD  )
        self.result_callback(mdb)


#生成统计实例
a = msg_callback_parquet(sys.argv[1:])
a()
