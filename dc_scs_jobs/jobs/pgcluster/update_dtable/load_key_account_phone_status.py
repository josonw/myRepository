
#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
import json
import urllib2
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date

class load_key_account_phone_status(BasePGCluster):
    def stat(self):
        sql = """delete from analysis.user_key_account_back_mid
                  where ftime >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
                    and ftime < to_date('%(ld_end)s', 'yyyy-mm-dd') """ % self.sql_dict
        print sql
        self.exe_hql(sql)

        sql = """ insert into analysis.user_key_account_back_mid
                  (fdate, ftype ,fid , fstatus ,ftime)
                  values(current_date, %(send_type)s, %(send_to)s, %(status)s, %(time)s)
               """
        url = "http://notice.boyaa.com/common_api/sendStatus?sendno=T_bill_000000003955&time=" + self.sql_dict["ld_begin"]
        print url
        try:
            data_str = urllib2.urlopen(url).read()
            data = json.loads(data_str)
        except Exception,e:
            print e
        self.exemany_hql(sql, data)

if __name__ == "__main__":
    stat_date = get_stat_date()
    #生成统计实例
    a = load_key_account_phone_status(stat_date)
    a()
