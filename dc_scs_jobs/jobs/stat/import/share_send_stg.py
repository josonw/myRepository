#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class share_send_stg(BaseStat):

    def create_tab(self):
        #�����ʱ����ʽִ��
        self.hq.debug = 0

        hql = """
        use stage;
        """
        res = self.hq.exe_sql(hql)

        if res != 0:
            return res

        return res


    def stat(self):
        """ ��Ҫ���֣�ͳ������ """
        self.hq.debug = 0

        args_dic = {
            "ld_begin": statDate
        }

        hql = """
        use stage;
        alter table share_send_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/share_send/%(ld_begin)s';
        alter table share_send_stg add if not exists partition(dt='%(ld_end)s') location '/dw/stage/share_send/%(ld_end)s';
        """ % self.hql_dict

        res = self.hq.exe_sql(hql)
        return res


#����ͳ��Ҫ��ʼ��
global statDate

if len(sys.argv) == 1:
    #û����������Ļ�������Ĭ��ȡ����
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #���������ȡ��ʵ���ǵڼ�����ȡ�ڼ���
    args = sys.argv[1].split(',')
    statDate = args[0]


#����ͳ��ʵ��
import_job = share_send_stg(statDate)
import_job()
