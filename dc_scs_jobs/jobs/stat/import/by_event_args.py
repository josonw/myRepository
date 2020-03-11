#-*- coding: UTF-8 -*-
import os
import sys
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BaseStat


class by_event_args(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
        use stage;
        """
        res = self.hq.exe_sql(hql)

        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        self.hq.debug = 0

        hql = """use stage;
            alter table by_event_args add if not exists partition(dt='%(ld_begin)s')
            location '/dw/stage/by_event_args/%(ld_begin)s';
            alter table by_event_args add if not exists partition(dt='%(ld_end)s')
            location '/dw/stage/by_event_args/%(ld_end)s';
          """ % self.hql_dict
        res = self.hq.exe_sql(hql)

        return res


#生成统计实例
a = by_event_args()
a()
