#-*- coding: UTF-8 -*-
# Author: AnsenWen
import os
import sys
import time
import datetime
import operator
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class agg_user_info_all(BaseStat):
    def get_hbase_struct(self, tbl_name):
        """
        获取表的结构(字段名和字段类型)，将其拼接成字符串，并保持字段顺序一致
        """
        hql = """describe %s""" % (tbl_name)
        res = self.hq.query(hql)
        struct_dict = {}
        for i, r in enumerate(res):
            struct_dict.update({r[0]: (i, r[1])})

        if "key" in struct_dict.keys():
            struct_dict.pop('key')   # 将key去掉
        if "fbpid" in struct_dict.keys() and "fuid" in struct_dict.keys():
            struct_dict.pop('fbpid')
            struct_dict.pop('fuid')

        struct_sorted = sorted(struct_dict.items(), key=operator.itemgetter(1))
        struct_str = ",\n".join([s[0] + " " + s[1][1] for s in struct_sorted])
        return struct_str

    # def drop_tab(self):
    #     """
    #     判断hbase的映射表与hive表是否一致，不一致的则删除重新建表
    #     """
    #     struct_str_hbase = self.get_hbase_struct("stage.user_info_all_hbase")
    #     print struct_str_hbase
    #     struct_str_hive = self.get_hbase_struct("stage.user_info_all")
    #     print struct_str_hive
    #     if struct_str_hive != struct_str_hbase:
    #         print "--------------------------"
    #         hql = """drop table if exists stage.user_info_all"""
    #         res = self.hq.query(hql)
    #     return struct_str_hbase

    def create_tab(self):
        """
        根据hbase的映射表的字段，来建个数据表
        """
        self.struct_str = self.get_hbase_struct("stage.user_info_all_hbase")
        print self.struct_str
        hql = """
        create table if not exists stage.user_info_all
        (
            fbpid       varchar(100),
            fuid        int,
            %s
        )partitioned by(dt date)
        """ % self.struct_str
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res

    def stat(self):
        """
        将HBase的映射表插入Hive表
        """
        dates_dict = PublicFunc.date_define(self.stat_date)
        query = {}
        query.update(dates_dict)

        hql = """describe stage.user_info_all"""
        res = self.hq.query(hql)
        colname_list = []
        for r in res:
            if r[0] == "dt":
                break
            elif r[0] not in ("fbpid", "fuid"):
                colname_list.append(r[0])

        colname_str = ",\n".join(colname_list)
        query.update({"colname": colname_str})

        if "flast_act_date" not in colname_list:
            print "flast_act_date not in the colname"
            return -1

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """
            insert overwrite table stage.user_info_all
            partition(dt='%(ld_daybegin)s')
            select key.fbpid,
                   key.fuid,
                    %(colname)s
             from stage.user_info_all_hbase
            where flast_act_date >= '%(ld_365dayago)s'
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

if __name__ == '__main__':
    #愉快的统计要开始啦
    statDate = get_stat_date()
    #生成统计实例
    a = agg_user_info_all(statDate)
    a()