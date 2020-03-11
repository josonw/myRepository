#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class load_gameparty_uid_mid(BaseStat):
    """建立维度表
    """
    def create_tab(self):
        hql = """create external table if not exists stage.finished_gameparty_uid_mid
                (
                fdate date,
                gameparty_id string,
                fbpid varchar(50),
                fname varchar(100),
                ftype bigint,
                fuid bigint,
                fmust_blind bigint,
                tbl_id varchar(138),
                fls_at string
                )
                partitioned by (dt date)
                stored as orc
                location '/dw/stage/finished_gameparty_uid_mid'
        """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """create table if not exists stage.finished_gameparty_uid_mid_tmp_%(num_begin)s
                (
                fdate date,
                gameparty_id string,
                fbpid varchar(50),
                fname varchar(100),
                ftype bigint,
                fuid bigint,
                fmust_blind bigint,
                tbl_id varchar(138),
                fls_at string
                ) partitioned by (slot int)
                stored as orc

        """ % self.hql_dict

        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res

    def stat(self):
        # self.hq.debug = 0
        query = PublicFunc.date_define( self.stat_date )
        query.update({ 'statdate':self.stat_date })
        res = self.hq.exe_sql("""
        use stage;
        set hive.exec.dynamic.partition.mode=nonstrict;
        """ % query)
        if res != 0: return res
        hql = """drop table if exists finished_gameparty_tmp_%(num_begin)s;
         create table if not exists finished_gameparty_tmp_%(num_begin)s
         as
        select concat( input__file__name, block__offset__inside__file) gameparty_id,
               fbpid,
               fname,
               ftype,
               fslot_uid_1,
               fslot_uid_2,
               fslot_uid_3,
               fslot_uid_4,
               fslot_uid_5,
               fslot_uid_6,
               fslot_uid_7,
               fslot_uid_8,
               fslot_uid_9,
               fslot_uid_10,
               fmust_blind,
               tbl_id,
               flts_at fls_at
          from stage.finished_gameparty_stg
         where dt = '%(statdate)s'; """ % query

        res = self.hq.exe_sql( hql )
        if res != 0: return res

        hql = """from stage.finished_gameparty_tmp_%(num_begin)s
        insert overwrite table stage.finished_gameparty_uid_mid_tmp_%(num_begin)s partition(slot=1)
        select '%(statdate)s' fdate, gameparty_id, fbpid, fname, ftype, fslot_uid_1, fmust_blind, tbl_id, fls_at where fslot_uid_1>0
        insert overwrite table stage.finished_gameparty_uid_mid_tmp_%(num_begin)s partition(slot=2)
        select '%(statdate)s' fdate, gameparty_id, fbpid, fname, ftype, fslot_uid_2, fmust_blind, tbl_id, fls_at where fslot_uid_2>0
        insert overwrite table stage.finished_gameparty_uid_mid_tmp_%(num_begin)s partition(slot=3)
        select '%(statdate)s' fdate, gameparty_id, fbpid, fname, ftype, fslot_uid_3, fmust_blind, tbl_id, fls_at where fslot_uid_3>0
        insert overwrite table stage.finished_gameparty_uid_mid_tmp_%(num_begin)s partition(slot=4)
        select '%(statdate)s' fdate, gameparty_id, fbpid, fname, ftype, fslot_uid_4, fmust_blind, tbl_id, fls_at where fslot_uid_4>0
        insert overwrite table stage.finished_gameparty_uid_mid_tmp_%(num_begin)s partition(slot=5)
        select '%(statdate)s' fdate, gameparty_id, fbpid, fname, ftype, fslot_uid_5, fmust_blind, tbl_id, fls_at where fslot_uid_5>0
        insert overwrite table stage.finished_gameparty_uid_mid_tmp_%(num_begin)s partition(slot=6)
        select '%(statdate)s' fdate, gameparty_id, fbpid, fname, ftype, fslot_uid_6, fmust_blind, tbl_id, fls_at where fslot_uid_6>0
        insert overwrite table stage.finished_gameparty_uid_mid_tmp_%(num_begin)s partition(slot=7)
        select '%(statdate)s' fdate, gameparty_id, fbpid, fname, ftype, fslot_uid_7, fmust_blind, tbl_id, fls_at where fslot_uid_7>0
        insert overwrite table stage.finished_gameparty_uid_mid_tmp_%(num_begin)s partition(slot=8)
        select '%(statdate)s' fdate, gameparty_id, fbpid, fname, ftype, fslot_uid_8, fmust_blind, tbl_id, fls_at where fslot_uid_8>0
        insert overwrite table stage.finished_gameparty_uid_mid_tmp_%(num_begin)s partition(slot=9)
        select '%(statdate)s' fdate, gameparty_id, fbpid, fname, ftype, fslot_uid_9, fmust_blind, tbl_id, fls_at where fslot_uid_9>0
        insert overwrite table stage.finished_gameparty_uid_mid_tmp_%(num_begin)s partition(slot=10)
        select '%(statdate)s' fdate, gameparty_id, fbpid, fname, ftype, fslot_uid_10, fmust_blind, tbl_id, fls_at where fslot_uid_10>0
        ;""" % query

        res = self.hq.exe_sql( hql )
        if res != 0: return res

        hql = """insert overwrite table stage.finished_gameparty_uid_mid
        partition (dt='%(statdate)s')
        select '%(statdate)s' fdate, gameparty_id, fbpid, fname, ftype, fuid, fmust_blind, tbl_id, fls_at
        from stage.finished_gameparty_uid_mid_tmp_%(num_begin)s
            union all
        select '%(statdate)s' fdate, '0' gameparty_id, fbpid, fsubname fname, 0 ftype, fuid,
            fblind_1 fmust_blind, concat_ws('0',ftbl_id, finning_id) tbl_id, flts_at fls_at
        from stage.user_gameparty_stg
        where dt='%(statdate)s';

        drop table if exists stage.finished_gameparty_uid_mid_tmp_%(num_begin)s;
        drop table if exists finished_gameparty_tmp_%(num_begin)s;
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0:return res


#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = load_gameparty_uid_mid(statDate)
a()
