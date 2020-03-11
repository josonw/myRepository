#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class load_game_info_dim(BaseStat):
    """建立游戏维度表
    """
    def create_tab(self):
        hql = """
        CREATE TABLE IF NOT EXISTS STAGE.VERSION_INFO_DIM
        (
        FSK BIGINT comment '唯一id',
        FVERSION_INFO VARCHAR(200) comment '版本',
        FVERSION_NAME VARCHAR(200) comment '版本别名',
        FMEMO VARCHAR(200)
        )
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        CREATE TABLE IF NOT EXISTS STAGE.ADVERTISE_ID_DIM
        (
        FAD_CODE VARCHAR(50) comment '广告id',
        FAD_NAME VARCHAR(50) comment '广告名称'
        )"""
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        CREATE TABLE IF NOT EXISTS STAGE.ENTRANCE_DIM
        (
        FSK BIGINT,
        FENTRANCE_ID BIGINT,
        FENTRANCE_NAME VARCHAR(100),
        FMEMO VARCHAR(200)
        )
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        CREATE TABLE IF NOT EXISTS STAGE.FLANG_DIM
        (
        FL_FSK VARCHAR(50),
        FL_ID VARCHAR(256),
        FL_NAME VARCHAR(256)
        )"""
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        res = self.hq.exe_sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
        if res != 0: return res
        # self.hq.debug = 0
        res = self.hq.exe_sql("""DROP TABLE IF EXISTS STAGE.GAME_INFO_DIM_TMP """)
        if res != 0: return res
        hql = """
        CREATE TABLE IF NOT EXISTS STAGE.GAME_INFO_DIM_TMP
        AS
        SELECT DISTINCT FLANG, FAD_CODE, FVERSION_INFO, FENTRANCE_ID FROM (
        SELECT FLANG, FAD_CODE, FVERSION_INFO, FENTRANCE_ID FROM STAGE.USER_LOGIN_STG
        WHERE dt = '%(statDate)s'
        UNION ALL
        SELECT FLANGUAGE FLANG, FAD_CODE, FVERSION_INFO, FENTRANCE_ID FROM STAGE.USER_DIM
        WHERE dt = '%(statDate)s'
        ) A """ % { 'statDate':self.stat_date }
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res



        return res


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
a = load_game_info_dim(statDate)
a()
