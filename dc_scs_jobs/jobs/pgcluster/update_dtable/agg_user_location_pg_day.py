#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date


class agg_user_location_pg_day(BasePGCluster):
    """将核心数据合并入 agg_user_location_pg_day 中

    """
    def stat(self):
        '''删除日期为今天的数据，预防重跑出现数据重复'''
        sql = """
        delete from analysis.user_location_country_info where fdate = date '%(ld_begin)s'
        """ % self.sql_dict
        self.append(sql)
        sql = """
        delete from analysis.user_location_province_info where fdate = date '%(ld_begin)s'
        """ % self.sql_dict
        self.append(sql)

        ''' 插入数据，conuntry 是按国家维度划分的数据，
            provice 按省份和国家简称划分的数据
        '''
        sql = """
            INSERT INTO analysis.user_location_country_info
            SELECT date '%(ld_begin)s' as fdate ,
                        a.fgamefsk ,
                        a.fplatformfsk ,
                        a.fversionfsk ,
                        a.fterminalfsk,
                        CASE
                            WHEN a.fip_countrycode IN ('TW','HK','MO')
                                THEN 'CN'
                            when b.fcountryname is null
				                then 'UNKNOW'
                            ELSE a.fip_countrycode
                        END AS fip_countrycode ,
                        a.fip_country ,
                        b.fcountry_en ,
                        sum(fregusercnt) AS fregusercnt,
                        sum(factusercnt) AS factusercnt ,
                        sum(fpayusercnt) AS fpayusercnt,
                        sum(fincome) AS fincome
            FROM analysis.user_location_info a
            LEFT JOIN analysis.COUNTRY_DIM b
                ON (CASE
                            WHEN a.fip_countrycode IN ('TW','HK','MO')
                                THEN 'CN'
                            ELSE a.fip_countrycode
                        END)  = b.fcountryname
            WHERE fdate = date '%(ld_begin)s'
            GROUP BY a.fgamefsk ,
                     a.fplatformfsk ,
                     a.fversionfsk ,
                     a.fterminalfsk,
                     fip_countrycode ,
                     fip_country,
                     b.fcountry_en,
                     b.fcountryname
        """ % self.sql_dict
        self.append(sql)


        sql = """
        INSERT INTO analysis.user_location_province_info
        SELECT date '%(ld_begin)s' as  fdate ,
                    a.fgamefsk ,
                    a.fplatformfsk ,
                    a.fversionfsk ,
                    a.fterminalfsk,
                    CASE
                        WHEN a.fip_countrycode IN ('TW','HK','MO')
                            THEN 'CN'
                        ELSE a.fip_countrycode
                    END AS fip_countrycode ,


                    CASE
                        WHEN fip_province = '中国' THEN '未知'
                        ELSE fip_province
                    END AS fip_province ,
                    sum(fregusercnt) AS fregusercnt,
                    sum(factusercnt) AS factusercnt ,
                    sum(fpayusercnt) AS fpayusercnt,
                    sum(fincome) AS fincome
        FROM analysis.user_location_info a
        WHERE fdate = date '%(ld_begin)s'
        GROUP BY a.fgamefsk ,
                 a.fplatformfsk ,
                 a.fversionfsk ,
                 a.fterminalfsk,
                 fip_countrycode ,
                 a.fip_province
        """ % self.sql_dict
        self.append(sql)

        self.exe_hql_list()


if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_user_location_pg_day(stat_date)
    a()
