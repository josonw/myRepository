#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_payment_channel_data(BaseStat):

    def create_tab(self):
        hql = """
        -- 特殊同步要求,全表
        create table if not exists analysis.payment_channel_dim
        (
            fm_fsk        varchar(50),
            fm_id         varchar(256),
            fm_name       varchar(256),
            fmobilename   varchar(64)
        );


        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []


        #pmodename要实时更新，也要保证statid的唯一性。但这种生成payment_channel_dim的方式会不会在数据重跑时找不到某些statid?此机制待后续更改完善
        # hql = """
        # insert overwrite table analysis.payment_channel_dim
        # SELECT b.statid ,
        #        b.statid ,
        #        b.pmodename ,
        #        NULL ,
        #        NULL
        # FROM
        #   (SELECT a.statid ,
        #           a.pmodename,
        #           row_number() over(PARTITION BY statid) AS flag
        #    FROM stage.paycenter_chanel a
        #    WHERE dt = '%(stat_date)s'
        #      AND statid IS NOT NULL
        #      AND pmodename IS NOT NULL
        #      AND use_status = 0) AS b
        # WHERE flag =1
        # ORDER BY statid
        # """ % self.hql_dict
        # hql_list.append( hql )

        hql = """
        insert into table analysis.payment_channel_dim

        SELECT b.fm_id fm_fsk,
               b.fm_id,
               b.fm_name,
               NULL fmobilename,
               NULL fpay_kind
        FROM analysis.payment_channel_dim a
        RIGHT JOIN
          ( SELECT  statid fm_id,
                   pmodename fm_name
           FROM stage.paycenter_chanel
           WHERE dt = '%(stat_date)s'
             and use_status = 0
             AND statid IS NOT NULL
             AND pmodename IS NOT NULL
           GROUP BY statid,
                    pmodename ) b
         ON a.fm_id = b.fm_id
        WHERE a.fm_id IS NULL;
        """ % self.hql_dict
        hql_list.append( hql )





        # 统计语句做了屏蔽，因为依赖次dim表的统计用了fsk字段
        res = 0
        res = self.exe_hql_list(hql_list)
        return res


if __name__ == "__main__":
    stat_date = ''

    if len(sys.argv) == 1:
        #没有输入参数的话，日期默认取昨天
        stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    else:
        #从输入参数取，实际是第几个就取第几个
        args = sys.argv[1].split(',')
        stat_date = args[0]

    #生成统计实例
    a = agg_payment_channel_data(stat_date)
    a()
