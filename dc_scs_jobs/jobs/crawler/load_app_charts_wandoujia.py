# -*- coding: utf-8 -*-
#Author: AnsenWen
from pyquery import PyQuery as pq
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from GetTopAppChart import GetTopAppChart, get_page_content
from BaseStat import BasePGStat, get_stat_date

Top_Number = 100

class get_app_charts_wandoujia(GetTopAppChart):
    def merge_url(self):
        self.url_list=["http://www.wandoujia.com/top/game"]

    def get_xml_info(self):
        self.reslut_list = []
        for url in self.url_list:
            html = pq(get_page_content(url))
            for li in html(".card"):
                doc = pq(li)
                g = {}
                g['fsourcel'] = 'wandoujia'
                g['frank'] = len(self.reslut_list) + 1
                g['ftop_type'] = 0
                g['fgamename'] = doc(".name").text().encode("utf-8")
                g['fgame_url'] = doc(".icon-area").attr('href')
                g['flogo_url'] = doc("img").attr('src')
                g['fgame_score'] = ''
                g['fdownload'] = doc(".install-count").text()
                try:
                    g['fdescription'] = pq(get_page_content(g['fgame_url']))(".con").text()+"..."
                except:
                    g['fdescription'] = "loading..."
                g['fdescription'] = g['fdescription'].encode("utf-8")
                g['fprice'] = 'Free'
                g['fversion'] = 0
                g['flinkurl'] = url
                self.reslut_list.append(g)
                if len(self.reslut_list) >= Top_Number:
                    return self.reslut_list

class load_app_charts_wandoujia(BasePGStat):
    """抓取排行榜数据的结果插入数据库
    """
    def stat(self):
        # 删除当天数据，避免重复
        hql = """
          delete from analysis.top_game_info
           where fdate >= current_date
             and fsourcel ='wandoujia'
        """ % self.sql_dict
        self.append(hql)

        # 获取数据
        g = get_app_charts_wandoujia()
        data = g()

        # 插入当天最新数据
        insert_sql = """
            insert into top_game_info
              (fsourcel,
               ftop_type,
               frank,
               fgamename,
               fgame_score,
               fgame_url,
               fdevelop,
               fdevelop_url,
               flogo_url,
               fprice,
               fdescription,
               fdownload,
               fversion,
               fdate,
               flinkurl,
               store,
               device)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, current_date, %s, %s, %s)
        """
        self.exemany_hql(insert_sql, data)

if __name__ == "__main__":
    starttime = datetime.datetime.now()
    print "Now begin our happy time!"
    #生成统计实例
    a = load_app_charts_wandoujia()
    a()
    endtime = datetime.datetime.now()
    print "Succeed! Running time is " + str((endtime - starttime).seconds) + " second"

