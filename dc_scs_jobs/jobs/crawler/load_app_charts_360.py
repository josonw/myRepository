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

class get_app_charts_360(GetTopAppChart):
    def merge_url(self):
        chess = [ "http://zhushou.360.cn/list/index/cid/54/size/all/lang/all/order/download/?page=%s" % i for i in range(1,6) ]
        game  = [ "http://zhushou.360.cn/list/index/cid/2/size/all/lang/all/order/download/?page=%s" % i for i in range(1,6) ]
        self.url_dict = {"chess_urls": chess, "game_urls":game}

    def get_xml_info(self):

        def get_each_type_info(key):
            reslut_list = []
            counter = 0
            for url in self.url_dict[key]:
                content = get_page_content(url)
                html = pq(content)
                for index, li in enumerate(html("#iconList li")):
                    doc = pq(li)
                    g = {}
                    g['fsourcel'] = 'game_360'
                    counter  = counter + 1
                    g['frank'] = counter
                    if key == "chess_urls":
                        g['ftop_type'] = 1
                    elif key == "game_urls":
                        g['ftop_type'] = 0
                    g['fgamename'] = doc("h3 a").text()
                    g['fgame_url'] = "http://zhushou.360.cn" + doc("h3 a").attr("href")
                    g['flogo_url'] = doc("img").attr("_src")
                    g['fprice'] = 'Free'
                    g['fdownload']= doc("span").text()
                    try:
                        sub_doc = pq(get_page_content(g['fgame_url']))
                        g['fgame_score'] = sub_doc(".pf .s-1.js-votepanel").text().split(" ")[0]
                    except:
                        g['fgame_score'] = 0
                    g['fversion'] = 0
                    g['flinkurl'] = url
                    reslut_list.append(g)
                    if counter >= Top_Number:
                        return reslut_list

        reslut_list = map(lambda k: get_each_type_info(k),self.url_dict.keys())
        self.reslut_list = []
        for r in reslut_list:
            self.reslut_list = self.reslut_list + r


class load_app_charts_360(BasePGStat):
    """抓取排行榜数据的结果插入数据库
    """
    def stat(self):
        # 删除当天数据，避免重复
        hql = """
          delete from analysis.top_game_info
           where fdate >= current_date
             and fsourcel ='game_360';
         commit;
        """ % self.sql_dict
        self.append(hql)

        # 获取数据
        g = get_app_charts_360()
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
    a = load_app_charts_360()
    a()
    endtime = datetime.datetime.now()
    print "Succeed! Running time is " + str((endtime - starttime).seconds) + " second"

