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

class get_app_charts_gplay(GetTopAppChart):
    def merge_url(self):
        url_list = []
        country = ["cn","tw","hk","us", "br", "gb", "jp", "au", "in", "fr", "th", "id"]
        genre = ['GAME_CARD','GAME']
        self.url_list = ["https://play.google.com/store/apps/category/%s/collection/topselling_free?hl=zh-CN&start=0&num=%s&gl=%s"
                    % (g, Top_Number, c) for g in genre for c in country]

    def get_xml_info(self):
        self.reslut_list = []
        for url in self.url_list:
            url_info = url.split("/")
            print url
            for index, li in enumerate(pq(get_page_content(url))(".card-list .card")):
                if index >= Top_Number:
                    break
                doc = pq(li)
                g = {}
                g['frank'] = index + 1
                g['fsourcel'] = 'google'

                if url_info[6] == "GAME_CARD":
                    g['ftop_type'] = 1
                elif url_info[6] == "GAME":
                    g['ftop_type'] = 0

                g['fgamename'] = doc(".details .title").attr('title')
                g['fgame_url'] ="https://play.google.com"+ doc(".details .title").attr('href') + "&hl=zh-CN"
                g['flogo_url'] = doc(".cover-image").attr('src')
                g['fdevelop'] = doc('.details .subtitle').text()
                g['fdevelop_url'] = "https://play.google.com"+ doc('.details .subtitle').attr('href')
                g['fdescription'] = doc('.details .description').text()
                if len(g['fdescription']):
                    g['fdescription'] = g['fdescription'][:200] + "..."
                g['fprice'] = 'Free'
                g['flinkurl'] = url
                g['store'] = url_info[-1].split("=")[-1]
                try:
                    sub_doc = pq(get_page_content(g['fgame_url']))
                    score_str = sub_doc(".reviews-stats").text().encode("utf-8").split(" ")[1]
                    if int("".join(score_str.split(","))) > 10000000:
                        g['fgame_score'] = '1000万+次评分'
                    else:
                        g['fgame_score'] = "".join(score_str.split(",")) + u"次评分"
                except:
                    g['fgame_score'] = u"暂无数据"
                self.reslut_list.append(g)


class load_app_charts_gplay(BasePGStat):
    """抓取排行榜数据的结果插入数据库
    """
    def stat(self):
        # 删除当天数据，避免重复
        hql = """
          delete from analysis.top_game_info
           where fdate >= current_date
             and fsourcel ='google'
        """ % self.sql_dict
        self.append(hql)

        # 获取数据
        g = get_app_charts_gplay()
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
    a = load_app_charts_gplay()
    a()
    endtime = datetime.datetime.now()
    print "Succeed! Running time is " + str((endtime - starttime).seconds) + " second"

