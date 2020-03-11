# -*- coding: utf-8 -*-
#Author: AnsenWen
import xml.dom.minidom as xdm
import os
import sys
import time
import datetime
import requests
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from GetTopAppChart import GetTopAppChart, get_page_content
from BaseStat import BasePGStat, get_stat_date
reload(sys)
sys.setdefaultencoding('utf-8')

Top_Number = 100


class get_app_charts_appstore(GetTopAppChart):
    def merge_url(self):
        country = ["cn","tw","hk","us", "br", "gb", "jp", "au", "in", "fr", "th", "id"]
        top_type = ["topfreeapplications", "topfreeipadapplications"]
        genre = ['7005','6014']
        self.url_list = ["https://itunes.apple.com/%s/rss/%s/limit=%s/genre=%s/xml"
                        %(c, t, Top_Number, g) for c in country for t in top_type for g in genre]

    def get_xml_info(self):
        search_url =  "https://itunes.apple.com/lookup?id="
        def get_each_url_info(url):
            reslut_list = []
            url_info = url.split("/")
            content = get_page_content(url)
            doc = xdm.parseString(content)
            for index,node in enumerate(doc.getElementsByTagName("entry")):
                g = {}
                g["fsourcel"] = "appstore"
                g["fgamename"] = node.getElementsByTagName("im:name")[0].childNodes[0].nodeValue.encode("utf-8")
                g["fgameid"] = node.getElementsByTagName("id")[0].getAttribute("im:id").encode("utf-8")
                g["frank"] = index + 1
                g["fgame_url"] = node.getElementsByTagName("id")[0].childNodes[0].nodeValue.encode("utf-8")
                g["flogo_url"] = node.getElementsByTagName("im:image")[2].childNodes[0].nodeValue.encode("utf-8")
                desc_str = node.getElementsByTagName("summary")[0].childNodes[0].nodeValue
                g["fdescription"] = (desc_str[:200] + " ...").encode("utf-8")
                try:
                    app_url = search_url + g["fgameid"]
                    app_content = get_page_content(app_url)
                    json_to_dict = json.loads(app_content)
                    score_str = str(json_to_dict["results"][0]["userRatingCount"]) + u'次评分'
                except:
                    score_str = u"暂无数据"
                g["fgame_score"] = score_str.encode("utf-8")
                g['fdevelop'] = node.getElementsByTagName("im:artist")[0].childNodes[0].nodeValue.encode("utf-8")
                g['fdevelop_url'] = node.getElementsByTagName("im:artist")[0].getAttribute("href").encode("utf-8")
                g["fprice"] = "Free"
                g["flinkurl"] = url
                g["store"] = url_info[3]

                if url_info[7] == "genre=7005":
                    g["ftop_type"] = 1
                elif url_info[7] == "genre=6014":
                    g["ftop_type"] = 0

                if url_info[5] == "topfreeapplications":
                    g["device"] = "iPhone"
                elif url_info[5] == "topfreeipadapplications":
                    g["device"] = "iPad"
                elif url_info[5] == "topfreemacapps":
                    g["device"] = "Mac"
                reslut_list.append(g)
            return reslut_list
        reslut_list = map(lambda u: get_each_url_info(u), self.url_list)
        self.reslut_list = []
        for r in reslut_list:
            self.reslut_list = self.reslut_list + r


class load_app_charts_appstore(BasePGStat):
    """抓取排行榜数据的结果插入数据库
    """
    def stat(self):
        # 删除当天数据，避免重复
        hql = """
          delete from analysis.top_game_info
           where fdate >= current_date
             and fsourcel ='appstore'
        """ % self.sql_dict
        self.append(hql)

        # 获取数据
        g = get_app_charts_appstore()
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
    a = load_app_charts_appstore()
    a()
    endtime = datetime.datetime.now()
    print "Succeed! Running time is " + str((endtime - starttime).seconds) + " second"

