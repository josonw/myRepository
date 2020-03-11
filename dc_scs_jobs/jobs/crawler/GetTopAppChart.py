# -*- coding: utf-8 -*-
#Author: AnsenWen
from pyquery import PyQuery as pq
import os
import urllib2
import requests
import datetime
top_number = 100

def get_page_content(url, coding="utf-8"):
    """
    模拟浏览器访问url，获取网页内容
    """
    headers = {"User-Agent":
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36"}
    req = requests.get(url = url, headers = headers, timeout=30)
    req.encoding = coding
    content = req.text
    return content


class GetTopAppChart():
    """
    爬取APP排行榜数据的父类
    """
    def __int__(self):
        self.top_number = top_number

    def merge_url(self):
        """
        将url放在list里面
        """
        pass

    def get_xml_info(self):
        pass

    def format_data(self):
        """
        字段按照给定的顺序进行格式化
        """
        field_list = ('fsourcel', 'ftop_type', 'frank', 'fgamename', 'fgame_score', 'fgame_url', 'fdevelop', 'fdevelop_url', 'flogo_url', 'fprice', 'fdescription', 'fdownload', 'fversion', 'flinkurl', 'store', 'device')
        self.result = [ [l.get(field) for field in field_list] for l in self.reslut_list]

    def __call__(self):
        self.merge_url()
        self.get_xml_info()
        self.format_data()
        return self.result