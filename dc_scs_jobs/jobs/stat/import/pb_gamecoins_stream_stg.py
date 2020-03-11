#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class pb_gamecoins_stream(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
        use stage;
        """
        res = self.hq.exe_sql(hql)

        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        self.hq.debug = 0

        args_dic = {
            "ld_begin": statDate
        }

        hql = """
        use stage;
        alter table pb_gamecoins_stream add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/pb_gamecoins_stream/%(ld_begin)s';
        alter table pb_gamecoins_stream add if not exists partition(dt='%(ld_end)s') location '/dw/stage/pb_gamecoins_stream/%(ld_end)s';
        """ % self.hql_dict

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        #用更新分区式语法插入统计数据，如果用动态分区的方式插入数据，请执行如下语句

        hql = """
        insert overwrite table stage.pb_gamecoins_stream_stg partition(dt='%(ld_begin)s')
        select distinct fbpid,
                        fuid,
                        lts_at,
                        act_id,
                        act_type,
                        act_num,
                        user_gamecoins_num,
                        fversion_info,
                        fchannel_code,
                        fvip_type,
                        fvip_level,
                        flevel,
                        fseq_no,
                        fgame_id,
                        fpartner_info,
                        fscene,
                        fbank_gamecoins
          from stage.pb_gamecoins_stream
         where dt = '%(ld_begin)s'
        distribute by fbpid, fuid
        """ % args_dic

        res = self.hq.exe_sql(hql)
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
import_job = pb_gamecoins_stream(statDate)
import_job()
