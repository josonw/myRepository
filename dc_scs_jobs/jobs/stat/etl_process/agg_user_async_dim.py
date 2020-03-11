#!/user/local/python272/bin/python
#-*- coding:UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_async_dim(BaseStat):

    def create_tab(self):
        hql = """
        use stage;
        create external table if not exists stage.user_async_dim
        (
            fbpid   varchar(50) ,
            fuid    bigint  ,
            fsync_at    string    ,
            fsync_action    bigint  ,
            fname   varchar(200)    ,
            fdisplay_name   varchar(400)    ,
            fgender bigint  ,
            fage    bigint  ,
            flanguage   varchar(200)    ,
            fcountry    varchar(200)    ,
            fcity   varchar(200)    ,
            ffriends_num    bigint  ,
            fappfriends_num bigint  ,
            fprofession bigint  ,
            finterest   varchar(100)    ,
            fpoint_num  bigint  ,
            fgamecoin_num   bigint  ,
            fbloodtype  varchar(100)    ,
            feducation  varchar(100)    ,
            fgoods_num  bigint  ,
            fbycoin_num bigint
        )
        stored as orc
        location '/dw/stage/user_async_dim';
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res
        hql = """drop table if exists tmp_user_async;
                    create table stage.tmp_user_async as
                    select * from stage.user_async_dim;"""
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        query = {'statdate': statDate}

        res = self.hq.exe_sql(
            """use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0:
            return res

        hql = """
        insert overwrite table stage.user_async_dim
        select fbpid,
             fuid,
             fsync_at,
             fsync_action,
             fname,
             fdisplay_name,
             fgender,
             fage,
             flanguage,
             fcountry,
             fcity,
             ffriends_num,
             fappfriends_num,
             fprofession,
             finterest,
             fpoint_num,
             fgamecoin_num,
             fbloodtype,
             feducation,
             fgoods_num,
             fbycoin_num
        from (select fbpid,
                     fuid,
                     fsync_at,
                     fsync_action,
                     fname,
                     fdisplay_name,
                     fgender,
                     fage,
                     flanguage,
                     fcountry,
                     fcity,
                     ffriends_num,
                     fappfriends_num,
                     fprofession,
                     finterest,
                     fpoint_num,
                     fgamecoin_num,
                     fbloodtype,
                     feducation,
                     fgoods_num,
                     fbycoin_num,
                     row_number() over(partition by fbpid, fuid order by fsync_at desc) as flag
                from (select fbpid,
                             fuid,
                             fsync_at,
                             fsync_action,
                             fname,
                             fdisplay_name,
                             fgender,
                             fage,
                             flanguage,
                             fcountry,
                             fcity,
                             ffriends_num,
                             fappfriends_num,
                             fprofession,
                             finterest,
                             fpoint_num,
                             fgamecoin_num,
                             fbloodtype,
                             feducation,
                             fgoods_num,
                             fbycoin_num
                        from stage.user_async_stg
                       where dt = "%(statdate)s"
                      union all
                      select fbpid,
                             fuid,
                             fsync_at,
                             fsync_action,
                             fname,
                             fdisplay_name,
                             fgender,
                             fage,
                             flanguage,
                             fcountry,
                             fcity,
                             ffriends_num,
                             fappfriends_num,
                             fprofession,
                             finterest,
                             fpoint_num,
                             fgamecoin_num,
                             fbloodtype,
                             feducation,
                             fgoods_num,
                             fbycoin_num
                        from stage.tmp_user_async) aa
         ) bb
         where flag = 1 """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res
        return res

# 愉快的统计要开始啦
global statDate
eid = 0
if len(sys.argv) == 1:
    # 没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(
        datetime.date.today() - datetime.timedelta(days=1), "%Y-%m-%d")
elif len(sys.argv) >= 3:
    # 从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
    eid = int(sys.argv[2])
else:
    args = sys.argv[1].split(',')
    statDate = args[0]


# 生成统计实例
a = agg_user_async_dim(statDate, eid)
a()
