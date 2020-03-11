#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat
import psycopg2

class load_party_dim(BaseStat):

    def create_tab(self):
        hql = """
        create external table if not exists dcnew.gameparty_name_dim
        (
          fsk        bigint,
          fgameparty_name varchar(100),
          fdis_name       varchar(100),
          fgamefsk        bigint
        )
        location '/dw/dcnew/gameparty_name_dim'
        """

        res = self.hq.exe_sql(hql)
        return res

    def stat(self):
        query = { 'statdate':statDate }

        hql = """
        select a.fgameparty_name fgameparty_name, a.fgameparty_name fdis_name,
            a.fgamefsk fgamefsk
        from (
            select distinct gs.fname fgameparty_name, bpm.fgamefsk fgamefsk
            from stage.finished_gameparty_uid_mid gs
            join dim.bpid_map bpm
                on gs.fbpid=bpm.fbpid
            where gs.dt="%(statdate)s" and gs.fname is not null
        ) a
        left outer join dcnew.gameparty_name_dim gd
            on gd.fgameparty_name = a.fgameparty_name and gd.fgamefsk=a.fgamefsk
        where gd.fgameparty_name is null
        """ % query
        res = self.hq.query(hql)

        conn = None
        cur = None
        try:
            conn = psycopg2.connect(host="10.30.101.240",
            port="5432",
            database="boyaadw",
            user="analysis",
            password="analysis")
            cur = conn.cursor()

            columns = ""
            for one in res:
                check_exist = "select fsk from dcnew.gameparty_name_dim where fgameparty_name = '%s' and fgamefsk=%d" % (one[0],one[2])
                cur.execute(check_exist)
                exist_fsk = cur.fetchone()
                if exist_fsk and len(exist_fsk) > 0:
                    if exist_fsk[0]:
                        continue

                sql = "insert into dcnew.gameparty_name_dim(fgameparty_name,fdis_name,fgamefsk) values (%(value)s)"
                values = ",".join([str(x) if isinstance(x,int) else "\'" + x + "\'" for x in one])
                sql = sql % {"value" : values}

                cur.execute(sql)
                conn.commit()
        except Exception, e:
            raise e
        finally:
            if not cur:
                cur.close()
            if not conn:
                conn.close()


#愉快的统计要开始啦
global statDate
eid = 0
if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
elif len(sys.argv) >= 3:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
    eid = int(sys.argv[2])
else :
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = load_party_dim(statDate, eid)
a()
