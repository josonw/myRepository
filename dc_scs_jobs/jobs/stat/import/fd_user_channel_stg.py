#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class fd_user_channel_stg(BaseStat):

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

        hql_list = []


        hql = """
        use stage;
        alter table fd_user_channel_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/user_channel/%(ld_begin)s';
        alter table fd_user_channel_stg add if not exists partition(dt='%(ld_end)s') location '/dw/stage/user_channel/%(ld_end)s';
        """ % self.hql_dict
        hql_list.append( hql )
 
        '''
        # 先备份表
        hql = """
        use stage;
        insert overwrite table stage.fd_user_channel_stg_bak partition(dt = "%(ld_begin)s")
        select
        fbpid,
        fuid,
        fplatform_uid,
        fchannel_id,
        fet_id,
        flts_at,
        fip,
        fcli_verinfo,
        fcli_os,
        fdevice_type,
        fpixel_info,
        fisp,
        fnw_type,
        fudid,
        fpay_money,
        fpay_rate,
        fpay_mode,
        fpid,
        fm_at,
        fvip_type,
        fvip_level,
        flevel,
        fimei,
        fremark
        from stage.fd_user_channel_stg where dt ="%(ld_begin)s"
        """ % self.hql_dict
        hql_list.append( hql )
        '''
        # 过滤港云的数据
        hql = """
        use stage;
        insert overwrite table stage.fd_user_channel_stg partition(dt = "%(ld_begin)s")
        select
        fbpid,
        fuid,
        fplatform_uid,
        a.fchannel_id,
        fet_id,
        flts_at,
        fip,
        fcli_verinfo,
        fcli_os,
        fdevice_type,
        fpixel_info,
        fisp,
        fnw_type,
        fudid,
        fpay_money,
        fpay_rate,
        fpay_mode,
        fpid,
        fm_at,
        fvip_type,
        fvip_level,
        flevel,
        fimei,
        fremark
        from stage.fd_user_channel_stg a
        left join stage.gangyu_delete_channel_id_list b
        on a.fchannel_id = b.fchannel_id
        where a.dt ="%(ld_begin)s"
        and b.fchannel_id is null
        """ % self.hql_dict
        hql_list.append( hql )

        result = self.exe_hql_list(hql_list)

        return result


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
import_job = fd_user_channel_stg(statDate)
import_job()
