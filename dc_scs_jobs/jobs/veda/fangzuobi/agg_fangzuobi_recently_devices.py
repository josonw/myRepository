# -*- coding: UTF-8 -*-
import os
import sys
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel

class agg_fangzuobi_recently_devices(BaseStatModel):

    def create_tab(self):
        hql="""
        create table if not exists veda.dfqp_fzb_recently_device
        (
            fuid                    bigint,         --用户ID,
            fm_dtype                varchar(100),    --登陆设备机型
            fm_os                   varchar(100),    --设备系统版本
            fm_imei                 varchar(100),    --设备号
            flogin_at               string,          --设备最近一次登录时间,
            fm_pixel                varchar(100),    --设备分辨率
            fm_operator             varchar(100),    --网络运营商
            fm_network              varchar(100),    --网络类型
            fversionname            string           --渠道
        )
        """
        res = self.sql_exe(hql)
        return res

    def stat(self):
        hql = """
            insert overwrite table veda.dfqp_fzb_recently_device
            select
                D.fuid fuid,
                D.fm_dtype fm_dtype,
                max(D.fm_os) fm_os,
                max(D.fm_imei) fm_imei,
                max(D.flogin_at) flogin_at,
                max(D.fm_pixel) fm_pixel,
                max(D.fm_operator) fm_operator,
                max(D.fm_network) fm_network,
                max(D.fversionname) fversionname
            from
            (
                select
                    fuid,
                    fm_dtype,
                    fm_os,
                    fm_imei,
                    flogin_at,
                    fm_pixel,
                    fm_operator,
                    fm_network,
                    fversionname
                    from veda.dfqp_fzb_recently_device

                    union all

                    select
                    x.fuid fuid,
                    x.fm_dtype fm_dtype,
                    x.fm_os fm_os,
                    x.fm_imei fm_imei,
                    x.flogin_at flogin_at,
                    x.fm_pixel fm_pixel,
                    x.fm_operator fm_operator,
                    x.fm_network fm_network,
                    x.fversionname fversionname
                    from
                    (
                        select A.fuid,A.fm_dtype,A.fm_os,A.fm_imei,A.flogin_at,A.fm_pixel,A.fm_operator,A.fm_network,A.fbpid,B.fversionname,
                        rank() OVER(PARTITION BY fuid,fm_dtype ORDER BY flogin_at desc) t
                        from
                            (
                                select *
                                  from stage.user_login_stg
                                 where dt="%(statdate)s"
                            )A
                            join
                            (
                                select fbpid,fversionname
                                  from dim.bpid_map
                                 where fgamename = '地方棋牌'
                            )B
                            on  A.fbpid = B.fbpid
                    )x
                    where x.t = 1
            )D
            group by D.fuid,D.fm_dtype;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res
if __name__ == '__main__':
    a = agg_fangzuobi_recently_devices()
    a()
