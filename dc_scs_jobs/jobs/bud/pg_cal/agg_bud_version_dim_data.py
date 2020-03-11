#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date


class agg_bud_version_dim_data(BasePGCluster):

    """bud版本维表更新
    """

    def stat(self):

        # sql = """--海外棋牌
        #       insert into bud_dm.bpid_map_bud
        #          select t.fbpid,
        #                 case when t.fgamename = '巴西棋牌' then 4619040442
        #                      when t.fgamename = '泰国棋牌' then 4619040443
        #                      when t.fgamename = '印尼棋牌' then 4619040444
        #                      when t.fgamename = '越南棋牌' then 4619040445
        #                  end fplatformfsk,
        #                 case when t.fgamename = '巴西棋牌' then '巴西'
        #                      when t.fgamename = '泰国棋牌' then '泰国'
        #                      when t.fgamename = '印尼棋牌' then '印尼'
        #                      when t.fgamename = '越南棋牌' then '越南'
        #                  end fplatformname,
        #                 4542620389 as fgamefsk,
        #                 '海外棋牌' as fgamename,
        #                 t.fgamefsk fhallfsk,
        #                 t.fgamename fhallname,
        #                 t.fterminaltypefsk,
        #                 t.fterminaltypename,
        #                 t.fgamefsk+t.fplatformfsk+t.fhallfsk*1000 +t.fversionfsk+t.fterminaltypefsk fversionfsk,
        #                 case when t.fbpid ='9C35999F2F60546C99AA1A4739D02382' then concat(regexp_replace(t.fhallname,'大厅',''),'_繁体_',t.fversionname) else concat(regexp_replace(t.fhallname,'大厅',''),'_',t.fversionname) end fversionname,
        #                 t.fterminalfsk,
        #                 t.fterminalname,
        #                 t.fsecret_key,
        #                 t.fterminalsysfsk,
        #                 t.fterminalsysname,
        #                 t.fdistrictfsk,
        #                 t.fdistrictname,
        #                 t.fareaname,
        #                 t.fareafsk,
        #                 t.flangtypename,
        #                 t.flangtypefsk,
        #                 t.fplatformappname,
        #                 t.fplatformappfsk,
        #                 t.fproductname,
        #                 t.fproductfsk,
        #                 t.fdevicetypename,
        #                 t.fdevicetypefsk,
        #                 t.fgroupfsk,
        #                 t.fgroupname,
        #                 t.fdip,
        #                 t.fjointfsk,
        #                 t.fjointname,
        #                 t.priority,
        #                 t.hallmode,
        #                 t.fversion_old,
        #                 t.fversionname_old,
        #                 t.fdau,
        #                 t.fprovince,
        #                 null fcity
        #            from dcbase.bpid_map t
        #            left join bud_dm.bpid_map_bud a
        #              on t.fbpid = a.fbpid
        #           where t.fgamename in ('巴西棋牌','泰国棋牌','印尼棋牌','越南棋牌')
        #             and a.fbpid is null;
        #          COMMIT;
        # """ % self.sql_dict
        # self.exe_hql(sql)

        sql = """--地方棋牌及其他
              insert into bud_dm.bpid_map_bud
                 select distinct t.fbpid
                        ,t.fplatformfsk
                        ,t.fplatformname
                        ,t.fgamefsk
                        ,t.fgamename
                        ,t.fhallfsk
                        ,t.fhallname
                        ,t.fterminaltypefsk
                        ,t.fterminaltypename
                        ,t.fversionfsk
                        ,t.fversionname
                        ,t.fterminalfsk
                        ,t.fterminalname
                        ,t.fsecret_key
                        ,t.fterminalsysfsk
                        ,t.fterminalsysname
                        ,t.fdistrictfsk
                        ,t.fdistrictname
                        ,t.fareaname
                        ,t.fareafsk
                        ,t.flangtypename
                        ,t.flangtypefsk
                        ,t.fplatformappname
                        ,t.fplatformappfsk
                        ,t.fproductname
                        ,t.fproductfsk
                        ,t.fdevicetypename
                        ,t.fdevicetypefsk
                        ,t.fgroupfsk
                        ,t.fgroupname
                        ,t.fdip
                        ,t.fjointfsk
                        ,t.fjointname
                        ,t.priority
                        ,t.hallmode
                        ,t.fversion_old
                        ,t.fversionname_old
                        ,t.fdau
                        ,t.fprovince
                        ,null fcity
                   from dcbase.bpid_map t
                   left join bud_dm.bpid_map_bud a
                     on t.fbpid = a.fbpid
                  where t.hallmode = 1
                    and a.fbpid is null;
                 COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

        sql = """  INSERT INTO bud_ddl.version_dim (fsk,fname,fullname,feffectivedate)
                 select distinct t.fversionfsk fsk
                        ,t.fversionname fname
                        ,t.fversionname fullname
                        ,NOW() feffectivedate
                   from bud_dm.bpid_map_bud t
                   left join bud_ddl.version_dim a
                     on t.fversionfsk = a.fsk
                  where a.fsk is null;
                 COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_bud_version_dim_data(stat_date)
    a()
