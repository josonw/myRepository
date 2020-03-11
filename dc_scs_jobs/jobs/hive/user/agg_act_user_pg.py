#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date


class agg_act_user_pg(BasePGStat):

    """地方棋牌分省数据增加在线在玩
    """

    def stat(self):

        sql = """
        insert into dcnew.act_user_mon
            select
                ua.fdate,
                ua.fgamefsk,
                ua.fplatformfsk,
                ua.fhallfsk,
                ua.fsubgamefsk,
                ua.fterminaltypefsk,
                ua.fversionfsk,
                ua.fchannelcode,
                fdactucnt,
                fdlgnucnt,
                fdlgncnt,
                fdgsucnt,
                fdgpucnt,
                fwaucnt,
                fmaucnt,
                f7dlgnucnt,
                f30dlgnucnt,
                f7dactucnt,
                f30dactucnt
            from
                dcnew.act_user ua
            where ua.fdate = '%(ld_begin)s';

        delete from dcnew.act_user where fdate= '%(ld_begin)s' ;

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

        sql = """
        insert into dcnew.act_user
            select
                ua.fdate,
                ua.fgamefsk,
                ua.fplatformfsk,
                ua.fhallfsk,
                ua.fsubgamefsk,
                ua.fterminaltypefsk,
                ua.fversionfsk,
                ua.fchannelcode,
                max(fdactucnt) fdactucnt,
                max(fdlgnucnt) fdlgnucnt,
                max(fdlgncnt) fdlgncnt,
                max(fdgsucnt) fdgsucnt,
                max(fdgpucnt) fdgpucnt,
                max(fwaucnt) fwaucnt,
                max(fmaucnt) fmaucnt,
                max(f7dlgnucnt) f7dlgnucnt,
                max(f30dlgnucnt) f30dlgnucnt,
                max(f7dactucnt) f7dactucnt,
                max(f30dactucnt) f30dactucnt
            from
                dcnew.act_user_mon ua
            where ua.fdate = '%(ld_begin)s'
        group by
            fdate,
            fgamefsk,
            fplatformfsk,
            fhallfsk,
            fsubgamefsk,
            fterminaltypefsk,
            fversionfsk,
            fchannelcode ;

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)


if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_act_user_pg(stat_date)
    a()
