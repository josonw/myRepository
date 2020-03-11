#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date

class agg_summary_data_byday_new(BasePGStat):

    def stat(self):
        # 删除当天数据，避免重复
        sql = """
              delete from dcnew.summary_data_byday_new where fdate= date'%(stat_date)s';
        commit;
              delete from dcnew.summary_data_byday_new_tmp where fdate= date'%(stat_date)s';
        commit;
        """ % self.sql_dict
        self.exe_hql(sql)

        sql = """
        --活跃
         insert into dcnew.summary_data_byday_new_tmp
         (fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,dau)
         SELECT '%(stat_date)s' fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
                sum(coalesce(fdactucnt, 0)) dau
           FROM dcnew.act_user
          WHERE fdate = '%(stat_date)s'
            and fsubgamefsk = -21379
            and fchannelcode= -21379
          GROUP BY fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode;
        commit;
        """% self.sql_dict
        self.exe_hql(sql)

        sql = """
        --新增
         insert into dcnew.summary_data_byday_new_tmp
         (fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,dsu)
         SELECT date'%(stat_date)s' fdate, fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
                sum(coalesce(fdregucnt, 0)) dsu
           FROM dcnew.reg_user
          WHERE fdate = '%(stat_date)s'
            and fsubgamefsk = -21379
            and fchannelcode= -21379
          GROUP BY fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode;
        commit;
        """% self.sql_dict
        self.exe_hql(sql)

        sql = """
        --玩牌
         insert into dcnew.summary_data_byday_new_tmp
         (fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,pun)
         SELECT date'%(stat_date)s' fdate, fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
                sum(coalesce(fplayusernum, 0)) pun
           FROM dcnew.gameparty_total
          WHERE fdate = '%(stat_date)s'
            and fsubgamefsk = -21379
            and fchannelcode= -21379
          GROUP BY fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode;
        commit;
        """% self.sql_dict
        self.exe_hql(sql)

        sql = """
        --付费
         insert into dcnew.summary_data_byday_new_tmp
         (fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,dpu,dip)
         SELECT date'%(stat_date)s' fdate, fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
                sum(fpayusercnt) dpu, sum(fincome) dip
           FROM dcnew.pay_user_income
          WHERE fdate = '%(stat_date)s'
            and fsubgamefsk = -21379
            and fchannelcode= -21379
          GROUP BY fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode;
        commit;
        """% self.sql_dict
        self.exe_hql(sql)

        sql = """
        --在线在玩
         insert into dcnew.summary_data_byday_new_tmp
         (fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,daou,daopu)
         SELECT date'%(stat_date)s' fdate, fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
                avg(favgonline) daou,
                avg(favgplay) daopu
           FROM dcnew.user_online_byday_new
          WHERE fdate = '%(stat_date)s'
            and fsubgamefsk = -21379
            and fchannelcode= -21379
            and (coalesce(favgonline,0) !=0 or coalesce(favgplay,0) !=0)
          GROUP BY fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode;
        commit;
        """% self.sql_dict
        self.exe_hql(sql)


        sql = """
        --汇总
         insert into dcnew.summary_data_byday_new
         (fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,dau,dsu,pun,dpu,dip,daou,daopu)
         select fdate,
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fsubgamefsk,
                fterminaltypefsk,
                fversionfsk,
                fchannelcode,
                sum(dau) dau,
                sum(dsu) dsu,
                sum(pun) pun,
                sum(dpu) dpu,
                sum(dip) dip,
                sum(daou) daou,
                sum(daopu) daopu
           from dcnew.summary_data_byday_new_tmp
          where fdate= date'%(stat_date)s'
          group by fdate,
                   fgamefsk,
                   fplatformfsk,
                   fhallfsk,
                   fsubgamefsk,
                   fterminaltypefsk,
                   fversionfsk,
                   fchannelcode;

                 COMMIT;
        """% self.sql_dict
        self.exe_hql(sql)

if __name__ == "__main__":
    stat_date = get_stat_date()
    a = agg_summary_data_byday_new(stat_date)
    a()
