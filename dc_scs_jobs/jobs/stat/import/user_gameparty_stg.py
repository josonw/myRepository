#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class user_gameparty(BaseStat):

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
        alter table user_gameparty add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/gameparty/%(ld_begin)s';
        alter table user_gameparty add if not exists partition(dt='%(ld_end)s') location '/dw/stage/gameparty/%(ld_end)s';
        """ % self.hql_dict

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        #用更新分区式语法插入统计数据，如果用动态分区的方式插入数据，请执行如下语句

        hql = """
        insert overwrite table stage.user_gameparty_stg
        partition(dt='%(ld_begin)s')
        select fbpid,
            flts_at,
            fuid,
            ftbl_id,
            finning_id,
            fpalyer_cnt,
            fs_timer,
            fe_timer,
            fgamecoins,
            fuser_gcoins,
            fcharge,
            fsubname,
            fpname,
            fblind_1,
            fblind_2,
            flimit_gcoins,
            fhas_fold,
            fhas_open,
            frobots_num,
            ftrustee_num,
            fis_king,
            fserv_charge,
            fcard_type,
            fversion_info,
            fchannel_code,
            fintegral_val,
            fintegral_balance,
            fis_weedout,
            fis_bankrupt,
            fis_end,
            fmatch_id,
            fms_time,
            fme_time,
            fvip_type,
            fvip_level,
            flevel,
            fgsubname,
            ftbuymin,
            ftbuymax,
            fprechip,
            ffirst_play,
            fgame_id,
            ffirst_play_sub,
            ffirst_match,
            fcoins_type,
            fparty_type,
            ffirst_play_gsub,
            fshift_party,
            fpartner_info,
            fbank_gamecoins,
            fbank_boyaacoins,
            fbank_currency1,
            fbank_currency2,
            fbank_currency3,
            faward_type,
            fround_num,
            fgame_num,
            fmatch_cfg_id,
            fmatch_log_id,
			froom_id,
			fmultiple,
            has_stand
        from stage.user_gameparty
        where dt = '%(ld_begin)s'
        distribute by fbpid, fuid;
        """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table stage.user_gameparty_stg
        partition(dt='%(ld_end)s')
        select fbpid,
            flts_at,
            fuid,
            ftbl_id,
            finning_id,
            fpalyer_cnt,
            fs_timer,
            fe_timer,
            fgamecoins,
            fuser_gcoins,
            fcharge,
            fsubname,
            fpname,
            fblind_1,
            fblind_2,
            flimit_gcoins,
            fhas_fold,
            fhas_open,
            frobots_num,
            ftrustee_num,
            fis_king,
            fserv_charge,
            fcard_type,
            fversion_info,
            fchannel_code,
            fintegral_val,
            fintegral_balance,
            fis_weedout,
            fis_bankrupt,
            fis_end,
            fmatch_id,
            fms_time,
            fme_time,
            fvip_type,
            fvip_level,
            flevel,
            fgsubname,
            ftbuymin,
            ftbuymax,
            fprechip,
            ffirst_play,
            fgame_id,
            ffirst_play_sub,
            ffirst_match,
            fcoins_type,
            fparty_type,
            ffirst_play_gsub,
            fshift_party,
            fpartner_info,
            fbank_gamecoins,
            fbank_boyaacoins,
            fbank_currency1,
            fbank_currency2,
            fbank_currency3,
            faward_type,
            fround_num,
            fgame_num,
            fmatch_cfg_id,
            fmatch_log_id,
            froom_id,
			fmultiple,
            has_stand
        from stage.user_gameparty
        where dt = '%(ld_end)s'
        distribute by fbpid, fuid;
        """ % self.hql_dict

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res
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
import_job = user_gameparty(statDate)
import_job()
