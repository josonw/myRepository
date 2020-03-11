#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class parquet_gameparty_stg(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        #numdate为当天日期，yesterday为昨天日期
        self.hql_dict.update({'numdate':self.hql_dict['ld_end'].replace('-',''),'yesterday':self.hql_dict['ld_begin'].replace('-','')})
        numdate7ago=self.hql_dict.get('ld_7dayago').replace('-','')
        self.hql_dict.update({'numdate7ago':numdate7ago})


        hql = """
        CREATE TABLE if not exists today.parquet_gameparty_%(numdate)s (
           dt STRING COMMENT '',
           bpid STRING COMMENT '',
           lts_at STRING COMMENT '',
           uid BIGINT COMMENT '',
           tbl_id STRING COMMENT '',
           inning_id STRING COMMENT '',
           player_cnt INT COMMENT '',
           s_timer STRING COMMENT '',
           e_timer STRING COMMENT '',
           gamecoins BIGINT COMMENT '',
           user_gcoins BIGINT COMMENT '',
           charge FLOAT COMMENT '',
           subname STRING COMMENT '',
           pname STRING COMMENT '',
           blind_1 BIGINT COMMENT '',
           blind_2 BIGINT COMMENT '',
           limit_gcoins BIGINT COMMENT '',
           has_fold INT COMMENT '',
           has_open INT COMMENT '',
           robots_num INT COMMENT '',
           trustee_num INT COMMENT '',
           is_king INT COMMENT '',
           serv_charge BIGINT COMMENT '',
           card_type STRING COMMENT '',
           version_info STRING COMMENT '',
           channel_code STRING COMMENT '',
           integral_val BIGINT COMMENT '',
           integral_balance BIGINT COMMENT '',
           is_weedout INT COMMENT '',
           is_bankrupt INT COMMENT '',
           is_end INT COMMENT '',
           match_id STRING COMMENT '',
           ms_time STRING COMMENT '',
           me_time STRING COMMENT '',
           vip_type STRING COMMENT '',
           vip_level BIGINT COMMENT '',
           level BIGINT COMMENT '',
           gsubname STRING COMMENT '',
           tbuymin INT COMMENT '',
           tbuymax INT COMMENT '',
           prechip INT COMMENT '',
           first_play INT COMMENT '',
           game_id INT COMMENT '',
           first_play_sub INT COMMENT '',
           first_match INT COMMENT '',
           coins_type INT COMMENT '',
           party_type STRING COMMENT '',
           first_play_gsub INT COMMENT '',
           shift_party INT COMMENT '',
           partner_info STRING COMMENT '',
           bank_gamecoins BIGINT COMMENT '',
           bank_boyaacoins BIGINT COMMENT '',
           bank_currency1 BIGINT COMMENT '',
           bank_currency2 BIGINT COMMENT '',
           bank_currency3 BIGINT COMMENT '',
           award_type STRING COMMENT '',
           round_num INT COMMENT '',
           game_num INT COMMENT '',
           match_cfg_id INT COMMENT '',
           match_log_id INT COMMENT '',
           room_id STRING COMMENT ''
        )
        STORED AS PARQUET
        LOCATION '/dw/today/parquet_gameparty/dt=%(ld_end)s';
        """% self.hql_dict

        res = self.hq.exe_sql(hql)

        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        self.hq.debug = 0

        dates_dict = PublicFunc.date_define(self.stat_date)
        query = {}
        query.update(dates_dict)

        hql = """
        INSERT OVERWRITE TABLE stage.parquet_gameparty_stg PARTITION (dt='%(ld_begin)s')
        SELECT bpid,
                lts_at,
                uid,
                tbl_id,
                inning_id,
                player_cnt,
                s_timer,
                e_timer,
                gamecoins,
                user_gcoins,
                charge,
                subname,
                pname,
                blind_1,
                blind_2,
                limit_gcoins,
                has_fold,
                has_open,
                robots_num,
                trustee_num,
                is_king,
                serv_charge,
                card_type,
                version_info,
                channel_code,
                integral_val,
                integral_balance,
                is_weedout,
                is_bankrupt,
                is_end,
                match_id,
                ms_time,
                me_time,
                vip_type,
                vip_level,
                level,
                gsubname,
                tbuymin,
                tbuymax,
                prechip,
                first_play,
                game_id,
                first_play_sub,
                first_match,
                coins_type,
                party_type,
                first_play_gsub,
                shift_party,
                partner_info,
                bank_gamecoins,
                bank_boyaacoins,
                bank_currency1,
                bank_currency2,
                bank_currency3,
                award_type,
                round_num,
                game_num,
                match_cfg_id,
                match_log_id,
                room_id
        FROM today.parquet_gameparty_%(yesterday)s;
        DROP TABLE IF EXISTS today.parquet_gameparty_%(numdate7ago)s;
        """ % self.hql_dict

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
import_job = parquet_gameparty_stg(statDate)
import_job()
