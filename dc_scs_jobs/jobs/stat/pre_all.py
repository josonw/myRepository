#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class StatTest(BaseStat):

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

        hql = """use stage;

              alter table by_event_args add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/by_event_args/%(ld_begin)s';
              alter table user_signup_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/user_signup/%(ld_begin)s';
              alter table user_login_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/user_login/%(ld_begin)s';
              alter table user_logout_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/user_logout/%(ld_begin)s';
              alter table pb_gamecoins_stream add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/pb_gamecoins_stream/%(ld_begin)s';
              alter table pb_bycoins_stream_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/pb_bycoins_stream/%(ld_begin)s';
              alter table pb_props_stream_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/pb_props_stream/%(ld_begin)s';
              alter table pb_gifts_stream_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/pb_gifts_stream/%(ld_begin)s';
              alter table user_bankrupt_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/user_bankrupt/%(ld_begin)s';
              alter table user_bankrupt_relieve_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/user_bankrupt_relieve/%(ld_begin)s';
              alter table day_user_balance_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/day_user_balance/%(ld_begin)s';
              alter table game_activity_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/game_activity/%(ld_begin)s';
              alter table user_gameparty add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/gameparty/%(ld_begin)s';
              alter table finished_gameparty_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/finished_gameparty/%(ld_begin)s';
              alter table join_gameparty_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/join_gameparty/%(ld_begin)s';
              alter table offline_gameparty_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/offline_gameparty/%(ld_begin)s';
              alter table poker_tbl_info add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/poker_tbl_info/%(ld_begin)s';
              alter table gameparty_settlement_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/gameparty_settlement/%(ld_begin)s';
              alter table user_order_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/user_order/%(ld_begin)s';
              alter table user_generate_order_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/user_generate_order/%(ld_begin)s';
              alter table feed_clicked_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/feed_clicked/%(ld_begin)s';
              alter table feed_send_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/feed_send/%(ld_begin)s';
              alter table push_send_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/push_send/%(ld_begin)s';
              alter table push_rece_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/push_rece/%(ld_begin)s';
              alter table push_edit_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/push_edit/%(ld_begin)s';
              alter table user_daytime_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/user_daytime/%(ld_begin)s';
              alter table role_created_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/role_created/%(ld_begin)s';
              alter table user_grade_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/user_grade/%(ld_begin)s';
              alter table user_vip_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/user_vip/%(ld_begin)s';
              alter table mission_spark_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/mission_spark/%(ld_begin)s';
              alter table mission_quit_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/mission_quit/%(ld_begin)s';
              alter table goods_balance_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/goods_balance/%(ld_begin)s';
              alter table game_qa_vote_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/game_qa_vote/%(ld_begin)s';
              alter table pay_page_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/pay_page/%(ld_begin)s';
              alter table user_bank_stage add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/user_bank/%(ld_begin)s';
              alter table gp_uiopen_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/gp_uiopen/%(ld_begin)s';
              alter table gp_pay_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/gp_pay/%(ld_begin)s';
              alter table pay_order_info_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/pay_order_info/%(ld_begin)s';
              alter table update_order_info_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/update_order_info/%(ld_begin)s';
              alter table payment_stream_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/pay_stream/%(ld_begin)s';
              alter table lobby_launch_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/lobby_launch/%(ld_begin)s';
              alter table lobby_download_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/lobby_download/%(ld_begin)s';
              alter table lobby_game_download_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/lobby_game_download/%(ld_begin)s';
              alter table fd_user_channel_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/user_channel/%(ld_begin)s';
              alter table pb_currencies_stream_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/pb_currencies_stream/%(ld_begin)s';
              alter table user_cheating_orgn add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/user_cheating_orgn/%(ld_begin)s';
              alter table user_async_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/user_async/%(ld_begin)s';
              alter table game_performance_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/game_performance/%(ld_begin)s';
              """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        #用更新分区式语法插入统计数据，如果用动态分区的方式插入数据，请执行如下语句

        hql = """
              insert overwrite table stage.pb_gamecoins_stream_stg
                partition(dt='%(ld_begin)s')
              select fbpid,
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
                     fseq_no
                from stage.pb_gamecoins_stream
               where dt = '%(ld_begin)s'
       distribute by fbpid, fuid;
              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

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
                      fgsubname
                from stage.user_gameparty
               where dt = '%(ld_begin)s'
       distribute by fbpid, fuid;
              """ % args_dic

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
a = StatTest(statDate)
a()
