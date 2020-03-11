#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class other_analysis_table_info(BaseStat):
    """建立游戏维度表
    """
    def create_tab(self):
        hql = """create table if not exists analysis.dc_analysis_table_info_fct
                (
                fdate string,
                ftablename string,
                frownum bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res

    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )
        # 注意开启动态分区
        res = self.hq.exe_sql("""use stage;
            set hive.exec.dynamic.partition=true;
            """)
        if res != 0: return res
        tables = [
        'user_active_first_pay_fct',
        'user_act_retained_fct',
        'user_paid_retained_fct',
        'user_pay_retained_fct',
        'user_back_retained_fct',
        'user_retained_fct',
        'user_back_fct',
        'user_month_retained_fct',
        'user_retained_rupt_fct',
        'user_retained_login_fct',
        'user_retained_grade_fct',
        'user_week_retained_fct',
        'user_act_days_gap_fct',
        'user_act_gender_fct',
        'user_act_lang_fct',
        'user_reg_act_days_fct',
        'active_user_feed_ad',
        'user_active_subdivide_fct',
        'advertise_user_fct',
        'user_bankrupt_fct',
        'user_bankrupt_pay_fct',
        'user_backflow_fct',
        'user_lc_loss_age_fct',
        'user_lc_loss_rupt_fct',
        'user_lc_loss_income_fct',
        'user_month_loss_fct',
        'user_lc_loss_grade_fct',
        'user_lc_loss_income_fct',
        'user_loss_gamecoin_property',
        'game_activity_cost_fct',
        'game_activity_fct',
        'game_activity_hour_fct',
        'game_activity_pay_retain_fct',
        'game_activity_retain_fct',
        'game_activity_rule_fct',
        'lobby_user_fct',
        'lobby_user_download_fct',
        'paid_user_loss_funnel_fct',
        'user_push_rule_fct',
        'user_push_tpl_fct',
        'user_backflow_age_fct',
        'user_backflow_fct',
        'user_reflux_fct',
        'user_backflow_grade_fct',
        'user_backflow_party_fct',
        'user_return_fct',
        'user_backflow_property_fct',
        'user_register_ext_fct',
        'user_new_pay_party_fct',
        'user_register_logincnt_fct',
        'user_new_pay_fct',
        'user_new_rupt_fct',
        'reg_user_feed_ad',
        'user_active_days_fct',
        'user_active_ext_fct',
        'user_active_version_fct',
        'user_active_avg_days_fct',
        'user_device_type',
        'user_device_pixel',
        'user_device_os',
        'user_info_fct',
        'user_old_active_fct',
        'user_online_byday_agg',
        'user_online_fct',
        'user_online_timenum_fct',
        'user_register_fct',
        'user_register_ext_fct',
        'user_active_ext_fct',
        'user_source_path_fct',
        'user_true_active_fct',
        'user_upgrade_fct',
        'user_game_version_fct',
        'fbthai_e2p_top100_user_fct',
        'ddz_user_login_info_new',
        'user_gamepaty_gamecoin',
        'gameparty_ante_currency_fct',
        'user_gameparty_ante_hour_fct',
        'gameparty_competition_fct',
        'user_gameparty_game_fct',
        'user_playuser_hour_fct',
        'gameparty_knockout_fct',
        'gameparty_mobile_fct',
        'user_gameparty_num_fct',
        'gameparty_mustblind_user_fct',
        'gameparty_play_user_fct',
        'gameparty_playercnt_fct',
        'gameparty_mustblind_fct',
        'user_gameparty_fct',
        'user_playuser_hour_fct_ext',
        'gameparty_type_fct',
        'user_nogameparty_fct',
        'user_gameparty_partytype_fct',
        'user_retained_party_fct',
        'gameparty_settlement_fct',
        'game_card_type_fct',
        'offline_gameparty_all_fct',
        'dc_data_info',
        'pay_game_coin_finace_fct',
        'user_bankrupt_back_fct',
        'user_bankrupt_fenqun_fct',
        'user_bankrupt_num_fct',
        'user_bankrupt_pouring_fct',
        'user_bankrupt_partytype_fct',
        'user_bankrupt_relieve_num_fct',
        'user_feed_back_fct',
        'user_feed_fct',
        'user_push_back_fct',
        'user_push_fct',
        'user_relieve_num_fct',
        'user_vip_fct',
        'user_feed_type_fct',
        'by_deferred_bycoin_fct',
        'by_deferred_gamecoin_fct',
        'by_deferred_goods_balance_fct',
        'user_payment_fct_lastmonth_part',
        'finance_mau_mpu_retained',
        'user_new_top_pay_fct',
        'user_pu_loss_funnel_fct',
        'payment_chan_hour_fct',
        'pay_coins_num',
        'user_reg_pay_fct',
        'user_paid_act_loss_funnel_fct',
        'user_pay_days_gap_fct',
        'user_pay_day_gap_fct',
        'user_pay_entrance_fct',
        'user_pay_num_loss_fct',
        'user_pay_num_retained_fct',
        'user_pay_entrance',
        'pay_channel_fct',
        'pay_product_fct',
        'pay_product_channel_fct',
        'user_payment_fct_order_bankrupt_part',
        'user_generate_order_usernum',
        'user_generate_order_fct',
        'user_payment_fct_continue_part',
        'user_payment_fct_day_part',
        'user_payment_hour_fct',
        'user_payment_income_fct',
        'user_payment_fct_new_pay_part',
        'user_payment_fct_year_season_part',
        'user_pay_cnt_range_fct',
        'user_pay_gap_range_fct',
        'user_pay_money_range_fct',
        'user_top_pay_fct',
        'user_top_pay_actday',
        'by_deferred_income_fct',
        'user_payment_fct',
        'channel_market_device_type_fct',
        'user_channel_market_fct',
        'user_channel_hour_fct',
        'channel_market_operator_flow',
        'user_channel_ru_retention',
        'marketing_pay_mode_money',
        'marketing_roi_cost_fct',
        'marketing_roi_cps_cost_fct',
        'markerting_channel_forecast',
        'marketing_forecast_show_fct',
        'marketing_roi_retain_pay',
        'user_newchannel_retention',
        'marketing_channel_test_data',
        'user_channel_fct_bankrupt_part',
        'user_channel_fct_day_loss_part',
        'user_channel_fct_main_part',
        'user_channel_fct_gmprty_cnt_part',
        'user_channel_fct',
        'channel_verinfo_fct',
        'pay_center_normal_order_fct',
        'pay_center_unnormal_order_fct',
        'pay_center_final_order_fct',
        'pay_center_repair_cheat_fct',
        'register_retain_pay_fct',
        'user_bankrupt_gamecoin',
        'gamecoin_balance_fct',
        'pay_game_coin_finace_fct',
        'user_gamecoin_balance',
        'gameworld_balance_fct',
        'gift_finace_fct',
        'user_new_gamecoin_balance',
        'props_finace_fct',
        'user_bycoin_stream_fct',
        'ddz_jinbi_fct',
        'gift_pay_way_fct',
        'dc_payuser_coin_stream',
        'by_event_fct',
        'by_event_args_fct'
        ]
        hql = """use analysis;
                 alter table dc_analysis_table_info_fct drop partition (dt='%(ld_daybegin)s')
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        for table in tables:
            try:
                query.update({'table': table})
                hql = """
                insert into table dc_analysis_table_info_fct
                partition( dt='%(ld_daybegin)s' )
                select '%(ld_daybegin)s', '%(table)s' fapi_name, count(1) frownum from
                %(table)s where dt='%(ld_daybegin)s'
                """ % query
                res = self.hq.exe_sql(hql)
            except Exception, e:
                print e
        if res != 0: return res


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
a = other_analysis_table_info(statDate)
a()
