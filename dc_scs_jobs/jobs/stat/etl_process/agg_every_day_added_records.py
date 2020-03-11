#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
from libs.warning_way import send_sms
from PublicFunc import PublicFunc
from BaseStat import BaseStat
import config


class agg_every_day_added_records(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.dc_data_info
        (
          fdate                date,
          fbpid                varchar(50),
          fuser_register       bigint,
          fuser_login          bigint,
          ffd_user_channel     bigint,
          fuser_bankrupt       bigint,
          fuser_vip            bigint,
          fuser_upgrade        bigint,
          fgoods_balance       bigint,
          ffeed_send           bigint,
          ffeed_clicked        bigint,
          fpush_send           bigint,
          fpush_rece           bigint,
          ffinished_gameparty  bigint,
          fjoin_gameparty      bigint,
          foffline_gameparty   bigint,
          fuser_gameparty      bigint,
          fuser_order          bigint,
          fpayref_view         bigint,
          flobby_download      bigint,
          flobby_launch        bigint,
          flobby_game_download bigint,
          fby_event            bigint,
          fgamecoins_stream    bigint,
          fbycoins_stream      bigint,
          fpayment_stream      bigint,
          fby_event_args       bigint,
          fuser_online         bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/dc_data_info'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0

        res = self.hq.exe_sql(
            """use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0:
            return res

        hql = """    --
        insert overwrite table analysis.dc_data_info
        partition( dt="%(stat_date)s" )
            select "%(stat_date)s" fdate, a.fbpid fbpid,
                sum(fuser_register) fuser_register,
                sum(fuser_login) fuser_login,
                sum(ffd_user_channel) ffd_user_channel,
                sum(fuser_bankrupt) fuser_bankrupt,
                sum(fuser_vip) fuser_vip,
                sum(fuser_upgrade) fuser_upgrade,
                sum(fgoods_balance) fgoods_balance,
                sum(ffeed_send) ffeed_send,
                sum(ffeed_clicked) ffeed_clicked,
                sum(fpush_send) fpush_send,
                sum(fpush_rece) fpush_rece,
                sum(ffinished_gameparty) ffinished_gameparty,
                sum(fjoin_gameparty) fjoin_gameparty,
                sum(foffline_gameparty) foffline_gameparty,
                sum(fuser_gameparty) fuser_gameparty,
                sum(fuser_order) fuser_order,
                sum(fpayref_view) fpayref_view,
                sum(flobby_download) flobby_download,
                sum(flobby_launch) flobby_launch,
                sum(flobby_game_download) flobby_game_download,
                sum(fby_event) fby_event,
                sum(fgamecoins_stream) fgamecoins_stream,
                sum(fbycoins_stream) fbycoins_stream,
                sum(fpayment_stream) fpayment_stream,
                sum(fby_event_args) fby_event_args,
                sum(fuser_online) fuser_online
            from (
                select fbpid, count(1) fuser_register, 0 fuser_login, 0 ffd_user_channel, 0 fuser_bankrupt,
                    0 fuser_vip, 0 fuser_upgrade, 0 fgoods_balance, 0 ffeed_send, 0 ffeed_clicked, 0 fpush_send,
                    0 fpush_rece, 0 ffinished_gameparty, 0 fjoin_gameparty, 0 foffline_gameparty, 0 fuser_gameparty,
                    0 fuser_order, 0 fpayref_view, 0 flobby_download, 0 flobby_launch, 0 flobby_game_download,
                    0 fby_event, 0 fgamecoins_stream, 0 fbycoins_stream, 0 fpayment_stream, 0 fby_event_args,
                    0 fuser_online
                from stage.user_dim where dt="%(stat_date)s" group by fbpid
                    union all
                select fbpid, 0 fuser_register, count(1) fuser_login, 0 ffd_user_channel, 0 fuser_bankrupt,
                    0 fuser_vip, 0 fuser_upgrade, 0 fgoods_balance, 0 ffeed_send, 0 ffeed_clicked, 0 fpush_send,
                    0 fpush_rece, 0 ffinished_gameparty, 0 fjoin_gameparty, 0 foffline_gameparty, 0 fuser_gameparty,
                    0 fuser_order, 0 fpayref_view, 0 flobby_download, 0 flobby_launch, 0 flobby_game_download,
                    0 fby_event, 0 fgamecoins_stream, 0 fbycoins_stream, 0 fpayment_stream, 0 fby_event_args,
                    0 fuser_online
                from stage.user_login_stg where dt="%(stat_date)s" group by fbpid
                    union all
                select fbpid, 0 fuser_register, 0 fuser_login, count(1) ffd_user_channel, 0 fuser_bankrupt,
                    0 fuser_vip, 0 fuser_upgrade, 0 fgoods_balance, 0 ffeed_send, 0 ffeed_clicked, 0 fpush_send,
                    0 fpush_rece, 0 ffinished_gameparty, 0 fjoin_gameparty, 0 foffline_gameparty, 0 fuser_gameparty,
                    0 fuser_order, 0 fpayref_view, 0 flobby_download, 0 flobby_launch, 0 flobby_game_download,
                    0 fby_event, 0 fgamecoins_stream, 0 fbycoins_stream, 0 fpayment_stream, 0 fby_event_args,
                    0 fuser_online
                from stage.fd_user_channel_stg where dt="%(stat_date)s" group by fbpid
                    union all
                select fbpid, 0 fuser_register, 0 fuser_login, 0 ffd_user_channel, 0 fuser_bankrupt,
                    0 fuser_vip, 0 fuser_upgrade, 0 fgoods_balance, 0 ffeed_send, 0 ffeed_clicked, 0 fpush_send,
                    0 fpush_rece, 0 ffinished_gameparty, 0 fjoin_gameparty, 0 foffline_gameparty, count(1) fuser_gameparty,
                    0 fuser_order, 0 fpayref_view, 0 flobby_download, 0 flobby_launch, 0 flobby_game_download,
                    0 fby_event, 0 fgamecoins_stream, 0 fbycoins_stream, 0 fpayment_stream, 0 fby_event_args,
                    0 fuser_online
                from stage.user_gameparty_stg where dt="%(stat_date)s" group by fbpid
                    union all
                select fbpid, 0 fuser_register, 0 fuser_login, 0 ffd_user_channel, 0 fuser_bankrupt,
                    0 fuser_vip, 0 fuser_upgrade, 0 fgoods_balance, 0 ffeed_send, 0 ffeed_clicked, 0 fpush_send,
                    0 fpush_rece, 0 ffinished_gameparty, 0 fjoin_gameparty, 0 foffline_gameparty, 0 fuser_gameparty,
                    0 fuser_order, 0 fpayref_view, 0 flobby_download, 0 flobby_launch, 0 flobby_game_download,
                    0 fby_event, count(1) fgamecoins_stream, 0 fbycoins_stream, 0 fpayment_stream, 0 fby_event_args,
                    0 fuser_online
                from stage.pb_gamecoins_stream_stg where dt="%(stat_date)s" group by fbpid
                    union all
                select fbpid, 0 fuser_register, 0 fuser_login, 0 ffd_user_channel, 0 fuser_bankrupt,
                    0 fuser_vip, 0 fuser_upgrade, 0 fgoods_balance, 0 ffeed_send, 0 ffeed_clicked, 0 fpush_send,
                    0 fpush_rece, 0 ffinished_gameparty, 0 fjoin_gameparty, 0 foffline_gameparty, 0 fuser_gameparty,
                    0 fuser_order, 0 fpayref_view, 0 flobby_download, 0 flobby_launch, 0 flobby_game_download,
                    0 fby_event, 0 fgamecoins_stream, 0 fbycoins_stream, count(1) fpayment_stream, 0 fby_event_args,
                    0 fuser_online
                from stage.payment_stream_stg where dt="%(stat_date)s" group by fbpid
            ) a
            group by a.fbpid
        """ % self.hql_dict
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    # 下面一些函数是监控数据入库相关
    def incrRatio(self, num1, num2):
        """ 获取num2对num1的比率"""
        if 0 == num1 or None == num1:
            return '100% '

        incr = num2 - num1
        return '%s%% ' % str(incr * 100 / num1)

    def getRecords(self):
        if self.stat_date != datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=1), "%Y-%m-%d"):
            """ 如果统计日期是不是昨天就不告警了，预防重跑多天的任务重复告警 """
            return 'ok'

        """ 获取当日以及 1天前、7天前、30天前核心表入库数据 """
        hql = """select fdate, sum(fuser_register) user_register, sum(fuser_login) user_login,
                sum(ffd_user_channel) user_channel, sum(fuser_gameparty) user_gameparty,
                sum(fgamecoins_stream) gamecoins_stream, sum(fpayment_stream) payment_stream
            from analysis.dc_data_info
            where cast(dt as string) in ('%(stat_date)s', '%(ld_begin_pre)s', '%(ld_7dayago)s', '%(ld_30dayago)s')
            group by fdate order by fdate desc
        """  % self.hql_dict

        res = self.hq.query(hql)
        call_flag = False
        call_msg=''
        if not res:
            warning_msg = 'HIVE入库预警: 数据获取失败'
            call_flag = True
            call_msg = warning_msg
        else:
            info = []
            for i in res:
                info.append(i)

            warning_msg = """HIVE入库预警:%s
            【表: 行数(同比1,7,30天前)】""" % info[0][0]
            warning_msg += "\nregister:" + str(info[0][1]) + " (" + self.incrRatio(info[1][1], info[0][
                1]) + self.incrRatio(info[2][1], info[0][1]) + self.incrRatio(info[3][1], info[0][1]) + ")"
            warning_msg += "\nlogin:" + str(info[0][2]) + " (" + self.incrRatio(info[1][2], info[0][
                2]) + self.incrRatio(info[2][2], info[0][2]) + self.incrRatio(info[3][2], info[0][2]) + ")"
            warning_msg += "\nchannel:" + str(info[0][3]) + " (" + self.incrRatio(info[1][3], info[0][
                3]) + self.incrRatio(info[2][3], info[0][3]) + self.incrRatio(info[3][3], info[0][3]) + ")"
            warning_msg += "\ngameparty:" + str(info[0][4]) + " (" + self.incrRatio(info[1][4], info[0][
                4]) + self.incrRatio(info[2][4], info[0][4]) + self.incrRatio(info[3][4], info[0][4]) + ")"
            warning_msg += "\ngamecoins:" + str(info[0][5]) + " (" + self.incrRatio(info[1][5], info[0][
                5]) + self.incrRatio(info[2][5], info[0][5]) + self.incrRatio(info[3][5], info[0][5]) + ")"
            warning_msg += "\npayment:" + str(info[0][6]) + " (" + self.incrRatio(info[1][6], info[0][
                6]) + self.incrRatio(info[2][6], info[0][6]) + self.incrRatio(info[3][6], info[0][6]) + ")"

            # 数据异常预警
            if str(info[0][1]) == '0':
                call_flag = True
                call_msg += '用户注册(register)入库数量为零\n'

            if str(info[0][2]) == '0':
                call_flag = True
                call_msg += '用户登录(login)入库数量为零\n'

            if str(info[0][3]) == '0':
                call_flag = True
                call_msg += '渠道(channel)入库数量为零\n'

            if str(info[0][4]) == '0':
                call_flag = True
                call_msg += '牌局(gameparty)入库数量为零\n'

            if str(info[0][5]) == '0':
                call_flag = True
                call_msg += '金币(gamecoins)入库数量为零\n'

            if str(info[0][6]) == '0':
                call_flag = True
                call_msg += '支付(payment)入库数量为零\n'

        send_sms(config.CONTACTS_LIST_ENAME, warning_msg)

        if call_flag:
            # 电话+短信告警
            send_sms(config.CONTACTS_LIST_ENAME, call_msg, 8)

        return 'ok'

# 生成统计实例
a = agg_every_day_added_records()
a()
a.getRecords()
