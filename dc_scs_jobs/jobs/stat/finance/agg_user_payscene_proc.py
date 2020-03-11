#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_payscene_proc(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists stage.user_payscene_mid
        (
        fbpid                               string,
        fuid                                bigint,
        fplatform_uid                       string,
        fentrance_id                        string,
        forder_at                           date,
        forder_id                           string,
        fcurrency_type                      bigint,
        fcurrency_num                       bigint,
        fitem_category                      bigint,
        fitem_id                            bigint,
        fitem_num                           bigint,
        fbalance                            bigint,
        fgrade                              bigint,
        fgameparty_pname                    string,
        fgameparty_subname                  string,
        fgameparty_anto                     bigint,
        fbankrupt                           bigint,
        fpay_scene                          string,
        fip                                 string,
        fplatform_order_id                  string,
        fb_order_id                         bigint,
        ffee                                decimal(20,2),
        fpm_name                            string,
        fincome                             decimal(20,4)
        )partitioned by (dt date);

        create table if not exists analysis.paycenter_apps_dim
        (
        appid               bigint,
        sid                 bigint,
        appname             string,
        bpid                string,
        is_lianyun          string,
        childid             string,
        game_id             bigint,
        lang_id             bigint
        );
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    # 付费场景数据，统一调度
    # 我们需要一支付的下单数据作为大盘数据，以业务上报作为辅助信息，确保支付数据口径一致，
    # 此中间表，提供后续场景运算。
    # 完成中间表之后，运行后续运算。
    # agg_user_generate_order('%(stat_date)s')
    # agg_user_generate_payscene('%(stat_date)s')
    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []

        hql = """
        insert overwrite table stage.user_payscene_mid partition (dt = "%(ld_begin)s")
        select  nvl(p.bpid,b.fbpid) fbpid,
                    b.fuid,
                    a.sitemid fplatform_uid,
                    fentrance_id,
                    pstarttime forder_at,
                    pid forder_id,
                    fcurrency_type,
                    fcurrency_num,
                    fitem_category,
                    fitem_id,
                    fitem_num,
                    fbalance,
                    fgrade,
                    fgameparty_pname,
                    nvl(fgameparty_subname,'其他') fgameparty_subname,
                    fgameparty_anto,
                    fbankrupt,
                    nvl(fpay_scene, '其他') fpay_scene,
                    b.fip,
                    fplatform_order_id,
                    fb_order_id,
                    ffee,
                    nvl(pcd.pmodename, concat(a.sid, '_', a.pmode)) fpm_name,
                    round(frate * fcoins_num, 4) fincome
              from stage.pay_order_info_stg a
              join analysis.paycenter_apps_dim p
                on a.appid = p.appid
               and a.sid = p.sid
              left join analysis.paycenter_chanel_dim pcd
                on a.sid = pcd.sid
               and a.pmode = pcd.pmode
              left join stage.user_generate_order_stg b
                on a.pid = b.forder_id
               and b.dt = '%(stat_date)s'
              left join stage.payment_stream_stg c
                on a.pid = c.forder_id
               and c.dt = '%(stat_date)s'
             where a.dt = '%(stat_date)s'

        """ % self.hql_dict
        hql_list.append( hql )

        res = self.exe_hql_list(hql_list)
        return res


if __name__ == "__main__":

    stat_date = ''
    if len(sys.argv) == 1:
        #没有输入参数的话，日期默认取昨天
        stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    else:
        #从输入参数取，实际是第几个就取第几个
        args = sys.argv[1].split(',')
        stat_date = args[0]

    #生成统计实例
    a = agg_user_payscene_proc(stat_date)
    a()
