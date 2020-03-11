#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

# 已经停止计算
class market_channel_operator_flow(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.user_gameparty_game_fct
        (
        fdate               date,
        fplatformfsk        bigint,
        fgamefsk            bigint,
        fversionfsk         bigint,
        fterminalfsk        bigint,
        fusernum            bigint,
        fpartynum           bigint,
        fcharge             decimal(20,4),
        fplayusernum        bigint,
        fregplayusernum     bigint,
        factplayusernum     bigint,
        fpaypartynum        bigint,
        fpayusernum         bigint
        )
        partitioned by (dt date);

        create table if not exists analysis.user_retained_fct
        (
        fdate                date,
        fplatformfsk         bigint,
        fgamefsk             bigint,
        fversionfsk          bigint,
        fterminalfsk         bigint,
        fagefsk              bigint,
        foccupationalfsk     bigint,
        fsexfsk              bigint,
        fcityfsk             bigint,
        f1daycnt             bigint,
        f2daycnt             bigint,
        f3daycnt             bigint,
        f4daycnt             bigint,
        f5daycnt             bigint,
        f6daycnt             bigint,
        f7daycnt             bigint,
        f14daycnt            bigint,
        f30daycnt            bigint,
        f60daycnt            bigint
        )
        partitioned by (dt date);

        create table if not exists analysis.user_online_byday_agg
        (
        fdate               date,
        fplatformfsk        bigint,
        fgamefsk            bigint,
        fversionfsk         bigint,
        fterminalfsk        bigint,
        fmaxonline          bigint,
        favgonline          bigint,
        fminonline          bigint,
        fmaxplay            bigint,
        favgplay            bigint,
        fminplay            bigint,
        fmaxnoplay          bigint,
        favgnoplay          bigint,
        fminnoplay          bigint,
        fmaxhall            bigint,
        favghall            bigint,
        fminhall            bigint,
        fmaxroom            bigint,
        favgroom            bigint,
        fminroom            bigint,
        fmaxreg             bigint,
        fminreg             bigint,
        favgreg             bigint
        )
        partitioned by (dt date);

        create table if not exists analysis.user_reg_pay_fct
        (
        fdate               date,
        fplatformfsk        bigint,
        fgamefsk            bigint,
        fversionfsk         bigint,
        fterminalfsk        bigint,
        fpaydate            date,
        fpayusernum         bigint,
        fincome             decimal(20,2),
        ffirstpayusernum    bigint,
        ffirstincome        decimal(20,2)
        )
        partitioned by (dt date);


        create table if not exists analysis.channel_market_operator_flow
        (
          fdate        date,
          fgamefsk     bigint,
          fplatformfsk bigint,
          fversionfsk  bigint,
          fmobilename  varchar(64),
          fdsu         bigint,
          fdau         bigint,
          f1drucnt     bigint,
          f7drucnt     bigint,
          f30drucnt    bigint,
          fpartynum    bigint,
          fplayusernum bigint,
          fdaou        bigint,
          f2drucnt     bigint,
          f3drucnt     bigint,
          f4drucnt     bigint,
          f5drucnt     bigint,
          f6drucnt     bigint,
          f14drucnt    bigint,
          fdspu        bigint,
          fspun        bigint,
          freg_income  decimal(20,2)
        )partitioned by (dt date);

        """

        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        # hql结尾不要带分号';'
        hql_list = []


        hql = """
        use analysis;
        alter table channel_market_operator_flow drop partition(dt = "%(ld_begin)s")
        """ % self.hql_dict
        hql_list.append(hql)


        for operate_name in ['移动MM', '移动基地', '联通沃商店',  '电信爱游戏', '电信天翼']:
            self.hql_dict['operate_name'] = operate_name

            hql = """
           insert into table analysis.channel_market_operator_flow partition(dt = "%(ld_begin)s")
            -- dsu
           select a.fdate, b.fgamefsk, b.fplatformfsk,b.fversion_old fversionfsk, '%(operate_name)s' fmobilename,
            sum(a.fregcnt) fdsu,
            null fdau,
            null f1drucnt,
            null f7drucnt,
            null f30drucnt,
            null fpartynum,
            null fplayusernum,
            null fdaou,
            null f2drucnt,
            null f3drucnt,
            null f4drucnt,
            null f5drucnt,
            null f6drucnt,
            null f14drucnt,
            null fdspu,
            null fspun,
            null freg_income
            from analysis.user_register_fct a
            join dim.bpid_map b
              on a.fgamefsk = b.fgamefsk
             and a.fplatformfsk = b.fplatformfsk
             and a.fversionfsk = b.fversion_old
           where a.dt = '%(ld_begin)s'
             and b.fversionname like concat('%%','%(operate_name)s','%%')
           group by a.fdate, b.fgamefsk, b.fplatformfsk,b.fversion_old
           """ % self.hql_dict
            hql_list.append( hql )

            hql = """
            -- dau
            insert into table analysis.channel_market_operator_flow partition(dt = "%(ld_begin)s")
            select a.fdate, b.fgamefsk, b.fplatformfsk,b.fversion_old fversionfsk, '%(operate_name)s' fmobilename,
            null fdsu,
            sum(a.factcnt) fdau,
            null f1drucnt,
            null f7drucnt,
            null f30drucnt,
            null fpartynum,
            null fplayusernum,
            null fdaou,
            null f2drucnt,
            null f3drucnt,
            null f4drucnt,
            null f5drucnt,
            null f6drucnt,
            null f14drucnt,
            null fdspu,
            null fspun,
            null freg_income
              from analysis.user_true_active_fct a
              join dim.bpid_map b
                on a.fgamefsk = b.fgamefsk
               and a.fplatformfsk = b.fplatformfsk
               and a.fversionfsk = b.fversion_old
             where a.dt = '%(ld_begin)s'
               and b.fversionname like concat('%%','%(operate_name)s','%%')
             group by a.fdate, b.fgamefsk, b.fplatformfsk,b.fversion_old
             """ % self.hql_dict
            hql_list.append( hql )

            hql = """
            -- 留存
            insert into table analysis.channel_market_operator_flow partition(dt = "%(ld_begin)s")
            select a.fdate, b.fgamefsk, b.fplatformfsk,b.fversion_old fversionfsk, '%(operate_name)s' fmobilename,
            null fdsu,
            null fdau,
            sum(a.f1daycnt) f1drucnt,
            sum(a.f7daycnt) f7drucnt,
            sum(a.f30daycnt) f30drucnt,
            null fpartynum,
            null fplayusernum,
            null fdaou,
            sum(a.f2daycnt) f2drucnt,
            sum(a.f3daycnt) f3drucnt,
            sum(a.f4daycnt) f4drucnt,
            sum(a.f5daycnt) f5drucnt,
            sum(a.f6daycnt) f6drucnt,
            sum(a.f14daycnt) f14drucnt,
            null fdspu,
            null fspun,
            null freg_income
            from analysis.user_retained_fct a
            join dim.bpid_map b
              on a.fgamefsk = b.fgamefsk
             and a.fplatformfsk = b.fplatformfsk
             and a.fversionfsk = b.fversion_old
           where a.dt = date_add('%(ld_begin)s', - 31)
             and b.fversionname like concat('%%','%(operate_name)s','%%')
           group by a.fdate, b.fgamefsk, b.fplatformfsk,b.fversion_old
           """ % self.hql_dict
            hql_list.append( hql )

            hql = """
            -- 玩牌
            insert into table analysis.channel_market_operator_flow partition(dt = "%(ld_begin)s")
            select a.fdate, b.fgamefsk, b.fplatformfsk,b.fversion_old fversionfsk, '%(operate_name)s' fmobilename,
            null fdsu,
            null fdau,
            null f1drucnt,
            null f7drucnt,
            null f30drucnt,
            sum(a.fpartynum) fpartynum,
            sum(a.fplayusernum) fplayusernum,
            null fdaou,
            null f2drucnt,
            null f3drucnt,
            null f4drucnt,
            null f5drucnt,
            null f6drucnt,
            null f14drucnt,
            null fdspu,
            null fspun,
            null freg_income
            from analysis.user_gameparty_game_fct a
            join dim.bpid_map b
              on a.fgamefsk = b.fgamefsk
             and a.fplatformfsk = b.fplatformfsk
             and a.fversionfsk = b.fversion_old
          where a.dt = '%(ld_begin)s'
             and b.fversionname like concat('%%','%(operate_name)s','%%')
           group by a.fdate, b.fgamefsk, b.fplatformfsk,b.fversion_old
           """ % self.hql_dict
            hql_list.append( hql )

            hql = """
            -- 平均在线人数 每分钟
            insert into table analysis.channel_market_operator_flow partition(dt = "%(ld_begin)s")
            select a.fdate, b.fgamefsk, b.fplatformfsk,b.fversion_old fversionfsk, '%(operate_name)s' fmobilename,
            null fdsu,
            null fdau,
            null f1drucnt,
            null f7drucnt,
            null f30drucnt,
            null fpartynum,
            null fplayusernum,
            sum(a.favgonline) fdaou,
            null f2drucnt,
            null f3drucnt,
            null f4drucnt,
            null f5drucnt,
            null f6drucnt,
            null f14drucnt,
            null fdspu,
            null fspun,
            null freg_income
            from analysis.user_online_byday_agg a
            join dim.bpid_map b
              on a.fgamefsk = b.fgamefsk
             and a.fplatformfsk = b.fplatformfsk
             and a.fversionfsk = b.fversion_old
           where a.dt = '%(ld_begin)s'
             and b.fversionname like concat('%%','%(operate_name)s','%%')
           group by a.fdate, b.fgamefsk, b.fplatformfsk,b.fversion_old
           """ % self.hql_dict
            # 已经不通过hive计算实时在线在玩
            # hql_list.append( hql )

            hql = """
           -- 当天注册并付费用户数
           insert into table analysis.channel_market_operator_flow partition(dt = "%(ld_begin)s")
            select a.fdate, b.fgamefsk, b.fplatformfsk,b.fversion_old fversionfsk, '%(operate_name)s' fmobilename,
            null fdsu,
            null fdau,
            null f1drucnt,
            null f7drucnt,
            null f30drucnt,
            null fpartynum,
            null fplayusernum,
            null fdaou,
            null f2drucnt,
            null f3drucnt,
            null f4drucnt,
            null f5drucnt,
            null f6drucnt,
            null f14drucnt,
            sum(a.fpayusernum) fdspu,
            null fspun,
            sum(a.fincome) freg_income
            from analysis.user_reg_pay_fct a
            join dim.bpid_map b
              on a.fgamefsk = b.fgamefsk
             and a.fplatformfsk = b.fplatformfsk
             and a.fversionfsk = b.fversion_old
           where a.dt = '%(ld_begin)s'
             and b.fversionname like concat('%%','%(operate_name)s','%%')
           group by a.fdate, b.fgamefsk, b.fplatformfsk,b.fversion_old
           """ % self.hql_dict
            hql_list.append( hql )

            hql = """
           -- 当天注册并玩牌的用户数
           insert into table analysis.channel_market_operator_flow partition(dt = "%(ld_begin)s")
            select a.fdate, b.fgamefsk, b.fplatformfsk,b.fversion_old fversionfsk, '%(operate_name)s' fmobilename,
            null fdsu,
            null fdau,
            null f1drucnt,
            null f7drucnt,
            null f30drucnt,
            null fpartynum,
            null fplayusernum,
            null fdaou,
            null f2drucnt,
            null f3drucnt,
            null f4drucnt,
            null f5drucnt,
            null f6drucnt,
            null f14drucnt,
            null fdspu,
            sum(a.fregplayusernum) fspun,
            null freg_income
            from analysis.user_gameparty_game_fct a
            join dim.bpid_map b
              on a.fgamefsk = b.fgamefsk
             and a.fplatformfsk = b.fplatformfsk
             and a.fversionfsk = b.fversion_old
           where a.dt = '%(ld_begin)s'
             and b.fversionname like concat('%%','%(operate_name)s','%%')
           group by a.fdate, b.fgamefsk, b.fplatformfsk,b.fversion_old """ % self.hql_dict

            hql_list.append( hql )



        hql = """
        insert overwrite table analysis.channel_market_operator_flow partition(dt = "%(ld_begin)s")
        select fdate, fgamefsk, fplatformfsk, fversionfsk, fmobilename,
        sum(fdsu)       fdsu,
        sum(fdau)       fdau,
        sum(f1drucnt)   f1drucnt,
        sum(f7drucnt)   f7drucnt,
        sum(f30drucnt)   f30drucnt,
        sum(fpartynum)   fpartynum,
        sum(fplayusernum)   fplayusernum,
        sum(fdaou)      fdaou,
        sum(f2drucnt)   f2drucnt,
        sum(f3drucnt)   f3drucnt,
        sum(f4drucnt)   f4drucnt,
        sum(f5drucnt)   f5drucnt,
        sum(f6drucnt)   f6drucnt,
        sum(f14drucnt)   f14drucnt,
        sum(fdspu)      fdspu,
        sum(fspun)      fspun,
        sum(freg_income)   freg_income
        from analysis.channel_market_operator_flow
        where dt = "%(ld_begin)s"
        group by
            fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            fmobilename
        """ % self.hql_dict
        hql_list.append( hql )


        result = self.exe_hql_list(hql_list)
        return result



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
    a = market_channel_operator_flow(stat_date)
    a()
