#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date, BasePGCluster

"""留存组合数据处理"""
class agg_update(BasePGCluster):

    def stat(self):

        sql = """
update dcnew.gameparty_hour_dis set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.gameparty_hour_dis set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.gameparty_hour_dis set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.gameparty_hour_dis set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.gameparty_hour_dis set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.gameparty_hour_dis set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.gameparty_hour_dis set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;

create table dcnew.gameparty_pname_play_user_actret_20170810_bak as select * from dcnew.gameparty_pname_play_user_actret;
update dcnew.gameparty_pname_play_user_actret set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.gameparty_pname_play_user_actret set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.gameparty_pname_play_user_actret set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.gameparty_pname_play_user_actret set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.gameparty_pname_play_user_actret set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.gameparty_pname_play_user_actret set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.gameparty_pname_play_user_actret set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;

create table dcnew.gameparty_pname_play_user_playret_20170810_bak as select * from dcnew.gameparty_pname_play_user_playret;
update dcnew.gameparty_pname_play_user_playret set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.gameparty_pname_play_user_playret set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.gameparty_pname_play_user_playret set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.gameparty_pname_play_user_playret set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.gameparty_pname_play_user_playret set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.gameparty_pname_play_user_playret set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.gameparty_pname_play_user_playret set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;

create table dcnew.gameparty_pname_property_dis_20170810_bak as select * from dcnew.gameparty_pname_property_dis;
update dcnew.gameparty_pname_property_dis set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.gameparty_pname_property_dis set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.gameparty_pname_property_dis set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.gameparty_pname_property_dis set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.gameparty_pname_property_dis set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.gameparty_pname_property_dis set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.gameparty_pname_property_dis set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;

create table dcnew.gameparty_pname_reg_user_actret_20170810_bak as select * from dcnew.gameparty_pname_reg_user_actret;
update dcnew.gameparty_pname_reg_user_actret set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.gameparty_pname_reg_user_actret set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.gameparty_pname_reg_user_actret set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.gameparty_pname_reg_user_actret set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.gameparty_pname_reg_user_actret set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.gameparty_pname_reg_user_actret set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.gameparty_pname_reg_user_actret set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;

create table dcnew.gameparty_pname_reg_user_playret_20170810_bak as select * from dcnew.gameparty_pname_reg_user_playret;
update dcnew.gameparty_pname_reg_user_playret set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.gameparty_pname_reg_user_playret set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.gameparty_pname_reg_user_playret set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.gameparty_pname_reg_user_playret set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.gameparty_pname_reg_user_playret set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.gameparty_pname_reg_user_playret set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.gameparty_pname_reg_user_playret set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;

create table dcnew.gameparty_pname_total_20170810_bak as select * from dcnew.gameparty_pname_total;
update dcnew.gameparty_pname_total set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.gameparty_pname_total set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.gameparty_pname_total set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.gameparty_pname_total set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.gameparty_pname_total set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.gameparty_pname_total set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.gameparty_pname_total set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;

create table dcnew.gameparty_settlement_20170810_bak as select * from dcnew.gameparty_settlement;
update dcnew.gameparty_settlement set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.gameparty_settlement set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.gameparty_settlement set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.gameparty_settlement set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.gameparty_settlement set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.gameparty_settlement set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.gameparty_settlement set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;

create table dcnew.gameparty_subname_play_actret_20170810_bak as select * from dcnew.gameparty_subname_play_actret;
update dcnew.gameparty_subname_play_actret set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.gameparty_subname_play_actret set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.gameparty_subname_play_actret set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.gameparty_subname_play_actret set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.gameparty_subname_play_actret set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.gameparty_subname_play_actret set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.gameparty_subname_play_actret set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;

create table dcnew.gameparty_total_20170810_bak as select * from dcnew.gameparty_total;
update dcnew.gameparty_total set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.gameparty_total set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.gameparty_total set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.gameparty_total set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.gameparty_total set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.gameparty_total set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.gameparty_total set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;

create table dcnew.gameparty_user_partynum_dis_20170810_bak as select * from dcnew.gameparty_user_partynum_dis;
update dcnew.gameparty_user_partynum_dis set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.gameparty_user_partynum_dis set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.gameparty_user_partynum_dis set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.gameparty_user_partynum_dis set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.gameparty_user_partynum_dis set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.gameparty_user_partynum_dis set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.gameparty_user_partynum_dis set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;

create table dcnew.loss_user_level_dis_20170810_bak as select * from dcnew.loss_user_level_dis;
update dcnew.loss_user_level_dis set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.loss_user_level_dis set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.loss_user_level_dis set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.loss_user_level_dis set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.loss_user_level_dis set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.loss_user_level_dis set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.loss_user_level_dis set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;

create table dcnew.loss_user_reflux_age_dis_20170810_bak as select * from dcnew.loss_user_reflux_age_dis;
update dcnew.loss_user_reflux_age_dis set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.loss_user_reflux_age_dis set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.loss_user_reflux_age_dis set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.loss_user_reflux_age_dis set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.loss_user_reflux_age_dis set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.loss_user_reflux_age_dis set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.loss_user_reflux_age_dis set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;

create table dcnew.loss_user_reflux_gameparty_dis_20170810_bak as select * from dcnew.loss_user_reflux_gameparty_dis;
update dcnew.loss_user_reflux_gameparty_dis set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.loss_user_reflux_gameparty_dis set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.loss_user_reflux_gameparty_dis set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.loss_user_reflux_gameparty_dis set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.loss_user_reflux_gameparty_dis set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.loss_user_reflux_gameparty_dis set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.loss_user_reflux_gameparty_dis set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;

create table dcnew.loss_user_reflux_level_dis_20170810_bak as select * from dcnew.loss_user_reflux_level_dis;
update dcnew.loss_user_reflux_level_dis set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.loss_user_reflux_level_dis set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.loss_user_reflux_level_dis set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.loss_user_reflux_level_dis set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.loss_user_reflux_level_dis set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.loss_user_reflux_level_dis set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.loss_user_reflux_level_dis set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;

create table dcnew.user_playhabit_20170810_bak as select * from dcnew.user_playhabit;
update dcnew.user_playhabit set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.user_playhabit set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.user_playhabit set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.user_playhabit set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.user_playhabit set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.user_playhabit set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.user_playhabit set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;



create table dcnew.event_20170810_bak as select * from dcnew.event;
update dcnew.event set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.event set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.event set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.event set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.event set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.event set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.event set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;

create table dcnew.event_args_20170810_bak as select * from dcnew.event_args;
update dcnew.event_args set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.event_args set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.event_args set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.event_args set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.event_args set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.event_args set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.event_args set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;

create table dcnew.event_args_new_20170810_bak as select * from dcnew.event_args_new;
update dcnew.event_args_new set fversionfsk = fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 ;
update dcnew.event_args_new set fhallfsk = fgamefsk where fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379;
update dcnew.event_args_new set fgamefsk = 4542620389 where fgamefsk in (70000071,70000062,70000025,70000070);
update dcnew.event_args_new set fplatformfsk = 4619040442 where fgamefsk = 4542620389 and fplatformfsk = 77000131;
update dcnew.event_args_new set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.event_args_new set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.event_args_new set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;


update dcnew.gameparty_hour_dis_detail set fplatformfsk = 4619040443 where fgamefsk = 4542620389 and fplatformfsk = 77000087;
update dcnew.gameparty_hour_dis_detail set fplatformfsk = 4619040444 where fgamefsk = 4542620389 and fplatformfsk in (4619040395,60784376);
update dcnew.gameparty_hour_dis_detail set fplatformfsk = 4619040445 where fgamefsk = 4542620389 and fplatformfsk = 4529621866;
        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)


if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_update(stat_date)
    a()
