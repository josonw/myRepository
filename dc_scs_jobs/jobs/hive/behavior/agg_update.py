# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_update(BaseStatModel):
    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

#         hql = """
# create table dcnew.game_activity_actret_20170810_bak as select * from dcnew.game_activity_actret where dt >= '2017-05-10';
#         """
#         res = self.sql_exe(hql)
#         if res != 0:
#             return res
#
#         hql = """
#     insert overwrite table dcnew.game_activity_actret partition (dt )
#     select
# fdate,
# case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
# case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
#      when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
#      when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
#      when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
# else fplatformfsk end fplatformfsk,
# case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
# fsubgamefsk,
# fterminaltypefsk,
# case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
# fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
# fchannelcode,
# fact_id,
# fusernum,
# f1daycnt,
# f2daycnt,
# f3daycnt,
# f4daycnt,
# f5daycnt,
# f6daycnt,
# f7daycnt,
# f14daycnt,
# f30daycnt,
# dt
# from dcnew.game_activity_actret where dt >='2017-05-10';
#         """
#         res = self.sql_exe(hql)
#         if res != 0:
#             return res
#
#         hql = """
#
#
# create table dcnew.game_activity_payret_20170810_bak as select * from dcnew.game_activity_payret where dt >= '2017-05-10';
#         """
#         res = self.sql_exe(hql)
#         if res != 0:
#             return res
#
        hql = """
insert overwrite table dcnew.game_activity_payret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
fact_id,
fusernum,
f1daycnt,
f2daycnt,
f3daycnt,
f4daycnt,
f5daycnt,
f6daycnt,
f7daycnt,
f14daycnt,
f30daycnt,
dt
from dcnew.game_activity_payret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

#         hql = """


# create table dcnew.pay_user_reg_pay_20170810_bak as select * from dcnew.pay_user_reg_pay where dt >= '2017-05-10';
#         """
#         res = self.sql_exe(hql)
#         if res != 0:
#             return res

        hql = """
insert overwrite table dcnew.pay_user_reg_pay partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
fpaydate,
fpayusernum,
fincome,
ffirstpayusernum,
ffirstincome,
dt
from dcnew.pay_user_reg_pay where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

#         hql = """


# create table dcnew.match_fplay_user_hall_actret_20170810_bak as select * from dcnew.match_fplay_user_hall_actret where dt >= '2017-05-10';
#         """
#         res = self.sql_exe(hql)
#         if res != 0:
#             return res

        hql = """
insert overwrite table dcnew.match_fplay_user_hall_actret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
fpname,
fsubname,
total_user,
f1daycnt,
f2daycnt,
f3daycnt,
f4daycnt,
f5daycnt,
f6daycnt,
f7daycnt,
f14daycnt,
f30daycnt,
dt
from dcnew.match_fplay_user_hall_actret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

#         hql = """


# create table dcnew.match_fplay_user_playret_20170810_bak as select * from dcnew.match_fplay_user_playret where dt >= '2017-05-10';
#         """
#         res = self.sql_exe(hql)
#         if res != 0:
#             return res

        hql = """
insert overwrite table dcnew.match_fplay_user_playret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
fpname,
fsubname,
total_user,
f1daycnt,
f2daycnt,
f3daycnt,
f4daycnt,
f5daycnt,
f6daycnt,
f7daycnt,
f14daycnt,
f30daycnt,
dt
from dcnew.match_fplay_user_playret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

#         hql = """


# create table dcnew.match_join_user_hall_actret_20170810_bak as select * from dcnew.match_join_user_hall_actret where dt >= '2017-05-10';
#         """
#         res = self.sql_exe(hql)
#         if res != 0:
#             return res

        hql = """
insert overwrite table dcnew.match_join_user_hall_actret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
fpname,
fsubname,
total_user,
f1daycnt,
f2daycnt,
f3daycnt,
f4daycnt,
f5daycnt,
f6daycnt,
f7daycnt,
f14daycnt,
f30daycnt,
dt
from dcnew.match_join_user_hall_actret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.match_join_user_joinret_20170810_bak as select * from dcnew.match_join_user_joinret where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.match_join_user_joinret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
fpname,
fsubname,
total_user,
f1daycnt,
f2daycnt,
f3daycnt,
f4daycnt,
f5daycnt,
f6daycnt,
f7daycnt,
f14daycnt,
f30daycnt,
dt
from dcnew.match_join_user_joinret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.match_user_hall_actret_20170810_bak as select * from dcnew.match_user_hall_actret where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.match_user_hall_actret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
fpname,
fsubname,
total_user,
f1daycnt,
f2daycnt,
f3daycnt,
f4daycnt,
f5daycnt,
f6daycnt,
f7daycnt,
f14daycnt,
f30daycnt,
dt
from dcnew.match_user_hall_actret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.match_user_playret_20170810_bak as select * from dcnew.match_user_playret where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.match_user_playret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
fpname,
fsubname,
total_user,
f1daycnt,
f2daycnt,
f3daycnt,
f4daycnt,
f5daycnt,
f6daycnt,
f7daycnt,
f14daycnt,
f30daycnt,
dt
from dcnew.match_user_playret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.act_user_actret_20170810_bak as select * from dcnew.act_user_actret where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.act_user_actret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
f1daycnt,
f2daycnt,
f3daycnt,
f4daycnt,
f5daycnt,
f6daycnt,
f7daycnt,
f14daycnt,
f30daycnt,
f60daycnt,
f90daycnt,
dt
from dcnew.act_user_actret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.act_user_actret_month_20170810_bak as select * from dcnew.act_user_actret_month where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.act_user_actret_month partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
fmonthaucnt,
f1monthcnt,
f2monthcnt,
f3monthcnt,
dt
from dcnew.act_user_actret_month where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.act_user_actret_week_20170810_bak as select * from dcnew.act_user_actret_week where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.act_user_actret_week partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
fweekaucnt,
f1weekcnt,
f2weekcnt,
f3weekcnt,
f4weekcnt,
f5weekcnt,
f6weekcnt,
f7weekcnt,
f8weekcnt,
dt
from dcnew.act_user_actret_week where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.reg_user_actret_week_20170810_bak as select * from dcnew.reg_user_actret_week where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.reg_user_actret_week partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
fweekregcnt,
f1weekcnt,
f2weekcnt,
f3weekcnt,
f4weekcnt,
f5weekcnt,
f6weekcnt,
f7weekcnt,
f8weekcnt,
dt
from dcnew.reg_user_actret_week where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.gameparty_subname_play_actret_20170810_bak as select * from dcnew.gameparty_subname_play_actret where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.gameparty_subname_play_actret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
fpname,
fsubname,
play_unum,
f1day_unum,
f2day_unum,
f3day_unum,
f4day_unum,
f5day_unum,
f6day_unum,
f7day_unum,
f14day_unum,
f30day_unum,
f60day_unum,
f90day_unum,
dt
from dcnew.gameparty_subname_play_actret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.paid_user_actret_20170810_bak as select * from dcnew.paid_user_actret where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.paid_user_actret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
f1daycnt,
f2daycnt,
f3daycnt,
f4daycnt,
f5daycnt,
f6daycnt,
f7daycnt,
f14daycnt,
f30daycnt,
dt
from dcnew.paid_user_actret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.pay_user_actret_20170810_bak as select * from dcnew.pay_user_actret where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.pay_user_actret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
f1daycnt,
f2daycnt,
f3daycnt,
f4daycnt,
f5daycnt,
f6daycnt,
f7daycnt,
f14daycnt,
f30daycnt,
dt
from dcnew.pay_user_actret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.pay_user_payret_20170810_bak as select * from dcnew.pay_user_payret where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.pay_user_payret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
f1daycnt,
f2daycnt,
f3daycnt,
f4daycnt,
f5daycnt,
f6daycnt,
f7daycnt,
f14daycnt,
f30daycnt,
dt
from dcnew.pay_user_payret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.play_user_actret_20170810_bak as select * from dcnew.play_user_actret where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.play_user_actret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
f1daycnt,
f2daycnt,
f3daycnt,
f4daycnt,
f5daycnt,
f6daycnt,
f7daycnt,
f14daycnt,
f30daycnt,
f60daycnt,
f90daycnt,
dt
from dcnew.play_user_actret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.reflux_user_actret_20170810_bak as select * from dcnew.reflux_user_actret where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.reflux_user_actret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
f1daycnt,
f2daycnt,
f3daycnt,
f4daycnt,
f5daycnt,
f6daycnt,
f7daycnt,
f14daycnt,
f30daycnt,
dt
from dcnew.reflux_user_actret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.reg_user_actret_20170810_bak as select * from dcnew.reg_user_actret where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.reg_user_actret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
f1daycnt,
f2daycnt,
f3daycnt,
f4daycnt,
f5daycnt,
f6daycnt,
f7daycnt,
f14daycnt,
f30daycnt,
f60daycnt,
f90daycnt,
dt
from dcnew.reg_user_actret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.reg_user_actret_partynum_20170810_bak as select * from dcnew.reg_user_actret_partynum where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.reg_user_actret_partynum partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
fptynumrange,
frpucnt,
fd1ucnt,
fd2ucnt,
fd3ucnt,
fd4ucnt,
fd5ucnt,
fd6ucnt,
fd7ucnt,
fd14ucnt,
fd30ucnt,
dt
from dcnew.reg_user_actret_partynum where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.reg_user_period_actret_20170810_bak as select * from dcnew.reg_user_period_actret where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.reg_user_period_actret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
f7daycnt,
f14daycnt,
f30daycnt,
dt
from dcnew.reg_user_period_actret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.gameparty_pname_play_user_actret_20170810_bak as select * from dcnew.gameparty_pname_play_user_actret where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.gameparty_pname_play_user_actret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
fpname,
total_user,
f1daycnt,
f2daycnt,
f3daycnt,
f4daycnt,
f5daycnt,
f6daycnt,
f7daycnt,
f14daycnt,
f30daycnt,
f60daycnt,
f90daycnt,
dt
from dcnew.gameparty_pname_play_user_actret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.gameparty_pname_reg_user_actret_20170810_bak as select * from dcnew.gameparty_pname_reg_user_actret where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.gameparty_pname_reg_user_actret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
fpname,
total_user,
f1daycnt,
f2daycnt,
f3daycnt,
f4daycnt,
f5daycnt,
f6daycnt,
f7daycnt,
f14daycnt,
f30daycnt,
f60daycnt,
f90daycnt,
dt
from dcnew.gameparty_pname_reg_user_actret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.gameparty_pname_play_user_playret_20170810_bak as select * from dcnew.gameparty_pname_play_user_playret where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.gameparty_pname_play_user_playret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
fpname,
total_user,
f1daycnt,
f2daycnt,
f3daycnt,
f4daycnt,
f5daycnt,
f6daycnt,
f7daycnt,
f14daycnt,
f30daycnt,
f60daycnt,
f90daycnt,
dt
from dcnew.gameparty_pname_play_user_playret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """


create table dcnew.gameparty_pname_reg_user_playret_20170810_bak as select * from dcnew.gameparty_pname_reg_user_playret where dt >= '2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
insert overwrite table dcnew.gameparty_pname_reg_user_playret partition (dt )
    select
fdate,
case when fgamefsk in (70000071,70000062,70000025,70000070) then 4542620389 else fgamefsk end fgamefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000131 then 4619040442
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 77000087 then 4619040443
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk in (4619040395,60784376) then 4619040444
     when fgamefsk in (70000071,70000062,70000025,70000070) and fplatformfsk = 4529621866 then 4619040445
else fplatformfsk end fplatformfsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fhallfsk <> -21379 then fgamefsk else fhallfsk end fhallfsk,
fsubgamefsk,
fterminaltypefsk,
case when fgamefsk in (70000071,70000062,70000025,70000070) and fversionfsk <> -21379 then
fgamefsk+fplatformfsk+fhallfsk*1000 +fversionfsk+fterminaltypefsk else fversionfsk end fversionfsk,
fchannelcode,
fpname,
total_user,
f1daycnt,
f2daycnt,
f3daycnt,
f4daycnt,
f5daycnt,
f6daycnt,
f7daycnt,
f14daycnt,
f30daycnt,
f60daycnt,
f90daycnt,
dt
from dcnew.gameparty_pname_reg_user_playret where dt >='2017-05-10';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


# 生成统计实例
a = agg_update(sys.argv[1:])
a()
