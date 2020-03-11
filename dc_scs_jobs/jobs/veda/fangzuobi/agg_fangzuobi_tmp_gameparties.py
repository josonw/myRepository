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


class agg_fangzuobi_tmp_gameparties(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """
insert overwrite table veda.dfqp_user_gameparty_relation
partition(dt='%(statdate)s')
select x.fpname, x.fsubname, x.ftbl_id, x.finning_id, x.fuid loser, x.fbpid floser_bpid, x.fgamecoins lostcoins, y.fuid winner, y.fbpid fwinner_bpid, y.fgamecoins wincoins, x. fs_timer, x.fe_timer from
(select m.fbpid, fuid, ftbl_id, finning_id, fgamecoins, fsubname, fpname, fs_timer, fe_timer from stage.user_gameparty_stg m left join dim.bpid_map n on m.fbpid = n.fbpid where n.fgamename = '地方棋牌' and m.dt = '%(statdate)s' and m.fgamecoins < 0) x cross join
(select o.fbpid, fuid, ftbl_id, finning_id, fgamecoins from stage.user_gameparty_stg o left join dim.bpid_map p on o.fbpid = p.fbpid where p.fgamename = '地方棋牌' and o.dt = '%(statdate)s' and o.fgamecoins > 0) y
on x.ftbl_id = y.ftbl_id and x.finning_id = y.finning_id;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fangzuobi_tmp_gameparties(sys.argv[1:])
a()
