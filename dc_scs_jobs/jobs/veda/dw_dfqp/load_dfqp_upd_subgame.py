#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime

from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class load_dfqp_upd_subgame(BaseStatModel):

    # 地方棋牌的牌局流水中抽取子游戏信息，放进增量表
    def create_tab(self):
        hql = """
        insert overwrite table dw_dfqp.upd_subgame
		partition(dt='%(statdate)s')
		select 
			distinct fsubname, fgame_id, fpname 
		from 
			stage_dfqp.user_gameparty_stg 
		where 
			dt = '%(statdate)s' and nvl(fgame_id, 0) <> 0 and nvl(fmatch_id, '0') = '0' 
				and fsubname not in (select subgame_room_id from dw_dfqp.upd_subgame where dt < '%(statdate)s');
				
		insert overwrite table dw_dfqp.dim_subgame
		select 
			m.subgame_room_id, 
			concat(subgame_desc, o.subgame_room_desc) subgame_room_desc, 
			m.subgame_id, 
			m.subgame_desc, 
			o.subgame_roomtype_id, 
			o.subgame_roomtype_desc, 
			m.room_type, 
			o.subgame_room_desc, 
			p.coinstype_id, 
			p.coinstype_desc, 
			nvl(n.is_gold, 0) subgame_type_id, 
			case when nvl(n.is_gold, 0) = 0 then '非金流' else '金流' end subgame_type_desc
		from 
		(select 
			subgame_room_id, 
			subgame_id, 
			subgame_desc, 
			case when length(split(subgame_room_id, '_')[1]) = 5 then substring(split(subgame_room_id, '_')[1], 1, 1) else 0 end coins_type, 
			case when length(split(subgame_room_id, '_')[1]) = 5 and substring(split(subgame_room_id, '_')[1], 3, 1) = '0' then substring(split(subgame_room_id, '_')[1], 4, 2)
			     when length(split(subgame_room_id, '_')[1]) = 4 and substring(split(subgame_room_id, '_')[1], 2, 1) = '0' then substring(split(subgame_room_id, '_')[1], 3, 2)
			     when length(split(subgame_room_id, '_')[1]) = 5 and substring(split(subgame_room_id, '_')[1], 3, 1) <> '0' then substring(split(subgame_room_id, '_')[1], 3, 3)
			     when length(split(subgame_room_id, '_')[1]) = 4 and substring(split(subgame_room_id, '_')[1], 2, 1) <> '0' then substring(split(subgame_room_id, '_')[1], 2, 3)
			     else split(subgame_room_id, '_')[1] end room_type
		from dw_dfqp.upd_subgame) m 
		left join 
		(select subgame_id, is_gold from dw_dfqp.conf_subgame_properties) n on m.subgame_id = n.subgame_id
		left join 
		(select subgame_room_id, subgame_room_desc, subgame_roomtype_id, subgame_roomtype_desc from dw_dfqp.conf_subgame_room_properties) o on m.room_type = o.subgame_room_id 
		left join 
		(select coinstype_id, coinstype_desc from dw_dfqp.conf_coinstype_properties) p on m.coins_type = p.coinstype_id;
		
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

# 生成统计实例
a = load_dfqp_upd_subgame(sys.argv[1:])
a()
