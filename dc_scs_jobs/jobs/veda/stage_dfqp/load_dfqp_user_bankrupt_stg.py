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


class load_dfqp_user_bankrupt_stg(BaseStatModel):

    # ���ط����Ƶ��û��Ʋ���ˮ�����������
    def create_tab(self):
        hql = """
        create table if not exists stage_dfqp.user_bankrupt_stg
        ( 	fbpid					varchar(50) 	comment 'BPID',
			fuid                    bigint          comment '�û�UID',
			frupt_at                string          comment '�Ʋ�ʱ��',
			fhas_relieve            bigint          comment '�Ƿ���ܾȼ�',
			fuser_grade             bigint          comment '�û��ȼ�',
			frelieve_game_coin      bigint          comment '�ȼ���Ϸ�Ҷ��',
			frelieve_cnt            int             comment '�ڼ������ȼ�',
			fuphill_pouring         bigint          comment '��ע/Сä',
			fplayground_title       varchar(100)    comment '�ƾֶ�������',
			fversion_info           varchar(50)     comment '�汾��',
			fchannel_code           varchar(100)    comment '��������ID',
			fvip_type               varchar(100)    comment '',
			fvip_level              int             comment '',
			flevel                  int             comment '',
			fpname                  varchar(100)    comment '�ƾ�һ������',
			fscene                  varchar(100)    comment '�Ʋ�����',
			fgame_id                int             comment '����ϷID',
			fgamefsk			bigint			comment '��ϷID',
			fgamename			string			comment '��Ϸ����',
			fplatformfsk		bigint			comment 'ƽ̨ID',
			fplatformname		string			comment 'ƽ̨����',
			fhallfsk			bigint			comment '����ID',
			fhallname			string			comment '��������',
			fterminaltypefsk	bigint			comment '�ն�ID',
			fterminaltypename	string			comment '�ն�����',
			fversionfsk			bigint			comment '�汾ID',
			fversionname		string			comment '�汾����'
        ) comment '�ط������û��Ʋ���ˮ'
        partitioned by(dt string comment '����')
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):

        hql = """
        insert overwrite table stage_dfqp.user_bankrupt_stg
        partition(dt='%(statdate)s')
        select distinct t1.fbpid,
				case when tt.fplatformfsk in (77000221) and fuid < 1000000000 then fuid + 1000000000 else fuid end fuid,
				frupt_at,
				fhas_relieve,
				fuser_grade,
				frelieve_game_coin,
				frelieve_cnt,
				fuphill_pouring,
				fplayground_title,
				fversion_info,
				fchannel_code,
				fvip_type,
				fvip_level,
				flevel,
				fpname,
				fscene,
				fgame_id,
				fgamefsk,
				fgamename,
				fplatformfsk,
				fplatformname,
				fhallfsk,
				fhallname,
				fterminaltypefsk,
				fterminaltypename,
				fversionfsk,
				fversionname
        from stage.user_bankrupt_stg t1
        join dim.bpid_map tt
          on t1.fbpid = tt.fbpid
         and fgamefsk = 4132314431
        where dt = '%(statdate)s'
        distribute by t1.fbpid, fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

# ����ͳ��ʵ��
a = load_dfqp_user_bankrupt_stg(sys.argv[1:])
a()
