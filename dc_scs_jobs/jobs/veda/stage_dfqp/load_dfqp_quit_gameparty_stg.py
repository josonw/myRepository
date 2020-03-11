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


class load_dfqp_quit_gameparty_stg(BaseStatModel):

    # ���ط����Ƶ�������ˮ�����������
    def create_tab(self):
        hql = """
        create table if not exists stage_dfqp.quit_gameparty_stg
        ( 	fbpid					varchar(50) 	comment 'BPID',
			fuid                    bigint          comment '�û�UID',
			flts_at                 string          comment '����ʱ��',
			fpname                  varchar(100)    comment '����������',
			fsubname                varchar(100)    comment '�����������ƣ��������䣩',
			fgsubname               varchar(100)    comment '�����������ƣ��������£�',
			fmatch_id               varchar(100)    comment '����id',
			fcause                  int             comment '����ԭ��',
			fgame_id                int             comment '����ϷID',
			fchannel_code           varchar(100)    comment '��������ID',
			fround_num              int             comment '����',
			fgame_num               int             comment '����',
			fpart_rank              int             comment '��������',
			fintegral_balance       int             comment '����ʱ�Ļ��ֽ���',
			fintegral_balance_ext   string          comment '����ʱ������ҵĻ��ֽ��࣬��ʽ�����1��mid�����ֽ��ࣻ���2��mid�����ֽ���',
			fmatch_cfg_id           int             comment '��������id',
			fmatch_log_id           int             comment '������־id',
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
        ) comment '�ط������˳�������ˮ'
        partitioned by(dt string comment '����')
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):

        hql = """
        insert overwrite table stage_dfqp.quit_gameparty_stg
        partition(dt='%(statdate)s')
        select distinct t1.fbpid,
				case when tt.fplatformfsk in (77000221) and fuid < 1000000000 then fuid + 1000000000 else fuid end fuid,
				flts_at,
				fpname,
				fsubname,
				fgsubname,
				fmatch_id,
				fcause,
				fgame_id,
				fchannel_code,
				fround_num,
				fgame_num,
				fpart_rank,
				fintegral_balance,
				fintegral_balance_ext,
				fmatch_cfg_id,
				fmatch_log_id,
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
        from stage.quit_gameparty_stg t1
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
a = load_dfqp_quit_gameparty_stg(sys.argv[1:])
a()
