#!/usr/local/python272/bin/python
#-*- coding: UTF-8 -*-

date_field_liset = ['fdate','flts_at', 'fclick_at',  'fcreate_at', 'ffeed_at', 'ffirst_pay_at',
        'fgrade_at', 'flogin_at', 'flogout_at', 'fls_at', 'forder_at', 'fpay_at', 'fquit_at',
        'frupt_at', 'fsignup_at', 'fspark_at', 'fvip_at', 'lts_at', 'pstarttime','fchange_time',
        'fsignup_month','forder_date']


special_ora_fields = {
    'props_finace_fct':" fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fdirection, ftype, fnum, fusernum, ctype, actid, fpname, fsubname ",
    'ddz_jinbi_fcnt':" fdate,fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,fcnt,flft,lrgt,fregcnt,fbrcnt,fpaycnt",
    'audit_result_report':'fdate,fgamefsk,fplatformfsk,ftype,fsolve_at,flog_num,flog_error_num,fdetail,fcomment,fstatus,f1validation,f1validation_at,f2validation,f2validation_at,fsystem_fault'
    }

special_pg_fields = {'audit_result_report':'fdate,fgamefsk,fplatformfsk,ftype,fsolve_at,flog_num,flog_error_num,fdetail,fcomment,fstatus,f1validation,f1validation_at,f2validation,f2validation_at,fsystem_fault'
    }


# 多少行数据才执行插入
ITER_BUF_SIZE = 3000
# 同时运行多少同步任务
TASK_BUF_SIZE = 5

# second
DAEMON_IDLE_SLEEP = 5*60