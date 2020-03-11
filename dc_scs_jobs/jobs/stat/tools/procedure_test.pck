
  /*
  用户金流数据中间表，存放2次计算后的结果集，
  用户各项行为的 数额，次数
  */
  procedure load_user_gamecoins_stream_mid(pd_date date default trunc(sysdate - 1))
  is
    ld_date date;
    ld_end date;

  begin
    ld_date := trunc(pd_date);
    ld_end := ld_date+1;

    pkg_manage_tools.add_common_log(ps_name => 'pkg_gamecoin',
                        ps_subname => 'load_user_gamecoins_sid',
                        ps_operation => 'begin',
                        ps_memo => to_char(ld_date));

    execute immediate 'truncate table stage.user_gamecoins_stream_mid';
    commit;

    insert into /*+ append */ stage.user_gamecoins_stream_mid
    select fbpid,
           a.fuid,
           a.act_type,
           a.act_id,
           sum(abs(a.act_num)),
           count(1)
      from stage.pb_gamecoins_stream_stg a
     where a.lts_at >= ld_date
       and a.lts_at < ld_end
     group by fbpid, a.fuid, a.act_type, a.act_id;

     commit;

     pkg_manage_tools.add_common_log(ps_name => 'pkg_gamn_info',
                        ps_subname => 'load_user_gamecoinam_mid',
                        ps_operation => 'end',
                        ps_memo => to_char(ld_date));

    exception when others then
    pkg_manage_tools.add_common_log(ps_name      => 'pkg_gameinfo',
                                    ps_subname   => 'load_user_ins_stream_mid',
                                    ps_operation => 'end',
                                    ps_err       => 'error');
  end;



  /* gamecoin detail */
 procedure agg_gamecoin_detail_data(pd_date date default trunc(sysdate - 1)) is
  ld_begin date;
  ld_end   date;
begin
  ld_begin := trunc(pd_date);
  ld_end   := ld_begin + 1;

  pkg_manage_tools.add_common_log(ps_name => 'pkg_gamecoin_info',
                                    ps_subname => 'agg_gamecoin_detail_data',
                                    ps_operation => 'begin',
                                    ps_memo => to_char(ld_begin,'yyyy-mm-dd hh24:mi:ss') || '--' || to_char(ld_end,'yyyy-mm-dd hh24:mi:ss'));

  delete from analysis.pay_game_coin_finace_fct ff
   where ff.fdate = ld_begin;
  commit;

  execute immediate 'truncate table stage.gamecoins_tmp';

  insert into gamecoins_tmp
    select fbpid, fuid, act_id, act_type, sum(abs(act_num)) act_num，count(fuid) fcnt
      from pb_gamecoins_stream_stg
     where lts_at >= ld_begin
       and lts_at < ld_end
       and act_type in (1, 2)
     group by fbpid, fuid, act_id, act_type;

  insert into analysis.pay_game_coin_finace_fct
    (fdate,
     fplatformfsk,
     fgamefsk,
     fversionfsk,
     fterminalfsk,
     fcointype,
     fdirection,
     ftype,
     fnum,
     fusernum,
     fcnt,
     fpaynum,
     fpayusernum,
     fpaidnum,
     fpaidusernum
     )
    select /*+ leading(bpm) */
     ld_begin fdate,
     fplatformfsk,
     fgamefsk,
     fversionfsk,
     fterminalfsk,
     'GAMECOIN' as fcointype,
     decode(act_type, 1, 'IN', 2, 'OUT', 0) fdirection,
     ftype,
     fnum,
     fusernum,
     fcnt,
     fpaynum,
     fpayusernum,
     fpaidnum,
     fpaidusernum
      from (select /*+ no_merge */
               bpm.fplatformfsk,
               bpm.fgamefsk,
               bpm.fversionfsk,
               bpm.fterminalfsk,
               m.act_type,
               m.act_id ftype,
               sum(abs(act_num)) fnum,
               count(distinct m.fuid) fusernum,
               sum(fcnt) fcnt,
               sum(abs(case
                         when pay_user =1 then
                          act_num
                       end)) fpaynum,
               count(distinct case
                       when pay_user=1 then
                        m.fuid
                     end) fpayusernum,
               sum(abs(case
                         when paid_user =1 then
                          act_num
                       end)) fpaidnum,
               count(distinct case
                       when paid_user =1 then
                        m.fuid
                     end) fpaidusernum
                from gamecoins_tmp m
                left join (select fbpid,
                                 fuid,
                                 max(paid_user) paid_user,
                                 max(pay_user) pay_user
                            from (select fbpid, fuid, 1 paid_user, 0 pay_user
                                    from stage.active_paid_user_mid
                                   where fdate = ld_begin
                                  union all
                                  select fbpid, fuid, 0 paid_user, 1 pay_user
                                    from stage.user_pay_info
                                   where fpay_at = ld_begin)
                           group by fbpid, fuid) n
                  on m.fbpid = n.fbpid
                 and m.fuid = n.fuid
                join analysis.bpid_platform_game_ver_map bpm
                  on m.fbpid = bpm.fbpid
            group by bpm.fplatformfsk,
                     bpm.fgamefsk,
                     bpm.fversionfsk,
                     bpm.fterminalfsk,
                     m.act_type,
                     m.act_id
            );



  commit;

    pkg_manage_tools.add_common_log(ps_name      => 'pkg_gamecoin_info',
                                    ps_subname   => 'agg_gamecoin_detail_data',
                                    ps_operation => 'begin_merge_in');

    pkg_manage_tools.add_common_log(ps_name      => 'pkg_gamecoin_info',
                                    ps_subname   => 'agg_gamecoin_detail_data',
                                    ps_operation => 'dim');

  insert into analysis.GAME_COIN_TYPE_DIM ed
    (FSK, FGAMEFSK, FCOINTYPE, FDIRECTION, FTYPE, FNAME)
    select analysis.common_seq.nextval,
           FGAMEFSK,
           FCOINTYPE,
           FDIRECTION,
           FTYPE,
           FTYPE FNAME
      from (select distinct FGAMEFSK, FCOINTYPE, FDIRECTION, FTYPE
              from analysis.PAY_GAME_COIN_FINACE_FCT a
             where fdate = ld_begin) et
     where not exists (select 1
              from analysis.GAME_COIN_TYPE_DIM edi
             where edi.FGAMEFSK = et.FGAMEFSK
               and edi.FCOINTYPE = et.FCOINTYPE
               and edi.FDIRECTION = et.FDIRECTION
               and edi.FTYPE = et.FTYPE);
  commit;


     pkg_manage_tools.add_common_log(ps_name => 'pkg_gamecoin',
                                   ps_subname => 'agg_gamecoin_detail',
                                   ps_operation => 'end');

     exception when others then
     pkg_manage_tools.add_common_log(ps_name      => 'pkg_gamecoin_',
                                     ps_subname   => 'agg_gamecoin_detail',
                                     ps_operation => 'end',
                                     ps_err       => 'error');

end;

  /*
  用户结余中间表
  */
  procedure agg_gamecoin_day(pd_date date default trunc(sysdate - 1))
  is
    ld_date date;
    ld_begin date;
    ld_end date;
  begin

    ld_date := trunc(pd_date);
    ld_begin := ld_date;
    ld_end := ld_begin + 1;

    pkg_manage_tools.add_common_log(ps_name => 'pkg_gamecoin',
                                  ps_subname => 'agg_gamecoiny',
                                  ps_operation => 'begin');

    delete pb_gamecoins_stream_mid where fdate >= ld_begin and fdate<ld_end;
    commit;

    pkg_manage_tools.add_common_log(ps_name => 'pkg_gamecoin_info',
                              ps_subname => 'agg_gamecoin_day',
                              ps_operation => 'pb_gamecoins_stream_mid');

    insert /*+ append */ into  pb_gamecoins_stream_mid
      (fdate, fbpid, fuid, user_gamecoins_num)
      select fdate, fbpid, fuid, user_gamecoins_num
        from (select lts_at fdate,
                     fbpid,
                     fuid,
                     user_gamecoins_num,
                     -- row_number() over(partition by fbpid, fuid order by lts_at desc) rown
                     row_number() over(partition by fbpid, fuid order by lts_at desc, nvl(fseq_no,0) desc, user_gamecoins_num desc) rown
                from pb_gamecoins_stream_stg ss
               where ss.lts_at >= ld_begin
                 and ss.lts_at < ld_end )
       where rown = 1;
    commit;

    pkg_manage_tools.add_common_log(ps_name => 'pkg_gamecoinfo',
                              ps_subname => 'agg_gamecoinay',
                              ps_operation => 'data_analysis');

    -- 采集数据，有利于执行计划的执行
    DBMS_STATS.GATHER_TABLE_STATS(ownname=>'STAGE',tabname=>'PB_GAMECOINS_STREAM_MID',partname=>'PB_GAMECOINS_MID_'||to_char(ld_begin,'yyyymmdd') ,estimate_percent=>0.00001,block_sample=>true,degree=>16);


    pkg_manage_tools.add_common_log(ps_name => 'pkg_gamecoinfo',
                            ps_subname => 'agg_gamecoi',
                            ps_operation => 'end');

    exception when others then
    pkg_manage_tools.add_common_log(ps_name      => 'pkg_gamecoio',
                                    ps_subname   => 'agg_gameco',
                                    ps_operation => 'end',
                                    ps_err       => 'error');
  end;

  -- 用户保险箱结余中间表
  procedure agg_bank_final_mid_data(pd_date date default trunc(sysdate - 1))
    is
      ld_begin date;
      ld_end date;
      ld_beyond date;
    begin
      ld_begin := trunc(pd_date);
      ld_end := ld_begin + 1;
      ld_beyond := ld_begin - 1;

      pkg_manage_tools.add_common_log(ps_name => 'pkg_gamecoin',
                                    ps_subname => 'agg_bank_finta',
                                    ps_operation => 'begin',
                                    ps_memo => to_char(ld_begin,'yyyy-mm-dd hh24:mi:ss') || '--' || to_char(ld_end,'yyyy-mm-dd hh24:mi:ss'));

      delete from stage.USER_BANK_FINAL_MID  where  fdate = ld_begin;
      commit;

      --保险箱余额中间表
      if trunc(sysdate, 'mm') = ld_begin then --每月1号重新生成中间表
          execute immediate 'truncate table stage.USER_BANK_FINAL_MID_TOW';

          insert into stage.USER_BANK_FINAL_MID_TOW
          (FDATE, FBPID, FUID, FBANK_GAMECOINS_NUM)
          select ld_beyond fdate, fbpid, fuid, fbank_gamecoins_num
          from
          (
              select fbpid,
                   fuid,
                   fbank_gamecoins_num,
                   row_number() over(partition by fbpid, fuid order by fdate desc, fbank_gamecoins_num desc) rown
              from USER_BANK_FINAL_MID
          )
          where rown = 1;
          commit;

          execute immediate 'truncate table stage.USER_BANK_FINAL_MID';

          --数据更新回来
          insert into stage.USER_BANK_FINAL_MID
          (FDATE, FBPID, FUID, FBANK_GAMECOINS_NUM)
          select FDATE, FBPID, FUID, FBANK_GAMECOINS_NUM
          from stage.USER_BANK_FINAL_MID_TOW;
          commit;

      end if;

      insert into stage.USER_BANK_FINAL_MID
      (FDATE, FBPID, FUID, FBANK_GAMECOINS_NUM)
      select ld_begin fdate, fbpid, fuid, fbank_gamecoins_num
      from
      (
            select fbpid, fuid, fbank_gamecoins_num,
                row_number() over(partition by fbpid, fuid order by flts_at desc, fbank_gamecoins_num desc) rown
            from USER_BANK_STAGE
            where fact_type in (0, 1)
                     and coalesce(fcurrencies_type,'0') = '0'
                and flts_at >= ld_begin and flts_at < ld_end
       )
       where rown = 1;

      commit;

      pkg_manage_tools.add_common_log(ps_name => 'pkg_gamecoin',
                                    ps_subname => 'agg_bank_fin',
                                    ps_operation => 'end');

      exception when others then
      pkg_manage_tools.add_common_log(ps_name      => 'pkg_gamecoin_info',
                                      ps_subname   => 'agg_bank_finala',
                                      ps_operation => 'end',
                                      ps_err       => 'error');
  end;