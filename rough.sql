SELECT * FROM `thcdnaproddata.cognos.fact_cfo_daily_census` where metric='CollDls'
/*
--SELECT * FROM `thcdnaproddata.staging.bq_table_mapping` where file_name like '%bap_f_an_record_summary%'
select temp_table_name from thcdnaproddata.staging.bq_temp_table_audit 
where date(update_time) = current_date('America/Chicago') and 
lower(table_name) = lower('statfile_daac') 
and (stg_load_chk is null or stg_load_chk !='P' or stg_load_chk = '-') and Load_status='Y';
--daac_mrng_statfile_daac_none_temp 
--post_sql_qhd_statfile_daac_none_temp 
/*
select temp_table_name from thcdnaproddata.staging.bq_temp_table_audit_1
where date(update_time) = current_date('America/Chicago') and 
lower(table_name) = lower('statfile_daac') 
and (stg_load_chk is null or stg_load_chk !='P' or stg_load_chk = '-') and Load_status='Y';
*/

select * from thcdnaproddata.staging.bq_temp_table_audit 
where date(update_time) = current_date('America/Chicago') and 
lower(table_name) = lower('statfile_daac') 
and (stg_load_chk is null or stg_load_chk !='P' or stg_load_chk = '-') and Load_status='Y';

--select * from thcdnaproddata.staging.bq_temp_table_audit_1 where cast(update_time as datetime) < (select cast(max(update_time) as datetime) from thcdnaproddata.staging.bq_temp_table_audit);



--select cast(max(update_time) as datetime)  from thcdnaproddata.staging.bq_temp_table_audit_1 where cast(update_time as datetime) > cast('2022-07-28T15:04:38' as datetime)


select (select cast(max(update_time) as datetime)  from thcdnaproddata.staging.bq_temp_table_audit_1)  >  (select cast(max(update_time) as datetime) from thcdnaproddata.staging.bq_temp_table_audit)

--select keys_columns from thcdnaproddata.staging.bq_table_mapping where lower(table_name)=lower('statfile_daac') and table_action is NULL limit 1

--delete from `thcdnaproddata.staging.statfile_daac` S WHERE EXISTS (SELECT 1 FROM `thcdnaproddata.thc_dna_temp_staging.post_sql_qhd_statfile_daac_none_temp` T WHERE S.STHOSP=T.STHOSP and S.SOURCE_SYSTEM=T.SOURCE_SYSTEM);

--CALL thcdnaproddata.staging.sp_temp_to_stg_bq_load_v4('staging','statfile_daac', 'thc_dna_temp_staging','daac_mrng_statfile_daac_none_temp ','INSERT', '', 20220728150434,'',''); 


select count(1) from idm.fact_wor_detail;

insert into thcdnaproddata.staging.bq_temp_table_audit select * from thcdnaproddata.staging.bq_temp_table_audit_1 where update_time > (select max(update_time) from thcdnaproddata.staging.bq_temp_table_audit);

--create table thcdnaproddata.thc_dna_temp_staging .fact_general_stats_20220801 as select * from thcdnadevdata.idm.fact_general_stats_20220801;
--create table thcdnaproddata.thc_dna_temp_staging .fact_gl_account_20220801 as select * from thcdnadevdata.idm.fact_gl_account_20220801;
--truncate table idm.fact_general_stats;
--truncate table idm.fact_gl_account;
--CALL thcdnaproddata.staging.sp_temp_to_stg_bq_load_v4('idm','fact_gl_account', 'thc_dna_temp_staging','fact_gl_account_20220801','INSERT', '', 202207261234,'','');

INSERT INTO idm.fact_gl_account(`dim_chart_of_account_sk`,`dim_facility_sk`,`dim_gl_facility_sk`,`company`,`comapny_desc`,`period`,`actuals`,`budget`,`forecast`,`source_system`,`create_ts`,`create_uid`,`update_ts`,`update_uid`,`unique_id`,`actual_glmors`) SELECT CAST(REPLACE(REGEXP_EXTRACT(dim_chart_of_account_sk,r"-?[0-9]+"), "." , "" ) AS INT64),CAST(REPLACE(REGEXP_EXTRACT(dim_facility_sk,r"-?[0-9]+"), "." , "" ) AS INT64),CAST(REPLACE(REGEXP_EXTRACT(dim_gl_facility_sk,r"-?[0-9]+"), "." , "" ) AS INT64),CAST(NULLIF(company,'') AS STRING),CAST(NULLIF(comapny_desc,'') AS STRING),CAST(NULLIF(period,'') AS STRING),CAST(NULLIF(actuals,'') AS NUMERIC),CAST(NULLIF(budget,'') AS NUMERIC),CAST(NULLIF(forecast,'') AS NUMERIC),CAST(NULLIF(source_system,'') AS STRING),CAST(CASE WHEN LENGTH(create_ts)=16 THEN CONCAT(create_ts ,":00" ) ELSE NULLIF(create_ts,'') END AS DATETIME),CAST(NULLIF(create_uid,'') AS STRING),CAST(CASE WHEN LENGTH(update_ts)=16 THEN CONCAT(update_ts ,":00" ) ELSE NULLIF(update_ts,'') END AS DATETIME),CAST(NULLIF(update_uid,'') AS STRING),CAST(NULLIF(unique_id,'') AS STRING),CAST(NULLIF(actual_glmors,'') AS STRING) FROM thc_dna_temp_staging.fact_gl_account_20220801;

CALL thcdnaproddata.staging.sp_temp_to_stg_bq_load_v4('idm','fact_charges', 'thc_dna_temp_staging','fact_charges_20220725','INSERT', '', 202207261234,'','');

CALL thcdnaproddata.staging.sp_temp_to_stg_bq_load_v4('idm','fact_order', 'thc_dna_temp_staging','fact_order_20220725','INSERT', '', 202207261234,'','');

CALL thcdnaproddata.staging.sp_temp_to_stg_bq_load_v4('idm','fact_order', 'thc_dna_temp_staging','fact_order_20220725','INSERT', '', 202207261234,'','');

CALL thcdnaproddata.staging.sp_temp_to_stg_bq_load_v4('staging','statfile_daac', 'thc_dna_temp_staging','statfile_daac_2022072','INSERT', '', 202207261234,'','');
statfile_daac_2022072


BAPTIST_EPIC : MSEQ_BAPTIST_IDM_STG_PROCESS_RUNME -> MSEQ_EPIC_BAPTIST_NM_LOAD -> MSEQ_NM_DRG_To_3MGrouper -> MSEQ_NM_DRG_From_3MGrouper

DAAC (PBAR) : MSEQX_DAAC_DAPBAR

DAAC (NON PBAR) : MSEQX_DAAC_NON_PBAR -> MSEQ_NON_PBAR_TO_NM_ALL

STATS : MSEQX_STATS_ALL_RUN_ME

-----------
IDM Load FINAL : MSEQX_FACT_ALL_RUN_ME [Dependent on last MSEQs from above 4 Sources]

 MSEQ_NON_PBAR_TO_NM_ALL

select * from thcdnaproddata.staging.bq_temp_table_audit where date(update_time) = current_date('America/Chicago') 
and lower(table_name) = lower('statfile_intg')  and Load_status='Y' and stg_load_chk is null;


select hosp_cd, COUNT(1) from thcdnaproddata.staging.bq_temp_table_audit where date(update_time) = current_date('America/Chicago') 
and lower(table_name) = lower('statfile_intg')  and Load_status='Y' and stg_load_chk is null GROUP BY 1 order by 2;


select * from `thcdnaproddata.staging.bq_file_audit_details` where LOWER(file_name) like '%statfile_intg%'  AND DATE(file_updatation_time)='2022-08-13'
AND File_operation !='OBJECT_DELETE' order by  file_updatation_time desc;


with abc
select * from thcdnaproddata.staging.bq_temp_table_audit where date(update_time) = current_date('America/Chicago') -1
and lower(table_name) = lower('statfile_intg')  and Load_status='Y' AND hosp_cd='FUH' limit 1

bash /tmp/nz_bq/dif_wrapper.sh -i /tmp/nz_bq/input_tables.csv