import argparse
import sys

import pandas as pd
import pymysql


def get_csv(analysis_group_id):
    sql = """
    SELECT
    L.library_name
    , L.delivery_name_array
    , L.kit_array
    , L.species
    , L.request_library_size
    , L.library_size
    , L.service_output
    , L.library_output
    , L.real_output
    , L.totalreads
    , L.readlength_array
    , auto_delivery_group_detail.`request_trimming`
    , auto_delivery_group_detail.`request_read`
    , auto_delivery_group_detail.`status`
    , L.n_rate
    , L.q30_more_bases_rate_r1
    , L.q30_more_bases_rate_r2
    , L.q20_more_bases_rate_r1
    , L.q20_more_bases_rate_r2
    , L.qc_result
    , L.fastqc_array
    , L.rawdata_path_array
FROM auto_delivery_group
INNER JOIN auto_delivery_group_detail
    ON auto_delivery_group_detail.`fk_auto_delivery_group_idx` = auto_delivery_group.`idx`
LEFT JOIN (
SELECT
    A.library_idx AS `library_idx`
    , A.library_name AS `library_name`
    , GROUP_CONCAT(DISTINCT A.kit ORDER BY A.library_index_idx SEPARATOR ',') AS `kit_array`
    , A.service_idx AS `service_idx`
    , FORMAT(A.library_output,2) AS `library_output`
    , A.request_library_size AS `request_library_size`
    , A.library_size AS `library_size`
    , FORMAT(SUM(A.planned_output),2) AS `planned_output`
    , FORMAT(SUM(A.totalbases)/1000000000,2) AS `real_output`
    , SUM(A.totalbases) AS `totalbases`
    , SUM(A.totalreads) AS `totalreads`
    , GROUP_CONCAT(DISTINCT A.readlength ORDER BY A.seq_idx SEPARATOR ',') AS `readlength_array`
    , A.delivery_name_array AS `delivery_name_array`
    , GROUP_CONCAT(DISTINCT
        CONCAT(A.flow_cell_id, ' Lane', A.lane, ': ', 'R1', ' ', IF(A.r2_report_path IS NULL OR TRIM(A.r2_report_path) = '', '', 'R2'))
        ORDER BY A.run_group_idx, A.lane ASC
        SEPARATOR ','
    ) AS `fastqc_array`
    , GROUP_CONCAT(DISTINCT 
        CONCAT(A.server_ip,':', A.rawdata_path)
        ORDER BY A.server_ip, A.rawdata_path_idx ASC
        SEPARATOR ','
    ) AS `rawdata_path_array`
    , FORMAT(SUM(A.n_count)*100/SUM(A.totalbases),2) AS `n_rate`
    , IF(LOCATE('PE',A.`read`)>0, TRUNCATE(SUM(A.q30_more_bases_R1)/SUM(A.totalbases_1)*100,1), TRUNCATE(SUM(A.q30_more_bases_R1)/SUM(A.totalbases_1)*100,1)) AS `q30_more_bases_rate_r1`
    , IF(LOCATE('PE',A.`read`)>0, TRUNCATE(SUM(A.q30_more_bases_R2)/SUM(A.totalbases_2)*100,1), TRUNCATE(SUM(A.q30_more_bases_R2)/SUM(A.totalbases_1)*100,1)) AS `q30_more_bases_rate_r2`
    , IF(LOCATE('PE',A.`read`)>0, TRUNCATE(SUM(A.q20_more_bases_R1)/SUM(A.totalbases_1)*100,1), TRUNCATE(SUM(A.q20_more_bases_R1)/SUM(A.totalbases_1)*100,1)) AS `q20_more_bases_rate_r1`
    , IF(LOCATE('PE',A.`read`)>0, TRUNCATE(SUM(A.q20_more_bases_R2)/SUM(A.totalbases_2)*100,1), TRUNCATE(SUM(A.q20_more_bases_R2)/SUM(A.totalbases_1)*100,1)) AS `q20_more_bases_rate_r2`
    , IF(SUM(A.totalbases) IS NULL,
        NULL,
        IF(LOCATE('PE',A.`read`)>0,
            IF(TRUNCATE(SUM(A.q30_more_bases_R1)/SUM(A.totalbases_1),2) >= 0.80
            AND TRUNCATE(SUM(A.q30_more_bases_R2)/SUM(A.totalbases_2),2) >= 0.80
            AND TRUNCATE(SUM(A.q20_more_bases_R1)/SUM(A.totalbases_1),2) >= 0.90
            AND TRUNCATE(SUM(A.q20_more_bases_R2)/SUM(A.totalbases_2),2) >= 0.90,
                '합격',
                '불합격'),
            IF(TRUNCATE(SUM(A.q30_more_bases_R1)/SUM(A.totalbases_1),2) >= 0.80
            AND TRUNCATE(SUM(A.q20_more_bases_R1)/SUM(A.totalbases_1),2) >= 0.90,
                '합격',
                '불합격'))) AS `qc_result`
    , IF(LOCATE('S', GROUP_CONCAT(A.customer_class)) != 0, 'S',
        IF(LOCATE('A', GROUP_CONCAT(A.customer_class)) != 0, 'A',
            IF(LOCATE('B', GROUP_CONCAT(A.customer_class)) != 0, 'B',
                IF(LOCATE('C', GROUP_CONCAT(A.customer_class)) != 0, 'C',
                    IF(LOCATE('D', GROUP_CONCAT(A.customer_class)) != 0, 'D',
                        IF(LOCATE('없음', GROUP_CONCAT(A.customer_class)) != 0, '없음', 'unknown')))))) as customer_class
    , (SELECT sd.value
       FROM service_descriptor sd
       JOIN service_descriptor_template sdt ON sdt.idx = sd.fk_service_descriptor_template_idx AND sdt.`domain` = 'Species' 
       WHERE sd.fk_service_idx = A.service_idx
    ) AS `species`
    , (SELECT sd.value
       FROM service_descriptor sd
       JOIN service_descriptor_template sdt ON sdt.idx = sd.fk_service_descriptor_template_idx AND sdt.`domain` = 'Service_output' 
       WHERE sd.fk_service_idx = A.service_idx
    ) AS `service_output`
FROM
    (
    SELECT
        run_group.idx AS `run_group_idx`
        , run_group.`read` AS `read`
        , run_group.flow_cell_id AS `flow_cell_id`
        , run.idx AS `run_idx`
        , run.name AS `run_name`
        , run.lane AS `lane`
        , run.used_i7_sequence AS `used_i7_sequence`
        , run.used_i5_sequence AS `used_i5_sequence`
        , run.planned_output AS `planned_output`
        , run.r1_report_path AS `r1_report_path`
        , run.r2_report_path AS `r2_report_path`
        , run.add_output AS `add_output`
        , library.idx AS `library_idx`
        , library.name AS `library_name`
        , library.library_output AS `library_output`
        , library.request_library_size AS `request_library_size`
        , library.library_size AS `library_size`
        , library.expected_completion_at AS `expected_completion_at`
        , library_index.idx AS `library_index_idx`
        , IFNULL(library_index.`kit`, IFNULL(library.`kit`, library.`alias_kit`)) AS `kit`
        , library_index.i7_name AS `i7_name`
        , library_index.i5_name AS `i5_name`
        , library_index.i7_sequence AS `i7_sequence`
        , library_index.i5_sequence AS `i5_sequence`
        , library_group.started_by AS `started_by`
        , GROUP_CONCAT(DISTINCT sample.delivery_name ORDER BY sample.idx SEPARATOR ',') AS `delivery_name_array`
        , seq.idx AS `seq_idx`
        , seq.n_count AS `n_count`
        , seq.readlength AS `readlength`
        , seq.totalreads AS `totalreads`
        , seq.q30_more_reads_R1 AS `q30_more_reads_R1`
        , seq.q30_more_reads_R2 AS `q30_more_reads_R2`
        , seq.q20_more_reads_R1 AS `q20_more_reads_R1`
        , seq.q20_more_reads_R2 AS `q20_more_reads_R2`
        , seq.totalbases AS `totalbases`
        , seq.q30_more_bases_R1 AS `q30_more_bases_R1`
        , seq.q30_more_bases_R2 AS `q30_more_bases_R2`
        , seq.q20_more_bases_R1 AS `q20_more_bases_R1`
        , seq.q20_more_bases_R2 AS `q20_more_bases_R2`
        , rawdata_path.idx AS `rawdata_path_idx`
        , rawdata_path.path AS `rawdata_path`
        , rawdata_path.server_ip AS `server_ip`
        , GROUP_CONCAT(DISTINCT service.idx ORDER BY service.idx SEPARATOR ',') AS `service_idx`
        , (SELECT GROUP_CONCAT(DISTINCT person_project.customer_class) FROM person_project WHERE  person_project.relation_customer = '고객' AND project.idx = person_project.fk_project_idx) AS `customer_class`
        , seq.totalbases * (seq.readlength_1/(seq.readlength_1+seq.readlength_2)) AS totalbases_1
        , seq.totalbases * (seq.readlength_2/(seq.readlength_1+seq.readlength_2)) AS totalbases_2
    FROM
        analysis_group
        JOIN analysis_analysis_group ON analysis_analysis_group.fk_analysis_group_idx = analysis_group.idx
        JOIN analysis ON analysis.idx = analysis_analysis_group.fk_analysis_idx
        JOIN library ON library.idx = analysis.fk_library_idx
        JOIN library_library_group ON library_library_group.fk_library_idx = library.idx
        JOIN library_group ON library_group.idx = library_library_group.fk_library_group_idx
        JOIN run_analysis ON run_analysis.fk_analysis_idx = analysis.idx
        JOIN run ON run.idx = run_analysis.fk_run_idx
        JOIN rawdata ON rawdata.fk_run_idx = run.idx
        JOIN rawdata_group ON rawdata_group.idx = rawdata.fk_rawdata_group_idx
        JOIN seq ON seq.idx = rawdata.fk_seq_idx
        JOIN rawdata_path ON rawdata_path.idx = rawdata.fk_rawdata_path_idx
        JOIN run_run_group ON run_run_group.fk_run_idx = run.idx
        JOIN run_group ON run_group.idx = run_run_group.fk_run_group_idx
        LEFT JOIN library_index ON library_index.idx = run.fk_library_index_idx
        JOIN sample_qc_library ON sample_qc_library.fk_library_idx = library.idx
        JOIN sample_qc ON sample_qc.idx = sample_qc_library.fk_sample_qc_idx
        JOIN sample_sample_qc ON sample_sample_qc.fk_sample_qc_idx = sample_qc.idx
        JOIN sample ON sample.idx = sample_sample_qc.fk_sample_idx
        JOIN service ON service.idx = analysis_group.fk_service_idx
        JOIN project ON project.idx = service.fk_project_idx
    WHERE
        analysis_group.idx IN (%s)
        AND analysis.is_pure = 0
        AND library.is_pure = 0
        AND run.is_use = 1
        AND run_group.status <> '실패'
        AND rawdata_group.is_final = 1
    GROUP BY
        run_group.idx
        , run_group.`read`
        , run_group.flow_cell_id
        , run.idx
        , run.name
        , run.lane
        , run.used_i7_sequence
        , run.used_i5_sequence
        , run.planned_output
        , run.r1_report_path
        , run.r2_report_path
        , run.add_output
        , library.idx
        , library.name
        , library.library_output
        , library.request_library_size
        , library.library_size
        , library.expected_completion_at
        , library_index.idx
        , library_index.kit
        , library_index.i7_name
        , library_index.i5_name
        , library_index.i7_sequence
        , library_index.i5_sequence
        , library_group.started_by
        , seq.idx
        , seq.n_count
        , seq.readlength
        , seq.totalreads
        , seq.q30_more_reads_R1
        , seq.q30_more_reads_R2
        , seq.q20_more_reads_R1
        , seq.q20_more_reads_R2
        , seq.totalbases
        , seq.q30_more_bases_R1
        , seq.q30_more_bases_R2
        , seq.q20_more_bases_R1
        , seq.q20_more_bases_R2
        , rawdata_path.idx
        , rawdata_path.path
        , rawdata_path.server_ip
        , seq.readlength_1
        , seq.readlength_2
    UNION ALL
    SELECT
        run_group.idx AS `run_group_idx`
        , run_group.`read` AS `read`
        , run_group.flow_cell_id AS `flow_cell_id`
        , run.idx AS `run_idx`
        , run.name AS `run_name`
        , run.lane AS `lane`
        , run.used_i7_sequence AS `used_i7_sequence`
        , run.used_i5_sequence AS `used_i5_sequence`
        , run.planned_output AS `planned_output`
        , run.r1_report_path AS `r1_report_path`
        , run.r2_report_path AS `r2_report_path`
        , run.add_output AS `add_output`
        , library.idx AS `library_idx`
        , library.name AS `library_name`
        , library.library_output AS `library_output`
        , library.request_library_size AS `request_library_size`
        , library.library_size AS `library_size`
        , library.expected_completion_at AS `expected_completion_at`
        , library_index.idx AS `library_index_idx`
        , IFNULL(library_index.`kit`, IFNULL(library.`kit`, library.`alias_kit`)) AS `kit`
        , library_index.i7_name AS `i7_name`
        , library_index.i5_name AS `i5_name`
        , library_index.i7_sequence AS `i7_sequence`
        , library_index.i5_sequence AS `i5_sequence`
        , library_group.started_by AS `started_by`
        , GROUP_CONCAT(DISTINCT sample.delivery_name ORDER BY sample.idx SEPARATOR ',') AS `delivery_name_array`
        , seq.idx AS `seq_idx`
        , seq.n_count AS `n_count`
        , seq.readlength AS `readlength`
        , seq.totalreads AS `totalreads`
        , seq.q30_more_reads_R1 AS `q30_more_reads_R1`
        , seq.q30_more_reads_R2 AS `q30_more_reads_R2`
        , seq.q20_more_reads_R1 AS `q20_more_reads_R1`
        , seq.q20_more_reads_R2 AS `q20_more_reads_R2`
        , seq.totalbases AS `totalbases`
        , seq.q30_more_bases_R1 AS `q30_more_bases_R1`
        , seq.q30_more_bases_R2 AS `q30_more_bases_R2`
        , seq.q20_more_bases_R1 AS `q20_more_bases_R1`
        , seq.q20_more_bases_R2 AS `q20_more_bases_R2`
        , rawdata_path.idx AS `rawdata_path_idx`
        , rawdata_path.path AS `rawdata_path`
        , rawdata_path.server_ip AS `server_ip`
        , GROUP_CONCAT(DISTINCT service.idx ORDER BY service.idx SEPARATOR ',') AS `service_idx`
        , (SELECT GROUP_CONCAT(DISTINCT person_project.customer_class) FROM person_project WHERE  person_project.relation_customer = '고객' AND project.idx = person_project.fk_project_idx) AS `customer_class`
        , seq.totalbases * (seq.readlength_1/(seq.readlength_1+seq.readlength_2)) AS totalbases_1
        , seq.totalbases * (seq.readlength_2/(seq.readlength_1+seq.readlength_2)) AS totalbases_2
    FROM
        analysis_group
        JOIN analysis_analysis_group ON analysis_analysis_group.fk_analysis_group_idx = analysis_group.idx
        JOIN analysis ON analysis.idx = analysis_analysis_group.fk_analysis_idx
        JOIN library ON library.idx = analysis.fk_library_idx
        JOIN library_library_group ON library_library_group.fk_library_idx = library.idx
        JOIN library_group ON library_group.idx = library_library_group.fk_library_group_idx
        JOIN run_analysis ON run_analysis.fk_analysis_idx = analysis.idx
        JOIN run ON run.idx = run_analysis.fk_run_idx
        JOIN rawdata ON rawdata.fk_run_idx = run.idx
        JOIN rawdata_group ON rawdata_group.idx = rawdata.fk_rawdata_group_idx
        JOIN seq ON seq.idx = rawdata.fk_seq_idx
        JOIN rawdata_path ON rawdata_path.idx = rawdata.fk_rawdata_path_idx
        JOIN run_run_group ON run_run_group.fk_run_idx = run.idx
        JOIN run_group ON run_group.idx = run_run_group.fk_run_group_idx
        LEFT JOIN library_index ON library_index.idx = run.fk_library_index_idx
        JOIN sample ON sample.idx = library.fk_sample_idx
        JOIN service ON service.idx = analysis_group.fk_service_idx
        JOIN project ON project.idx = service.fk_project_idx
    WHERE
        analysis_group.idx IN (%s)
        AND analysis.is_pure = 0
        AND library.is_pure = 1
        AND run.is_use = 1
        AND run_group.status <> '실패'
        AND rawdata_group.is_final = 1
    GROUP BY
        run_group.idx
        , run_group.`read`
        , run_group.flow_cell_id
        , run.idx
        , run.name
        , run.lane
        , run.used_i7_sequence
        , run.used_i5_sequence
        , run.planned_output
        , run.r1_report_path
        , run.r2_report_path
        , run.add_output
        , library.idx
        , library.name
        , library.library_output
        , library.request_library_size
        , library.library_size
        , library.expected_completion_at
        , library_index.idx
        , library_index.kit
        , library_index.i7_name
        , library_index.i5_name
        , library_index.i7_sequence
        , library_index.i5_sequence
        , library_group.started_by
        , seq.idx
        , seq.n_count
        , seq.readlength
        , seq.totalreads
        , seq.q30_more_reads_R1
        , seq.q30_more_reads_R2
        , seq.q20_more_reads_R1
        , seq.q20_more_reads_R2
        , seq.totalbases
        , seq.q30_more_bases_R1
        , seq.q30_more_bases_R2
        , seq.q20_more_bases_R1
        , seq.q20_more_bases_R2
        , rawdata_path.idx
        , rawdata_path.path
        , rawdata_path.server_ip
        , seq.readlength_1
        , seq.readlength_2
    UNION ALL
    SELECT
        NULL AS `run_group_idx`
        , NULL AS `read`
        , NULL AS `flow_cell_id`
        , NULL AS `run_idx`
        , NULL AS `run_name`
        , NULL AS `lane`
        , NULL AS `used_i7_sequence`
        , NULL AS `used_i5_sequence`
        , NULL AS `planned_output`
        , NULL AS `r1_report_path`
        , NULL AS `r2_report_path`
        , NULL AS `add_output`
        , NULL AS `library_idx`
        , NULL AS `library_name`
        , NULL AS `library_output`
        , NULL AS `request_library_size`
        , NULL AS `library_size`
        , NULL AS `expected_completion_at`
        , NULL AS `library_index_idx`
        , NULL AS `kit`
        , NULL AS `i7_name`
        , NULL AS `i5_name`
        , NULL AS `i7_sequence`
        , NULL AS `i5_sequence`
        , NULL AS `started_by`
        , sample.delivery_name AS `delivery_name_array`
        , NULL AS `seq_idx`
        , NULL AS `n_count`
        , NULL AS `readlength`
        , NULL AS `totalreads`
        , NULL AS `q30_more_reads_R1`
        , NULL AS `q30_more_reads_R2`
        , NULL AS `q20_more_reads_R1`
        , NULL AS `q20_more_reads_R2`
        , NULL AS `totalbases`
        , NULL AS `q30_more_bases_R1`
        , NULL AS `q30_more_bases_R2`
        , NULL AS `q20_more_bases_R1`
        , NULL AS `q20_more_bases_R2`
        , NULL AS `rawdata_path_idx`
        , NULL AS `rawdata_path`
        , NULL AS `server_ip`
        , GROUP_CONCAT(DISTINCT service.idx ORDER BY service.idx SEPARATOR ',') AS `service_idx`
        , (SELECT GROUP_CONCAT(DISTINCT person_project.customer_class) FROM person_project WHERE  person_project.relation_customer = '고객' AND project.idx = person_project.fk_project_idx) AS `customer_class`
        , NULL AS totalbases_1
        , NULL AS totalbases_2
    FROM
        analysis_group
        JOIN analysis_analysis_group ON analysis_analysis_group.fk_analysis_group_idx = analysis_group.idx
        JOIN analysis ON analysis.idx = analysis_analysis_group.fk_analysis_idx
        JOIN sample ON sample.idx = analysis.fk_sample_idx
        JOIN service ON service.idx = analysis_group.fk_service_idx
        JOIN project ON project.idx = service.fk_project_idx
    WHERE
        analysis_group.idx IN (%s)
        AND analysis.is_pure = 1
    GROUP BY
        sample.idx
    ) A 
GROUP BY
    A.library_idx
    , A.library_name
    , A.library_output
    , A.request_library_size
    , A.library_size
    , A.delivery_name_array
) AS `L` ON L.`library_idx` = auto_delivery_group_detail.`fk_library_idx`
WHERE
    auto_delivery_group.`idx` = (SELECT ag.fk_auto_delivery_group_idx FROM analysis_group ag WHERE ag.idx = %s)
ORDER BY library_name asc
    """

    connection = pymysql.connect(host='magpie.ptbio.kr',
        user='nipton_dev',
        password='7890uiop',
        database='nipton',
        cursorclass=pymysql.cursors.DictCursor)

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, (analysis_group_id, analysis_group_id, analysis_group_id, analysis_group_id, ))
            result = cursor.fetchall()

    return result


def main():
    parser = argparse.ArgumentParser(description='Download csv from NIPTON')
    parser.add_argument('--id', required=True, help='analysis group id (merged or none) (Example: 15403, 14742)')

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        sys.exit(0)
    

    analysis_group_id = args.id
    connection = pymysql.connect(host='magpie.ptbio.kr',
        user='nipton_dev',
        password='7890uiop',
        database='nipton',
        cursorclass=pymysql.cursors.DictCursor)

    #-- 1-1. Merge한 분석작업번호가 없을 경우, 조회하고자 하는 분석작업번호를 2번 단계에서 그대로 사용
    #-- 1-2. Merge한 분석작업번호가 있을 경우, Merge된 분석작업번호들을 2번 단계에서 사용

    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT ag.merge_analysis_group_indexes FROM analysis_group ag WHERE ag.idx = %s"
            cursor.execute(sql, (analysis_group_id,))
            ag_run_ids = cursor.fetchone()


    if 'merge_analysis_group_indexes' in ag_run_ids and not ag_run_ids['merge_analysis_group_indexes']:
        ag_ids = [ analysis_group_id ]
    else:
        ag_ids = ag_run_ids['merge_analysis_group_indexes'].split(',')


    for ag_id in ag_ids:
        res = get_csv(ag_id)

        df = pd.DataFrame(res)
    
        if df.empty:
            print(f"Error! Check your id {ag_id}. Skipped this.")
            continue

        out_csv = f"result_{ag_id}.csv"
        print(out_csv)
        df.to_csv(out_csv, index=None)

 
    
if __name__ == '__main__':
    main()


