"""
JOB 2: 배분 결과 (SA 변환·집계)

2차 파일 수신 후 실행 (job1 완료 후):
  python sas_to_duckdb.py --ym 202601 --job jobs/job2.py
"""
from sas_to_duckdb import sql, table_exists, check, row_count

NAME = "job2"
DESC = "배분 결과 (SA 변환·집계)"

# ══════════════════════════════════════════════
# 테이블 정의
# ══════════════════════════════════════════════
TABLES = {
    "sa01": {
        "type": "pipe",
        "file": "RS100_{yyyymm}.DAT",
        "desc": "유지비·신계약비 배분결과 (RS100)",
        "month_col": "SLPDT",
        "numeric": ["DV_RT", "DVAMT"],
        "cols": [
            "SLPDT", "SLPNO", "SLP_LNNO", "SNO", "RR_ORGCD",
            "BZCS_TPCD", "INS_IMCD", "SL_TPCD", "CVRCD",
            "DV_RT", "DVAMT", "INP_USR_ID", "INP_DTHMS",
            "MDF_USR_ID", "MDF_DTHMS", "NTACC_CD", "SUB_SNO",
        ],
    },

    "sa02": {
        "type": "pipe",
        "file": "RS101_{yyyymm}.DAT",
        "desc": "손해조사비 배분결과 (RS101)",
        "month_col": "SLPDT",
        "numeric": ["DV_RT", "DVAMT"],
        "cols": [
            "SLPDT", "SLPNO", "SLP_LNNO", "SNO", "RR_ORGCD",
            "BZCS_TPCD", "INS_IMCD", "SL_TPCD", "CVRCD",
            "CR_CVRCD", "DV_RT", "DVAMT", "BZCS_INS_IMCD",
            "INP_USR_ID", "INP_DTHMS", "MDF_USR_ID", "MDF_DTHMS",
            "NTACC_CD",
        ],
    },
}

EXPORT_SHEETS = {
    "sa01": "SA01_유지비배분",
    "sa02": "SA02_손해조사비배분",
}


# ══════════════════════════════════════════════
# 비즈니스 로직
# ══════════════════════════════════════════════
def logic(con, yyyymm):

    sql(con, "SA11 (sa01 변환)", """
        CREATE OR REPLACE TABLE sa11 AS
        SELECT SLPDT AS YYYYMM, NTACC_CD, BZCS_TPCD, RR_ORGCD, SL_TPCD, INS_IMCD,
               CASE WHEN CVRCD = '*' THEN '0' ELSE CVRCD END AS DGB,
               DVAMT AS S
        FROM sa01
    """)

    sql(con, "SA12 (sa02 변환)", """
        CREATE OR REPLACE TABLE sa12 AS
        SELECT SLPDT AS YYYYMM, NTACC_CD, BZCS_TPCD, RR_ORGCD, SL_TPCD, INS_IMCD,
               CASE WHEN CVRCD = '*' THEN '0' ELSE CVRCD END AS DGB,
               DVAMT AS S
        FROM sa02
    """)

    sql(con, "SA20 (SA11 + SA12)", """
        CREATE OR REPLACE TABLE sa20 AS
        SELECT * FROM sa11
        UNION ALL
        SELECT * FROM sa12
    """)

    sql(con, f"SA_{yyyymm} (최종 배분집계)", f"""
        CREATE OR REPLACE TABLE sa_{yyyymm} AS
        SELECT DISTINCT YYYYMM, NTACC_CD, RR_ORGCD, BZCS_TPCD, SL_TPCD, INS_IMCD,
               DGB AS CVRCD, SUM(S) AS S
        FROM sa20
        GROUP BY YYYYMM, NTACC_CD, RR_ORGCD, BZCS_TPCD, SL_TPCD, INS_IMCD, DGB
    """)

    # ── 이연 계산 (ey_a2601 필요) ──────────────
    if table_exists(con, "ey_a2601"):
        sql(con, "tmp_01 (이연액 매핑)", """
            CREATE OR REPLACE TABLE tmp_01 AS
            SELECT '일반관리비' AS GB,
                   CASE WHEN SL_TPCD IN ('11','21','31','42') THEN '5125030001'
                        WHEN SL_TPCD = '41'                   THEN '5125010001'
                        WHEN SL_TPCD IN ('12','13','22','32') THEN '5125050001'
                   END AS NTACC_CD,
                   CLS_YYMM AS YYYYMM, 'LA' AS BGB, '0' AS CGB,
                   RL_CREW_NWCRT * -1 AS S
            FROM ey_a2601
            WHERE SL_TPCD IN ('11','21','31','41','42','12','13','22','32')
        """)

        sql(con, "tmp_02 (상각비 매핑)", """
            CREATE OR REPLACE TABLE tmp_02 AS
            SELECT '일반관리비' AS GB,
                   CASE WHEN SL_TPCD IN ('11','21','31','42') THEN '5131030001'
                        WHEN SL_TPCD = '41'                   THEN '5131010001'
                        WHEN SL_TPCD IN ('12','13','22','32') THEN '5131050001'
                   END AS NTACC_CD,
                   CLS_YYMM AS YYYYMM, 'LA' AS BGB, '0' AS CGB,
                   RL_NWCRT_DPCS AS S
            FROM ey_a2601
            WHERE SL_TPCD IN ('11','21','31','41','42','12','13','22','32')
        """)

        sql(con, "TEMP01 (이연 집계)", """
            CREATE OR REPLACE TABLE TEMP01 AS
            SELECT DISTINCT YYYYMM, GB, NTACC_CD, BGB, CGB, SUM(S) AS S
            FROM (SELECT * FROM tmp_01 UNION ALL SELECT * FROM tmp_02)
            GROUP BY YYYYMM, GB, NTACC_CD, BGB, CGB
        """)

        con.execute("DROP TABLE IF EXISTS tmp_01")
        con.execute("DROP TABLE IF EXISTS tmp_02")

    # ── 순사업비 요약 ──────────────────────────
    sql(con, "out_SA000 (순사업비 요약)", f"""
        CREATE OR REPLACE TABLE out_SA000 AS
        SELECT DISTINCT INS_IMCD,
            CASE
                WHEN SUBSTR(INS_IMCD,1,2) = 'CA' THEN '자동차'
                WHEN SUBSTR(INS_IMCD,1,2) IN ('FA','MA') THEN '일반'
                WHEN INS_IMCD IN (
                    'LA02660','LA02661','LA02662','LA02663','LA02664','LA02665',
                    'LA02666','LA02667','LA02668','LA02669','LA02670','LA02699',
                    'LA02800','LA02901','LA02802','LA02803','LA02804','LA02805',
                    'LA02806','LA02807','LA02808','LA02809','LA02810','LA02812',
                    'LA02813','LA02815','LA02814','LA02816','LA02818','LA02819',
                    'LA02820','LA02821','LA02822','LA02823','LA02824','LA02827',
                    'LA02825','LA02826','LA02828','LA02829','LA02830','LA02831',
                    'LA02833','LA02835','LA02836','LA02838','LA02839','LA02840',
                    'LA02841','LA02842','LA02844','LA02845','LA02846','LA02847',
                    'LA02848','LA02850','LA02851','LA02852','LA02854','LA02855',
                    'LA02856','LA02858','LA02859','LA02860','LA02861','LA02862',
                    'LA02863','LA02864','LA02867','LA02868','LA02869','LA02852',
                    'LA02866','LA02872','LA02873','LA02874','LA02875','LA02878',
                    'LA02879','LA02880','LA02881','LA02882','LA02883','LA02886',
                    'LA02887','LA02885','LA02888','LA02891','LA02889','LA02890',
                    'LA02892','LA02900','LA02901','LA02910','LA02920','LA02930',
                    'LA02903','LA02904','LA02911','LA02921','LA02931','LA02905',
                    'LA02906','LA02941','LA02951','LA02961','LA02907','LA02908',
                    'LA02971','LA02981','LA02991'
                ) THEN '장기연금'
                WHEN INS_IMCD IN (
                    'LA02660','LA02661','LA02662','LA02663','LA02664','LA02665',
                    'LA02667','LA02668','LA02669','LA02670','LA02699','LA02800',
                    'LA02801','LA02802','LA02803','LA02804','LA02805','LA02806',
                    'LA02807','LA02808','LA02809','LA02810','LA02812','LA02813',
                    'LA02815','LA02814','LA02816','LA02818','LA02819','LA02821',
                    'LA02823','LA02824','LA02827','LA02825','LA02826','LA02828',
                    'LA02829','LA02830','LA02831','LA02833','LA02835','LA02836',
                    'LA02838','LA02839','LA02840','LA02841','LA02842','LA02844',
                    'LA02845','LA02846','LA02847','LA02848','LA02850','LA02851',
                    'LA02852','LA02854','LA02855','LA02856','LA02858','LA02859',
                    'LA02860','LA02861','LA02862','LA02863','LA02864','LA02867',
                    'LA02868','LA02852','LA02866','LA02872','LA02873','LA02874',
                    'LA02875','LA02878','LA02879','LA02880','LA02881','LA02882',
                    'LA02883','LA02886','LA02885','LA02888','LA02891','LA02889',
                    'LA02890','LA02892','LA02900','LA02901','LA02910','LA02920',
                    'LA02930','LA02903','LA02911','LA02921','LA02931','LA02905',
                    'LA02906','LA02941','LA02951','LA02961','LA02907','LA02908',
                    'LA02971','LA02981','LA02991'
                ) THEN '장기우매당'
                WHEN SUBSTR(INS_IMCD,1,2) = 'RA' THEN '퇴직'
                ELSE '에러'
            END AS BGB,
            S3GUB, B_CH, SL_TPCD,
            CASE
                WHEN INS_IMCD = 'CA91001'        THEN '1'
                WHEN SUBSTR(INS_IMCD,1,2) = 'CA' THEN CVRCD
                ELSE '0'
            END AS CGB,
            SUM(S) AS S
        FROM sa_{yyyymm}
        WHERE YYYYMM = '{yyyymm}' AND BZCS_TPCD <> '02'
        GROUP BY INS_IMCD, BGB, S3GUB, B_CH, SL_TPCD, CGB
    """)

    sql(con, "out_SA001 (신계약비·수금비 추세)", f"""
        CREATE OR REPLACE TABLE out_SA001 AS
        SELECT DISTINCT YYYYMM, NTACC_CD, SUBSTR(INS_IMCD,1,2) AS BGB,
               SL_TPCD, SUM(S) AS S
        FROM sa_{yyyymm}
        WHERE SUBSTR(NTACC_CD,1,4) IN ('5121','5127','5128','5129')
        GROUP BY YYYYMM, NTACC_CD, BGB, SL_TPCD
    """)


# ══════════════════════════════════════════════
# 검증
# ══════════════════════════════════════════════
def validate(con, yyyymm):
    for tbl in ["sa11", "sa12", "sa20", f"sa_{yyyymm}"]:
        row_count(con, tbl)

    # 전표 vs 배분결과 대사 (차이 0이어야 정상)
    if table_exists(con, "erp") and table_exists(con, f"sa_{yyyymm}"):
        check(con, "전표 vs 배분결과 계정별 차이", f"""
            SELECT COUNT(*) FROM (
                SELECT NTACC_CD, SUM(S) AS diff FROM (
                    SELECT NTACC_CD, SUM(WONCR_POAMT) AS S FROM erp GROUP BY NTACC_CD
                    UNION ALL
                    SELECT NTACC_CD, -SUM(S) AS S FROM sa_{yyyymm} GROUP BY NTACC_CD
                ) GROUP BY NTACC_CD
                HAVING ABS(SUM(S)) > 1
            )
        """, expect="zero")
