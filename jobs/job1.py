"""
JOB 1: 원천 데이터 집계 (보험료)

1차 파일 수신 후 실행:
  python sas_to_duckdb.py --ym 202601 --job jobs/job1.py
"""
from sas_to_duckdb import sql, table_exists

NAME = "job1"
DESC = "원천 데이터 집계 (보험료)"

# ══════════════════════════════════════════════
# 테이블 정의
# ══════════════════════════════════════════════
TABLES = {
    "fio841": {
        "type": "fwf",
        "file": "fioBtLtrJ841_01_{YYYYMM}.DAT",
        "desc": "수입보험료(유지비·신계약비 원천)",
        "month_col": "CLS_YYMM",
        "numeric": ["INCM_PRM_CR_SEQNO", "PYM_SEQ", "RP_PRM", "AF_PRM",
                     "CONDT_I_PRM", "MDF_CVAV", "MPY_CV_PRM"],
        "cols": [
            ((0,    6),  "CLS_YYMM"),
            ((16,  32),  "PLYNO"),
            ((32,  37),  "INCM_PRM_CR_SEQNO"),
            ((37,  45),  "CNRDT"),
            ((45,  55),  "GDCD"),
            ((55,  65),  "IKD_GRPCD"),
            ((65,  75),  "INS_IMCD"),
            ((75,  85),  "CVRCD"),
            ((85,  95),  "FNL_CR_STCD"),
            ((95, 105),  "FNL_CR_DT_STCD"),
            ((105,113),  "FNL_CR_ST_CHDT"),
            ((113,123),  "NW_RNW_FLGCD"),
            ((123,128),  "PYM_SEQ"),
            ((128,134),  "FNL_PYM_YYMM"),
            ((134,149),  "RP_PRM"),
            ((149,164),  "AF_PRM"),
            ((164,174),  "DP_CASCD"),
            ((174,184),  "DP_DI_CASCD"),
            ((184,192),  "RV_CCLDT"),
            ((192,200),  "PPDT"),
            ((200,210),  "PYM_CYCCD"),
            ((210,217),  "DH_ORGCD"),
            ((217,224),  "DH_STFNO"),
            ((224,226),  "SL_TPCD"),
            ((226,241),  "CONDT_I_PRM"),
            ((241,251),  "UDRTK_TYCD"),
            ((251,252),  "MDF_CVAV"),
            ((252,253),  "MPY_CV_PRM"),
        ],
    },

    "fio843": {
        "type": "fwf",
        "file": "fioBtLtrJ843_01_{YYYYMM}.DAT",
        "desc": "사업비배분 보유건수",
        "month_col": "CLS_YYMM",
        "numeric": ["AP_PRM", "BZCS_DV_PS"],
        "cols": [
            ((0,    6),  "CLS_YYMM"),
            ((16,  32),  "PLYNO"),
            ((32,  40),  "CNRDT"),
            ((40,  50),  "GDCD"),
            ((50,  60),  "IKD_GRPCD"),
            ((60,  70),  "INS_IMCD"),
            ((70,  85),  "AP_PRM"),
            ((85,  92),  "DH_ORGCD"),
            ((92,  99),  "DH_STFNO"),
            ((99, 101),  "SL_TPCD"),
            ((101,111),  "UDRTK_TYCD"),
            ((111,116),  "BZCS_DV_PS"),
        ],
    },
}

EXPORT_SHEETS = {
    "fio841"          : "fio841",
    "fio843"          : "fio843",
    "BASE_DATA_CH"    : "집계_841보험료",
    "BASE_DATA_CH_843": "집계_843보험료",
    "BASE_DATA_CH2"   : "집계_843건수",
    "CH"              : "취급기관이상체크",
}


# ══════════════════════════════════════════════
# 비즈니스 로직
# ══════════════════════════════════════════════
def logic(con, yyyymm):

    sql(con, "BASE_DATA_CH (841 집계)", """
        CREATE OR REPLACE TABLE BASE_DATA_CH AS
        SELECT INS_IMCD, SUM(RP_PRM) AS RP_PRM, SUM(AP_PRM) AS AP_PRM
        FROM fio841
        GROUP BY INS_IMCD
    """)

    sql(con, "BASE_DATA_CH_843 (843 집계)", """
        CREATE OR REPLACE TABLE BASE_DATA_CH_843 AS
        SELECT INS_IMCD, SUM(AP_PRM) AS AP_PRM
        FROM fio843
        GROUP BY INS_IMCD
    """)

    sql(con, "BASE_DATA_CH2 (843 건수집계)", """
        CREATE OR REPLACE TABLE BASE_DATA_CH2 AS
        SELECT CLS_YYMM, INS_IMCD, SUM(AP_PRM) AS AP_PRM, SUM(BZCS_DV_PS) AS BZCS_DV_PS
        FROM fio843
        GROUP BY CLS_YYMM, INS_IMCD
    """)

    sql(con, "CH (취급기관 이상체크)", """
        CREATE OR REPLACE TABLE CH AS
        SELECT DISTINCT DH_ORGCD, DH_STFNO
        FROM fio843
        WHERE SUBSTR(DH_STFNO, 1, 2) IN ('2S')
          AND SUBSTR(DH_ORGCD, 1, 1) NOT IN ('E')
    """)


# ══════════════════════════════════════════════
# 검증
# ══════════════════════════════════════════════
def validate(con, yyyymm):
    for tbl in ["fio841", "fio843", "BASE_DATA_CH", "BASE_DATA_CH_843", "BASE_DATA_CH2", "CH"]:
        if table_exists(con, tbl):
            cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"  {tbl:25s}: {cnt:>10,}건")
