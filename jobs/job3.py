"""
JOB 3: 마감 검증 (ERP·BS)

3차 파일 수신 후 실행 (job1, job2 완료 후):
  python sas_to_duckdb.py --ym 202601 --job jobs/job3.py
"""
from sas_to_duckdb import sql, table_exists, require_tables, check, row_count

NAME = "job3"
DESC = "마감 검증 (ERP·BS)"

# ══════════════════════════════════════════════
# 테이블 정의
# ══════════════════════════════════════════════
TABLES = {
    "erp": {
        "type": "pipe",
        "file": "BS100_{yyyymm}.DAT",
        "desc": "ERP 전표 (BS100)",
        "month_col": "SLPDT",
        "numeric": ["CLS_DA_SEQNO", "WONCR_POAMT"],
        "cols": [
            "SLPDT", "CLS_DA_SEQNO", "NTACC_CD", "DP_ORGCD",
            "IKD_GRPCD", "INS_IMCD", "RR_ORGCD", "SLP_AMT_DFRN_YN",
            "SLPNO", "SLP_LNNO", "BZCS_01_DVCD", "BZCS_02_DVCD",
            "FNDCD", "WONCR_POAMT", "NOTS_MTT", "BZCS_DV_EXEC_YN",
            "INP_USR_ID", "INP_DTHMS", "MDF_USR_ID", "MDF_DTHMS",
        ],
    },

    "bs101": {
        "type": "pipe",
        "file": "BS101_{yyyymm}.DAT",
        "desc": "원수실적마감 (BS101)",
        "month_col": "CLS_YYMM",
        "numeric": ["CLS_DA_SEQNO", "INCM_PRM_CR_SEQNO", "PYM_SEQ",
                     "RP_PRM", "AP_PRM", "CONDT_I_PRM", "MDF_CVAV", "MPY_CV_PRM"],
        "cols": [
            "CLS_YYMM", "CLS_DA_SEQNO", "IKD_GRPCD", "INS_IMCD",
            "PLYNO", "INCM_PRM_CR_SEQNO", "CNRDT", "GDCD",
            "FNL_CR_STCD", "FNL_CR_DT_STCD", "FNL_CR_ST_CHDT",
            "NW_RNW_FLGCD", "CVRCD", "DH_ORGCD", "DH_STFNO",
            "SL_TPCD", "UDRTK_TYCD", "RV_CCLDT", "PYM_CYCCD",
            "NANUM_TRI_YN", "PYM_SEQ", "FNL_PYM_YYMM",
            "RP_PRM", "AP_PRM", "DP_CASCD", "DP_DT_CASCD",
            "PPDT", "CONDT_I_PRM", "MDF_CVAV", "MPY_CV_PRM",
            "RR_ORGCD", "INP_USR_ID", "INP_DTHMS",
            "MDF_USR_ID", "MDF_DTHMS",
        ],
    },

    "bs104": {
        "type": "pipe",
        "file": "BS104_{yyyymm}.DAT",
        "desc": "지급보험금마감 (BS104)",
        "month_col": "CLS_YYMM",
        "numeric": ["CLS_DA_SEQNO", "AP_PRM", "PY_IBAMT",
                     "BZCS_DV_CLMCT", "BZCS_DV_PYCT"],
        "cols": [
            "CLS_YYMM", "CLS_DA_SEQNO", "IKD_GRPCD", "INS_IMCD",
            "CVRCD", "PLYNO", "CNRDT", "GDCD",
            "FNL_CR_STCD", "FNL_CR_DT_STCD", "FNL_CR_ST_CHDT",
            "DH_ORGCD", "DH_STFNO", "SL_TPCD", "UDRTK_TYCD",
            "AP_PRM", "RCP_YYMM", "RCP_NV_SEQNO", "IDM_CLM_CVRCD",
            "EX_RCV_FLGCD", "PY_IBAMT", "BZCS_DV_CLMCT",
            "BZCS_DV_PYCT", "PPDT", "RR_ORGCD",
            "INP_USR_ID", "INP_DTHMS", "MDF_USR_ID", "MDF_DTHMS",
            "NANUM_TRI_YN", "CR_CVRCD",
        ],
    },

    "bs105": {
        "type": "pipe",
        "file": "BS105_{yyyymm}.DAT",
        "desc": "지급준비금마감 (BS105)",
        "month_col": "CLS_YYMM",
        "numeric": ["CLS_DA_SEQNO", "AP_PRM", "ISAMT", "PY_RFAMT"],
        "cols": [
            "CLS_YYMM", "CLS_DA_SEQNO", "IKD_GRPCD", "INS_IMCD",
            "PLYNO", "CNRDT", "GDCD",
            "FNL_CR_STCD", "FNL_CR_DT_STCD", "FNL_CR_ST_CHDT",
            "CLM_CVRCD", "DH_ORGCD", "DH_STFNO", "SL_TPCD",
            "UDRTK_TYCD", "AP_PRM", "ISAMT", "PY_RFAMT",
            "RR_ORGCD", "INP_USR_ID", "INP_DTHMS",
            "MDF_USR_ID", "MDF_DTHMS", "NANUM_TRI_YN", "CR_CVRCD",
        ],
    },
}

EXPORT_SHEETS = {
    "erp"   : "ERP_BS100",
    "bs101" : "BS101_원수실적",
    "bs104" : "BS104_지급보험금",
    "bs105" : "BS105_지급준비금",
}


# ══════════════════════════════════════════════
# 비즈니스 로직
# ══════════════════════════════════════════════
def logic(con, yyyymm):
    # 마감 데이터 기반 추가 로직이 필요하면 여기에 작성
    pass


# ══════════════════════════════════════════════
# 검증
# ══════════════════════════════════════════════
def validate(con, yyyymm):
    for tbl in ["erp", "bs101", "bs104", "bs105"]:
        row_count(con, tbl)

    # bs104 IBNR 담보코드 누락 (0건이어야 정상)
    if require_tables(con, "bs104"):
        check(con, "bs104 IBNR 담보코드 누락", """
            SELECT COUNT(*) FROM bs104
            WHERE IKD_GRPCD = 'LA' AND CVRCD IS NULL
        """, expect="zero")

    # bs105 IBNR 담보코드 누락 (0건이어야 정상)
    if require_tables(con, "bs105"):
        check(con, "bs105 IBNR 담보코드 누락", """
            SELECT COUNT(*) FROM bs105
            WHERE IKD_GRPCD = 'LA' AND CLM_CVRCD IS NULL
        """, expect="zero")

    # 자동차 TM/CM 비비례 배분 (0건이어야 정상)
    if require_tables(con, f"sa_{yyyymm}", "erp"):
        check(con, "자동차 TM/CM 비비례 배분 누락", f"""
            SELECT COUNT(*) FROM sa_{yyyymm}
            WHERE SUBSTR(BZCS_02_DVCD, 1, 1) = 'E'
              AND NTACC_CD IN (
                  SELECT DISTINCT NTACC_CD FROM erp
                  WHERE SUBSTR(BZCS_02_DVCD, 1, 1) = 'E'
              )
              AND SUBSTR(INS_IMCD, 1, 2) = 'CA'
              AND SL_TPCD = '21'
        """, expect="zero")

        # 재마감 자동차 금액 확인 (참고값 로깅)
        row = con.execute(f"""
            SELECT COALESCE(SUM(S), 0) FROM sa_{yyyymm}
            WHERE SUBSTR(BZCS_02_DVCD, 1, 1) = 'E'
              AND BZCS_02_DVCD NOT IN ('E07','E08','E09','E10','E11','E12','E13')
              AND NTACC_CD IN (
                  SELECT DISTINCT NTACC_CD FROM erp
                  WHERE SUBSTR(BZCS_02_DVCD, 1, 1) = 'E'
              )
              AND SUBSTR(INS_IMCD, 1, 2) = 'CA'
              AND SL_TPCD = '14'
        """).fetchone()
        from sas_to_duckdb import log
        log.info(f"  [참고] 재마감 자동차 금액: {row[0]:,.0f}")
