"""
SAS → DuckDB 전환 파이프라인

3개 JOB이 순차 실행되며, 각 JOB은 load → logic → validate → export 순서로 진행.
동일 DB에 월별 데이터가 누적됨 (YYYYMM 기준 교체).

실행 예시:
  python sas_to_duckdb.py --ym 202601                     # 전체 (job1→job2→job3)
  python sas_to_duckdb.py --ym 202601 --job 1              # job1만
  python sas_to_duckdb.py --ym 202601 --job 2 3            # job2, job3만
  python sas_to_duckdb.py --ym 202601 --job 1 --skip-load  # job1 로직만 (로드 생략)
  python sas_to_duckdb.py --list                            # 테이블 목록
"""

import re
import sys
import logging
import time
import argparse
import unicodedata
from pathlib import Path
from datetime import datetime

# ══════════════════════════════════════════════
# 로거
# ══════════════════════════════════════════════
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.DEBUG)

    class AlignedFormatter(logging.Formatter):
        _WIDTH = len("[CRITICAL]")
        def format(self, record):
            bracket = f"[{record.levelname}]"
            record.bracket = f"{bracket:<{self._WIDTH}}"
            return super().format(record)

    fmt = AlignedFormatter(
        fmt="%(asctime)s %(bracket)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.info(f"로그 파일: {log_file}")
    return logger

log = setup_logger()

# ── 의존성 ───────────────────────────────────
log.info(f"Python {sys.version.split()[0]}")

try:
    import duckdb
    log.info(f"duckdb {duckdb.__version__}")
except ImportError:
    log.critical("pip install duckdb"); sys.exit(1)

try:
    import pandas as pd
    log.info(f"pandas {pd.__version__}")
except ImportError:
    log.critical("pip install pandas"); sys.exit(1)

try:
    from dat_loader import read_fwf_dat, read_pipe_dat, read_sas7bdat_file
except ImportError:
    log.critical("dat_loader.py 없음"); sys.exit(1)


# ══════════════════════════════════════════════
# 설정
# ══════════════════════════════════════════════
ROOT      = Path(__file__).parent
ENCODINGS = ["cp949", "utf-8"]
FILE_EXTENSIONS = [".zip", ".dat.gz", ".DAT", ".dat", ".prn", ".csv", ".csv.gz", ".sas7bdat"]

PREFIX_VAL = "val_"    # 검증용 임시 → validate 후 DROP
PREFIX_OUT = "out_"    # Excel 출력 대상 → 시트명 = out_ 뗀 이름


# ══════════════════════════════════════════════
# 테이블 레지스트리
#
# month_col : 월별 누적 시 해당 월 식별에 사용할 컬럼명
#             지정 안 하면 테이블 전체 교체
# ══════════════════════════════════════════════
TABLE_REGISTRY = {

    # ── 고정폭 (fwf) ──────────────────────────
    "fio841": {
        "type": "fwf",
        "file": "fioBtLtrJ841_01_{YYYYMM}.DAT",
        "desc": "수입보험료(유지비·신계약비 원천)",
        "month_col": "CLS_YYMM",
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

    # ── 파이프 구분자 (pipe) ──────────────────
    "erp": {
        "type": "pipe",
        "file": "BS100_{YYYYMM}.DAT",
        "desc": "ERP 전표 (BS100)",
        "month_col": "SLPDT",
        "cols": [
            "SLPDT", "CLS_DA_SEQNO", "NTACC_CD", "DP_ORGCD",
            "IKD_GRPCD", "INS_IMCD", "RR_ORGCD", "SLP_AMT_DFRN_YN",
            "SLPNO", "SLP_LNNO", "BZCS_01_DVCD", "BZCS_02_DVCD",
            "FNDCD", "WONCR_POAMT", "NOTS_MTT", "BZCS_DV_EXEC_YN",
            "INP_USR_ID", "INP_DTHMS", "MDF_USR_ID", "MDF_DTHMS",
        ],
    },

    "sa01": {
        "type": "pipe",
        "file": "RS100_{YYYYMM}.DAT",
        "desc": "유지비·신계약비 배분결과 (RS100)",
        "month_col": "SLPDT",
        "cols": [
            "SLPDT", "SLPNO", "SLP_LNNO", "SNO", "RR_ORGCD",
            "BZCS_TPCD", "INS_IMCD", "SL_TPCD", "CVRCD",
            "DV_RT", "DVAMT", "INP_USR_ID", "INP_DTHMS",
            "MDF_USR_ID", "MDF_DTHMS", "NTACC_CD", "SUB_SNO",
        ],
    },

    "sa02": {
        "type": "pipe",
        "file": "RS101_{YYYYMM}.DAT",
        "desc": "손해조사비 배분결과 (RS101)",
        "month_col": "SLPDT",
        "cols": [
            "SLPDT", "SLPNO", "SLP_LNNO", "SNO", "RR_ORGCD",
            "BZCS_TPCD", "INS_IMCD", "SL_TPCD", "CVRCD",
            "CR_CVRCD", "DV_RT", "DVAMT", "BZCS_INS_IMCD",
            "INP_USR_ID", "INP_DTHMS", "MDF_USR_ID", "MDF_DTHMS",
            "NTACC_CD",
        ],
    },

    "bs101": {
        "type": "pipe",
        "file": "BS101_{YYYYMM}.DAT",
        "desc": "원수실적마감 (BS101)",
        "month_col": "CLS_YYMM",
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
        "file": "BS104_{YYYYMM}.DAT",
        "desc": "지급보험금마감 (BS104)",
        "month_col": "CLS_YYMM",
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
        "file": "BS105_{YYYYMM}.DAT",
        "desc": "지급준비금마감 (BS105)",
        "month_col": "CLS_YYMM",
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

NUMERIC_COLS = {
    "fio841": ["INCM_PRM_CR_SEQNO", "PYM_SEQ", "RP_PRM", "AF_PRM",
               "CONDT_I_PRM", "MDF_CVAV", "MPY_CV_PRM"],
    "fio843": ["AP_PRM", "BZCS_DV_PS"],
    "erp":    ["CLS_DA_SEQNO", "WONCR_POAMT"],
    "sa01":   ["DV_RT", "DVAMT"],
    "sa02":   ["DV_RT", "DVAMT"],
    "bs101":  ["CLS_DA_SEQNO", "INCM_PRM_CR_SEQNO", "PYM_SEQ",
               "RP_PRM", "AP_PRM", "CONDT_I_PRM", "MDF_CVAV", "MPY_CV_PRM"],
    "bs104":  ["CLS_DA_SEQNO", "AP_PRM", "PY_IBAMT",
               "BZCS_DV_CLMCT", "BZCS_DV_PYCT"],
    "bs105":  ["CLS_DA_SEQNO", "AP_PRM", "ISAMT", "PY_RFAMT"],
}


# ══════════════════════════════════════════════
# JOB 정의
#
# 3개 JOB이 순차 실행됨
# 각 JOB: load(테이블) → logic → validate → export
#
# ⚠ 아래 테이블 배분은 SAS JOB 순서에 맞게 조정하세요
# ══════════════════════════════════════════════
JOBS = [
    {
        "name": "job1",
        "desc": "원천 데이터 집계 (보험료)",
        "tables": ["fio841", "fio843"],
        "export_sheets": {
            "fio841"          : "fio841",
            "fio843"          : "fio843",
            "BASE_DATA_CH"    : "집계_841보험료",
            "BASE_DATA_CH_843": "집계_843보험료",
            "BASE_DATA_CH2"   : "집계_843건수",
            "CH"              : "취급기관이상체크",
        },
    },
    {
        "name": "job2",
        "desc": "배분 결과 (SA 변환·집계)",
        "tables": ["sa01", "sa02"],
        "export_sheets": {
            "sa01"  : "SA01_유지비배분",
            "sa02"  : "SA02_손해조사비배분",
            # sa11, sa12, sa20 은 중간 테이블 — 필요하면 추가
        },
    },
    {
        "name": "job3",
        "desc": "마감 검증 (ERP·BS)",
        "tables": ["erp", "bs101", "bs104", "bs105"],
        "export_sheets": {
            "erp"   : "ERP_BS100",
            "bs101" : "BS101_원수실적",
            "bs104" : "BS104_지급보험금",
            "bs105" : "BS105_지급준비금",
        },
    },
]


# ══════════════════════════════════════════════
# 유틸
# ══════════════════════════════════════════════

def _dw(s):
    """터미널 표시 너비 (한글 2칸)"""
    return sum(2 if unicodedata.east_asian_width(c) in ('W', 'F') else 1 for c in s)


def _exec(con, label, sql):
    """SQL 실행 + CREATE TABLE이면 건수 로깅"""
    t = time.time()
    con.execute(sql)
    m = re.search(r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)", sql, re.IGNORECASE)
    if m:
        tbl = m.group(1)
        cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        log.info(f"  {label + ' ' * max(0, 50 - _dw(label))}  {cnt:>12,}건  ({time.time()-t:.1f}초)")


def _resolve_path(base, file_template, yyyymm):
    """파일 확장자 순서대로 탐색"""
    stem = Path(file_template.format(YYYYMM=yyyymm)).stem
    if stem.lower().endswith(".dat"):
        stem = stem[:-4]
    for ext in FILE_EXTENSIONS:
        candidate = base / f"{stem}{ext}"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"파일 없음: {stem}.* (위치: {base})")


def _table_exists(con, name):
    cnt = con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_schema='main' AND table_name='{name}'"
    ).fetchone()[0]
    return cnt > 0


def _drop_prefix(con, prefix):
    """prefix로 시작하는 테이블 전부 DROP"""
    tbls = [r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall() if r[0].startswith(prefix)]
    for t in tbls:
        con.execute(f"DROP TABLE IF EXISTS {t}")
    return tbls


# ══════════════════════════════════════════════
# LOAD — 파일 → DuckDB (월별 누적)
#
# 전략:
#   테이블 없으면 → CREATE
#   month_col 있으면 → 해당 월 DELETE 후 INSERT (누적)
#   month_col 없으면 → 전체 교체
# ══════════════════════════════════════════════

def _load_file(path, cfg, name):
    """단일 파일 읽기 → DataFrame"""
    if cfg["type"] == "fwf":
        return read_fwf_dat(path, cfg["cols"],
                            numeric=NUMERIC_COLS.get(name, []),
                            encodings=ENCODINGS)
    elif cfg["type"] == "pipe":
        return read_pipe_dat(path, cfg["cols"],
                             numeric=NUMERIC_COLS.get(name, []),
                             encodings=ENCODINGS,
                             delimiter=cfg.get("delimiter", "|"))
    elif cfg["type"] == "sas7bdat":
        return read_sas7bdat_file(path,
                                  numeric=NUMERIC_COLS.get(name, []),
                                  encoding=cfg.get("encoding", "cp949"))
    raise ValueError(f"Unknown type: {cfg['type']}")


def _upsert(con, name, df, yyyymm, month_col):
    """월별 누적 적재: 해당 월 DELETE → INSERT"""
    tmp = f"_df_{name}"
    con.register(tmp, df)

    if not _table_exists(con, name):
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM {tmp}")
        return len(df)

    if month_col:
        con.execute(f"DELETE FROM {name} WHERE {month_col} = '{yyyymm}'")
    else:
        con.execute(f"DELETE FROM {name}")

    con.execute(f"INSERT INTO {name} SELECT * FROM {tmp}")
    return len(df)


def do_load(con, yyyymm, table_names):
    """테이블 목록 로드"""
    base_path = ROOT / "data" / yyyymm
    loaded, failed = [], []

    for name in table_names:
        cfg = TABLE_REGISTRY.get(name)
        if not cfg:
            log.error(f"[{name}] 레지스트리에 없음")
            failed.append(name)
            continue
        try:
            path = _resolve_path(base_path, cfg["file"], yyyymm)
            log.info(f"  [{name}] {cfg['desc']}  ← {path.name}")
            ts = time.time()
            df = _load_file(path, cfg, name)
            cnt = _upsert(con, name, df, yyyymm, cfg.get("month_col"))
            log.info(f"  [{name}] {cnt:,}건 적재  ({time.time()-ts:.1f}초)")
            loaded.append(name)
        except FileNotFoundError as e:
            log.warning(f"  [{name}] 파일 없음 — 건너뜀")
            failed.append(name)
        except Exception as e:
            log.error(f"  [{name}] 실패: {e}")
            failed.append(name)

    return loaded, failed


# ══════════════════════════════════════════════
# LOGIC — 비즈니스 로직 (JOB별)
# ══════════════════════════════════════════════

def logic_job1(con, yyyymm):
    """보험료 집계"""

    _exec(con, "BASE_DATA_CH (841 집계)", """
        CREATE OR REPLACE TABLE BASE_DATA_CH AS
        SELECT INS_IMCD, SUM(RP_PRM) AS RP_PRM, SUM(AP_PRM) AS AP_PRM
        FROM fio841
        GROUP BY INS_IMCD
    """)

    _exec(con, "BASE_DATA_CH_843 (843 집계)", """
        CREATE OR REPLACE TABLE BASE_DATA_CH_843 AS
        SELECT INS_IMCD, SUM(AP_PRM) AS AP_PRM
        FROM fio843
        GROUP BY INS_IMCD
    """)

    _exec(con, "BASE_DATA_CH2 (843 건수집계)", """
        CREATE OR REPLACE TABLE BASE_DATA_CH2 AS
        SELECT CLS_YYMM, INS_IMCD, SUM(AP_PRM) AS AP_PRM, SUM(BZCS_DV_PS) AS BZCS_DV_PS
        FROM fio843
        GROUP BY CLS_YYMM, INS_IMCD
    """)

    _exec(con, "CH (취급기관 이상체크)", """
        CREATE OR REPLACE TABLE CH AS
        SELECT DISTINCT DH_ORGCD, DH_STFNO
        FROM fio843
        WHERE SUBSTR(DH_STFNO, 1, 2) IN ('2S')
          AND SUBSTR(DH_ORGCD, 1, 1) NOT IN ('E')
    """)


def logic_job2(con, yyyymm):
    """SA 변환 → 집계"""

    _exec(con, "SA11 (sa01 변환)", """
        CREATE OR REPLACE TABLE sa11 AS
        SELECT SLPDT AS YYYYMM, NTACC_CD, BZCS_TPCD, RR_ORGCD, SL_TPCD, INS_IMCD,
               CASE WHEN CVRCD = '*' THEN '0' ELSE CVRCD END AS DGB,
               DVAMT AS S
        FROM sa01
    """)

    _exec(con, "SA12 (sa02 변환)", """
        CREATE OR REPLACE TABLE sa12 AS
        SELECT SLPDT AS YYYYMM, NTACC_CD, BZCS_TPCD, RR_ORGCD, SL_TPCD, INS_IMCD,
               CASE WHEN CVRCD = '*' THEN '0' ELSE CVRCD END AS DGB,
               DVAMT AS S
        FROM sa02
    """)

    _exec(con, "SA20 (SA11 + SA12)", """
        CREATE OR REPLACE TABLE sa20 AS
        SELECT * FROM sa11
        UNION ALL
        SELECT * FROM sa12
    """)

    _exec(con, f"SA_{yyyymm} (최종 배분집계)", f"""
        CREATE OR REPLACE TABLE sa_{yyyymm} AS
        SELECT DISTINCT YYYYMM, NTACC_CD, RR_ORGCD, BZCS_TPCD, SL_TPCD, INS_IMCD,
               DGB AS CVRCD, SUM(S) AS S
        FROM sa20
        GROUP BY YYYYMM, NTACC_CD, RR_ORGCD, BZCS_TPCD, SL_TPCD, INS_IMCD, DGB
    """)

    # ── 이연 계산 (ey_a2601 필요) ──────────────
    if _table_exists(con, "ey_a2601"):
        _exec(con, "tmp_01 (이연액 매핑)", """
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

        _exec(con, "tmp_02 (상각비 매핑)", """
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

        _exec(con, "TEMP01 (이연 집계)", """
            CREATE OR REPLACE TABLE TEMP01 AS
            SELECT DISTINCT YYYYMM, GB, NTACC_CD, BGB, CGB, SUM(S) AS S
            FROM (SELECT * FROM tmp_01 UNION ALL SELECT * FROM tmp_02)
            GROUP BY YYYYMM, GB, NTACC_CD, BGB, CGB
        """)

        con.execute("DROP TABLE IF EXISTS tmp_01")
        con.execute("DROP TABLE IF EXISTS tmp_02")
    else:
        log.warning("  ey_a2601 없음 — 이연 계산 스킵")

    # ── 순사업비 요약 ──────────────────────────
    _exec(con, "out_SA000 (순사업비 요약)", f"""
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

    _exec(con, "out_SA001 (신계약비·수금비 추세)", f"""
        CREATE OR REPLACE TABLE out_SA001 AS
        SELECT DISTINCT YYYYMM, NTACC_CD, SUBSTR(INS_IMCD,1,2) AS BGB,
               SL_TPCD, SUM(S) AS S
        FROM sa_{yyyymm}
        WHERE SUBSTR(NTACC_CD,1,4) IN ('5121','5127','5128','5129')
        GROUP BY YYYYMM, NTACC_CD, BGB, SL_TPCD
    """)


def logic_job3(con, yyyymm):
    """마감 데이터 기반 로직 (필요하면 여기에 추가)"""
    pass


# ══════════════════════════════════════════════
# VALIDATE — 검증 (JOB별)
# ══════════════════════════════════════════════

def validate_job1(con, yyyymm):
    """job1 검증: 보험료 집계 건수 확인"""
    for tbl in ["fio841", "fio843", "BASE_DATA_CH", "BASE_DATA_CH_843", "BASE_DATA_CH2", "CH"]:
        if _table_exists(con, tbl):
            cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            log.info(f"  {tbl:25s}: {cnt:>10,}건")


def validate_job2(con, yyyymm):
    """job2 검증: 시산표 비교"""

    # 배분 결과 건수
    for tbl in ["sa11", "sa12", "sa20", f"sa_{yyyymm}"]:
        if _table_exists(con, tbl):
            cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            log.info(f"  {tbl:25s}: {cnt:>10,}건")

    # 전표 vs 배분결과 비교
    if _table_exists(con, "erp") and _table_exists(con, f"sa_{yyyymm}"):
        _exec(con, "val_전표집계", """
            CREATE OR REPLACE TABLE val_TEMP00 AS
            SELECT DISTINCT '전표' AS GB, NTACC_CD, SUM(WONCR_POAMT) AS S
            FROM erp GROUP BY GB, NTACC_CD
        """)
        _exec(con, "val_배분결과집계", f"""
            CREATE OR REPLACE TABLE val_TEMP01 AS
            SELECT DISTINCT '결과' AS GB, NTACC_CD, SUM(S) AS S
            FROM sa_{yyyymm} GROUP BY GB, NTACC_CD
        """)
        _exec(con, "val_시산표비교", """
            CREATE OR REPLACE TABLE val_TEMP02 AS
            SELECT * FROM val_TEMP00 UNION ALL SELECT * FROM val_TEMP01
        """)


def validate_job3(con, yyyymm):
    """job3 검증: IBNR 담보코드 누락, 자동차 TM/CM"""

    for tbl in ["erp", "bs101", "bs104", "bs105"]:
        if _table_exists(con, tbl):
            cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            log.info(f"  {tbl:25s}: {cnt:>10,}건")

    # bs104 IBNR 담보코드 누락 (0건이어야 정상)
    if _table_exists(con, "bs104"):
        _exec(con, "val_PY_IBAMT_CH (bs104 IBNR 누락)", """
            CREATE OR REPLACE TABLE val_PY_IBAMT_CH AS
            SELECT * FROM bs104
            WHERE IKD_GRPCD = 'LA' AND CVRCD IS NULL
        """)

    # bs105 IBNR 담보코드 누락 (0건이어야 정상)
    if _table_exists(con, "bs105"):
        _exec(con, "val_PY_RFAMT_CH (bs105 IBNR 누락)", """
            CREATE OR REPLACE TABLE val_PY_RFAMT_CH AS
            SELECT * FROM bs105
            WHERE IKD_GRPCD = 'LA' AND CLM_CVRCD IS NULL
        """)

    # 자동차 TM/CM 비비례 배분 누락 (0건이어야 정상)
    if _table_exists(con, f"sa_{yyyymm}") and _table_exists(con, "erp"):
        _exec(con, "val_CA_TMCM_Chk (자동차 TM/CM 오류)", f"""
            CREATE OR REPLACE TABLE val_CA_TMCM_Chk AS
            SELECT * FROM sa_{yyyymm}
            WHERE SUBSTR(BZCS_02_DVCD, 1, 1) = 'E'
              AND NTACC_CD IN (
                  SELECT DISTINCT NTACC_CD FROM erp
                  WHERE SUBSTR(BZCS_02_DVCD, 1, 1) = 'E'
              )
              AND SUBSTR(INS_IMCD, 1, 2) = 'CA'
              AND SL_TPCD = '21'
        """)

        # 재마감 자동차 금액 확인
        _exec(con, "val_CA_2601_Chk (재마감 자동차 금액)", f"""
            CREATE OR REPLACE TABLE val_CA_2601_Chk AS
            SELECT SUM(S) AS TOTAL_S FROM sa_{yyyymm}
            WHERE SUBSTR(BZCS_02_DVCD, 1, 1) = 'E'
              AND BZCS_02_DVCD NOT IN ('E07','E08','E09','E10','E11','E12','E13')
              AND NTACC_CD IN (
                  SELECT DISTINCT NTACC_CD FROM erp
                  WHERE SUBSTR(BZCS_02_DVCD, 1, 1) = 'E'
              )
              AND SUBSTR(INS_IMCD, 1, 2) = 'CA'
              AND SL_TPCD = '14'
        """)
        row = con.execute("SELECT TOTAL_S FROM val_CA_2601_Chk").fetchone()
        if row and row[0]:
            log.info(f"  val_CA_2601_Chk TOTAL_S = {row[0]:,.0f}  "
                     f"(기준: 재작업전 200,449,694 / 재작업후 107,627,489)")


# JOB → 함수 매핑
LOGIC_FN    = {0: logic_job1,    1: logic_job2,    2: logic_job3}
VALIDATE_FN = {0: validate_job1, 1: validate_job2, 2: validate_job3}


# ══════════════════════════════════════════════
# EXPORT — Excel 출력
# ══════════════════════════════════════════════

def do_export(con, yyyymm, job_name, sheet_map):
    """시트맵 기반 Excel 출력"""
    out_dir = ROOT / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{job_name}_{yyyymm}.xlsx"

    # out_ 접두사 테이블 자동 추가
    db_tables = [r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='main' ORDER BY table_name"
    ).fetchall()]
    for tbl in db_tables:
        if tbl.startswith(PREFIX_OUT) and tbl not in sheet_map:
            sheet_map[tbl] = tbl[len(PREFIX_OUT):]

    summary = []
    with pd.ExcelWriter(out_file, engine="openpyxl") as writer:
        for tbl, sheet in sheet_map.items():
            try:
                ts = time.time()
                df = con.execute(f"SELECT * FROM {tbl}").df()
                sname = sheet[:31]
                df.to_excel(writer, sheet_name=sname, index=False)
                el = time.time() - ts
                summary.append((tbl, sname, len(df), el))
                log.info(f"    {sname:25s}  {len(df):>10,}건  ({el:.1f}초)")
            except Exception as e:
                log.warning(f"    {sheet} 건너뜀: {e}")

        _write_summary_sheet(writer, yyyymm, summary)

    log.info(f"  → {out_file}")
    return out_file


def _write_summary_sheet(writer, yyyymm, summary):
    """요약 시트 생성"""
    from openpyxl.utils import get_column_letter
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

    wb = writer.book
    ws = wb.create_sheet("요약", 0)

    hdr_fill = PatternFill("solid", fgColor="1F4E79")
    hdr_font = Font(color="FFFFFF", bold=True, size=11)
    alt_fill = PatternFill("solid", fgColor="EBF3FB")
    link_font = Font(color="1F4E79", underline="single")
    center = Alignment(horizontal="center", vertical="center")
    left   = Alignment(horizontal="left",   vertical="center")
    right  = Alignment(horizontal="right",  vertical="center")
    thin   = Side(style="thin", color="BFBFBF")
    bdr    = Border(left=thin, right=thin, top=thin, bottom=thin)

    # 타이틀
    ws.merge_cells("A1:E1")
    c = ws["A1"]
    c.value = f"출력 요약  |  {yyyymm}  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    c.font, c.alignment = Font(bold=True, size=13, color="1F4E79"), left
    ws.row_dimensions[1].height = 28

    # 헤더
    for col, (h, w) in enumerate(zip(
        ["No", "테이블명", "시트명", "건수", "소요(초)"],
        [6, 28, 28, 16, 10]
    ), 1):
        c = ws.cell(row=2, column=col, value=h)
        c.fill, c.font, c.alignment, c.border = hdr_fill, hdr_font, center, bdr
        ws.column_dimensions[get_column_letter(col)].width = w

    # 데이터
    for i, (tbl, sname, cnt, el) in enumerate(summary, 1):
        row = i + 2
        fill = alt_fill if i % 2 == 0 else PatternFill()
        ws.cell(row=row, column=1, value=i).alignment = center
        ws.cell(row=row, column=2, value=tbl).alignment = left
        c_s = ws.cell(row=row, column=3, value=sname)
        c_s.hyperlink, c_s.font, c_s.alignment = f"#{sname}!A1", link_font, left
        c_c = ws.cell(row=row, column=4, value=cnt)
        c_c.number_format, c_c.alignment = "#,##0", right
        ws.cell(row=row, column=5, value=round(el, 1)).alignment = center
        for col in range(1, 6):
            ws.cell(row=row, column=col).border = bdr
            ws.cell(row=row, column=col).fill = fill

    # 합계
    sr = len(summary) + 3
    total = sum(r[2] for r in summary)
    ws.merge_cells(f"A{sr}:C{sr}")
    c_l = ws.cell(row=sr, column=1, value="합계")
    c_l.font, c_l.alignment, c_l.border = Font(bold=True), center, bdr
    c_t = ws.cell(row=sr, column=4, value=total)
    c_t.number_format, c_t.font, c_t.alignment, c_t.border = "#,##0", Font(bold=True), right, bdr


# ══════════════════════════════════════════════
# JOB 실행기
# ══════════════════════════════════════════════

def run_job(con, job_idx, yyyymm, skip_load=False):
    """단일 JOB 실행: load → logic → validate → export"""
    job = JOBS[job_idx]
    name = job["name"]

    log.info("=" * 60)
    log.info(f"[{name}] {job['desc']}  (월: {yyyymm})")
    log.info("=" * 60)
    t0 = time.time()

    # 1. LOAD
    if skip_load:
        log.info(f"[{name}] LOAD 스킵 (--skip-load)")
    else:
        log.info(f"[{name}] LOAD")
        loaded, failed = do_load(con, yyyymm, job["tables"])
        if failed:
            log.warning(f"[{name}] 로드 실패: {failed}")

    # 2. LOGIC
    logic_fn = LOGIC_FN.get(job_idx)
    if logic_fn:
        log.info(f"[{name}] LOGIC")
        logic_fn(con, yyyymm)

    # 3. VALIDATE
    validate_fn = VALIDATE_FN.get(job_idx)
    if validate_fn:
        log.info(f"[{name}] VALIDATE")
        validate_fn(con, yyyymm)

    # val_ 임시 테이블 정리
    dropped = _drop_prefix(con, PREFIX_VAL)
    if dropped:
        log.info(f"[{name}] 임시 정리: {dropped}")

    # 4. EXPORT
    log.info(f"[{name}] EXPORT")
    do_export(con, yyyymm, name, dict(job["export_sheets"]))

    log.info(f"[{name}] 완료  소요: {time.time()-t0:.1f}초")


# ══════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="SAS → DuckDB 사업비배분 파이프라인 (3-JOB 순차)",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
예시:
  python sas_to_duckdb.py --ym 202601                      # 전체 (job1→2→3)
  python sas_to_duckdb.py --ym 202601 --job 1              # job1만
  python sas_to_duckdb.py --ym 202601 --job 2 3            # job2, job3
  python sas_to_duckdb.py --ym 202601 --job 1 --skip-load  # 로드 스킵
  python sas_to_duckdb.py --list                            # 테이블 목록
        """
    )
    parser.add_argument("--ym", type=str, default="202601",
                        help="처리할 년월 (기본: 202601)")
    parser.add_argument("--job", "-j", nargs="+", type=int, default=None,
                        help="실행할 JOB 번호 (1, 2, 3)")
    parser.add_argument("--skip-load", action="store_true",
                        help="LOAD 단계 생략 (이미 적재된 데이터 사용)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="DEBUG 로그 콘솔 출력")
    parser.add_argument("--list", "-l", action="store_true",
                        help="JOB·테이블 목록 출력")

    args = parser.parse_args()

    if args.verbose:
        for h in log.handlers:
            if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler):
                h.setLevel(logging.DEBUG)

    if args.list:
        for i, job in enumerate(JOBS):
            log.info(f"  JOB {i+1}: {job['name']} — {job['desc']}")
            for t in job["tables"]:
                cfg = TABLE_REGISTRY[t]
                log.info(f"    {t:10s}  [{cfg['type']:4s}]  {cfg['desc']}")
        return

    yyyymm = args.ym

    # DB: 월별이 아닌 하나의 DB에 누적
    db_path = ROOT / "db" / "pipeline.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    log.info(f"대상 월: {yyyymm}")
    log.info(f"DB    : {db_path}")

    # 실행할 JOB 결정 (기본: 전체)
    job_nums = args.job or [1, 2, 3]
    invalid = [j for j in job_nums if j not in (1, 2, 3)]
    if invalid:
        log.error(f"잘못된 JOB 번호: {invalid}  (1, 2, 3 중 선택)")
        return

    con = duckdb.connect(str(db_path))
    try:
        t_total = time.time()
        for num in job_nums:
            run_job(con, num - 1, yyyymm, skip_load=args.skip_load)
        log.info(f"전체 완료  총 소요: {time.time()-t_total:.1f}초")
    finally:
        con.close()


if __name__ == "__main__":
    main()
