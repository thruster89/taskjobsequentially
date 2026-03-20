"""
JOB 예시: FTP 다운로드 (prejob) → 로드 → 집계

사용법:
  python sas_to_duckdb.py --ym 202601 --job jobs/job_ftp_example.py
  python sas_to_duckdb.py --ym 202601 --job jobs/job_ftp_example.py --stage prejob   # 다운로드만
  python sas_to_duckdb.py --ym 202601 --job jobs/job_ftp_example.py --stage load     # 로드만

사전 설정:
  ftp_config.example.py → ftp_config.py 복사 후 비밀번호 입력
"""
import logging
from ftplib import FTP
from pathlib import Path
from sas_to_duckdb import sql, check, ROOT

log = logging.getLogger("pipeline")

NAME = "ftp_example"
DESC = "FTP 다운로드 → 로드 예시"

# ══════════════════════════════════════════════
# FTP 접속 정보 (ftp_config.py에서 관리)
# ══════════════════════════════════════════════
from ftp_config import FTP_IFRS4


def download_ftp(cfg, yyyymm, patterns=None, excludes=None):
    """FTP에서 파일 다운로드 → data/{yyyymm}/

    cfg      : FTP_IFRS4 등 접속 정보 dict
    patterns : 다운받을 파일명 패턴 리스트 (None이면 전체)
               예: ["btLtrJ930_020_", "RS100_"]
    excludes : 제외할 패턴 리스트
               예: ["_all_"]
    """
    host = cfg["host"]
    port = cfg.get("port", 21)
    log.info(f"  [FTP] 연결 시도: {host}:{port}")

    local_dir = ROOT / "data" / yyyymm
    local_dir.mkdir(parents=True, exist_ok=True)

    with FTP() as ftp:
        ftp.encoding = cfg.get("encoding", "utf-8")
        ftp.connect(host, port, timeout=10)
        log.info(f"  [FTP] 접속 성공")
        ftp.login(cfg["user"], cfg["password"])
        log.info(f"  [FTP] 로그인: {cfg['user']}")
        remote_dir = cfg.get("remote_dir", "/")
        ftp.cwd(remote_dir)
        log.info(f"  [FTP] 원격 경로: {ftp.pwd()}")
        log.info(f"  [FTP] 로컬 경로: {local_dir}")

        files = ftp.nlst()
        log.info(f"  [FTP] 전체 파일: {len(files)}개")

        if patterns:
            log.info(f"  [FTP] 패턴 필터: {patterns}")
            files = [f for f in files
                     if any(p in f for p in patterns)]
        if excludes:
            log.info(f"  [FTP] 제외 필터: {excludes}")
            files = [f for f in files
                     if not any(x in f for x in excludes)]

        log.info(f"  [FTP] 대상 파일: {len(files)}개")
        for fname in files:
            local_path = local_dir / fname
            if local_path.exists():
                log.info(f"  [FTP] 이미 존재, 스킵: {fname}")
                continue
            with open(local_path, "wb") as f:
                ftp.retrbinary(f"RETR {fname}", f.write)
            log.info(f"  [FTP] 다운로드: {fname}")


def prejob(yyyymm):
    """LOAD 전에 FTP에서 필요한 파일만 다운로드"""
    log.info(f"  [FTP] config: host={FTP_IFRS4.get('host')}, port={FTP_IFRS4.get('port')}")
    download_ftp(FTP_IFRS4, yyyymm, patterns=[
        "btLtrJ930_020_",       # 930_020 파일
        # "RS100_",             # 필요한 패턴 추가
    ], excludes=[
        "_all_",                # 020_all 제외
    ])


# ══════════════════════════════════════════════
# 테이블 정의 (다운받은 파일을 로드)
# ══════════════════════════════════════════════
TABLES = {
    "bt930_020": {
        "type": "pipe",
        "file": "btLtrJ930_020_{yyyymm}??_*.dat.gz",
        "desc": "930_020",
        "month_col": None,
        "cols": ["COL1", "COL2", "COL3"],
    },
    "bt930_020_all": {
        "type": "pipe",
        "file": "btLtrJ930_020_all_{yyyymm}??_*.dat.gz",
        "desc": "930_020 전체(all)",
        "month_col": None,
        "cols": ["COL1", "COL2", "COL3"],
    },
}


def logic(con, yyyymm):
    sql(con, f"""
        CREATE OR REPLACE TABLE sample_agg AS
        SELECT COL1, COUNT(*) AS CNT
        FROM bt930_020
        GROUP BY COL1
    """)


def validate(con, yyyymm):
    check(con, "sample_agg", min_rows=1)
