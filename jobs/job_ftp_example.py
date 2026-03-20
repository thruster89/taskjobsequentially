"""
JOB 예시: FTP 다운로드 (prejob) → 로드 → 집계

사용법:
  python sas_to_duckdb.py --ym 202601 --job jobs/job_ftp_example.py
  python sas_to_duckdb.py --ym 202601 --job jobs/job_ftp_example.py --stage prejob   # 다운로드만
  python sas_to_duckdb.py --ym 202601 --job jobs/job_ftp_example.py --stage load     # 로드만 (이미 다운로드 됨)
"""
from ftplib import FTP
from pathlib import Path
from sas_to_duckdb import sql, check, ROOT

NAME = "ftp_example"
DESC = "FTP 다운로드 → 로드 예시"

# ══════════════════════════════════════════════
# FTP 접속 정보
# ══════════════════════════════════════════════
FTP_HOST = "서버주소"
FTP_USER = "아이디"
FTP_PASS = "비밀번호"
FTP_DIR  = "/서버/폴더/경로"


def prejob(yyyymm):
    """LOAD 전에 FTP에서 파일 다운로드 → data/{yyyymm}/ 폴더에 저장"""
    local_dir = ROOT / "data" / yyyymm
    local_dir.mkdir(parents=True, exist_ok=True)

    with FTP(FTP_HOST) as ftp:
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_DIR)
        files = ftp.nlst()
        for fname in files:
            local_path = local_dir / fname
            with open(local_path, "wb") as f:
                ftp.retrbinary(f"RETR {fname}", f.write)


# ══════════════════════════════════════════════
# 테이블 정의 (다운받은 파일을 로드)
# ══════════════════════════════════════════════
TABLES = {
    "sample_data": {
        "type": "pipe",
        "file": "sample_{yyyymm}.DAT",
        "month_col": "BASE_YM",
        "cols": ["BASE_YM", "CODE", "AMT"],
        "numeric": ["AMT"],
    },
}


def logic(con, yyyymm):
    sql(con, f"""
        CREATE OR REPLACE TABLE sample_agg AS
        SELECT CODE, SUM(AMT) AS TOTAL_AMT
        FROM sample_data
        WHERE BASE_YM = '{yyyymm}'
        GROUP BY CODE
    """)


def validate(con, yyyymm):
    check(con, "sample_agg", min_rows=1)
