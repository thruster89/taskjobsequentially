"""
SAS → DuckDB 전환 파이프라인 (범용 실행기)

JOB 파일을 지정하여 실행합니다. 각 JOB 파일은 독립적으로 정의됩니다.

실행 예시:
  python sas_to_duckdb.py --ym 202601 --job jobs/job1.py
  python sas_to_duckdb.py --ym 202601 --job jobs/job2.py --skip-load
  python sas_to_duckdb.py --ym 202601 --job jobs/job1.py jobs/job2.py jobs/job3.py
  python sas_to_duckdb.py --ym 202601 --job jobs/job1.py --stage load
  python sas_to_duckdb.py --ym 202601 --job jobs/job1.py --stage logic validate
"""

import re
import sys
import logging
import time
import argparse
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
import importlib.util
from pathlib import Path
from datetime import datetime

# ══════════════════════════════════════════════
# 로거
# ══════════════════════════════════════════════
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("pipeline")
    if logger.handlers:
        return logger
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
try:
    import duckdb
except ImportError:
    log.critical("pip install duckdb"); sys.exit(1)

try:
    import pandas as pd
except ImportError:
    log.critical("pip install pandas"); sys.exit(1)

try:
    from dat_loader import read_fwf_dat, read_pipe_dat, read_sas7bdat_file
except ImportError:
    log.critical("dat_loader.py 없음"); sys.exit(1)


# ══════════════════════════════════════════════
# 설정
# ══════════════════════════════════════════════
ROOT = Path(__file__).parent
MAX_READ_WORKERS = 4
ENCODINGS = ["cp949", "utf-8"]
FILE_EXTENSIONS = [".zip", ".dat.gz", ".DAT", ".dat", ".prn", ".csv", ".csv.gz", ".sas7bdat"]

PREFIX_OUT = "out_"


# ══════════════════════════════════════════════
# 유틸 (JOB 파일에서도 import하여 사용)
# ══════════════════════════════════════════════

def _dw(s):
    """터미널 표시 너비 (한글 2칸)"""
    return sum(2 if unicodedata.east_asian_width(c) in ('W', 'F') else 1 for c in s)


def sql(con, label, query):
    """SQL 실행 + CREATE TABLE이면 건수 로깅"""
    t = time.time()
    con.execute(query)
    m = re.search(r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)",
                  query, re.IGNORECASE)
    if m:
        tbl = m.group(1)
        cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        log.info(f"  {label + ' ' * max(0, 50 - _dw(label))}  {cnt:>12,}건  ({time.time()-t:.1f}초)")


def table_exists(con, name):
    """테이블 존재 여부"""
    cnt = con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_schema='main' AND table_name='{name}'"
    ).fetchone()[0]
    return cnt > 0


def check(con, label, query, expect="zero"):
    """
    검증용 헬퍼. SELECT 결과를 로깅만 하고 테이블은 만들지 않음.

    expect:
      "zero"    — 0건이어야 정상 (이상 데이터 탐지)
      "nonzero" — 1건 이상이어야 정상 (데이터 존재 확인)
      int       — 정확히 N건이어야 정상
    """
    row = con.execute(query).fetchone()
    cnt = row[0] if row else 0
    if expect == "zero":
        ok = cnt == 0
    elif expect == "nonzero":
        ok = cnt > 0
    else:
        ok = cnt == expect
    mark = "OK" if ok else "NG"
    log.info(f"  [{mark}] {label + ' ' * max(0, 45 - _dw(label))}  {cnt:>12,}건")
    return ok


def row_count(con, table):
    """테이블 건수 로깅"""
    if not table_exists(con, table):
        log.warning(f"  [--] {table:45s}  테이블 없음")
        return 0
    cnt = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    log.info(f"  [OK] {table + ' ' * max(0, 45 - _dw(table))}  {cnt:>12,}건")
    return cnt


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


def _load_file(path, cfg, name):
    """단일 파일 읽기 → DataFrame"""
    numeric = cfg.get("numeric", [])
    if cfg["type"] == "fwf":
        return read_fwf_dat(path, cfg["cols"], numeric=numeric, encodings=ENCODINGS)
    elif cfg["type"] == "pipe":
        return read_pipe_dat(path, cfg["cols"], numeric=numeric,
                             encodings=ENCODINGS, delimiter=cfg.get("delimiter", "|"))
    elif cfg["type"] == "sas7bdat":
        return read_sas7bdat_file(path, numeric=numeric,
                                  encoding=cfg.get("encoding", "cp949"))
    raise ValueError(f"Unknown type: {cfg['type']}")


def _upsert(con, name, df, yyyymm, month_col):
    """월별 누적 적재: 해당 월 DELETE → INSERT"""
    tmp = f"_df_{name}"
    con.register(tmp, df)

    if not table_exists(con, name):
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM {tmp}")
        return len(df)

    if month_col:
        con.execute(f"DELETE FROM {name} WHERE {month_col} = '{yyyymm}'")
    else:
        log.warning(f"  [{name}] month_col=null → 전체 교체 (기존 데이터 삭제)")
        con.execute(f"DELETE FROM {name}")

    con.execute(f"INSERT INTO {name} SELECT * FROM {tmp}")
    return len(df)


# ══════════════════════════════════════════════
# JOB 모듈 로더
# ══════════════════════════════════════════════

def load_job_module(job_path: str):
    """
    JOB 파일(.py)을 동적 로드.

    JOB 파일 필수 정의:
      NAME          : str   — JOB 이름 (예: "job1")
      TABLES        : dict  — 테이블 정의 {name: {type, file, cols, ...}}
      logic(con, yyyymm)    — 비즈니스 로직
      validate(con, yyyymm) — 검증 로직

    선택 정의:
      DESC          : str   — 설명
      EXPORT_SHEETS : dict  — {table_name: sheet_name}
    """
    path = Path(job_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"JOB 파일 없음: {path}")

    spec = importlib.util.spec_from_file_location(path.stem, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    for attr in ("NAME", "TABLES", "logic", "validate"):
        if not hasattr(mod, attr):
            raise AttributeError(f"JOB 파일에 '{attr}' 없음: {path}")

    return mod


# ══════════════════════════════════════════════
# 파이프라인 (load → logic → validate → export)
# ══════════════════════════════════════════════

def _read_one(name, cfg, base_path, yyyymm):
    """단일 테이블 파일 읽기 (병렬 실행용)"""
    if "month_col" not in cfg:
        raise KeyError(
            f"[{name}] TABLES 정의에 'month_col' 필수 "
            f"(누적: 컬럼명 / 전체교체: null)")
    path = _resolve_path(base_path, cfg["file"], yyyymm)
    log.info(f"  [{name}] {cfg.get('desc', '')}  ← {path.name}")
    ts = time.time()
    df = _load_file(path, cfg, name)
    log.info(f"  [{name}] {len(df):,}건 읽기완료  ({time.time()-ts:.1f}초)")
    return name, cfg, df


def do_load(con, yyyymm, tables: dict):
    """TABLES dict 기반 로드 (파일 읽기 병렬, DB 적재 순차)"""
    base_path = ROOT / "data" / yyyymm
    loaded, failed = [], []
    results = []

    # 1) 파일 읽기 — 병렬
    workers = min(MAX_READ_WORKERS, len(tables))
    log.info(f"  파일 읽기 병렬 시작 (workers={workers}, 테이블={len(tables)}개)")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_read_one, name, cfg, base_path, yyyymm): name
            for name, cfg in tables.items()
        }
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                results.append(fut.result())
            except FileNotFoundError:
                log.warning(f"  [{name}] 파일 없음 — 건너뜀")
                failed.append(name)
            except Exception as e:
                log.error(f"  [{name}] 읽기 실패: {e}")
                failed.append(name)

    # 2) DB 적재 — 순차 (DuckDB 단일 커넥션)
    for name, cfg, df in results:
        try:
            ts = time.time()
            cnt = _upsert(con, name, df, yyyymm, cfg.get("month_col"))
            log.info(f"  [{name}] {cnt:,}건 적재  ({time.time()-ts:.1f}초)")
            loaded.append(name)
        except Exception as e:
            log.error(f"  [{name}] 적재 실패: {e}")
            failed.append(name)

    return loaded, failed


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

    ws.merge_cells("A1:E1")
    c = ws["A1"]
    c.value = f"출력 요약  |  {yyyymm}  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    c.font, c.alignment = Font(bold=True, size=13, color="1F4E79"), left
    ws.row_dimensions[1].height = 28

    for col, (h, w) in enumerate(zip(
        ["No", "테이블명", "시트명", "건수", "소요(초)"], [6, 28, 28, 16, 10]
    ), 1):
        c = ws.cell(row=2, column=col, value=h)
        c.fill, c.font, c.alignment, c.border = hdr_fill, hdr_font, center, bdr
        ws.column_dimensions[get_column_letter(col)].width = w

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

    sr = len(summary) + 3
    total = sum(r[2] for r in summary)
    ws.merge_cells(f"A{sr}:C{sr}")
    c_l = ws.cell(row=sr, column=1, value="합계")
    c_l.font, c_l.alignment, c_l.border = Font(bold=True), center, bdr
    c_t = ws.cell(row=sr, column=4, value=total)
    c_t.number_format, c_t.font, c_t.alignment, c_t.border = "#,##0", Font(bold=True), right, bdr


def run_job(con, job_mod, yyyymm, skip_load=False, stages=None,
            only_tables=None):
    """단일 JOB 실행: load → logic → validate → export"""
    name = job_mod.NAME
    desc = getattr(job_mod, "DESC", "")
    # stages가 지정되면 해당 단계만 실행, 아니면 전체 실행
    run_all = stages is None

    log.info("=" * 60)
    log.info(f"[{name}] {desc}  (월: {yyyymm})")
    if not run_all:
        log.info(f"[{name}] 선택 단계: {stages}")
    if only_tables:
        log.info(f"[{name}] 선택 테이블: {only_tables}")
    log.info("=" * 60)
    t0 = time.time()

    # 1. LOAD
    if run_all and skip_load:
        log.info(f"[{name}] LOAD 스킵")
    elif run_all or "load" in stages:
        tables = job_mod.TABLES
        if only_tables:
            unknown = set(only_tables) - set(tables)
            if unknown:
                log.warning(f"[{name}] 존재하지 않는 테이블 무시: {unknown}")
            tables = {k: v for k, v in tables.items() if k in only_tables}
            if not tables:
                log.warning(f"[{name}] 로드할 테이블이 없습니다")
        log.info(f"[{name}] LOAD ({len(tables)}개 테이블)")
        loaded, failed = do_load(con, yyyymm, tables)
        if failed:
            log.warning(f"[{name}] 로드 실패: {failed}")

    # 2. LOGIC
    if run_all or "logic" in stages:
        log.info(f"[{name}] LOGIC")
        job_mod.logic(con, yyyymm)

    # 3. VALIDATE
    if run_all or "validate" in stages:
        log.info(f"[{name}] VALIDATE")
        job_mod.validate(con, yyyymm)

    # 4. EXPORT
    if run_all or "export" in stages:
        sheets = dict(getattr(job_mod, "EXPORT_SHEETS", {}))
        if sheets:
            log.info(f"[{name}] EXPORT")
            do_export(con, yyyymm, name, sheets)

    log.info(f"[{name}] 완료  소요: {time.time()-t0:.1f}초")


# ══════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="SAS → DuckDB 사업비배분 파이프라인",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
예시:
  python sas_to_duckdb.py --ym 202601 --job jobs/job1.py
  python sas_to_duckdb.py --ym 202601 --job jobs/job2.py --skip-load
  python sas_to_duckdb.py --ym 202601 --job jobs/job1.py jobs/job2.py jobs/job3.py
  python sas_to_duckdb.py --ym 202601 --job jobs/job1.py --stage load
  python sas_to_duckdb.py --ym 202601 --job jobs/job1.py -s logic validate
  python sas_to_duckdb.py --ym 202601 --job jobs/job1.py -t fio841 fio842
        """
    )
    parser.add_argument("--ym", type=str, default="202601",
                        help="처리할 년월 (기본: 202601)")
    parser.add_argument("--job", "-j", nargs="+", required=True,
                        help="실행할 JOB 파일 경로 (예: jobs/job1.py)")
    parser.add_argument("--skip-load", action="store_true",
                        help="LOAD 단계 생략")
    parser.add_argument("--stage", "-s", nargs="+",
                        choices=["load", "logic", "validate", "export"],
                        help="실행할 단계만 지정 (예: --stage load)")
    parser.add_argument("--tables", "-t", nargs="+",
                        help="로드할 테이블명만 지정 (예: --tables fio841 fio842)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="DEBUG 로그 콘솔 출력")

    args = parser.parse_args()

    log.info(f"Python {sys.version.split()[0]}")
    log.info(f"duckdb {duckdb.__version__}")
    log.info(f"pandas {pd.__version__}")

    if args.verbose:
        for h in log.handlers:
            if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler):
                h.setLevel(logging.DEBUG)

    yyyymm = args.ym
    db_path = ROOT / "db" / "pipeline.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    log.info(f"대상 월: {yyyymm}")
    log.info(f"DB    : {db_path}")

    # JOB 모듈 로드
    job_mods = []
    for jp in args.job:
        try:
            job_mods.append(load_job_module(jp))
        except (FileNotFoundError, AttributeError) as e:
            log.error(str(e))
            return

    # 순차 실행
    con = duckdb.connect(str(db_path))
    try:
        t_total = time.time()
        for mod in job_mods:
            run_job(con, mod, yyyymm, skip_load=args.skip_load,
                    stages=args.stage, only_tables=args.tables)
        log.info(f"전체 완료  총 소요: {time.time()-t_total:.1f}초")
    finally:
        con.close()


if __name__ == "__main__":
    main()
